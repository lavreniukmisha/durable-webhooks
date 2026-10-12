from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


class IdempotencyConflict(Exception):
    pass


@dataclass(frozen=True)
class Lease:
    event_id: str
    token: str
    attempt: int
    body: bytes


class Store:
    """Each operation owns its connection; API threads and workers can share a file."""

    def __init__(self, path: str):
        self.path = str(Path(path))
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""
                CREATE TABLE IF NOT EXISTS deliveries (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    request_hash TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    body BLOB NOT NULL,
                    status TEXT NOT NULL CHECK(status IN
                        ('pending', 'leased', 'delivered', 'dead_letter')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    replays INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    available_at REAL NOT NULL,
                    lease_token TEXT,
                    lease_until REAL,
                    last_http_status INTEGER,
                    last_error TEXT,
                    completed_at REAL
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS due_jobs ON deliveries(status, available_at)")

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def enqueue(self, key: str, event_type: str, data: dict, *, now=None):
        if not isinstance(key, str) or not key.strip() or len(key) > 200:
            raise ValueError("Idempotency-Key must contain 1–200 characters")
        if not isinstance(event_type, str) or not event_type.strip() or len(event_type) > 100:
            raise ValueError("type must contain 1–100 characters")
        if not isinstance(data, dict):
            raise ValueError("data must be a JSON object")
        canonical = json.dumps(
            {"type": event_type, "data": data}, sort_keys=True,
            separators=(",", ":"), allow_nan=False, ensure_ascii=False,
        )
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        now = time.time() if now is None else now
        with self.transaction() as db:
            previous = db.execute(
                "SELECT id, request_hash FROM deliveries WHERE idempotency_key=?", (key,),
            ).fetchone()
            if previous:
                if previous["request_hash"] != digest:
                    raise IdempotencyConflict("key already belongs to a different event")
                return previous["id"], False
            event_id = str(uuid.uuid4())
            body = json.dumps(
                {"id": event_id, "type": event_type, "data": data},
                sort_keys=True, separators=(",", ":"), allow_nan=False, ensure_ascii=False,
            ).encode()
            db.execute("""
                INSERT INTO deliveries
                (id, idempotency_key, request_hash, event_type, body,
                 status, created_at, available_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
            """, (event_id, key, digest, event_type, body, now, now))
            return event_id, True

    def claim(self, *, now=None, lease_seconds=30, max_attempts=5):
        if lease_seconds <= 0 or max_attempts < 1:
            raise ValueError("lease_seconds and max_attempts must be positive")
        now = time.time() if now is None else now
        with self.transaction() as db:
            db.execute("""
                UPDATE deliveries SET status='dead_letter',
                    last_error='attempt limit reached'
                WHERE status='pending' AND attempts>=?
            """, (max_attempts,))
            # A crash on the last attempt must not leave a job leased forever.
            db.execute("""
                UPDATE deliveries SET status='dead_letter', lease_token=NULL,
                    lease_until=NULL, last_error='lease expired on final attempt'
                WHERE status='leased' AND lease_until<=? AND attempts>=?
            """, (now, max_attempts))
            row = db.execute("""
                SELECT * FROM deliveries
                WHERE (status='pending' AND available_at<=?)
                   OR (status='leased' AND lease_until<=?)
                ORDER BY available_at, created_at, id LIMIT 1
            """, (now, now)).fetchone()
            if row is None:
                return None
            token = str(uuid.uuid4())
            attempt = row["attempts"] + 1
            db.execute("""
                UPDATE deliveries SET status='leased', attempts=?,
                    lease_token=?, lease_until=? WHERE id=?
            """, (attempt, token, now + lease_seconds, row["id"]))
            return Lease(row["id"], token, attempt, bytes(row["body"]))

    def finish(self, lease: Lease, *, success: bool, retry: bool = False,
               http_status=None, error=None, next_at=None, max_attempts=5, now=None):
        now = time.time() if now is None else now
        status = "delivered" if success else (
            "pending" if retry and lease.attempt < max_attempts else "dead_letter"
        )
        if status == "pending" and next_at is None:
            raise ValueError("retry requires next_at")
        with self.transaction() as db:
            result = db.execute("""
                UPDATE deliveries SET status=?, available_at=?, lease_token=NULL,
                    lease_until=NULL, last_http_status=?, last_error=?, completed_at=?
                WHERE id=? AND status='leased' AND lease_token=? AND lease_until>?
            """, (status, next_at if status == "pending" else now, http_status, error,
                  now if success else None, lease.event_id, lease.token, now))
            # The token AND expiry fence a slow or previously crashed worker.
            return result.rowcount == 1

    def get(self, event_id: str):
        with self.connection() as db:
            row = db.execute("""
                SELECT id, event_type, status, attempts, replays, created_at,
                    available_at, lease_until, last_http_status, last_error, completed_at
                FROM deliveries WHERE id=?
            """, (event_id,)).fetchone()
            return dict(row) if row else None

    def replay(self, event_id: str, *, now=None):
        now = time.time() if now is None else now
        with self.transaction() as db:
            result = db.execute("""
                UPDATE deliveries SET status='pending', attempts=0, replays=replays+1,
                    available_at=?, lease_token=NULL, lease_until=NULL,
                    last_http_status=NULL, last_error=NULL, completed_at=NULL
                WHERE id=? AND status='dead_letter'
            """, (now, event_id))
            return result.rowcount == 1

    def stats(self):
        result = dict.fromkeys(("pending", "leased", "delivered", "dead_letter"), 0)
        with self.connection() as db:
            for row in db.execute("SELECT status, count(*) AS n FROM deliveries GROUP BY status"):
                result[row["status"]] = row["n"]
        return result
