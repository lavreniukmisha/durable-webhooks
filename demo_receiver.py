"""Local receiver: checks signatures and remembers event IDs across restarts."""
import hashlib
import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from relay.delivery import verify_signature


def create_receiver(host, port, secret, db_path):
    if not secret:
        raise ValueError("WEBHOOK_SECRET is required")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE IF NOT EXISTS received (id TEXT PRIMARY KEY, digest TEXT NOT NULL)")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/webhook":
                self.send_error(404)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 128 * 1024:
                    self.send_error(413)
                    return
                body = self.rfile.read(length)
                valid = verify_signature(
                    secret, self.headers.get("X-Webhook-Timestamp", ""),
                    body, self.headers.get("X-Webhook-Signature", ""),
                )
                if not valid:
                    self.send_error(401)
                    return
                payload = json.loads(body)
                event_id = payload.get("id")
                if not isinstance(event_id, str) or self.headers.get("X-Webhook-Id") != event_id:
                    self.send_error(400)
                    return
                digest = hashlib.sha256(body).hexdigest()
                with sqlite3.connect(db_path, timeout=10) as db:
                    db.execute("BEGIN IMMEDIATE")
                    previous = db.execute("SELECT digest FROM received WHERE id=?", (event_id,)).fetchone()
                    if previous and previous[0] != digest:
                        self.send_error(409)
                        return
                    db.execute("INSERT OR IGNORE INTO received VALUES (?, ?)", (event_id, digest))
                duplicate = previous is not None
                response = json.dumps({"accepted": True, "duplicate": duplicate}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
                print(f"received event={event_id} duplicate={duplicate}", flush=True)
            except (ValueError, TypeError, AttributeError, UnicodeError):
                self.send_error(400)

    return ThreadingHTTPServer((host, port), Handler)


if __name__ == "__main__":
    receiver = create_receiver(
        os.environ.get("RECEIVER_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_PORT", "9000")),
        os.environ.get("WEBHOOK_SECRET", ""),
        os.environ.get("RECEIVER_DB", "data/receiver.db"),
    )
    try:
        receiver.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        receiver.server_close()
