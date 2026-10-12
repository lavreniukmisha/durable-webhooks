import contextlib
import http.server
import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from demo_receiver import create_receiver
from relay.delivery import run_once, signature, verify_signature
from relay.store import Store


@contextlib.contextmanager
def running(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class SignatureTests(unittest.TestCase):
    def test_exact_body_and_timestamp_are_signed(self):
        body = b'{"test":1}'
        signed = signature("test-secret", 1000, body)
        self.assertTrue(verify_signature("test-secret", "1000", body, signed, now=1000))
        self.assertFalse(verify_signature("test-secret", "1000", b'{"test":2}', signed, now=1000))
        self.assertFalse(verify_signature("other-secret", "1000", body, signed, now=1000))

    def test_old_future_and_malformed_timestamps_are_rejected(self):
        signed = signature("secret", 1000, b"body")
        for stamp, now in [("1000", 1301), ("1000", 699), ("bad", 1000)]:
            self.assertFalse(verify_signature("secret", stamp, b"body", signed, now=now))


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.directory.name) / "outbox.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_real_receiver_accepts_signed_delivery_and_deduplicates_it(self):
        receiver = create_receiver("127.0.0.1", 0, "test-secret",
                                   str(Path(self.directory.name) / "received.db"))
        event_id, _ = self.store.enqueue("key", "test", {"value": 1})
        # Real time is intentional here: the receiver checks timestamp freshness.
        with running(receiver) as url:
            self.assertTrue(run_once(self.store, url + "/webhook", "test-secret"))
            self.assertEqual(self.store.get(event_id)["status"], "delivered")
            with self.store.connection() as db:
                body = bytes(db.execute("SELECT body FROM deliveries WHERE id=?", (event_id,)).fetchone()[0])
            import time
            stamp = int(time.time())
            request = urllib.request.Request(url + "/webhook", data=body, headers={
                "Content-Type": "application/json", "X-Webhook-Id": event_id,
                "X-Webhook-Timestamp": str(stamp),
                "X-Webhook-Signature": signature("test-secret", stamp, body),
            })
            with urllib.request.urlopen(request, timeout=5) as response:
                self.assertTrue(json.load(response)["duplicate"])

    def serve_status(self, status, seen):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                seen.append(self.rfile.read(int(self.headers["Content-Length"])))
                self.send_response(status)
                if status == 302:
                    self.send_header("Location", "/redirected")
                self.end_headers()

            def log_message(self, *args):
                pass
        return http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)

    def test_transient_statuses_use_bounded_backoff_and_jitter(self):
        for status in (408, 429, 500, 503):
            with self.subTest(status=status):
                event_id, _ = self.store.enqueue(str(status), "test", {}, now=100)
                seen = []
                with running(self.serve_status(status, seen)) as url:
                    run_once(self.store, url, "secret", clock=lambda: 100, jitter=lambda low, high: high)
                row = self.store.get(event_id)
                self.assertEqual(row["status"], "pending")
                self.assertEqual(row["last_http_status"], status)
                self.assertEqual(row["available_at"], 101)
                # Keep earlier rows from being picked up by the next subtest.
                lease = self.store.claim(now=101)
                self.store.finish(lease, success=True, now=101)

    def test_permanent_failures_and_redirects_go_to_dead_letter(self):
        for status in (302, 400, 401, 404):
            with self.subTest(status=status):
                event_id, _ = self.store.enqueue(str(status), "test", {}, now=100)
                seen = []
                with running(self.serve_status(status, seen)) as url:
                    run_once(self.store, url, "secret", clock=lambda: 100)
                self.assertEqual(self.store.get(event_id)["status"], "dead_letter")
                self.assertEqual(len(seen), 1)

    def test_second_retry_uses_exponential_delay(self):
        event_id, _ = self.store.enqueue("key", "test", {}, now=100)
        with running(self.serve_status(503, [])) as url:
            run_once(self.store, url, "secret", clock=lambda: 100, jitter=lambda low, high: high)
            run_once(self.store, url, "secret", clock=lambda: 101, jitter=lambda low, high: high)
        self.assertEqual(self.store.get(event_id)["available_at"], 103)

    def test_invalid_configuration_does_not_consume_an_attempt(self):
        event_id, _ = self.store.enqueue("key", "test", {}, now=100)
        for url, secret, lease in [("file:///tmp/file", "secret", 30),
                                   ("http://user:password@localhost", "secret", 30),
                                   ("http://localhost", "", 30),
                                   ("http://localhost", "secret", 5)]:
            with self.assertRaises(ValueError):
                run_once(self.store, url, secret, lease_seconds=lease, timeout=5)
        self.assertEqual(self.store.get(event_id)["attempts"], 0)
