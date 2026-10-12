import concurrent.futures
import json
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from relay.server import create_server
from relay.store import Store
from tests.test_delivery import running


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.directory.name) / "outbox.db"))
        self.context = running(create_server(self.store, "127.0.0.1", 0, "test-api-token"))
        self.url = self.context.__enter__()

    def tearDown(self):
        self.context.__exit__(None, None, None)
        self.directory.cleanup()

    def request(self, path, data=None, *, key="key", token="test-api-token", method=None):
        headers = {"Content-Type": "application/json", "Idempotency-Key": key}
        if token:
            headers["Authorization"] = "Bearer " + token
        body = json.dumps(data).encode() if data is not None else None
        request = urllib.request.Request(self.url + path, data=body, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(request, timeout=5)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            content = response.read().decode()
            return response.status, (json.loads(content) if response.headers.get_content_type() == "application/json" else content)

    def test_health_is_public_but_queue_routes_require_auth(self):
        self.assertEqual(self.request("/healthz", token=None)[0], 200)
        for route in ("/stats", "/metrics", "/deliveries/unknown"):
            self.assertEqual(self.request(route, token=None)[0], 401)

    def test_enqueue_duplicate_and_conflict_over_http(self):
        payload = {"type": "invoice.paid", "data": {"amount": 100}}
        status, first = self.request("/events", payload)
        self.assertEqual(status, 202)
        self.assertTrue(first["created"])
        _, second = self.request("/events", payload)
        self.assertEqual(first["id"], second["id"])
        self.assertFalse(second["created"])
        self.assertEqual(self.request("/events", {"type": "invoice.paid", "data": {"amount": 101}})[0], 409)
        status, row = self.request("/deliveries/" + first["id"])
        self.assertEqual(status, 200)
        self.assertEqual(row["status"], "pending")

    def test_real_concurrent_http_requests_share_one_event(self):
        payload = {"type": "test", "data": {}}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda _: self.request("/events", payload), range(16)))
        self.assertTrue(all(status == 202 for status, _ in responses))
        self.assertEqual(len({body["id"] for _, body in responses}), 1)

    def test_bad_payload_and_oversized_body_are_rejected(self):
        self.assertEqual(self.request("/events", {"type": "test", "data": []})[0], 400)
        self.assertEqual(self.request("/events", {"type": "test", "data": {}}, key="")[0], 400)
        self.assertEqual(self.request("/events", {"type": "test", "data": {"value": "x" * 65536}})[0], 413)

    def test_stats_metrics_and_missing_event(self):
        self.request("/events", {"type": "test", "data": {}})
        self.assertEqual(self.request("/stats")[1]["pending"], 1)
        self.assertIn('webhook_deliveries{status="pending"} 1', self.request("/metrics")[1])
        self.assertEqual(self.request("/deliveries/missing")[0], 404)

    def test_replay_route_only_accepts_dead_letters(self):
        _, event = self.request("/events", {"type": "test", "data": {}})
        path = "/deliveries/" + event["id"] + "/replay"
        self.assertEqual(self.request(path, method="POST")[0], 409)
        lease = self.store.claim()
        self.store.finish(lease, success=False, error="HTTP 400")
        self.assertEqual(self.request(path, method="POST")[0], 202)
        self.assertEqual(self.request("/deliveries/" + event["id"])[1]["replays"], 1)
