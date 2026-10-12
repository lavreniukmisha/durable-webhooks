import concurrent.futures
import json
import tempfile
import unittest
from pathlib import Path

from relay.store import IdempotencyConflict, Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "queue.db")
        self.store = Store(self.path)

    def tearDown(self):
        self.directory.cleanup()

    def event(self, key="key"):
        return self.store.enqueue(key, "invoice.paid", {"amount": 100}, now=100)[0]

    def test_duplicate_enqueue_returns_same_id(self):
        first, created = self.store.enqueue("key", "test", {"a": 1, "b": 2}, now=100)
        second, reused = self.store.enqueue("key", "test", {"b": 2, "a": 1}, now=101)
        self.assertEqual(first, second)
        self.assertTrue(created)
        self.assertFalse(reused)
        self.assertEqual(self.store.stats()["pending"], 1)

    def test_different_payload_with_same_key_is_a_conflict(self):
        self.event()
        with self.assertRaises(IdempotencyConflict):
            self.store.enqueue("key", "invoice.paid", {"amount": 101})

    def test_concurrent_enqueue_creates_one_event(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            ids = list(pool.map(lambda _: self.event(), range(24)))
        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(self.store.stats()["pending"], 1)

    def test_concurrent_workers_cannot_claim_the_same_live_lease(self):
        self.event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            leases = list(pool.map(lambda _: self.store.claim(now=100), range(8)))
        self.assertEqual(sum(lease is not None for lease in leases), 1)

    def test_restart_keeps_body_and_queue_state(self):
        event_id = self.event()
        reopened = Store(self.path)
        lease = reopened.claim(now=100)
        self.assertEqual(lease.event_id, event_id)
        self.assertEqual(json.loads(lease.body)["data"], {"amount": 100})

    def test_unicode_payload_is_not_expanded_into_escape_sequences(self):
        data = {"label": "Ж" * 30000}
        self.store.enqueue("unicode", "test", data, now=100)
        lease = self.store.claim(now=100)
        self.assertLess(len(lease.body), 64 * 1024)
        self.assertEqual(json.loads(lease.body)["data"], data)

    def test_expired_lease_is_reclaimed_and_old_worker_is_fenced(self):
        event_id = self.event()
        old = self.store.claim(now=100, lease_seconds=10)
        self.assertIsNone(self.store.claim(now=109))
        new = self.store.claim(now=110, lease_seconds=10)
        self.assertNotEqual(old.token, new.token)
        self.assertFalse(self.store.finish(old, success=True, now=111))
        self.assertTrue(self.store.finish(new, success=True, now=111))
        self.assertEqual(self.store.get(event_id)["status"], "delivered")

    def test_expired_worker_cannot_ack_even_without_a_new_claim(self):
        self.event()
        lease = self.store.claim(now=100, lease_seconds=10)
        self.assertFalse(self.store.finish(lease, success=True, now=110))

    def test_retry_waits_until_its_due_time(self):
        self.event()
        lease = self.store.claim(now=100)
        self.assertTrue(self.store.finish(
            lease, success=False, retry=True, next_at=120, error="HTTP 503", now=101,
        ))
        self.assertIsNone(self.store.claim(now=119))
        self.assertEqual(self.store.claim(now=120).attempt, 2)

    def test_attempt_budget_ends_in_dead_letter_then_replay_keeps_event_id(self):
        event_id = self.event()
        first = self.store.claim(now=100, max_attempts=2)
        self.store.finish(first, success=False, retry=True, next_at=101, max_attempts=2, now=100)
        second = self.store.claim(now=101, max_attempts=2)
        self.store.finish(second, success=False, retry=True, next_at=102, max_attempts=2, now=101)
        self.assertEqual(self.store.get(event_id)["status"], "dead_letter")
        self.assertTrue(self.store.replay(event_id, now=103))
        replay = self.store.claim(now=103)
        self.assertEqual(replay.event_id, event_id)
        self.assertEqual(replay.attempt, 1)
        self.assertEqual(self.store.get(event_id)["replays"], 1)

    def test_crash_on_final_attempt_does_not_leave_a_stuck_job(self):
        event_id = self.event()
        self.store.claim(now=100, lease_seconds=10, max_attempts=1)
        self.assertIsNone(self.store.claim(now=110, max_attempts=1))
        self.assertEqual(self.store.get(event_id)["status"], "dead_letter")

    def test_lowering_attempt_budget_applies_to_pending_retries(self):
        event_id = self.event()
        lease = self.store.claim(now=100)
        self.store.finish(lease, success=False, retry=True, next_at=101, now=100)
        self.assertIsNone(self.store.claim(now=101, max_attempts=1))
        self.assertEqual(self.store.get(event_id)["status"], "dead_letter")

    def test_successful_event_cannot_be_replayed(self):
        event_id = self.event()
        lease = self.store.claim(now=100)
        self.store.finish(lease, success=True, now=101)
        self.assertFalse(self.store.replay(event_id))

    def test_non_json_numbers_and_invalid_inputs_are_rejected(self):
        for key, event_type, data in [("", "test", {}), ("key", "", {}),
                                      ("key", "test", []), ("key", "test", {"x": float("nan")})]:
            with self.assertRaises((ValueError, TypeError)):
                self.store.enqueue(key, event_type, data)
