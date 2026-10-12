# durable-webhooks

[![tests](https://github.com/lavreniukmisha/durable-webhooks/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/lavreniukmisha/durable-webhooks/actions/workflows/tests.yml)
[![Python 3.11, 3.12, 3.13](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.13-3776AB?logo=python&logoColor=white)](https://github.com/lavreniukmisha/durable-webhooks/blob/main/.github/workflows/tests.yml)

A small webhook outbox in Python. Accept an event, keep it on disk, and let a
worker deliver it. No runtime dependencies.

The interesting part is what happens between sending a request and recording
the response: a crash, a timeout, or another worker picking up an expired lease.

## Try it

Python 3.11+ is enough for the code and tests. Docker Compose runs the API,
worker, and a receiver that checks signatures and remembers event IDs.

```sh
cp .env.example .env
docker compose up --build -d

curl -s http://localhost:8000/events \
  -H 'Authorization: Bearer local-demo-api-token-change-me' \
  -H 'Idempotency-Key: invoice-42' \
  -H 'Content-Type: application/json' \
  -d '{"type":"invoice.paid","data":{"invoice_id":"inv_42","amount_cents":12500}}'

curl -s http://localhost:8000/stats \
  -H 'Authorization: Bearer local-demo-api-token-change-me'

docker compose logs receiver
```

The example credentials are for this local demo. Repeating the event request
returns the same ID with `created: false`. Reusing its key with a different
payload returns `409`.

Without Docker, run these in three terminals from the repository root:

```sh
API_TOKEN=local-demo-api-token-change-me python -m relay serve
WEBHOOK_SECRET=local-demo-signing-secret-change-me python demo_receiver.py
WEBHOOK_SECRET=local-demo-signing-secret-change-me python -m relay worker --url http://127.0.0.1:9000/webhook
```

The API and worker use the same `data/outbox.db` file by default.

## Delivery behaviour

- The enqueue transaction stores both the event and its unique idempotency key.
- Workers claim jobs inside `BEGIN IMMEDIATE` transactions. Each lease has an
  expiry and a random token. An expired or superseded worker cannot acknowledge
  the job.
- Timeouts, `408`, `425`, `429`, and `5xx` get bounded exponential backoff
  with jitter. Other unsuccessful responses go straight to dead letter.
- A crash on the final attempt also ends in dead letter once the lease expires.
- Redirects are not followed. Signed event bodies stay at the configured target.
- Replaying a dead letter resets its attempt budget and keeps its event ID.

A receiver can accept the event just before a worker crashes. That event may
arrive again. The demo receiver deduplicates `X-Webhook-Id` in its own SQLite
database; the sender does not claim exactly-once delivery.

The signature is `v1=<hex HMAC-SHA256>` over `timestamp + "." + raw_body`.
The receiver checks a five-minute timestamp window and compares signatures in
constant time. Headers are `X-Webhook-Id`, `X-Webhook-Timestamp`, and
`X-Webhook-Signature`.

## API

Except for `GET /healthz`, routes require `Authorization: Bearer <API_TOKEN>`.

| Route | Purpose |
| --- | --- |
| `POST /events` | Enqueue `{"type": "...", "data": {...}}` with `Idempotency-Key` |
| `GET /deliveries/<id>` | State, attempts, last error and HTTP status |
| `POST /deliveries/<id>/replay` | Retry a dead letter |
| `GET /stats` | Counts for pending, leased, delivered and dead-letter jobs |
| `GET /metrics` | The same queue counts as Prometheus gauges |

Requests are limited to 64 KiB. The destination is worker configuration, not
an event field. Workers sharing a database must use the same destination,
signing secret, and retry settings.

## Tests

```sh
python -m unittest discover -s tests -v
```

Tests cover concurrent producers and workers, expired leases, fencing stale
acknowledgements, restart persistence, retry budgets, signature validation,
redirect handling, and real HTTP delivery to the demo receiver.

## Notes

SQLite keeps the queue easy to inspect and run on one machine. There is one
writer at a time, and the HTTP API uses Python's standard-library server. This
is a reliability lab, not a high-throughput hosting service.

For a larger deployment, the next changes would be a PostgreSQL-backed claim
query, a production HTTP server, per-destination scheduling, and an attempt
history table. Event retention and signing-key rotation are also left open.

The core is in `relay/store.py`; HTTP delivery is in `relay/delivery.py`.
