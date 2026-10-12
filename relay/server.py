from __future__ import annotations

import hmac
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from .store import IdempotencyConflict, Store

MAX_BODY = 64 * 1024


def create_server(store: Store, host: str, port: int, api_token: str):
    if not api_token:
        raise ValueError("API_TOKEN is required")

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, value, content_type="application/json"):
            body = (json.dumps(value, allow_nan=False).encode()
                    if content_type == "application/json" else value.encode())
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def authorized(self):
            actual = self.headers.get("Authorization", "")
            if not hmac.compare_digest(actual.encode(), ("Bearer " + api_token).encode()):
                self.reply(401, {"error": "unauthorized"})
                return False
            return True

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == "/healthz":
                self.reply(200, {"status": "ok"})
                return
            if not self.authorized():
                return
            if path == "/stats":
                self.reply(200, store.stats())
            elif path == "/metrics":
                text = "# HELP webhook_deliveries Jobs by current state.\n"
                text += "# TYPE webhook_deliveries gauge\n"
                for state, count in store.stats().items():
                    text += f'webhook_deliveries{{status="{state}"}} {count}\n'
                self.reply(200, text, "text/plain; version=0.0.4; charset=utf-8")
            elif path.startswith("/deliveries/") and len(path.split("/")) == 3:
                result = store.get(path.split("/")[2])
                self.reply(200, result) if result else self.reply(404, {"error": "not found"})
            else:
                self.reply(404, {"error": "not found"})

        def do_POST(self):
            if not self.authorized():
                return
            path = urlsplit(self.path).path
            if path.startswith("/deliveries/") and path.endswith("/replay"):
                parts = path.split("/")
                if len(parts) != 4:
                    self.reply(404, {"error": "not found"})
                elif store.replay(parts[2]):
                    self.reply(202, store.get(parts[2]))
                elif store.get(parts[2]):
                    self.reply(409, {"error": "only dead-letter deliveries can be replayed"})
                else:
                    self.reply(404, {"error": "not found"})
                return
            if path != "/events":
                self.reply(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0:
                    self.reply(400, {"error": "a JSON body is required"})
                    return
                if length > MAX_BODY:
                    self.reply(413, {"error": "body exceeds 64 KiB"})
                    return
                if self.headers.get_content_type() != "application/json":
                    self.reply(415, {"error": "Content-Type must be application/json"})
                    return
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ValueError("body must be an object")
                event_id, created = store.enqueue(
                    self.headers.get("Idempotency-Key", ""),
                    payload.get("type"), payload.get("data"),
                )
                self.reply(202, {"id": event_id, "created": created})
            except IdempotencyConflict as exc:
                self.reply(409, {"error": str(exc)})
            except (ValueError, TypeError, UnicodeError) as exc:
                self.reply(400, {"error": str(exc)})

        def log_message(self, format, *args):
            logging.getLogger("relay.http").info(format, *args)

    return ThreadingHTTPServer((host, port), Handler)
