import argparse
import logging
import os
import signal
import threading

from .delivery import run_once
from .server import create_server
from .store import Store


def main():
    parser = argparse.ArgumentParser(description="Durable webhook outbox")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--db", default="data/outbox.db")
    worker = commands.add_parser("worker")
    worker.add_argument("--db", default="data/outbox.db")
    worker.add_argument("--url", default=os.environ.get("WEBHOOK_TARGET_URL"))
    worker.add_argument("--max-attempts", type=int, default=5)
    worker.add_argument("--timeout", type=float, default=5)
    worker.add_argument("--lease", type=float, default=30)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    if args.command == "serve":
        token = os.environ.get("API_TOKEN")
        if not token:
            parser.error("set API_TOKEN")
        store = Store(args.db)
        server = create_server(store, args.host, args.port, token)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
    else:
        secret = os.environ.get("WEBHOOK_SECRET")
        if not args.url or not secret:
            parser.error("set WEBHOOK_SECRET and pass --url")
        if args.timeout <= 0 or args.lease <= args.timeout or args.max_attempts < 1:
            parser.error("lease must exceed timeout; max-attempts must be positive")
        store = Store(args.db)
        stop = threading.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        while not stop.is_set():
            if not run_once(store, args.url, secret, timeout=args.timeout,
                            lease_seconds=args.lease, max_attempts=args.max_attempts):
                stop.wait(0.2)


if __name__ == "__main__":
    main()
