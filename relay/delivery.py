from __future__ import annotations

import hashlib
import hmac
import logging
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from .store import Store

log = logging.getLogger(__name__)


def signature(secret: str, timestamp: int, body: bytes) -> str:
    digest = hmac.new(secret.encode(), str(timestamp).encode() + b"." + body,
                      hashlib.sha256).hexdigest()
    return "v1=" + digest


def verify_signature(secret: str, timestamp: str, body: bytes, supplied: str,
                     *, now=None, tolerance=300) -> bool:
    try:
        stamp = int(timestamp)
        current = time.time() if now is None else now
        if abs(current - stamp) > tolerance:
            return False
        return hmac.compare_digest(signature(secret, stamp, body), supplied)
    except (TypeError, ValueError, OverflowError):
        return False


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


@dataclass(frozen=True)
class DeliveryResult:
    success: bool
    retry: bool
    status: int | None = None
    error: str | None = None


def send(lease, url: str, secret: str, *, timeout=5, now=None):
    target = urllib.parse.urlsplit(url)
    if target.scheme not in ("http", "https") or not target.hostname or target.username is not None:
        raise ValueError("target must be an HTTP(S) URL without embedded credentials")
    stamp = int(time.time() if now is None else now)
    request = urllib.request.Request(url, data=lease.body, method="POST", headers={
        "Content-Type": "application/json",
        "User-Agent": "durable-webhooks/0.1",
        "X-Webhook-Id": lease.event_id,
        "X-Webhook-Timestamp": str(stamp),
        "X-Webhook-Signature": signature(secret, stamp, lease.body),
    })
    # Do not forward a signed payload to a redirect destination.
    opener = urllib.request.build_opener(NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        return DeliveryResult(False, True, error=type(exc).__name__)
    if 200 <= status < 300:
        return DeliveryResult(True, False, status=status)
    retry = status in (408, 425, 429) or 500 <= status < 600
    return DeliveryResult(False, retry, status, f"HTTP {status}")


def run_once(store: Store, url: str, secret: str, *, timeout=5, lease_seconds=30,
             max_attempts=5, base_delay=1, max_delay=300,
             clock=time.time, jitter=random.uniform) -> bool:
    if timeout <= 0 or lease_seconds <= timeout or base_delay <= 0 or max_delay <= 0:
        raise ValueError("lease must exceed timeout; delays must be positive")
    if not secret:
        raise ValueError("WEBHOOK_SECRET is required")
    # Check the target before consuming an attempt.
    target = urllib.parse.urlsplit(url)
    if target.scheme not in ("http", "https") or not target.hostname or target.username is not None:
        raise ValueError("invalid webhook target")
    lease = store.claim(now=clock(), lease_seconds=lease_seconds, max_attempts=max_attempts)
    if lease is None:
        return False
    result = send(lease, url, secret, timeout=timeout, now=clock())
    cap = min(max_delay, base_delay * 2 ** min(lease.attempt - 1, 30))
    delay = jitter(cap / 2, cap)
    updated = store.finish(
        lease, success=result.success, retry=result.retry, http_status=result.status,
        error=result.error, next_at=clock() + delay, max_attempts=max_attempts, now=clock(),
    )
    log.info("event=%s attempt=%s http=%s persisted=%s",
             lease.event_id, lease.attempt, result.status, updated)
    return True
