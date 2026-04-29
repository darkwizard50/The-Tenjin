"""Telegram message sender with retry-with-backoff."""

from __future__ import annotations

import requests

from .config import BOT_TOKEN, CHAT_ID
from .retry import with_backoff
from .utils import get_logger

log = get_logger("telegram")

_RETRYABLE = (
    requests.ConnectionError,
    requests.Timeout,
    requests.HTTPError,
)


@with_backoff(attempts=4, base_delay=1.5, exceptions=_RETRYABLE, on_failure_return=None)
def _post(message: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    resp = requests.post(url, json=payload, timeout=15)
    # Treat transient HTTP errors as retryable
    if resp.status_code in (408, 429, 500, 502, 503, 504):
        raise requests.HTTPError(
            f"telegram retryable status={resp.status_code}", response=resp
        )
    return resp


def send(message: str) -> bool:
    try:
        resp = _post(message)
        if resp is None:
            log.error("Telegram send failed after retries")
            return False
        if resp.status_code == 200:
            log.info("Telegram send success")
            return True
        log.error(
            "Telegram send failed: status=%d body=%s",
            resp.status_code, resp.text[:200],
        )
        return False
    except Exception as exc:
        log.exception("Telegram send exception: %s", exc)
        return False


def check_bot() -> bool:
    """Used by startup diagnostics — confirms BOT_TOKEN is valid."""
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False
