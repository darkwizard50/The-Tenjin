"""Startup diagnostics — verifies critical subsystems before scanning."""

from __future__ import annotations

import os

import requests

from .config import NEWS_SOURCES
from .db import AlertStore
from .telegram_sender import check_bot
from .utils import get_logger

log = get_logger("diagnostics")


def _check_news_primary() -> tuple[str, str]:
    if not NEWS_SOURCES:
        return ("skipped", "no sources configured")
    name, url = NEWS_SOURCES[0]
    try:
        r = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if r.status_code == 200:
            return ("ok", f"{name} 200")
        return ("warn", f"{name} status={r.status_code}")
    except Exception as exc:
        return ("error", f"{name} {exc}")


def _check_secrets() -> tuple[str, str]:
    missing = []
    if not os.environ.get("DATABASE_URL"):
        missing.append("DATABASE_URL")
    upstox = os.environ.get("UPSTOX_ACCESS_TOKEN")
    note = "Upstox: enabled" if upstox else "Upstox: optional, not set"
    if missing:
        return ("warn", f"missing: {','.join(missing)} ({note})")
    return ("ok", note)


def run_startup_checks(store: AlertStore) -> dict[str, str]:
    results: dict[str, str] = {}

    results["database"] = "ok" if store.enabled else "disabled"
    sec_status, sec_msg = _check_secrets()
    results["secrets"] = f"{sec_status}: {sec_msg}"
    results["telegram"] = "ok" if check_bot() else "error"
    news_status, news_msg = _check_news_primary()
    results["news_primary"] = f"{news_status}: {news_msg}"

    log.info("=" * 60)
    log.info("STARTUP DIAGNOSTICS")
    for k, v in results.items():
        log.info("  %-15s : %s", k, v)
    log.info("=" * 60)

    if store.enabled:
        store.log_health("startup", "complete", "diagnostics", results)
    return results
