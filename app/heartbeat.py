"""Periodic heartbeat — emits a JSON-shaped status line and logs to db."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from .db import AlertStore
from .utils import get_logger

log = get_logger("heartbeat")


class Heartbeat:
    def __init__(
        self,
        store: AlertStore,
        getters: Optional[dict[str, Callable[[], object]]] = None,
        cleaners: Optional[list[Callable[[], object]]] = None,
        interval_seconds: int = 300,
    ) -> None:
        self.store = store
        self.getters = getters or {}
        self.cleaners = cleaners or []
        self.interval = interval_seconds
        self.start_monotonic = time.monotonic()
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def _collect_metrics(self) -> dict[str, object]:
        uptime = int(time.monotonic() - self.start_monotonic)
        metrics: dict[str, object] = {
            "uptime_seconds": uptime,
            "uptime_human": str(timedelta(seconds=uptime)),
            "db_enabled": bool(self.store and self.store.enabled),
        }
        if self.store and self.store.enabled:
            since = datetime.now(timezone.utc) - timedelta(hours=1)
            metrics["alerts_last_hour"] = self.store.count_alerts_since(since)
        for name, getter in self.getters.items():
            try:
                metrics[name] = getter()
            except Exception as exc:
                metrics[name] = f"error: {exc}"
        return metrics

    def _run_cleaners(self) -> None:
        for cleaner in self.cleaners:
            try:
                cleaner()
            except Exception as exc:
                log.warning("Cleaner failed: %s", exc)

    def run(self) -> None:
        log.info("Heartbeat started (interval=%ds)", self.interval)
        while not self._stop.is_set():
            if self._stop.wait(self.interval):
                break
            try:
                metrics = self._collect_metrics()
                log.info("HEARTBEAT alive %s", metrics)
                if self.store and self.store.enabled:
                    self.store.log_health(
                        "heartbeat", "ok", "alive", metrics,
                    )
                self._run_cleaners()
            except Exception as exc:
                log.exception("Heartbeat tick failed: %s", exc)
