"""Unified scheduler for periodic Telegram reports.

Each scheduled slot is a (hour, minute, label, callable) tuple in IST.
Designed to be extended with evening, sectoral, or weekly reports later
without rewriting the scheduler itself.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from .utils import get_logger

log = get_logger("scheduler")

IST = timezone(timedelta(hours=5, minutes=30))


@dataclass
class Slot:
    hour: int
    minute: int
    label: str
    handler: Callable[[], None]


class ReportScheduler:
    def __init__(self, slots: list[Slot]) -> None:
        self.slots = slots
        self._last_fired: dict[tuple[int, int], str] = {}

    def run(self) -> None:
        log.info(
            "Scheduler armed: %s",
            ", ".join(
                f"{s.hour:02d}:{s.minute:02d} IST ({s.label})" for s in self.slots
            ),
        )
        while True:
            try:
                now = datetime.now(IST)
                today = now.strftime("%Y-%m-%d")
                for slot in self.slots:
                    key = (slot.hour, slot.minute)
                    if (
                        now.hour == slot.hour
                        and now.minute == slot.minute
                        and self._last_fired.get(key) != today
                    ):
                        log.info(
                            "Triggering %s at %s IST",
                            slot.label, now.strftime("%H:%M"),
                        )
                        thread = threading.Thread(
                            target=self._safe_invoke,
                            args=(slot,),
                            name=f"slot-{slot.label}",
                            daemon=True,
                        )
                        thread.start()
                        self._last_fired[key] = today
            except Exception as exc:
                log.exception("Scheduler error: %s", exc)
            time.sleep(30)

    def _safe_invoke(self, slot: Slot) -> None:
        try:
            slot.handler()
        except Exception as exc:
            log.exception("Handler %s crashed: %s", slot.label, exc)
