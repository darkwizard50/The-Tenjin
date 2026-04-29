"""In-memory news summary buffer for the 09:00 / 16:00 IST flushes.

Pre-market (08:15) is handled by `pre_market_engine.PreMarketEngine`,
which pulls from the persistent alert store instead of this buffer.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .config import PRIORITY_CRITICAL, PRIORITY_HIGH, PRIORITY_MEDIUM
from .telegram_sender import send as telegram_send
from .utils import get_logger

log = get_logger("summary")

IST = timezone(timedelta(hours=5, minutes=30))

MAX_TELEGRAM_LEN = 4000


@dataclass
class NewsItem:
    timestamp: datetime
    priority: str
    symbol: str
    headline: str
    score: int


class SummaryEngine:
    def __init__(self) -> None:
        self._items: deque[NewsItem] = deque(maxlen=500)
        self._lock = threading.Lock()

    def add(self, priority: str, symbol: str, headline: str, score: int) -> None:
        # Only HIGH and MEDIUM news enter the summary; CRITICAL alerts are
        # already loud on their own, low-score items are just noise.
        if not (PRIORITY_MEDIUM <= score < PRIORITY_CRITICAL):
            return
        with self._lock:
            self._items.append(NewsItem(
                timestamp=datetime.now(IST),
                priority=priority,
                symbol=symbol,
                headline=headline,
                score=score,
            ))
            log.info("Queued for summary: %s (score=%d)", symbol, score)

    def flush(self, label: str) -> None:
        with self._lock:
            items = list(self._items)
            self._items.clear()

        if not items:
            log.info("%s summary: nothing to report (skipping)", label)
            return

        high = [i for i in items if i.score >= PRIORITY_HIGH]
        medium = [i for i in items if i.score < PRIORITY_HIGH]
        now = datetime.now(IST)

        lines = [
            f"📊 {label} SUMMARY",
            f"Window: {items[0].timestamp.strftime('%d %b %H:%M')}"
            f" → {now.strftime('%d %b %H:%M IST')}",
            f"Total: {len(items)} alerts"
            f" ({len(high)} HIGH, {len(medium)} MEDIUM)",
            "",
        ]

        if high:
            lines.append("🔥 HIGH")
            for i in high:
                lines.append(
                    f"• [{i.timestamp.strftime('%H:%M')}] {i.symbol}"
                    f" (score {i.score}) — {i.headline[:120]}"
                )
            lines.append("")

        if medium:
            lines.append("📢 MEDIUM")
            for i in medium:
                lines.append(
                    f"• [{i.timestamp.strftime('%H:%M')}] {i.symbol}"
                    f" (score {i.score}) — {i.headline[:120]}"
                )

        msg = "\n".join(lines)
        if len(msg) > MAX_TELEGRAM_LEN:
            msg = msg[:MAX_TELEGRAM_LEN - 30] + "\n\n…(truncated)"

        if telegram_send(msg):
            log.info("%s summary sent (%d items)", label, len(items))
        else:
            log.error("%s summary failed to send", label)
