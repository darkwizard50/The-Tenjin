"""Per-symbol cooldown tracking — restart-safe via Postgres."""

from __future__ import annotations

import time
from threading import Lock
from typing import Optional

from .config import NEWS_COOLDOWN_SECONDS, RSI_COOLDOWN_SECONDS
from .db import AlertStore
from .utils import get_logger

log = get_logger("cooldown")


class CooldownManager:
    def __init__(self, store: Optional[AlertStore] = None) -> None:
        self._rsi: dict[tuple[str, str], float] = {}
        self._news: dict[str, float] = {}
        self._lock = Lock()
        self.store = store
        if store is not None and store.enabled:
            self._warm_from_db()

    def _warm_from_db(self) -> None:
        try:
            rows = self.store.load_cooldowns()
            for kind, symbol, direction, ts in rows:
                if kind == "rsi":
                    self._rsi[(symbol, direction or "up")] = ts
                else:
                    self._news[symbol] = ts
            log.info("Warmed %d cooldown(s) from DB", len(rows))
        except Exception as exc:
            log.warning("Cooldown warm-up failed: %s", exc)

    def can_send_rsi(self, symbol: str, direction: str) -> bool:
        with self._lock:
            now = time.time()
            key = (symbol, direction)
            last = self._rsi.get(key, 0.0)
            elapsed = now - last
            if elapsed < RSI_COOLDOWN_SECONDS:
                remaining = int(RSI_COOLDOWN_SECONDS - elapsed)
                log.info(
                    "Cooldown blocked RSI alert: %s %s (%ds left)",
                    symbol, direction, remaining,
                )
                return False
            self._rsi[key] = now
        if self.store is not None:
            self.store.save_cooldown("rsi", symbol, direction)
        return True

    def can_send_news(self, symbol: str) -> bool:
        with self._lock:
            now = time.time()
            last = self._news.get(symbol, 0.0)
            elapsed = now - last
            if elapsed < NEWS_COOLDOWN_SECONDS:
                remaining = int(NEWS_COOLDOWN_SECONDS - elapsed)
                log.info(
                    "Cooldown blocked news alert: %s (%ds left)",
                    symbol, remaining,
                )
                return False
            self._news[symbol] = now
        if self.store is not None:
            self.store.save_cooldown("news", symbol, "")
        return True

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"rsi": len(self._rsi), "news": len(self._news)}
