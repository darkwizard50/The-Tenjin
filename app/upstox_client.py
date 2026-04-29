"""Optional Upstox API client used as a secondary confirmation source.

If `UPSTOX_ACCESS_TOKEN` is not set, the client is disabled and all callers
fall back to Yahoo-only operation.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import threading
import time
from typing import Optional

import pandas as pd
import requests

from .config import (
    UPSTOX_CACHE_TTL_SECONDS,
    UPSTOX_CANDLES_URL,
    UPSTOX_INSTRUMENTS_URL,
)
from .utils import get_logger

log = get_logger("upstox")


class UpstoxClient:
    def __init__(self) -> None:
        self._token = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
        self._enabled = bool(self._token)
        self._symbol_map: dict[str, str] = {}
        self._loaded = False
        self._load_lock = threading.Lock()
        self._cache: dict[str, tuple[float, pd.DataFrame]] = {}

        if not self._enabled:
            log.warning(
                "UPSTOX_ACCESS_TOKEN not set — Upstox confirmation disabled."
                " Bot will run Yahoo-only with Medium confidence."
            )
        else:
            log.info("Upstox client enabled (token configured)")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _load_instruments(self) -> None:
        with self._load_lock:
            if self._loaded:
                return
            try:
                log.info("Downloading Upstox NSE instruments map…")
                r = requests.get(UPSTOX_INSTRUMENTS_URL, timeout=30)
                r.raise_for_status()
                with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as fp:
                    data = json.load(fp)
                count = 0
                for inst in data:
                    if (
                        inst.get("segment") == "NSE_EQ"
                        and inst.get("instrument_type") == "EQ"
                    ):
                        sym = inst.get("trading_symbol")
                        key = inst.get("instrument_key")
                        if sym and key:
                            self._symbol_map[sym] = key
                            count += 1
                log.info("Loaded %d NSE EQ instruments from Upstox", count)
                self._loaded = True
            except Exception as exc:
                log.exception("Failed to load Upstox instruments: %s", exc)
                self._loaded = False

    def get_5m_candles(self, ns_ticker: str) -> Optional[pd.DataFrame]:
        """Return 5-minute candles for the given Yahoo-style ticker, or None."""
        if not self._enabled:
            return None

        symbol = ns_ticker.replace(".NS", "")
        now = time.time()
        cached = self._cache.get(symbol)
        if cached and (now - cached[0] < UPSTOX_CACHE_TTL_SECONDS):
            return cached[1]

        if not self._loaded:
            self._load_instruments()

        key = self._symbol_map.get(symbol)
        if not key:
            log.info("Upstox: no instrument_key for %s", symbol)
            return None

        url = UPSTOX_CANDLES_URL.format(key=key)
        try:
            r = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Accept": "application/json",
                },
                timeout=10,
            )
            if r.status_code != 200:
                log.warning(
                    "Upstox %s status=%d body=%s",
                    symbol, r.status_code, r.text[:200],
                )
                return None
            payload = r.json().get("data", {}).get("candles", [])
            if not payload:
                return None
            df = pd.DataFrame(
                payload,
                columns=["ts", "Open", "High", "Low", "Close", "Volume", "OI"],
            )
            df["ts"] = pd.to_datetime(df["ts"])
            df = df.sort_values("ts").set_index("ts")
            self._cache[symbol] = (now, df)
            return df
        except Exception as exc:
            log.exception("Upstox fetch failed for %s: %s", symbol, exc)
            return None

    def cleanup(self) -> None:
        """Drop expired cache entries to prevent unbounded memory growth."""
        if not self._cache:
            return
        now = time.time()
        self._cache = {
            k: v for k, v in self._cache.items()
            if now - v[0] < UPSTOX_CACHE_TTL_SECONDS * 4
        }
