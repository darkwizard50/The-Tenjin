"""Multi-source RSI(14) crossover scanner.

- Primary candle source: Yahoo Finance (yfinance), 5-minute candles
- Optional confirmation: Upstox API, 5-minute candles
- Optional MTF: Yahoo Finance 15-minute RSI direction
- Optional trend filter: 20 EMA alignment

A crossover only fires an alert when:
  1. Yahoo RSI(14) crosses 59 (up) or 41 (down) on the 5m candle
  2. Current 5m volume >= 1.8x recent 20-bar average
  3. Per-stock per-direction cooldown (30 min) has elapsed
"""

from __future__ import annotations

import time
from datetime import datetime
from threading import Lock
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

from .config import (
    BATCH_SIZE,
    CANDLE_INTERVAL,
    CANDLE_PERIOD,
    EMA_PERIOD,
    MTF_INTERVAL,
    MTF_PERIOD,
    RSI_LOWER_THRESHOLD,
    RSI_PERIOD,
    RSI_UPPER_THRESHOLD,
    VOLUME_LOOKBACK,
    VOLUME_SPIKE_MULTIPLIER,
)
from .cooldowns import CooldownManager
from .db import AlertStore
from .retry import with_backoff
from .scoring import confidence_for, priority_for, score_rsi_alert
from .telegram_sender import send as telegram_send
from .upstox_client import UpstoxClient
from .utils import get_logger

log = get_logger("rsi")

MTF_CACHE_TTL = 600  # seconds — 15m candles only update every 15min


class RSIState:
    """Tracks last seen RSI per symbol to detect crossovers.

    Restart-safe: on init, warms `_last` from the persisted `rsi_state`
    table so a crossover that already happened before a restart is not
    re-fired (and one that's about to fire is still detected correctly).
    """

    def __init__(self, store: Optional[AlertStore] = None) -> None:
        self._last: dict[str, float] = {}
        self._lock = Lock()
        self.store = store
        if store is not None and store.enabled:
            try:
                self._last = store.load_rsi_state()
                log.info("Warmed %d RSI state entries from DB", len(self._last))
            except Exception as exc:
                log.warning("RSI state warm-up failed: %s", exc)

    def update_and_check(self, symbol: str, rsi_value: float) -> Optional[str]:
        with self._lock:
            prev = self._last.get(symbol)
            self._last[symbol] = rsi_value
        if self.store is not None:
            self.store.save_rsi_state(symbol, rsi_value)
        if prev is None:
            return None
        if prev <= RSI_UPPER_THRESHOLD < rsi_value:
            return "up"
        if prev >= RSI_LOWER_THRESHOLD > rsi_value:
            return "down"
        return None


def calc_rsi(closes: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """Wilder's RSI using exponential smoothing."""
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


class RSIEngine:
    def __init__(
        self,
        universe: list[str],
        cooldowns: CooldownManager,
        store: Optional[AlertStore] = None,
    ) -> None:
        self.universe = universe
        self.cooldowns = cooldowns
        self.store = store
        self.state = RSIState(store=store)
        self.upstox = UpstoxClient()
        self._candle_cache: dict[str, pd.Timestamp] = {}
        self._mtf_cache: dict[str, tuple[float, str]] = {}
        self._batches_ok = 0
        self._batches_fail = 0

    # ------------------------------------------------------------------ scan
    def scan(self) -> None:
        log.info("Starting RSI scan over %d stocks", len(self.universe))
        batches = [
            self.universe[i:i + BATCH_SIZE]
            for i in range(0, len(self.universe), BATCH_SIZE)
        ]
        for batch in batches:
            try:
                self._scan_batch(batch)
            except Exception as exc:
                log.exception("Batch crashed (continuing): %s", exc)

    @staticmethod
    @with_backoff(attempts=3, base_delay=2.0, on_failure_return=None)
    def _yf_download(batch, interval, period):
        return yf.download(
            tickers=batch,
            interval=interval,
            period=period,
            group_by="ticker",
            progress=False,
            auto_adjust=False,
            threads=True,
        )

    def _scan_batch(self, batch: list[str]) -> None:
        try:
            data = self._yf_download(batch, CANDLE_INTERVAL, CANDLE_PERIOD)
        except Exception as exc:
            self._batches_fail += 1
            log.exception("Batch download failed after retries: %s", exc)
            return
        if data is None or data.empty:
            self._batches_fail += 1
            log.info("Batch returned no data (markets may be closed)")
            return
        self._batches_ok += 1

        for symbol in batch:
            try:
                df = data[symbol] if len(batch) > 1 else data
                if df is None or df.empty:
                    continue
                df = df.dropna()
                if len(df) < RSI_PERIOD + 2:
                    continue
                self._evaluate(symbol, df)
            except KeyError:
                continue
            except Exception as exc:
                log.exception("Eval failed for %s: %s", symbol, exc)

    # ------------------------------------------------------------ evaluation
    def _evaluate(self, symbol: str, df: pd.DataFrame) -> None:
        last_ts = df.index[-1]
        if self._candle_cache.get(symbol) == last_ts:
            return
        self._candle_cache[symbol] = last_ts

        closes = df["Close"]
        volumes = df["Volume"]

        rsi_series = calc_rsi(closes)
        yahoo_rsi = float(rsi_series.iloc[-1])
      telegram_send(f"TEST RSI WORKING {symbol} → {yahoo_rsi}")
        if pd.isna(yahoo_rsi):
            return

        log.debug("%s yahoo_rsi=%.2f", symbol, yahoo_rsi)

        crossover = self.state.update_and_check(symbol, yahoo_rsi)
        if not crossover:
            return

        log.info(
            "RSI crossover %s on %s (yahoo_rsi=%.2f)",
            crossover, symbol, yahoo_rsi,
        )

        # --- Volume filter (gating) ---
        recent_vol = volumes.iloc[-(VOLUME_LOOKBACK + 1):-1]
        avg_vol = float(recent_vol.mean()) if len(recent_vol) else 0.0
        cur_vol = float(volumes.iloc[-1])
        if avg_vol <= 0 or pd.isna(avg_vol):
            log.info("Skipping %s: insufficient volume baseline", symbol)
            return
        spike_ratio = cur_vol / avg_vol
        if spike_ratio < VOLUME_SPIKE_MULTIPLIER:
            log.info(
                "Volume filter rejected %s (ratio=%.2fx < %.2fx)",
                symbol, spike_ratio, VOLUME_SPIKE_MULTIPLIER,
            )
            return
        log.info("Volume confirmed %s (ratio=%.2fx)", symbol, spike_ratio)

        # --- Cooldown ---
        if not self.cooldowns.can_send_rsi(symbol, crossover):
            return

        # --- Upstox confirmation ---
        upstox_rsi: Optional[float] = None
        upstox_agrees = False
        if self.upstox.enabled:
            up_df = self.upstox.get_5m_candles(symbol)
            if up_df is not None and len(up_df) >= RSI_PERIOD + 2:
                upstox_rsi = float(calc_rsi(up_df["Close"]).iloc[-1])
                if not pd.isna(upstox_rsi):
                    if crossover == "up" and upstox_rsi > RSI_UPPER_THRESHOLD:
                        upstox_agrees = True
                    elif crossover == "down" and upstox_rsi < RSI_LOWER_THRESHOLD:
                        upstox_agrees = True
                    diff = abs(yahoo_rsi - upstox_rsi)
                    if upstox_agrees:
                        log.info(
                            "Upstox CONFIRMS %s: yahoo=%.2f upstox=%.2f Δ=%.2f",
                            symbol, yahoo_rsi, upstox_rsi, diff,
                        )
                    else:
                        log.info(
                            "Upstox DISAGREES %s: yahoo=%.2f upstox=%.2f Δ=%.2f",
                            symbol, yahoo_rsi, upstox_rsi, diff,
                        )
            else:
                log.info(
                    "Upstox unavailable for %s — fallback to Yahoo-only",
                    symbol,
                )
        else:
            log.info("Upstox disabled — Yahoo-only confirmation for %s", symbol)

        # --- MTF (15m) ---
        mtf_dir = self._get_mtf_direction(symbol)
        mtf_aligned = (
            (crossover == "up" and mtf_dir == "up")
            or (crossover == "down" and mtf_dir == "down")
        )
        if mtf_dir is not None:
            log.info(
                "MTF(15m) direction for %s: %s (aligned=%s)",
                symbol, mtf_dir, mtf_aligned,
            )

        # --- EMA(20) alignment ---
        ema_aligned = self._ema_aligned(closes, crossover)
        log.info("EMA(20) aligned for %s: %s", symbol, ema_aligned)

        # --- Confidence + score ---
        confidence = confidence_for(
            dual_source=upstox_agrees, volume_spike=True,
        )
        score = score_rsi_alert(
            volume_spike=True,
            fno=True,
            dual_source=upstox_agrees,
            mtf_aligned=mtf_aligned,
            ema_aligned=ema_aligned,
        )
        log.info(
            "Confidence=%s Score=%d (sources=%s, mtf=%s, ema=%s, vol=%.2fx)",
            confidence, score,
            "Yahoo+Upstox" if upstox_agrees else "Yahoo",
            mtf_aligned, ema_aligned, spike_ratio,
        )

        # --- Build + send alert ---
        signal = "Bullish Momentum" if crossover == "up" else "Reversal Watch"
        sources = "Yahoo + Upstox" if upstox_agrees else "Yahoo"
        if not self.upstox.enabled:
            sources += " (Upstox unavailable)"
        elif not upstox_agrees and upstox_rsi is None:
            sources += " (Upstox unavailable)"
        elif not upstox_agrees:
            sources += f" (Upstox disagreed @ {upstox_rsi:.2f})"

        mtf_line = (
            f"MTF (15m): aligned ↑" if mtf_aligned and crossover == "up"
            else f"MTF (15m): aligned ↓" if mtf_aligned and crossover == "down"
            else f"MTF (15m): {mtf_dir}" if mtf_dir is not None
            else "MTF (15m): n/a"
        )
        ema_line = (
            "20 EMA: price above (bullish aligned)"
            if ema_aligned and crossover == "up"
            else "20 EMA: price below (bearish aligned)"
            if ema_aligned and crossover == "down"
            else "20 EMA: price not aligned with signal"
        )

        price = float(closes.iloc[-1])
        clean_symbol = symbol.replace(".NS", "")
        msg = (
            "🚨🚨 RSI MOMENTUM ALERT 🚨🚨\n\n"
            f"Stock: {clean_symbol}\n"
            f"Price: ₹{price:,.2f}\n\n"
            f"RSI(14): {yahoo_rsi:.1f} (5m)\n"
            f"Signal: {signal}\n\n"
            f"Volume Spike: {spike_ratio:.2f}x\n"
            f"Confidence: {confidence}\n"
            f"Sources: {sources}\n"
            f"{mtf_line}\n"
            f"{ema_line}\n\n"
            f"Timeframe: 5m\n"
            f"Priority Score: {score} ({priority_for(score).strip()})\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        telegram_send(msg)
        if self.store is not None:
            self.store.insert(
                alert_type="RSI",
                symbol=symbol,
                headline=f"{signal} crossover (RSI {yahoo_rsi:.1f})",
                score=score,
                priority=priority_for(score).strip(),
                source=sources,
                direction=crossover,
                rsi_value=yahoo_rsi,
                confidence=confidence,
                metadata={
                    "price": price,
                    "spike_ratio": spike_ratio,
                    "mtf_aligned": mtf_aligned,
                    "ema_aligned": ema_aligned,
                    "upstox_rsi": upstox_rsi,
                },
            )

    # ----------------------------------------------------------- helpers
    def _ema_aligned(self, closes: pd.Series, crossover: str) -> bool:
        ema = closes.ewm(span=EMA_PERIOD, adjust=False).mean()
        price = float(closes.iloc[-1])
        ema_val = float(ema.iloc[-1])
        if pd.isna(ema_val):
            return False
        if crossover == "up":
            return price > ema_val
        return price < ema_val

    # ------------------------------------------------------------ stats
    def stats(self) -> dict[str, object]:
        return {
            "universe_size": len(self.universe),
            "rsi_state_size": len(self.state._last),
            "candle_cache_size": len(self._candle_cache),
            "mtf_cache_size": len(self._mtf_cache),
            "batches_ok": self._batches_ok,
            "batches_fail": self._batches_fail,
        }

    def cleanup(self) -> None:
        """Trim transient caches to prevent unbounded growth."""
        if len(self._candle_cache) > len(self.universe) * 2:
            keep = set(self.universe)
            self._candle_cache = {
                k: v for k, v in self._candle_cache.items() if k in keep
            }
        now = time.time()
        self._mtf_cache = {
            k: v for k, v in self._mtf_cache.items()
            if now - v[0] < MTF_CACHE_TTL * 2
        }

    def _get_mtf_direction(self, symbol: str) -> Optional[str]:
        """Return 'up' or 'down' based on the slope of 15m RSI(14)."""
        cached = self._mtf_cache.get(symbol)
        now = time.time()
        if cached and now - cached[0] < MTF_CACHE_TTL:
            return cached[1]
        try:
            df = yf.download(
                tickers=symbol,
                interval=MTF_INTERVAL,
                period=MTF_PERIOD,
                progress=False,
                auto_adjust=False,
                threads=False,
            )
            if df is None or df.empty or len(df) < RSI_PERIOD + 2:
                return None
            df = df.dropna()
            rsi_series = calc_rsi(df["Close"]).dropna()
            if len(rsi_series) < 2:
                return None
            direction = "up" if rsi_series.iloc[-1] > rsi_series.iloc[-2] else "down"
            self._mtf_cache[symbol] = (now, direction)
            return direction
        except Exception as exc:
            log.warning("MTF fetch failed for %s: %s", symbol, exc)
            return None
