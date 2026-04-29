"""Postgres-backed persistence layer.

Tables:
- alert_history  — every fired alert
- cooldowns      — last-fired timestamp per (kind, symbol, direction)
- rsi_state      — last seen RSI per symbol (for crossover detection)
- dedup_cache    — recently-seen news headlines
- health_log     — heartbeat + diagnostic records

Degrades gracefully: if `DATABASE_URL` is missing or psycopg2 is not
available, all writes/queries become no-ops and callers continue running.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Optional

from .utils import get_logger

log = get_logger("db")

try:
    import psycopg2
    import psycopg2.extras
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore[assignment]
    log.exception("psycopg2 not available — alert history will be in-memory only")

_DDL = """
CREATE TABLE IF NOT EXISTS alert_history (
    id          SERIAL PRIMARY KEY,
    fired_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_type  TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    headline    TEXT,
    score       INTEGER NOT NULL,
    priority    TEXT NOT NULL,
    source      TEXT,
    direction   TEXT,
    rsi_value   REAL,
    confidence  TEXT,
    metadata    JSONB
);
CREATE INDEX IF NOT EXISTS idx_alert_history_fired_at
    ON alert_history (fired_at);
CREATE INDEX IF NOT EXISTS idx_alert_history_score
    ON alert_history (score);
CREATE INDEX IF NOT EXISTS idx_alert_history_type
    ON alert_history (alert_type);

CREATE TABLE IF NOT EXISTS cooldowns (
    kind          TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    direction     TEXT NOT NULL DEFAULT '',
    last_fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, symbol, direction)
);

CREATE TABLE IF NOT EXISTS rsi_state (
    symbol       TEXT PRIMARY KEY,
    last_rsi     REAL NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dedup_cache (
    headline_hash TEXT PRIMARY KEY,
    headline      TEXT NOT NULL,
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dedup_seen ON dedup_cache (last_seen_at);

CREATE TABLE IF NOT EXISTS health_log (
    id        SERIAL PRIMARY KEY,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    component TEXT NOT NULL,
    status    TEXT NOT NULL,
    message   TEXT,
    metrics   JSONB
);
CREATE INDEX IF NOT EXISTS idx_health_log_logged_at
    ON health_log (logged_at);
"""


def headline_hash(headline: str) -> str:
    return hashlib.md5(headline.lower().strip().encode("utf-8")).hexdigest()


class AlertStore:
    def __init__(self) -> None:
        self._url = os.environ.get("DATABASE_URL", "").strip()
        self._lock = threading.Lock()
        self._enabled = bool(self._url and psycopg2 is not None)
        self._conn = None
        if not self._enabled:
            log.warning("Persistence disabled (DATABASE_URL or psycopg2 missing)")
            return
        try:
            self._connect()
            self._init_schema()
            log.info("Persistence connected (Postgres) — schema verified")
        except Exception as exc:
            log.exception("Failed to initialise persistence: %s", exc)
            self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _connect(self) -> None:
        self._conn = psycopg2.connect(self._url, connect_timeout=10)
        self._conn.autocommit = True

    @contextmanager
    def _cursor(self):
        with self._lock:
            try:
                if self._conn is None or self._conn.closed:
                    self._connect()
                cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                try:
                    yield cur
                finally:
                    cur.close()
            except Exception:
                try:
                    if self._conn is not None:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
                raise

    def _init_schema(self) -> None:
        with self._cursor() as cur:
            cur.execute(_DDL)

    # ----------------------------------------------------------- alerts
    def insert(
        self,
        *,
        alert_type: str,
        symbol: str,
        headline: Optional[str],
        score: int,
        priority: str,
        source: Optional[str] = None,
        direction: Optional[str] = None,
        rsi_value: Optional[float] = None,
        confidence: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        if not self._enabled:
            return
        try:
            with self._cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO alert_history (
                        alert_type, symbol, headline, score, priority,
                        source, direction, rsi_value, confidence, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        alert_type, symbol, headline, score, priority,
                        source, direction, rsi_value, confidence,
                        json.dumps(metadata) if metadata else None,
                    ),
                )
        except Exception as exc:
            log.warning("Alert insert failed: %s", exc)

    def fetch_since(self, since: datetime) -> list[dict[str, Any]]:
        if not self._enabled:
            return []
        try:
            with self._cursor() as cur:
                cur.execute(
                    """
                    SELECT fired_at, alert_type, symbol, headline, score,
                           priority, source, direction, rsi_value, confidence
                    FROM alert_history
                    WHERE fired_at >= %s
                    ORDER BY fired_at ASC
                    """,
                    (since,),
                )
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            log.warning("Alert query failed: %s", exc)
            return []

    def count_alerts_since(self, since: datetime) -> int:
        if not self._enabled:
            return 0
        try:
            with self._cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM alert_history WHERE fired_at >= %s",
                    (since,),
                )
                row = cur.fetchone()
                return int(row["n"]) if row else 0
        except Exception as exc:
            log.warning("Alert count failed: %s", exc)
            return 0

    # --------------------------------------------------------- cooldowns
    def load_cooldowns(self) -> list[tuple[str, str, str, float]]:
        if not self._enabled:
            return []
        try:
            with self._cursor() as cur:
                cur.execute(
                    "SELECT kind, symbol, direction,"
                    " EXTRACT(EPOCH FROM last_fired_at) AS ts FROM cooldowns"
                )
                return [
                    (r["kind"], r["symbol"], r["direction"], float(r["ts"]))
                    for r in cur.fetchall()
                ]
        except Exception as exc:
            log.warning("Cooldown load failed: %s", exc)
            return []

    def save_cooldown(self, kind: str, symbol: str, direction: str = "") -> None:
        if not self._enabled:
            return
        try:
            with self._cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cooldowns (kind, symbol, direction, last_fired_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (kind, symbol, direction)
                    DO UPDATE SET last_fired_at = EXCLUDED.last_fired_at
                    """,
                    (kind, symbol, direction),
                )
        except Exception as exc:
            log.warning("Cooldown save failed: %s", exc)

    def count_active_cooldowns(self, ttl_seconds: int) -> int:
        if not self._enabled:
            return 0
        try:
            with self._cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM cooldowns"
                    " WHERE last_fired_at >= NOW() - (%s || ' seconds')::interval",
                    (str(ttl_seconds),),
                )
                row = cur.fetchone()
                return int(row["n"]) if row else 0
        except Exception:
            return 0

    # --------------------------------------------------------- RSI state
    def load_rsi_state(self) -> dict[str, float]:
        if not self._enabled:
            return {}
        try:
            with self._cursor() as cur:
                cur.execute("SELECT symbol, last_rsi FROM rsi_state")
                return {r["symbol"]: float(r["last_rsi"]) for r in cur.fetchall()}
        except Exception as exc:
            log.warning("RSI state load failed: %s", exc)
            return {}

    def save_rsi_state(self, symbol: str, value: float) -> None:
        if not self._enabled:
            return
        try:
            with self._cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rsi_state (symbol, last_rsi, last_updated)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (symbol)
                    DO UPDATE SET last_rsi = EXCLUDED.last_rsi,
                                  last_updated = EXCLUDED.last_updated
                    """,
                    (symbol, value),
                )
        except Exception as exc:
            log.warning("RSI state save failed for %s: %s", symbol, exc)

    # --------------------------------------------------------- dedup
    def load_dedup(self, since_hours: int = 24) -> list[str]:
        if not self._enabled:
            return []
        try:
            with self._cursor() as cur:
                cur.execute(
                    "SELECT headline FROM dedup_cache"
                    " WHERE last_seen_at >= NOW() - (%s || ' hours')::interval",
                    (str(since_hours),),
                )
                return [r["headline"] for r in cur.fetchall()]
        except Exception as exc:
            log.warning("Dedup load failed: %s", exc)
            return []

    def save_dedup(self, headline: str) -> None:
        if not self._enabled:
            return
        try:
            h = headline_hash(headline)
            with self._cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dedup_cache (headline_hash, headline, last_seen_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (headline_hash)
                    DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
                    """,
                    (h, headline[:500]),
                )
        except Exception as exc:
            log.warning("Dedup save failed: %s", exc)

    def prune_dedup(self, older_than_hours: int = 48) -> int:
        if not self._enabled:
            return 0
        try:
            with self._cursor() as cur:
                cur.execute(
                    "DELETE FROM dedup_cache"
                    " WHERE last_seen_at < NOW() - (%s || ' hours')::interval",
                    (str(older_than_hours),),
                )
                return cur.rowcount or 0
        except Exception as exc:
            log.warning("Dedup prune failed: %s", exc)
            return 0

    # --------------------------------------------------------- health
    def log_health(
        self,
        component: str,
        status: str,
        message: Optional[str] = None,
        metrics: Optional[dict[str, Any]] = None,
    ) -> None:
        if not self._enabled:
            return
        try:
            with self._cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO health_log (component, status, message, metrics)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        component, status, message,
                        json.dumps(metrics) if metrics else None,
                    ),
                )
        except Exception as exc:
            log.warning("Health log write failed: %s", exc)
