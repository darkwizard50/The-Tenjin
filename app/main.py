"""Entry point: news scanner, RSI scanner, scheduler, heartbeat, and shutdown."""

from __future__ import annotations
from flask import Flask

import signal
import sys
import threading
import time
from typing import Callable

from .config import (
    NEWS_SCAN_INTERVAL,
    PRE_MARKET_HOUR,
    PRE_MARKET_MINUTE,
    RSI_SCAN_INTERVAL,
)
from .cooldowns import CooldownManager
from .db import AlertStore
from .diagnostics import run_startup_checks
from .evening_engine import EveningEngine
from .fno_universe import FNO_STOCKS
from .health_server import start_health_server
from .heartbeat import Heartbeat
from .news_engine import NewsEngine
from .pre_market_engine import PreMarketEngine
from .rsi_engine import RSIEngine
from .scheduler import ReportScheduler, Slot
from .summary_engine import SummaryEngine
from .utils import get_logger

log = get_logger("main")
app = Flask(__name__)


@app.route("/")
def home():
    return "Bot Running"


def _loop(
    name: str, fn: Callable[[], None], interval: int, stop_event: threading.Event
) -> None:
    """Resilient scan loop — never exits on a handler exception."""
    log.info("Loop %s started (interval=%ds)", name, interval)
    while not stop_event.is_set():
        try:
            log.info("Running %s scan", name)
            fn()
        except Exception as exc:
            log.exception("%s scan crashed (continuing): %s", name, exc)
        if stop_event.wait(interval):
            break
    log.info("Loop %s stopped", name)


def main() -> None:
    start_health_server()

    store = AlertStore()
    diag = run_startup_checks(store)

    cooldowns = CooldownManager(store=store)
    summary = SummaryEngine()
    pre_market = PreMarketEngine(store)
    evening = EveningEngine(store)

    news = NewsEngine(cooldowns, summary=summary, store=store)
    rsi = RSIEngine(FNO_STOCKS, cooldowns, store=store)

    log.info(
        "Starting F&O scanner (universe=%d stocks, db=%s, telegram=%s)",
        len(FNO_STOCKS),
        "on" if store.enabled else "off",
        diag.get("telegram", "?"),
    )

    scheduler = ReportScheduler(
        [
            Slot(PRE_MARKET_HOUR, PRE_MARKET_MINUTE, "PRE-MARKET", pre_market.generate),
            Slot(9, 0, "MORNING", lambda: summary.flush("MORNING")),
            Slot(15, 45, "EVENING-CLOSE", evening.generate),
            Slot(16, 0, "END-OF-DAY", lambda: summary.flush("END-OF-DAY")),
            # Future slots — uncomment when handlers are added:
            # Slot(17, 0, "SECTOR-RECAP", sector.generate),
            # Slot(18, 0, "WEEKLY", lambda: weekly.generate() if friday() else None),
        ]
    )

    heartbeat = Heartbeat(
        store=store,
        getters={
            "universe_size": lambda: len(FNO_STOCKS),
            "rsi": rsi.stats,
            "news": news.stats,
            "cooldowns": cooldowns.stats,
            "active_cooldowns_db": lambda: store.count_active_cooldowns(60 * 60),
        },
        cleaners=[
            rsi.cleanup,
            rsi.upstox.cleanup,
            lambda: store.prune_dedup(older_than_hours=72),
        ],
        interval_seconds=300,
    )

    stop_event = threading.Event()

    def _shutdown(signum, _frame):
        log.info("Received signal %d — shutting down gracefully", signum)
        if store.enabled:
            store.log_health(
                "shutdown",
                "stopping",
                f"signal={signum}",
                {"signal": int(signum)},
            )
        stop_event.set()
        heartbeat.stop()
        # Daemon threads will exit when main does; state is incrementally
        # persisted to Postgres so no flush is required.
        sys.exit(0)

    try:
        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)
    except ValueError:
        log.warning("Signal handlers unavailable in background thread")

    workers = [
        threading.Thread(
            target=_loop,
            args=("news", news.scan, NEWS_SCAN_INTERVAL, stop_event),
            daemon=True,
            name="news-loop",
        ),
        threading.Thread(
            target=_loop,
            args=("rsi", rsi.scan, RSI_SCAN_INTERVAL, stop_event),
            daemon=True,
            name="rsi-loop",
        ),
        threading.Thread(
            target=scheduler.run,
            daemon=True,
            name="scheduler",
        ),
        threading.Thread(
            target=heartbeat.run,
            daemon=True,
            name="heartbeat",
        ),
    ]
    for w in workers:
        w.start()

    if store.enabled:
        store.log_health(
            "startup",
            "running",
            "all loops armed",
            {
                "workers": [w.name for w in workers],
            },
        )

    while not stop_event.is_set():
        time.sleep(3600)


if __name__ == "__main__":
    bot_thread = threading.Thread(target=main, daemon=True)

    bot_thread.start()

    app.run(host="0.0.0.0", port=3000)
