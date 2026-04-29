"""Evening close summary at 15:45 IST.

Pulls today's intraday alerts (since 09:00 IST) from the alert store,
ranks movers by activity + score, computes RSI hit-rate by confidence,
and lists the highest-scoring calls of the session.

Same shape/architecture as `pre_market_engine.py` so future evening,
sectoral, and weekly reports can share these helpers.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from .db import AlertStore
from .telegram_sender import send as telegram_send
from .utils import get_logger

log = get_logger("evening")

IST = timezone(timedelta(hours=5, minutes=30))
MAX_TELEGRAM_LEN = 4000


class EveningEngine:
    def __init__(self, store: AlertStore) -> None:
        self.store = store

    def generate(self) -> None:
        now = datetime.now(IST)
        start = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now < start:
            start -= timedelta(days=1)
        items = self.store.fetch_since(start.astimezone(timezone.utc))
        log.info(
            "Evening close: %d items in window %s → %s IST",
            len(items),
            start.strftime("%d %b %H:%M"), now.strftime("%H:%M"),
        )

        rsi_items = [i for i in items if i.get("alert_type") == "RSI"]
        news_items = [i for i in items if i.get("alert_type") == "NEWS"]
        total = len(items)

        symbol_counts: Counter[str] = Counter()
        symbol_score: dict[str, int] = defaultdict(int)
        for it in items:
            sym = (it.get("symbol") or "").replace(".NS", "")
            if sym and sym != "MARKET":
                symbol_counts[sym] += 1
                symbol_score[sym] += int(it.get("score") or 0)
        top_movers = sorted(
            symbol_counts.items(),
            key=lambda x: (symbol_score[x[0]], x[1]),
            reverse=True,
        )[:8]

        confidence_counts: Counter[str] = Counter()
        for r in rsi_items:
            confidence_counts[r.get("confidence") or "MEDIUM"] += 1
        bullish = sum(1 for r in rsi_items if r.get("direction") == "up")
        bearish = sum(1 for r in rsi_items if r.get("direction") == "down")

        best = sorted(items, key=lambda x: int(x.get("score") or 0), reverse=True)[:6]

        date_str = now.strftime("%a, %d %b %Y")
        lines = [
            "🌇 EVENING CLOSE SUMMARY",
            f"Date: {date_str}  ·  Window: 09:00 → {now.strftime('%H:%M IST')}",
            f"Total alerts: {total}"
            f"  ({len(rsi_items)} RSI, {len(news_items)} news)",
            "",
        ]

        lines.append("📊 RSI ACTIVITY")
        if rsi_items:
            lines.append(
                f"Bullish (above 59): {bullish}"
                f"  ·  Bearish (below 41): {bearish}"
            )
            for conf, n in confidence_counts.most_common():
                lines.append(f"  • {conf}: {n}")
        else:
            lines.append("(no RSI alerts today)")
        lines.append("")

        lines.append("🔥 TOP MOVERS  (by alert activity + aggregate score)")
        if top_movers:
            for sym, cnt in top_movers:
                lines.append(
                    f"• {sym}  ({cnt} alerts, agg score {symbol_score[sym]})"
                )
        else:
            lines.append("(none)")
        lines.append("")

        lines.append("⭐ BEST CALLS")
        if best:
            for it in best:
                ts = it.get("fired_at")
                tlabel = (
                    ts.astimezone(IST).strftime("%H:%M") if ts else "—"
                )
                sym = (it.get("symbol") or "").replace(".NS", "")
                head = (it.get("headline") or "")[:120]
                lines.append(
                    f"• [{tlabel}] {sym} (score {it.get('score')}) — {head}"
                )
        else:
            lines.append("(none)")

        msg = "\n".join(lines)
        if len(msg) > MAX_TELEGRAM_LEN:
            msg = msg[:MAX_TELEGRAM_LEN - 30] + "\n\n…(truncated)"

        if telegram_send(msg):
            log.info("Evening summary sent (%d items)", total)
        else:
            log.error("Evening summary failed to send")
