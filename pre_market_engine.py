"""Pre-market summary generator.

Pulls the previous overnight window of stored alerts (news + RSI),
clusters similar headlines, builds a structured pre-market briefing,
and sends it to Telegram at 08:15 IST.

Architecture is generic: `SummaryReport` + `_render` can be reused for
evening, sectoral, or weekly reports later.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from rapidfuzz import fuzz

from .config import (
    BEARISH_KEYWORDS,
    BULLISH_KEYWORDS,
    PRE_MARKET_DEDUPE_RATIO,
    PRE_MARKET_LOOKBACK_HOURS,
    PRE_MARKET_MAX_HIGH,
    PRE_MARKET_MAX_MEDIUM,
    PRE_MARKET_MAX_WATCHLIST,
    PRIORITY_CRITICAL,
    PRIORITY_HIGH,
    PRIORITY_MEDIUM,
)
from .db import AlertStore
from .telegram_sender import send as telegram_send
from .utils import get_logger

log = get_logger("premarket")

IST = timezone(timedelta(hours=5, minutes=30))
MAX_TELEGRAM_LEN = 4000


@dataclass
class Cluster:
    symbol: str
    representative: dict[str, Any]
    members: list[dict[str, Any]] = field(default_factory=list)

    @property
    def best_score(self) -> int:
        return max((m.get("score", 0) for m in self.members), default=0)

    @property
    def headline(self) -> str:
        return self.representative.get("headline") or "(no headline)"

    @property
    def sources(self) -> list[str]:
        seen, ordered = set(), []
        for m in self.members:
            s = m.get("source")
            if s and s not in seen:
                seen.add(s)
                ordered.append(s)
        return ordered


def _overnight_window(now: Optional[datetime] = None) -> tuple[datetime, datetime]:
    now = now or datetime.now(IST)
    cutoff = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if now < cutoff:
        cutoff -= timedelta(days=1)
    # Safety: never go further back than the configured lookback.
    earliest = now - timedelta(hours=PRE_MARKET_LOOKBACK_HOURS)
    if cutoff < earliest:
        cutoff = earliest
    return cutoff, now


def _bias_label(items: Iterable[dict[str, Any]]) -> tuple[str, int, int]:
    bull = 0
    bear = 0
    for it in items:
        text = (it.get("headline") or "").lower()
        if it.get("alert_type") == "RSI":
            if it.get("direction") == "up":
                bull += 2
            elif it.get("direction") == "down":
                bear += 2
            continue
        if any(k in text for k in BULLISH_KEYWORDS):
            bull += 1
        if any(k in text for k in BEARISH_KEYWORDS):
            bear += 1
    if bull == 0 and bear == 0:
        return "Mixed", bull, bear
    if bull >= bear * 1.5:
        return "Bullish", bull, bear
    if bear >= bull * 1.5:
        return "Bearish", bull, bear
    return "Mixed", bull, bear


def _cluster(items: list[dict[str, Any]]) -> list[Cluster]:
    """Cluster items by symbol + similar headline (rapidfuzz)."""
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for it in items:
        by_symbol[it.get("symbol") or "MARKET"].append(it)

    clusters: list[Cluster] = []
    for symbol, group in by_symbol.items():
        # Sort highest-scoring first so the representative is the strongest.
        group.sort(key=lambda x: x.get("score", 0), reverse=True)
        used = [False] * len(group)
        for i, item in enumerate(group):
            if used[i]:
                continue
            cluster = Cluster(symbol=symbol, representative=item, members=[item])
            used[i] = True
            base = (item.get("headline") or "").lower()
            for j in range(i + 1, len(group)):
                if used[j]:
                    continue
                cand = (group[j].get("headline") or "").lower()
                if not base or not cand:
                    continue
                if fuzz.ratio(base, cand) >= PRE_MARKET_DEDUPE_RATIO:
                    cluster.members.append(group[j])
                    used[j] = True
            clusters.append(cluster)
    # Order clusters by best score descending.
    clusters.sort(key=lambda c: c.best_score, reverse=True)
    return clusters


def _format_line(cluster: Cluster) -> str:
    extra = len(cluster.members) - 1
    suffix = f" (+{extra} similar)" if extra > 0 else ""
    src = ",".join(cluster.sources[:3]) if cluster.sources else "—"
    headline = cluster.headline
    if len(headline) > 140:
        headline = headline[:137] + "…"
    return (
        f"• {cluster.symbol}: {headline}{suffix}"
        f"\n   ↳ score {cluster.best_score} · {src}"
    )


def _watchlist(items: list[dict[str, Any]], clusters: list[Cluster]) -> list[str]:
    """Build watchlist from RSI activity + news cluster intensity."""
    score_by_symbol: Counter[str] = Counter()
    for it in items:
        symbol = it.get("symbol") or "MARKET"
        if symbol == "MARKET":
            continue
        score_by_symbol[symbol] += int(it.get("score", 0))
    # Also boost symbols with multiple clusters (multi-event activity).
    for c in clusters:
        if c.symbol == "MARKET":
            continue
        score_by_symbol[c.symbol] += len(c.members) * 5

    top = score_by_symbol.most_common(PRE_MARKET_MAX_WATCHLIST)
    lines = []
    for symbol, agg in top:
        sym_items = [i for i in items if (i.get("symbol") or "") == symbol]
        rsi_count = sum(1 for i in sym_items if i.get("alert_type") == "RSI")
        news_count = sum(1 for i in sym_items if i.get("alert_type") == "NEWS")
        tags = []
        if rsi_count:
            tags.append(f"{rsi_count} RSI")
        if news_count:
            tags.append(f"{news_count} news")
        tag_str = " · ".join(tags) if tags else "activity"
        lines.append(f"• {symbol.replace('.NS', '')}  ({tag_str}, agg {agg})")
    return lines


class PreMarketEngine:
    def __init__(self, store: AlertStore) -> None:
        self.store = store

    def generate(self) -> None:
        start, end = _overnight_window()
        log.info(
            "Generating pre-market summary for window %s → %s IST",
            start.strftime("%d %b %H:%M"), end.strftime("%d %b %H:%M"),
        )
        items = self.store.fetch_since(start.astimezone(timezone.utc))
        items = [i for i in items if (i.get("score") or 0) >= PRIORITY_MEDIUM]
        log.info("Pre-market: %d alerts in window", len(items))

        clusters = _cluster(items)

        critical_high = [
            c for c in clusters
            if c.best_score >= PRIORITY_HIGH and c.symbol != "MARKET"
            or c.best_score >= PRIORITY_CRITICAL
        ][:PRE_MARKET_MAX_HIGH]
        medium = [
            c for c in clusters
            if PRIORITY_MEDIUM <= c.best_score < PRIORITY_HIGH
        ][:PRE_MARKET_MAX_MEDIUM]

        bias, bull, bear = _bias_label(items)
        watchlist = _watchlist(items, clusters)

        date_str = end.strftime("%a, %d %b %Y")
        lines = [
            "🌅 PRE-MARKET SUMMARY",
            f"Date: {date_str}  ·  Window: {start.strftime('%d %b %H:%M')}"
            f" → {end.strftime('%H:%M IST')}",
            f"Total alerts in window: {len(items)}"
            f" ({len(critical_high)} HIGH/CRITICAL clusters,"
            f" {len(medium)} MEDIUM clusters)",
            "",
        ]

        lines.append("🚨 HIGH PRIORITY NEWS")
        if critical_high:
            for c in critical_high:
                lines.append(_format_line(c))
        else:
            lines.append("(none)")
        lines.append("")

        lines.append("📢 IMPORTANT DEVELOPMENTS")
        if medium:
            for c in medium:
                lines.append(_format_line(c))
        else:
            lines.append("(none)")
        lines.append("")

        lines.append("🔥 TOP WATCHLIST")
        if watchlist:
            lines.extend(watchlist)
        else:
            lines.append("(no significant overnight activity)")
        lines.append("")

        lines.append(f"📈 PRE-MARKET BIAS: {bias}  (bull {bull} · bear {bear})")

        msg = "\n".join(lines)
        if len(msg) > MAX_TELEGRAM_LEN:
            msg = msg[:MAX_TELEGRAM_LEN - 30] + "\n\n…(truncated)"

        if telegram_send(msg):
            log.info("Pre-market summary sent (%d items, %d clusters, bias=%s)",
                     len(items), len(clusters), bias)
        else:
            log.error("Pre-market summary failed to send")
