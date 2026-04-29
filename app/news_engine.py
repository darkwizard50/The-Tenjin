"""Multi-source market news scanner.

- Iterates over `NEWS_SOURCES` with a real-browser User-Agent.
- Fetches each feed under retry-with-backoff.
- Persists deduplication state to Postgres so similar headlines are not
  re-alerted across restarts.
- Failures in one source NEVER stop the others.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import feedparser
import requests
from rapidfuzz import fuzz

from .config import NEWS_FEED_TIMEOUT, NEWS_SOURCES, PRIORITY_MEDIUM
from .cooldowns import CooldownManager
from .db import AlertStore
from .fno_universe import FNO_LOOKUP
from .retry import with_backoff
from .scoring import priority_for, score_news_alert
from .summary_engine import SummaryEngine
from .telegram_sender import send as telegram_send
from .utils import get_logger

log = get_logger("news")

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "application/rss+xml, application/atom+xml, application/xml;q=0.9, "
        "text/xml;q=0.8, */*;q=0.5"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

_RETRYABLE = (requests.ConnectionError, requests.Timeout)
_MAX_DEDUPE_IN_MEMORY = 500


def _match_fno(headline: str) -> Optional[str]:
    text = headline.lower()
    for base, ticker in FNO_LOOKUP.items():
        if base in text:
            return ticker
    return None


@with_backoff(attempts=3, base_delay=1.0, exceptions=_RETRYABLE, on_failure_return=None)
def _fetch_url(url: str) -> Optional[requests.Response]:
    return requests.get(url, timeout=NEWS_FEED_TIMEOUT, headers=_BROWSER_HEADERS)


class NewsEngine:
    def __init__(
        self,
        cooldowns: CooldownManager,
        summary: Optional[SummaryEngine] = None,
        store: Optional[AlertStore] = None,
    ) -> None:
        self.cooldowns = cooldowns
        self.summary = summary
        self.store = store
        self.recent: list[str] = []
        self._source_failures: dict[str, int] = {}
        if store is not None and store.enabled:
            self._warm_dedup()

    def _warm_dedup(self) -> None:
        try:
            cached = self.store.load_dedup(since_hours=24)
            self.recent = cached[-_MAX_DEDUPE_IN_MEMORY:]
            log.info("Warmed %d dedupe headline(s) from DB", len(self.recent))
        except Exception as exc:
            log.warning("Dedup warm-up failed: %s", exc)

    def _is_duplicate(self, headline: str) -> bool:
        text = headline.lower()
        for old in self.recent:
            if fuzz.ratio(text, old.lower()) > 85:
                log.info("Duplicate skipped: %r", headline[:80])
                return True
        self.recent.append(headline)
        if len(self.recent) > _MAX_DEDUPE_IN_MEMORY:
            self.recent.pop(0)
        if self.store is not None:
            self.store.save_dedup(headline)
        return False

    def _fetch_feed(self, source: str, url: str) -> list:
        try:
            resp = _fetch_url(url)
            if resp is None:
                self._source_failures[source] = self._source_failures.get(source, 0) + 1
                log.warning("Feed %s unreachable after retries", source)
                return []
            if resp.status_code != 200:
                log.warning("Feed %s status=%d", source, resp.status_code)
                return []
            self._source_failures.pop(source, None)
            feed = feedparser.parse(resp.content)
            return list(feed.entries[:20])
        except Exception as exc:
            self._source_failures[source] = self._source_failures.get(source, 0) + 1
            log.warning("Feed %s failed: %s", source, exc)
            return []

    def scan(self) -> None:
        sent = 0
        ok_sources = 0
        for source, url in NEWS_SOURCES:
            try:
                entries = self._fetch_feed(source, url)
            except Exception as exc:
                log.exception("Source %s fatal error: %s", source, exc)
                continue
            if entries:
                ok_sources += 1
            log.info("Source %s returned %d entries", source, len(entries))
            for entry in entries:
                try:
                    headline = entry.get("title") or ""
                    if not headline or self._is_duplicate(headline):
                        continue
                    ticker = _match_fno(headline)
                    score = score_news_alert(headline, fno_match=bool(ticker))
                    if score < PRIORITY_MEDIUM:
                        continue
                    symbol = ticker or "MARKET"
                    if not self.cooldowns.can_send_news(symbol):
                        continue
                    priority = priority_for(score)
                    msg = (
                        f"{priority}\n\n"
                        f"Stock: {symbol}\n"
                        f"Headline: {headline}\n"
                        f"Score: {score}\n"
                        f"Source: {source}\n"
                        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    telegram_send(msg)
                    if self.summary is not None:
                        self.summary.add(priority, symbol, headline, score)
                    if self.store is not None:
                        self.store.insert(
                            alert_type="NEWS",
                            symbol=symbol,
                            headline=headline,
                            score=score,
                            priority=priority,
                            source=source,
                        )
                    sent += 1
                except Exception as exc:
                    log.exception("Entry handler crashed for %s: %s", source, exc)
        log.info(
            "News scan complete — %d alerts sent (%d/%d sources OK)",
            sent, ok_sources, len(NEWS_SOURCES),
        )

    def stats(self) -> dict[str, object]:
        return {
            "dedup_in_memory": len(self.recent),
            "failing_sources": list(self._source_failures.keys()),
        }
