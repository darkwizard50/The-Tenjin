"""Alert scoring and priority labelling."""

from __future__ import annotations

from .config import (
    EARNINGS_KEYWORDS,
    FILING_KEYWORDS,
    PRIORITY_CRITICAL,
    PRIORITY_HIGH,
    PRIORITY_MEDIUM,
    SCORE_DUAL_SOURCE,
    SCORE_EARNINGS,
    SCORE_EMA_ALIGNED,
    SCORE_FILING,
    SCORE_FNO_STOCK,
    SCORE_MTF_ALIGNED,
    SCORE_RSI_CROSSOVER,
    SCORE_VOLUME_SPIKE,
)
from .utils import get_logger

log = get_logger("scoring")


def score_rsi_alert(
    *,
    volume_spike: bool,
    fno: bool = True,
    dual_source: bool = False,
    mtf_aligned: bool = False,
    ema_aligned: bool = False,
) -> int:
    """Compute RSI alert score. Floor with volume confirmation = 90."""
    score = SCORE_RSI_CROSSOVER
    if volume_spike:
        score += SCORE_VOLUME_SPIKE
    if fno:
        score += SCORE_FNO_STOCK
    if dual_source:
        score += SCORE_DUAL_SOURCE
    if mtf_aligned:
        score += SCORE_MTF_ALIGNED
    if ema_aligned:
        score += SCORE_EMA_ALIGNED
    log.info(
        "RSI score=%d (vol=%s, fno=%s, dual=%s, mtf=%s, ema=%s)",
        score, volume_spike, fno, dual_source, mtf_aligned, ema_aligned,
    )
    return score


def score_news_alert(headline: str, fno_match: bool) -> int:
    text = headline.lower()
    score = 0
    if any(k in text for k in EARNINGS_KEYWORDS):
        score += SCORE_EARNINGS
    if any(k in text for k in FILING_KEYWORDS):
        score += SCORE_FILING
    if fno_match:
        score += SCORE_FNO_STOCK
    log.info("News score=%d for: %r", score, headline[:80])
    return score


def priority_for(score: int) -> str:
    if score >= PRIORITY_CRITICAL:
        return "🚨 CRITICAL"
    if score >= PRIORITY_HIGH:
        return "🔥 HIGH"
    if score >= PRIORITY_MEDIUM:
        return "📢 MEDIUM"
    return ""


def confidence_for(*, dual_source: bool, volume_spike: bool) -> str:
    """Confidence label based on source agreement and volume."""
    if dual_source and volume_spike:
        return "VERY HIGH"
    if dual_source:
        return "HIGH"
    return "MEDIUM"
