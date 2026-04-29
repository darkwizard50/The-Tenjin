"""Retry-with-exponential-backoff decorator for flaky calls."""

from __future__ import annotations

import functools
import random
import time
from typing import Any, Callable

from .utils import get_logger

log = get_logger("retry")


def with_backoff(
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,),
    on_failure_return: Any = None,
    raise_on_exhaust: bool = False,
):
    """Wrap a callable with exponential backoff retry.

    On final failure: returns `on_failure_return` (default None) or raises
    the last exception when `raise_on_exhaust=True`.
    """

    def decorator(fn: Callable):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: BaseException | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt >= attempts:
                        log.error(
                            "Giving up on %s after %d attempts: %s",
                            fn.__name__, attempts, exc,
                        )
                        if raise_on_exhaust:
                            raise
                        return on_failure_return
                    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    delay += random.uniform(0, delay * 0.2)
                    log.warning(
                        "Retry %d/%d for %s in %.1fs: %s",
                        attempt, attempts, fn.__name__, delay, exc,
                    )
                    time.sleep(delay)
            return on_failure_return  # unreachable
        return wrapper
    return decorator
