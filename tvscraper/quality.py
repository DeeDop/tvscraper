"""Conservative validation for quote fields used by short-horizon research.

TradingView's quote session may include synthetic index volume, crossed index
bid/ask fields, and exchange timestamps older than the message reception time.
These fields must never be treated as clean trading signals by default.
"""
from __future__ import annotations

import math
import time
from typing import Any

MAX_PRICE_AGE_SECONDS = 30.0
MAX_VOLUME = 1e15  # guard against sentinel / malformed enormous integers


def finite_number(value: Any) -> float | None:
    """Return a finite numeric value; reject strings, bools, NaN and infinities."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        value = float(value)
    except (OverflowError, ValueError):
        return None
    return value if math.isfinite(value) else None


def clean_volume(symbol: str, value: Any) -> float | None:
    """TVC indexes/yields have no meaningful exchange-traded volume."""
    if symbol.startswith("TVC:"):
        return None
    number = finite_number(value)
    if number is None or number < 0 or number > MAX_VOLUME:
        return None
    return number


def clean_book(symbol: str, bid: Any, ask: Any) -> tuple[float | None, float | None]:
    """Don't export synthetic index books or crossed/invalid bid-ask quotes."""
    if symbol.startswith("TVC:"):
        return None, None
    b, a = finite_number(bid), finite_number(ask)
    if b is None or a is None or b <= 0 or a <= 0 or b > a:
        return None, None
    return b, a


def price_age_seconds(exchange_epoch: Any, now: float | None = None) -> float | None:
    """Age of the underlying last price, not age of the most recent WS message."""
    timestamp = finite_number(exchange_epoch)
    if timestamp is None or timestamp <= 0:
        return None
    return (time.time() if now is None else now) - timestamp


def short_term_usable(
    symbol: str,
    values: dict[str, Any],
    now: float | None = None,
    max_age: float = MAX_PRICE_AGE_SECONDS,
) -> bool:
    """Fail closed when market-data freshness or entitlement cannot be proven.

    "streaming" describes the vendor's update mode; it does not guarantee that
    the underlying instrument had a recent price print.
    """
    last = finite_number(values.get("lp"))
    age = price_age_seconds(values.get("lp_time"), now)
    if last is None or last <= 0 or age is None:
        return False
    if values.get("update_mode") != "streaming" or not (-5.0 <= age <= max_age):
        return False
    # A crossed book on a tradable symbol is a quote-integrity failure.
    if not symbol.startswith("TVC:"):
        bid, ask = values.get("bid"), values.get("ask")
        if bid is not None and ask is not None and clean_book(symbol, bid, ask) == (None, None):
            return False
    return True
