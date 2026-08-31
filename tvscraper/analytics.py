"""Aggregates the raw streams into what the dashboard renders and verifies.

Two things happen here. ``MarketState`` keeps a rolling window of bars and the
latest quote per instrument and derives the series the charts draw; alongside
that it runs a set of correctness checks, because a scraper that silently
returns stale or wrong numbers is worse than one that fails loudly.

The strongest check is ``cross`` below: quotes and bars arrive over two
*independent* server-side sessions (a quote session and a chart session). If the
last traded price and the forming bar's close agree, both extraction paths are
almost certainly being decoded correctly.
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .bars import Bar
from .instruments import WATCHLIST, Instrument
from .quotes import Quote

# Check severities, mapped to the status palette in the UI. Every check ships an
# icon and a label as well, so status is never carried by colour alone.
GOOD, WARN, SERIOUS, CRITICAL, UNKNOWN = "good", "warning", "serious", "critical", "unknown"

STALE_GOOD_S = 60.0
STALE_WARN_S = 300.0
CROSS_GOOD_BPS = 10.0      # quote vs bar close agreement, basis points
CROSS_WARN_BPS = 50.0
MIN_CORR_POINTS = 30       # below this, correlation is noise; report it as such

# TradingView refuses sub-minute bars to unentitled accounts ("seconds_not_
# entitled"), so 30-second candles are aggregated here from the live tick
# stream instead. They therefore start empty and fill in as the process runs -
# there is no history to backfill.
MICRO_SECONDS = 30
MICRO_KEEP = 480           # 4 hours of 30s buckets


@dataclass
class Check:
    id: str
    label: str
    status: str
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "label": self.label, "status": self.status, "detail": self.detail}


@dataclass
class InstrumentState:
    instrument: Instrument
    quote: dict[str, Any] = field(default_factory=dict)
    bars: dict[float, Bar] = field(default_factory=dict)   # bar open epoch -> bar
    micro: dict[float, list[float]] = field(default_factory=dict)  # bucket -> [o,h,l,c,v]
    ticks: int = 0
    bar_updates: int = 0
    last_tick_wall: float | None = None
    last_volume: float | None = None
    first_seen: float = field(default_factory=time.time)


class MarketState:
    """Thread-safe store fed by the quote and bar callbacks."""

    def __init__(
        self,
        instruments: tuple[Instrument, ...] = WATCHLIST,
        window_minutes: int = 360,
        resolution_seconds: int = 60,
    ) -> None:
        self.instruments = instruments
        self.window_minutes = window_minutes
        self.resolution_seconds = resolution_seconds
        self._lock = threading.Lock()
        self._states = {i.key: InstrumentState(i) for i in instruments}
        self._by_symbol = {i.symbol: i.key for i in instruments}
        self.started = time.time()

    # -- ingest ----------------------------------------------------------

    def on_quote(self, quote: Quote) -> None:
        key = self._by_symbol.get(quote.symbol)
        if key is None:
            return
        with self._lock:
            state = self._states[key]
            state.quote = dict(quote.values)
            state.ticks += 1
            now = time.time()
            state.last_tick_wall = now
            last = quote.values.get("lp")
            if isinstance(last, (int, float)):
                self._fold_micro(state, float(last), quote.values.get("volume"), now)

    def _fold_micro(self, state: InstrumentState, price: float, volume: Any, now: float) -> None:
        """Fold one tick into its 30-second bucket.

        Quote volume is cumulative for the session, so the per-bucket figure is
        the increment between ticks; a decrease means the session rolled over
        and the baseline is simply re-anchored.
        """
        delta = 0.0
        if isinstance(volume, (int, float)):
            if state.last_volume is not None and volume >= state.last_volume:
                delta = float(volume) - state.last_volume
            state.last_volume = float(volume)

        bucket = math.floor(now / MICRO_SECONDS) * MICRO_SECONDS
        cell = state.micro.get(bucket)
        if cell is None:
            state.micro[bucket] = [price, price, price, price, delta]
            if len(state.micro) > MICRO_KEEP:
                for old in sorted(state.micro)[: len(state.micro) - MICRO_KEEP]:
                    del state.micro[old]
        else:
            cell[1] = max(cell[1], price)
            cell[2] = min(cell[2], price)
            cell[3] = price
            cell[4] += delta

    def on_bar(self, bar: Bar) -> None:
        key = self._by_symbol.get(bar.symbol)
        if key is None:
            return
        with self._lock:
            state = self._states[key]
            state.bars[bar.time.timestamp()] = bar
            state.bar_updates += 1
            self._trim(state)

    def _trim(self, state: InstrumentState) -> None:
        limit = self.window_minutes * 60
        if not state.bars:
            return
        newest = max(state.bars)
        stale = [t for t in state.bars if newest - t > limit]
        for t in stale:
            del state.bars[t]

    # -- derived ---------------------------------------------------------

    def snapshot(self, with_series: bool = True) -> dict[str, Any]:
        """Everything the dashboard needs for one render, as plain JSON types.

        ``with_series=False`` drops the bar arrays and the correlation matrix,
        which is the bulk of the payload. The dashboard uses the light form for
        its 1s heartbeat and the full form on a slower cadence.
        """
        now = time.time()
        with self._lock:
            states = {k: _copy_state(s) for k, s in self._states.items()}

        instruments = []
        closes: dict[str, dict[float, float]] = {}
        for key, state in states.items():
            series = sorted(state.bars.items())
            if with_series:
                closes[key] = {t: b.close for t, b in series}
            instruments.append(self._describe(state, series, now, with_series))

        payload: dict[str, Any] = {
            "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "uptime_seconds": round(now - self.started, 1),
            "resolution_seconds": self.resolution_seconds,
            "micro_seconds": MICRO_SECONDS,
            "with_series": with_series,
            "instruments": instruments,
        }
        if with_series:
            payload["correlation"] = self._correlation(closes)
        return payload

    def _describe(
        self,
        state: InstrumentState,
        series: list[tuple[float, Bar]],
        now: float,
        with_series: bool = True,
    ) -> dict[str, Any]:
        inst = state.instrument
        quote = state.quote
        last = quote.get("lp")
        decimals = _decimals(quote.get("pricescale"), last)

        checks = self._checks(state, series, now)
        worst = _worst([c.status for c in checks])

        payload = {
            "key": inst.key,
            "symbol": inst.symbol,
            "name": inst.name,
            "group": inst.group,
            "slot": inst.slot,
            "decimals": decimals,
            "currency": quote.get("currency_code"),
            "description": quote.get("description"),
            "last": last,
            "change": quote.get("ch"),
            "change_pct": quote.get("chp"),
            "bid": quote.get("bid") or None,
            "ask": quote.get("ask") or None,
            "open": quote.get("open_price"),
            "high": quote.get("high_price"),
            "low": quote.get("low_price"),
            "prev_close": quote.get("prev_close_price"),
            "volume": quote.get("volume"),
            "update_mode": quote.get("update_mode"),
            "delay_seconds": _delay_seconds(quote.get("update_mode")),
            "exchange_time": quote.get("lp_time"),
            "ticks": state.ticks,
            "bar_updates": state.bar_updates,
            "age_seconds": round(now - state.last_tick_wall, 1) if state.last_tick_wall else None,
            "range_position": _range_position(last, quote.get("low_price"), quote.get("high_price")),
            "realized_vol_pct": _realized_vol([b.close for _, b in series], self.resolution_seconds),
            "bar_count": len(series),
            "status": worst,
            "checks": [c.as_dict() for c in checks],
        }
        if with_series:
            payload["indexed"] = _indexed(series)
            payload["bars"] = [
                {
                    "t": int(t),
                    "o": b.open, "h": b.high, "l": b.low, "c": b.close,
                    "v": b.volume, "closed": b.closed,
                }
                for t, b in series
            ]
            cutoff = now - MICRO_SECONDS
            payload["micro_bars"] = [
                {
                    "t": int(t),
                    "o": cell[0], "h": cell[1], "l": cell[2], "c": cell[3],
                    "v": cell[4] or None, "closed": t <= cutoff,
                }
                for t, cell in sorted(state.micro.items())
            ]
        return payload

    # -- verification ----------------------------------------------------

    def _checks(self, state: InstrumentState, series: list[tuple[float, Bar]], now: float) -> list[Check]:
        quote = state.quote
        last = quote.get("lp")
        checks: list[Check] = []

        # 1. Is the feed live, and is it real time or exchange-delayed?
        mode = quote.get("update_mode")
        delay = _delay_seconds(mode)
        if not quote:
            checks.append(Check("feed", "Feed", UNKNOWN, "no data received yet"))
        elif mode == "streaming":
            checks.append(Check("feed", "Feed", GOOD, "real-time streaming"))
        elif delay:
            checks.append(Check("feed", "Feed", WARN, f"exchange-delayed {delay // 60} min"))
        else:
            checks.append(Check("feed", "Feed", WARN, str(mode or "unknown mode")))

        # 2. Are updates still arriving? A quiet feed is normal outside session
        #    hours, so this degrades rather than screaming.
        age = (now - state.last_tick_wall) if state.last_tick_wall else None
        if age is None:
            checks.append(Check("fresh", "Freshness", UNKNOWN, "awaiting first tick"))
        elif age <= STALE_GOOD_S:
            checks.append(Check("fresh", "Freshness", GOOD, f"tick {age:.0f}s ago"))
        elif age <= STALE_WARN_S:
            checks.append(Check("fresh", "Freshness", WARN, f"quiet for {age:.0f}s"))
        else:
            checks.append(
                Check("fresh", "Freshness", SERIOUS, f"no tick for {age / 60:.0f} min - market likely closed")
            )

        # 3. The independent-path check: quote session vs chart session.
        forming = series[-1][1] if series else None
        if last is None or forming is None:
            checks.append(Check("cross", "Quote vs bar", UNKNOWN, "waiting for both streams"))
        else:
            bps = abs(last - forming.close) / forming.close * 10_000 if forming.close else 0.0
            detail = f"{bps:.1f} bps apart ({last:g} vs {forming.close:g})"
            if bps <= CROSS_GOOD_BPS:
                checks.append(Check("cross", "Quote vs bar", GOOD, detail))
            elif bps <= CROSS_WARN_BPS:
                checks.append(Check("cross", "Quote vs bar", WARN, detail))
            else:
                checks.append(Check("cross", "Quote vs bar", CRITICAL, detail))

        # 4. Last price must sit inside the session range it is reported against.
        low, high = quote.get("low_price"), quote.get("high_price")
        if last is None or low is None or high is None:
            checks.append(Check("range", "Range sanity", UNKNOWN, "no session range yet"))
        elif low <= last <= high:
            checks.append(Check("range", "Range sanity", GOOD, f"within {low:g}-{high:g}"))
        else:
            checks.append(Check("range", "Range sanity", CRITICAL, f"{last:g} outside {low:g}-{high:g}"))

        # 5. A crossed book means the decode is wrong (or the fields are stale).
        bid, ask = quote.get("bid"), quote.get("ask")
        if not bid or not ask:
            checks.append(Check("spread", "Spread", UNKNOWN, "no book quoted"))
        elif bid <= ask:
            checks.append(Check("spread", "Spread", GOOD, f"{(ask - bid) / ask * 10_000:.1f} bps"))
        else:
            checks.append(Check("spread", "Spread", CRITICAL, f"crossed: bid {bid:g} > ask {ask:g}"))

        # 6. Has the bar stream itself stalled?
        checks.append(self._continuity(series, delay, now))
        return checks

    def _continuity(self, series: list[tuple[float, Bar]], delay: int, now: float) -> Check:
        """Check that bars are still landing, and count minutes that never traded.

        A missing minute is *not* a fault: a thinly traded futures contract
        prints no bar for a minute in which nothing changed hands, which is
        routine overnight. What would be a fault is the bar stream stalling
        while quotes keep ticking, so that is what this measures. The idle
        count is reported alongside as information, not as a problem.
        """
        if len(series) < 3:
            return Check("bars", "Bar continuity", UNKNOWN, f"{len(series)} bars so far")

        times = [t for t, _ in series]
        step = self.resolution_seconds
        idle = sum(int((b - a) / step) - 1 for a, b in zip(times, times[1:]) if b - a > step)

        # Delayed feeds legitimately sit one delay-window behind the clock.
        age = now - (times[-1] + step)
        allowance = step * 3 + delay
        if age > allowance:
            return Check("bars", "Bar continuity", WARN, f"newest bar {age / 60:.0f} min old")
        suffix = f", {idle} idle min" if idle else ", no gaps"
        return Check("bars", "Bar continuity", GOOD, f"{len(series)} bars{suffix}")

    # -- cross-instrument ------------------------------------------------

    def _correlation(self, closes: dict[str, dict[float, float]]) -> dict[str, Any]:
        keys = [i.key for i in self.instruments]
        matrix: list[list[float | None]] = []
        counts: list[list[int]] = []
        for a in keys:
            row: list[float | None] = []
            count_row: list[int] = []
            for b in keys:
                rho, n = _pearson_returns(closes.get(a, {}), closes.get(b, {}))
                row.append(rho)
                count_row.append(n)
            matrix.append(row)
            counts.append(count_row)
        return {"keys": keys, "matrix": matrix, "counts": counts, "min_points": MIN_CORR_POINTS}


# -- helpers -------------------------------------------------------------


def _copy_state(state: InstrumentState) -> InstrumentState:
    return InstrumentState(
        instrument=state.instrument,
        quote=dict(state.quote),
        bars=dict(state.bars),
        micro={t: list(cell) for t, cell in state.micro.items()},
        ticks=state.ticks,
        bar_updates=state.bar_updates,
        last_tick_wall=state.last_tick_wall,
        last_volume=state.last_volume,
        first_seen=state.first_seen,
    )


def _worst(statuses: list[str]) -> str:
    for level in (CRITICAL, SERIOUS, WARN, UNKNOWN):
        if level in statuses:
            return level
    return GOOD


def _delay_seconds(mode: Any) -> int:
    """``delayed_streaming_600`` -> 600. Real-time modes -> 0."""
    text = str(mode or "")
    if "delayed" not in text:
        return 0
    tail = text.rsplit("_", 1)[-1]
    return int(tail) if tail.isdigit() else 900


def _decimals(pricescale: Any, last: Any) -> int:
    """Display precision, taken from TradingView's own ``pricescale``.

    Gold quotes to 0.1 and copper to 0.0001; hard-coding a decimal count would
    misprint one of them.
    """
    try:
        scale = int(pricescale)
        if scale > 0:
            return max(0, min(8, round(math.log10(scale))))
    except (TypeError, ValueError):
        pass
    if isinstance(last, (int, float)) and last:
        return 2 if abs(last) >= 100 else 4
    return 2


def _range_position(last: Any, low: Any, high: Any) -> float | None:
    """Where the last price sits in the session range, 0 (low) to 1 (high)."""
    if None in (last, low, high) or high == low:
        return None
    return max(0.0, min(1.0, (last - low) / (high - low)))


def _realized_vol(closes: list[float], resolution_seconds: int) -> float | None:
    """Annualised realised volatility from bar returns, in percent.

    Gives the metals and crypto a comparable risk number despite price scales
    that differ by four orders of magnitude.
    """
    returns = _log_returns(closes)
    if len(returns) < 20:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    periods_per_year = (365 * 24 * 3600) / resolution_seconds
    return math.sqrt(variance * periods_per_year) * 100


def _log_returns(values: list[float]) -> list[float]:
    out = []
    for previous, current in zip(values, values[1:]):
        if previous > 0 and current > 0:
            out.append(math.log(current / previous))
    return out


def _indexed(series: list[tuple[float, Bar]]) -> list[dict[str, Any]]:
    """Rebase closes to 100 at the first bar in the window.

    This is what makes copper at $6 and Bitcoin at $64,000 comparable on a
    single axis, instead of reaching for a second y-scale.
    """
    if not series:
        return []
    base = series[0][1].close
    if not base:
        return []
    return [{"t": int(t), "v": b.close / base * 100.0} for t, b in series]


def _pearson_returns(a: dict[float, float], b: dict[float, float]) -> tuple[float | None, int]:
    """Correlation of log returns over bars the two instruments share.

    Crypto trades around the clock while the metals and index futures keep
    session hours, so the series are intersected on timestamp before any
    comparison - otherwise a gap in one would silently shift the other.
    """
    common = sorted(set(a) & set(b))
    if len(common) < MIN_CORR_POINTS + 1:
        return None, len(common)

    ra, rb = [], []
    for previous, current in zip(common, common[1:]):
        pa, ca = a[previous], a[current]
        pb, cb = b[previous], b[current]
        if pa > 0 and ca > 0 and pb > 0 and cb > 0:
            ra.append(math.log(ca / pa))
            rb.append(math.log(cb / pb))

    n = len(ra)
    if n < MIN_CORR_POINTS:
        return None, n
    mean_a, mean_b = sum(ra) / n, sum(rb) / n
    cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(ra, rb))
    var_a = sum((x - mean_a) ** 2 for x in ra)
    var_b = sum((y - mean_b) ** 2 for y in rb)
    if var_a <= 0 or var_b <= 0:
        return None, n
    return cov / math.sqrt(var_a * var_b), n
