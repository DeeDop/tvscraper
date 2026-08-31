"""Live OHLCV bar stream built on TradingView chart sessions."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from . import protocol
from .client import TradingViewClient

log = logging.getLogger(__name__)

# Resolutions the chart endpoint accepts: seconds with an S suffix, minutes as
# bare numbers, then hour/day/week/month suffixes.
VALID_RESOLUTIONS = {
    "1S", "5S", "10S", "15S", "30S",
    "1", "3", "5", "15", "30", "45",
    "60", "120", "180", "240",
    "1D", "1W", "1M",
}


@dataclass(frozen=True)
class Bar:
    symbol: str
    resolution: str
    time: datetime          # bar open time, UTC
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    closed: bool            # False while the bar is still forming

    def as_row(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "resolution": self.resolution,
            "time_utc": self.time.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "closed": self.closed,
        }


class BarStream:
    """Stream OHLCV bars for one or more symbols at a single resolution.

    On connect the server replays ``history`` bars, then pushes an update on
    every tick that moves the forming bar. ``on_bar`` fires for both; check
    ``bar.closed`` to act only on completed bars.
    """

    def __init__(
        self,
        client: TradingViewClient,
        symbols: list[str],
        resolution: str = "1",
        history: int = 300,
        on_bar: Callable[[Bar], None] | None = None,
        include_forming: bool = True,
        extended_hours: bool = False,
    ) -> None:
        if resolution not in VALID_RESOLUTIONS:
            raise ValueError(
                f"unsupported resolution {resolution!r}; expected one of "
                + ", ".join(sorted(VALID_RESOLUTIONS))
            )
        self.client = client
        self.symbols = list(symbols)
        self.resolution = resolution
        self.history = history
        self.on_bar = on_bar
        self.include_forming = include_forming
        self.session_kind = "extended" if extended_hours else "regular"

        self._lock = threading.Lock()
        # A chart session carries exactly one series, so each symbol gets its
        # own session. Creating several series in one session is rejected with
        # "exceed limit of series in the session" and the server hangs up.
        self._sessions: dict[str, str] = {}               # chart session -> symbol
        self._forming: dict[str, tuple[float, Bar]] = {}  # symbol -> (epoch, bar)
        self._replaying: set[str] = set()                 # sessions still replaying history

        client.on_connect(self._setup)
        client.on("timescale_update", self._on_data)
        client.on("du", self._on_data)
        client.on("series_completed", self._on_series_completed)
        client.on("series_error", self._on_series_error)
        client.on("symbol_error", self._on_symbol_error)

    # -- session ---------------------------------------------------------

    def _setup(self) -> None:
        with self._lock:
            self._sessions.clear()
            self._forming.clear()
            self._replaying.clear()

        for symbol in self.symbols:
            chart = protocol.session_id("cs")
            with self._lock:
                self._sessions[chart] = symbol
                self._replaying.add(chart)

            self.client.send("chart_create_session", [chart, ""])
            self.client.send(
                "resolve_symbol",
                [chart, "sds_sym_1", protocol.symbol_spec(symbol, session=self.session_kind)],
            )
            self.client.send(
                "create_series",
                [chart, "sds_1", "s1", "sds_sym_1", self.resolution, self.history, ""],
            )

    # -- inbound ---------------------------------------------------------

    def _on_data(self, params: list[Any]) -> None:
        """Handle ``timescale_update`` (history) and ``du`` (live) alike."""
        if len(params) < 2 or not isinstance(params[1], dict):
            return
        with self._lock:
            symbol = self._sessions.get(params[0])
            replaying = params[0] in self._replaying
        if symbol is None:
            return
        for block in params[1].values():
            if not isinstance(block, dict):
                continue
            for point in block.get("s") or ():
                values = point.get("v") or []
                if len(values) < 5:
                    continue
                self._ingest(symbol, values, replaying)

    def _ingest(self, symbol: str, values: list[Any], replaying: bool) -> None:
        """Fold one raw ``[time, o, h, l, c, v]`` tuple into the bar state."""
        epoch = float(values[0])
        bar = Bar(
            symbol=symbol,
            resolution=self.resolution,
            time=datetime.fromtimestamp(epoch, tz=timezone.utc),
            open=values[1],
            high=values[2],
            low=values[3],
            close=values[4],
            volume=values[5] if len(values) > 5 else None,
            closed=False,
        )

        to_emit: list[Bar] = []
        with self._lock:
            previous = self._forming.get(symbol)
            if previous is None or epoch > previous[0]:
                # A new bar opened, so the one before it is final.
                if previous is not None:
                    to_emit.append(_as_closed(previous[1]))
                self._forming[symbol] = (epoch, bar)
                if not replaying:
                    to_emit.append(bar)
            elif epoch == previous[0]:
                if bar == previous[1]:
                    return  # server repeated an unchanged forming bar
                self._forming[symbol] = (epoch, bar)
                to_emit.append(bar)
            else:
                return  # a late/duplicate historical point

        if self.on_bar is None:
            return
        for item in to_emit:
            if item.closed or self.include_forming:
                self.on_bar(item)

    def _on_series_completed(self, params: list[Any]) -> None:
        if not params:
            return
        chart = params[0]
        with self._lock:
            self._replaying.discard(chart)
            symbol = self._sessions.get(chart)
        log.info("history loaded for %s", symbol or chart)

    def _on_series_error(self, params: list[Any]) -> None:
        log.error("series error: %s", params)

    def _on_symbol_error(self, params: list[Any]) -> None:
        log.error("symbol rejected by server: %s", params)

    # -- access ----------------------------------------------------------

    def forming(self, symbol: str) -> Bar | None:
        with self._lock:
            entry = self._forming.get(symbol)
            return entry[1] if entry else None


def _as_closed(bar: Bar) -> Bar:
    return Bar(
        bar.symbol, bar.resolution, bar.time,
        bar.open, bar.high, bar.low, bar.close, bar.volume,
        closed=True,
    )
