"""Live quote stream: last price, bid/ask, session high/low, volume."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from . import protocol
from .client import TradingViewClient
from .quality import clean_book, clean_volume, finite_number, price_age_seconds, short_term_usable

log = logging.getLogger(__name__)

# Fields the server will push. Trimming this list reduces bandwidth but the
# defaults are what you normally want on a ticker tape.
DEFAULT_FIELDS = [
    "lp",                 # last price
    "ch",                 # change vs previous close
    "chp",                # change percent
    "volume",
    "bid",
    "ask",
    "open_price",
    "high_price",
    "low_price",
    "prev_close_price",
    "lp_time",            # exchange timestamp of the last print, epoch seconds
    "update_mode",        # "streaming" | "delayed_streaming" | "snapshot"
    "currency_code",
    "description",
    "exchange",
    "short_name",
    "type",
    "pricescale",        # display precision; gold ticks 0.1, copper 0.0001
    "minmov",
]


@dataclass
class Quote:
    """A merged snapshot for one symbol.

    The server sends *partial* updates, so each ``qsd`` message is folded into
    the running snapshot rather than replacing it.
    """

    symbol: str
    values: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_price_received_at: datetime | None = None

    @property
    def last(self) -> float | None:
        return finite_number(self.values.get("lp"))

    @property
    def change(self) -> float | None:
        return self.values.get("ch")

    @property
    def change_pct(self) -> float | None:
        return self.values.get("chp")

    @property
    def volume(self) -> float | None:
        return clean_volume(self.symbol, self.values.get("volume"))

    @property
    def bid(self) -> float | None:
        return clean_book(self.symbol, self.values.get("bid"), self.values.get("ask"))[0]

    @property
    def ask(self) -> float | None:
        return clean_book(self.symbol, self.values.get("bid"), self.values.get("ask"))[1]

    @property
    def is_delayed(self) -> bool:
        return "delayed" in str(self.values.get("update_mode", ""))

    @property
    def exchange_time(self) -> datetime | None:
        ts = finite_number(self.values.get("lp_time"))
        if ts is None or ts <= 0:
            return None
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    def as_row(self) -> dict[str, Any]:
        """Flat dict suitable for CSV/JSONL sinks."""
        exch_time = self.exchange_time
        last_received = self.last_price_received_at
        age = price_age_seconds(self.values.get("lp_time"))
        return {
            "symbol": self.symbol,
            "received_utc": self.updated_at.isoformat(timespec="milliseconds"),
            "exchange_utc": exch_time.isoformat() if exch_time else "",
            "last_price_received_utc": last_received.isoformat(timespec="milliseconds") if last_received else "",
            "price_age_s": round(age, 2) if age is not None else None,
            "usable_short_term": short_term_usable(self.symbol, self.values),
            "last": self.last,
            "change": self.change,
            "change_pct": self.change_pct,
            "volume": self.volume,
            "bid": self.bid,
            "ask": self.ask,
            "open": self.values.get("open_price"),
            "high": self.values.get("high_price"),
            "low": self.values.get("low_price"),
            "prev_close": self.values.get("prev_close_price"),
            "update_mode": self.values.get("update_mode"),
        }


class QuoteStream:
    """Subscribe to tick-by-tick quotes for a set of symbols.

    Symbols use TradingView's ``EXCHANGE:TICKER`` form, e.g. ``BINANCE:BTCUSDT``,
    ``NASDAQ:AAPL``, ``FX:EURUSD``.
    """

    def __init__(
        self,
        client: TradingViewClient,
        symbols: list[str],
        fields: list[str] | None = None,
        on_update: Callable[[Quote], None] | None = None,
    ) -> None:
        self.client = client
        self.symbols = list(symbols)
        self.fields = list(fields or DEFAULT_FIELDS)
        self.on_update = on_update

        self._session = protocol.session_id("qs")
        self._lock = threading.Lock()
        self._snapshots: dict[str, Quote] = {}
        # The server's first message for a symbol is often a bare bid/ask
        # fragment. Hold updates back until a last price has arrived so
        # downstream sinks never see a half-empty row.
        self._gate_on_last = "lp" in self.fields

        client.on_connect(self._setup)
        client.on("qsd", self._on_quote_data)
        client.on("quote_completed", self._on_completed)
        client.on("symbol_error", self._on_symbol_error)

    # -- session ---------------------------------------------------------

    def _setup(self) -> None:
        """(Re)create the quote session. Drop merged state from old connections."""
        with self._lock:
            self._snapshots.clear()
        self._session = protocol.session_id("qs")
        self.client.send("quote_create_session", [self._session])
        self.client.send("quote_set_fields", [self._session, *self.fields])
        if self.symbols:
            self.client.send("quote_add_symbols", [self._session, *self.symbols])
            # Promotes symbols to the fast-update tier (real-time ticks rather
            # than the ~1s throttled cadence).
            self.client.send("quote_fast_symbols", [self._session, *self.symbols])

    def add(self, symbol: str) -> None:
        with self._lock:
            if symbol in self.symbols:
                return
            self.symbols.append(symbol)
        self.client.send("quote_add_symbols", [self._session, symbol])
        self.client.send("quote_fast_symbols", [self._session, symbol])

    def remove(self, symbol: str) -> None:
        with self._lock:
            if symbol not in self.symbols:
                return
            self.symbols.remove(symbol)
            self._snapshots.pop(symbol, None)
        self.client.send("quote_remove_symbols", [self._session, symbol])

    # -- inbound ---------------------------------------------------------

    def _on_quote_data(self, params: list[Any]) -> None:
        if len(params) < 2 or not isinstance(params[1], dict):
            return
        payload = params[1]
        if payload.get("s") != "ok":
            log.debug("quote status %r for %r", payload.get("s"), payload.get("n"))
            return
        symbol = payload.get("n")
        update = payload.get("v") or {}
        if not symbol or not update:
            return

        with self._lock:
            quote = self._snapshots.setdefault(symbol, Quote(symbol=symbol))
            price_update = (
                ("lp" in update and update["lp"] != quote.values.get("lp"))
                or ("lp_time" in update and update["lp_time"] != quote.values.get("lp_time"))
            )
            quote.values.update(update)
            quote.updated_at = datetime.now(timezone.utc)
            if price_update and finite_number(quote.values.get("lp")) is not None:
                quote.last_price_received_at = quote.updated_at
            if self._gate_on_last and finite_number(quote.values.get("lp")) is None:
                return
            snapshot = Quote(symbol, dict(quote.values), quote.updated_at, quote.last_price_received_at)

        if self.on_update is not None:
            self.on_update(snapshot)

    def _on_completed(self, params: list[Any]) -> None:
        if len(params) > 1:
            log.debug("initial snapshot complete for %s", params[1])

    def _on_symbol_error(self, params: list[Any]) -> None:
        log.error("symbol rejected by server: %s", params)

    # -- access ----------------------------------------------------------

    def snapshot(self, symbol: str) -> Quote | None:
        with self._lock:
            quote = self._snapshots.get(symbol)
            return Quote(quote.symbol, dict(quote.values), quote.updated_at, quote.last_price_received_at) if quote else None

    def snapshots(self) -> dict[str, Quote]:
        with self._lock:
            return {s: Quote(q.symbol, dict(q.values), q.updated_at, q.last_price_received_at) for s, q in self._snapshots.items()}
