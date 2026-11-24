"""Headless market-data library for consumers such as a separate Kalshi Gold bot.

No dashboard, trading strategy, credentials, or order placement. One quote
session over one TradingView WebSocket connection, exposed as callbacks and
timestamped snapshots. All data must be treated as an unofficial research feed.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

from .client import TradingViewClient
from .instruments import GOLD, Instrument
from .quotes import Quote, QuoteStream


class MarketDataFeed:
    """Reusable read-only quote feed for a configured set of instruments.

    Example:
        feed = MarketDataFeed(GOLD)
        feed.start()
        if feed.wait_connected(20):
            print(feed.snapshot()["quotes"]["gold"])
        feed.stop()

    For the fastest access, supply on_quote: it fires on received quote updates
    and receives a Quote object with the original TradingView price timestamp.
    A callback does not imply a *new price*; use last_price_received_at or
    exchange_time and check freshness.
    """

    def __init__(
        self,
        instruments: tuple[Instrument, ...] = GOLD,
        *,
        client: TradingViewClient | None = None,
        on_quote: Callable[[Quote], None] | None = None,
        name: str = "gold",
    ) -> None:
        if not instruments:
            raise ValueError("at least one instrument is required")
        keys = [inst.key for inst in instruments]
        symbols = [inst.symbol for inst in instruments]
        if len(set(keys)) != len(keys) or len(set(symbols)) != len(symbols):
            raise ValueError("instrument keys and symbols must be unique")
        self.instruments = tuple(instruments)
        self.name = name
        self.client = client if client is not None else TradingViewClient()
        self._callback = on_quote
        self._updates = threading.Condition()
        self._sequence = 0
        self.stream = QuoteStream(self.client, symbols, on_update=self._on_quote)

    @property
    def connected(self) -> bool:
        return self.client.connected

    def _on_quote(self, quote: Quote) -> None:
        with self._updates:
            self._sequence += 1
            self._updates.notify_all()
        if self._callback is not None:
            self._callback(quote)

    def start(self) -> None:
        self.client.start()

    def wait_connected(self, timeout: float = 20.0) -> bool:
        return self.client.wait_connected(timeout)

    def stop(self) -> None:
        self.client.stop()

    def wait_for_update(self, after_sequence: int, timeout: float = 15.0) -> int:
        """Wait for an actual WS quote message, or until timeout (SSE heartbeat)."""
        with self._updates:
            if self._sequence <= after_sequence:
                self._updates.wait_for(lambda: self._sequence > after_sequence, timeout)
            return self._sequence

    def snapshot(self) -> dict[str, Any]:
        """Atomic-enough copy of all quotes with freshness recomputed NOW.

        Missing symbols are included as no_data; disconnected feeds fail closed.
        The snapshot is JSON-serializable and has stable keys for bot clients.
        """
        received = self.stream.snapshots()
        connected = self.connected
        rows: dict[str, dict[str, Any]] = {}
        for inst in self.instruments:
            quote = received.get(inst.symbol)
            row = quote.as_row() if quote else {
                "symbol": inst.symbol,
                "last": None,
                "received_utc": None,
                "exchange_utc": None,
                "last_price_received_utc": None,
                "price_age_s": None,
                "usable_short_term": False,
                "update_mode": None,
                "volume": None,
                "bid": None,
                "ask": None,
            }
            row["key"] = inst.key
            row["name"] = inst.name
            row["usable_short_term"] = bool(connected and row["usable_short_term"])
            if not connected:
                row["status"] = "disconnected"
            elif quote is None or quote.last is None:
                row["status"] = "no_data"
            elif quote.is_delayed:
                row["status"] = "delayed"
            elif row["price_age_s"] is None:
                row["status"] = "unknown_age"
            elif row["price_age_s"] > 30.0 or row["price_age_s"] < -5.0:
                row["status"] = "stale"
            elif row["usable_short_term"]:
                row["status"] = "ready"
            else:
                row["status"] = "invalid"
            rows[inst.key] = row

        with self._updates:
            sequence = self._sequence
        return {
            "schema_version": 1,
            "source": "tradingview_unofficial",
            "watchlist": self.name,
            "generated_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "connected": connected,
            "sequence": sequence,
            "quotes": rows,
        }
