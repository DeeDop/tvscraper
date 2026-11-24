"""Offline tests for the local read-only endpoint and reusable Gold feed.

Run: python -m unittest discover -s tests -v
No external network connection or TradingView credentials required.
"""
from __future__ import annotations

import json
import threading
import time
import unittest
from functools import partial
from http.server import ThreadingHTTPServer
from urllib.request import urlopen

from tvscraper.api import FeedHandler
from tvscraper.cli import build_parser
from tvscraper.feed import MarketDataFeed
from tvscraper.instruments import GOLD, MARKETS15, WATCHLISTS


class GoldFeedTests(unittest.TestCase):
    def setUp(self):
        self.feed = MarketDataFeed(GOLD)
        # Simulate socket-connected state; no external connection is made.
        self.feed.client._connected.set()

    def publish(self, symbol, last, age=1, mode="streaming", **fields):
        data = {"lp": last, "lp_time": time.time() - age, "update_mode": mode}
        data.update(fields)
        self.feed.stream._on_quote_data([
            "test_session", {"s": "ok", "n": symbol, "v": data},
        ])

    def test_high_priority_only_gold_watchlist(self):
        symbols = {i.symbol for i in GOLD}
        self.assertEqual(symbols, {
            "TVC:DXY", "FX:EURUSD", "FX:USDJPY", "TVC:US10Y",
            "OANDA:XAUUSD", "OANDA:XAGUSD", "FX:AUDUSD",
            "TVC:US02Y", "TVC:US05Y",
        })
        self.assertIs(WATCHLISTS["gold"], GOLD)
        self.assertEqual({i.slot for i in GOLD}, set(range(1, 10)))

    def test_curated_markets15_has_seven_targets_and_five_context_quotes(self):
        expected = {
            "gold", "eurusd", "gbpusd", "usdjpy", "audusd", "usdcad", "usdmxn",
            "silver", "dxy", "us02y", "us05y", "us10y",
        }
        self.assertEqual({i.key for i in MARKETS15}, expected)
        self.assertEqual(len(MARKETS15), 12)
        self.assertIs(WATCHLISTS["markets15"], MARKETS15)
        self.assertFalse({"platinum", "palladium", "wti", "brent",
                          "natgas", "us30y", "sp500"} & expected)

    def test_missing_data_has_explicit_status(self):
        payload = self.feed.snapshot()
        self.assertTrue(payload["connected"])
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(len(payload["quotes"]), 9)
        self.assertEqual(payload["quotes"]["silver"]["status"], "no_data")
        self.assertFalse(payload["quotes"]["silver"]["usable_short_term"])

    def test_quotes_and_yields_sanitized_for_consumer(self):
        self.publish("OANDA:XAUUSD", 3627.22, age=0.5, bid=3627.1, ask=3627.3)
        self.publish("TVC:US05Y", 4.50, age=550, bid=4.7, ask=4.6, volume=10 ** 120)
        result = self.feed.snapshot()["quotes"]
        self.assertEqual(result["gold"]["status"], "ready")
        self.assertTrue(result["gold"]["usable_short_term"])
        self.assertEqual(result["us05y"]["status"], "stale")
        self.assertIsNone(result["us05y"]["volume"])
        self.assertIsNone(result["us05y"]["bid"])
        self.assertIsNone(result["us05y"]["ask"])
        self.assertFalse(result["us05y"]["usable_short_term"])

    def test_disconnected_fails_closed_even_when_price_is_fresh(self):
        self.publish("FX:AUDUSD", 0.7, age=0.2)
        self.assertTrue(self.feed.snapshot()["quotes"]["audusd"]["usable_short_term"])
        self.feed.client._connected.clear()
        row = self.feed.snapshot()["quotes"]["audusd"]
        self.assertEqual(row["status"], "disconnected")
        self.assertFalse(row["usable_short_term"])

    def test_quote_fragment_updates_sequence_not_last_print(self):
        self.publish("TVC:DXY", 105.0, age=2.0)
        snap1 = self.feed.snapshot()
        last_print = self.feed.stream.snapshot("TVC:DXY").last_price_received_at
        self.feed.stream._on_quote_data(["test_session", {
            "s": "ok", "n": "TVC:DXY", "v": {"ch": -0.12},
        }])
        snap2 = self.feed.snapshot()
        self.assertEqual(snap2["sequence"], snap1["sequence"] + 1)
        self.assertEqual(self.feed.stream.snapshot("TVC:DXY").last_price_received_at, last_print)

    def test_cli_has_headless_serve_option(self):
        parsed = build_parser().parse_args(["serve"])
        self.assertEqual(parsed.watchlist, "gold")
        self.assertEqual(parsed.port, 8788)

    def test_local_http_api_serves_json_without_trading_actions(self):
        stopped = threading.Event()
        server = ThreadingHTTPServer(("127.0.0.1", 0), partial(FeedHandler, self.feed, stopped))
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with urlopen(f"http://127.0.0.1:{port}/api/quotes", timeout=3) as response:
                self.assertEqual(response.status, 200)
                self.assertIn("application/json", response.headers["Content-Type"])
                data = json.load(response)
            self.assertEqual(data["watchlist"], "gold")
            self.assertIn("us02y", data["quotes"])
            with urlopen(f"http://127.0.0.1:{port}/api/health", timeout=3) as response:
                health = json.load(response)
            self.assertEqual(health["total"], 9)
            self.assertTrue(health["connected"])
        finally:
            stopped.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
