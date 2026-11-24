"""Offline regression tests for quote sanitation and freshness.

Run: python -m unittest discover -s tests -v
No TradingView connection or credentials required.
"""
import time
import unittest
from datetime import datetime, timezone

from tvscraper.analytics import MarketState
from tvscraper.client import TradingViewClient
from tvscraper.instruments import Instrument
from tvscraper.quality import clean_book, clean_volume, price_age_seconds, short_term_usable
from tvscraper.quotes import Quote, QuoteStream


class QuoteQualityTests(unittest.TestCase):
    def test_synthetic_index_volume_is_not_tradable_volume(self):
        giant = 10 ** 120
        self.assertIsNone(clean_volume("TVC:DXY", giant))
        self.assertIsNone(clean_volume("TVC:US10Y", giant))
        self.assertIsNone(clean_volume("OANDA:XAUUSD", giant))
        self.assertEqual(clean_volume("OANDA:XAUUSD", 152.0), 152.0)

    def test_index_book_and_crossed_book_not_exposed(self):
        self.assertEqual(clean_book("TVC:US10Y", 4.9935, 4.9909), (None, None))
        self.assertEqual(clean_book("FX:EURUSD", 1.3, 1.2), (None, None))
        self.assertEqual(clean_book("FX:EURUSD", 1.1, 1.2), (1.1, 1.2))

    def test_freshness_does_not_equal_message_reception(self):
        now = time.time()
        values = {"lp": 4.916, "lp_time": now - 440, "update_mode": "streaming"}
        self.assertGreater(price_age_seconds(values["lp_time"], now), 400)
        self.assertFalse(short_term_usable("TVC:US10Y", values, now))
        values["lp_time"] = now - 2
        self.assertTrue(short_term_usable("TVC:US10Y", values, now))
        values["update_mode"] = "delayed_streaming_600"
        self.assertFalse(short_term_usable("TVC:US10Y", values, now))

    def test_bad_bid_ask_blocks_tradable_symbol_but_not_index(self):
        now = time.time()
        payload = {"lp": 4.916, "lp_time": now, "update_mode": "streaming",
                   "bid": 4.9935, "ask": 4.9909}
        self.assertTrue(short_term_usable("TVC:US10Y", payload, now))
        self.assertFalse(short_term_usable("FX:EURUSD", payload, now))

    def test_partial_quote_updates_do_not_rejuvenate_last_print(self):
        client = TradingViewClient()
        stream = QuoteStream(client, ["TVC:US10Y"])
        now = time.time()
        stream._on_quote_data(["session", {"s": "ok", "n": "TVC:US10Y", "v": {
            "lp": 4.916, "lp_time": now - 440, "update_mode": "streaming",
            "volume": 10 ** 120, "bid": 4.9935, "ask": 4.9909,
        }}])
        first = stream.snapshot("TVC:US10Y")
        self.assertIsNotNone(first)
        original_time = first.last_price_received_at
        stream._on_quote_data(["session", {"s": "ok", "n": "TVC:US10Y", "v": {
            "volume": 10 ** 119,
        }}])
        second = stream.snapshot("TVC:US10Y")
        self.assertEqual(second.last_price_received_at, original_time)
        self.assertFalse(second.as_row()["usable_short_term"])
        self.assertIsNone(second.as_row()["volume"])
        self.assertIsNone(second.as_row()["bid"])
        self.assertIsNone(second.as_row()["ask"])

    def test_dashboard_reports_old_index_price_not_new_ws_message(self):
        now = time.time()
        inst = Instrument("us10y", "TVC:US10Y", "US10Y", "Yield", 1)
        state = MarketState((inst,))
        quote = Quote("TVC:US10Y", {
            "lp": 4.916, "lp_time": now - 440, "update_mode": "streaming",
            "volume": 10 ** 120, "bid": 4.9935, "ask": 4.9909,
        }, datetime.now(timezone.utc), datetime.now(timezone.utc))
        state.on_quote(quote)
        record = state.snapshot(with_series=False)["instruments"][0]
        self.assertIsNone(record["volume"])
        self.assertIsNone(record["bid"])
        self.assertIsNone(record["ask"])
        self.assertFalse(record["usable_short_term"])
        self.assertGreater(record["price_age_seconds"], 400)
        freshness = next(c for c in record["checks"] if c["id"] == "fresh")
        self.assertEqual(freshness["status"], "serious")
        self.assertEqual(record["micro_bars"] if "micro_bars" in record else [], [])


if __name__ == "__main__":
    unittest.main()
