"""Library usage: watch a few symbols and print an alert on a price threshold.

Run with:  python examples/price_alert.py
"""

import time

from tvscraper import CsvSink, Quote, QuoteStream, TradingViewClient

THRESHOLDS = {
    "BINANCE:BTCUSDT": 70_000.0,
    "BINANCE:ETHUSDT": 2_500.0,
}

csv = CsvSink("data/alerts.csv", ["symbol", "received_utc", "last", "change_pct"])
fired: set[str] = set()


def on_update(quote: Quote) -> None:
    level = THRESHOLDS.get(quote.symbol)
    if level is None or quote.last is None:
        return
    print(f"{quote.symbol:20} {quote.last:>12,.2f}  {quote.change_pct or 0:+.2f}%")
    if quote.last >= level and quote.symbol not in fired:
        fired.add(quote.symbol)
        print(f"  *** {quote.symbol} crossed {level:,.2f} ***")
        csv.write(quote.as_row())


client = TradingViewClient()
QuoteStream(client, list(THRESHOLDS), on_update=on_update)
client.start()
client.wait_connected()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    client.stop()
    csv.close()
