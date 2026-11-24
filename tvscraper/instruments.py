"""The tracked instrument set.

``slot`` is the categorical palette slot (1-8) each instrument owns. Colour
follows the entity, so a slot is fixed here and never reassigned by rank or by
what happens to be visible - filtering the dashboard must not repaint the rest.

Two watchlists ship. The default is real time for anonymous users; the futures
contracts are the same exposures but carry a 10-minute exchange delay unless the
account is entitled to CME/COMEX data. See ``WATCHLIST_NOTES`` below.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Instrument:
    key: str          # short id used in the API and the DOM
    symbol: str       # TradingView EXCHANGE:TICKER
    name: str         # display name
    group: str        # for grouping in the UI
    slot: int         # categorical colour slot, 1-8


# Real-time under the anonymous token: OANDA's spot metals and cash index quote
# continuously with no exchange delay, and Binance crypto is always real time.
REALTIME: tuple[Instrument, ...] = (
    Instrument("copper", "OANDA:XCUUSD",    "Copper",     "Metals",   1),
    Instrument("silver", "OANDA:XAGUSD",    "Silver",     "Metals",   2),
    Instrument("gold",   "OANDA:XAUUSD",    "Gold",       "Metals",   3),
    Instrument("btc",    "BINANCE:BTCUSDT", "Bitcoin",    "Crypto",   4),
    Instrument("eth",    "BINANCE:ETHUSDT", "Ethereum",   "Crypto",   5),
    Instrument("nq",     "OANDA:NAS100USD", "Nasdaq-100", "Index",    6),
    Instrument("spx",    "OANDA:SPX500USD", "S&P 500",    "Index",    7),
    Instrument("vix",    "TVC:VIX",         "VIX",        "Vol",      8),
)

# The exchange-traded futures contracts. Identical layout, so the dashboard and
# every colour slot carry over unchanged.
FUTURES: tuple[Instrument, ...] = (
    Instrument("copper", "COMEX:HG1!",      "Copper",     "Metals",   1),
    Instrument("silver", "COMEX:SI1!",      "Silver",     "Metals",   2),
    Instrument("gold",   "COMEX:GC1!",      "Gold",       "Metals",   3),
    Instrument("btc",    "BINANCE:BTCUSDT", "Bitcoin",    "Crypto",   4),
    Instrument("eth",    "BINANCE:ETHUSDT", "Ethereum",   "Crypto",   5),
    Instrument("nq",     "CME_MINI:NQ1!",   "Nasdaq-100", "Index",    6),
    Instrument("spx",    "CME_MINI:ES1!",   "S&P 500",    "Index",    7),
    # No free real-time VIX futures feed exists, so the cash index serves both
    # watchlists. It updates only during the US cash session.
    Instrument("vix",    "TVC:VIX",         "VIX",        "Vol",      8),
)

# Gold Kalshi research watchlist. These are TradingView's actual exchange-prefixed
# identifiers (not arbitrary renamings). Some yields only print sporadically;
# freshness must be checked from lp_time, never inferred from WebSocket activity.
GOLD: tuple[Instrument, ...] = (
    Instrument("dxy",    "TVC:DXY",       "Dollar Index", "FX/Index", 1),
    Instrument("eurusd", "FX:EURUSD",     "EUR/USD",      "FX",       2),
    Instrument("usdjpy", "FX:USDJPY",     "USD/JPY",      "FX",       3),
    Instrument("us10y",  "TVC:US10Y",     "US 10Y Yield", "Yield",    4),
    Instrument("gold",   "OANDA:XAUUSD",  "Gold Spot",    "Metals",   5),
)

WATCHLIST_NOTES = {
    "realtime": "spot metals and cash index - real time, no exchange delay",
    "futures": "COMEX/CME contracts - 10 min delayed without a data subscription",
    "gold": "five TradingView symbols for short-horizon gold research; check fresh/usable flags",
}

WATCHLIST = REALTIME       # default
WATCHLISTS = {"realtime": REALTIME, "futures": FUTURES, "gold": GOLD}

BY_SYMBOL = {i.symbol: i for i in WATCHLIST}
BY_KEY = {i.key: i for i in WATCHLIST}
SYMBOLS = [i.symbol for i in WATCHLIST]
