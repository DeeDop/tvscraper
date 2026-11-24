"""The tracked instrument set.

``slot`` is the categorical palette slot (1-9) each instrument owns. Colour
follows the entity, so a slot is fixed here and never reassigned by rank or by
what happens to be visible - filtering the dashboard must not repaint the rest.

Three watchlists ship. The default is real time for anonymous users; the futures
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
    slot: int         # categorical colour slot, 1-9


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
    # Existing five symbols.
    Instrument("dxy",    "TVC:DXY",       "Dollar Index", "FX/Index", 1),
    Instrument("eurusd", "FX:EURUSD",     "EUR/USD",      "FX",       2),
    Instrument("usdjpy", "FX:USDJPY",     "USD/JPY",      "FX",       3),
    Instrument("us10y",  "TVC:US10Y",     "US 10Y Yield", "Yield",    4),
    Instrument("gold",   "OANDA:XAUUSD",  "Gold Spot",    "Metals",   5),
    # Only the proposed high / very-high relevance candidates; no medium/low.
    # "High" is a research priority, NOT a measured 15-minute predictive edge.
    Instrument("silver", "OANDA:XAGUSD",  "Silver Spot",  "Metals",   6),
    Instrument("audusd", "FX:AUDUSD",     "AUD/USD",      "FX",       7),
    Instrument("us02y",  "TVC:US02Y",     "US 2Y Yield",  "Yield",    8),
    Instrument("us05y",  "TVC:US05Y",     "US 5Y Yield",  "Yield",    9),
)

# Complete 15-minute commodity and financial market research input list.
# Provider quotes are *proxies*; the Kalshi contracts settle against their
# published Pyth references. Slot colors repeat for this headless collection.
# Each symbol must pass the live freshness check before use.
MARKETS15: tuple[Instrument, ...] = (
    Instrument("gold",      "OANDA:XAUUSD",     "Gold Spot",         "Commodity",  1),
    Instrument("silver",    "OANDA:XAGUSD",     "Silver Spot",       "Commodity",  2),
    Instrument("platinum",  "OANDA:XPTUSD",     "Platinum Spot",     "Commodity",  3),
    Instrument("palladium", "OANDA:XPDUSD",     "Palladium Spot",    "Commodity",  4),
    Instrument("copper",    "OANDA:XCUUSD",     "Copper Spot",       "Commodity",  5),
    Instrument("wti",       "OANDA:WTICOUSD",   "WTI Oil CFD",       "Commodity",  6),
    Instrument("brent",     "OANDA:BCOUSD",     "Brent Oil CFD",     "Commodity",  7),
    Instrument("natgas",    "OANDA:NATGASUSD",  "Natural Gas CFD",   "Commodity",  8),
    Instrument("eurusd",    "FX:EURUSD",        "EUR/USD",           "Forex",      1),
    Instrument("gbpusd",    "FX:GBPUSD",        "GBP/USD",           "Forex",      2),
    Instrument("usdjpy",    "FX:USDJPY",        "USD/JPY",           "Forex",      3),
    Instrument("audusd",    "FX:AUDUSD",        "AUD/USD",           "Forex",      4),
    Instrument("usdcad",    "FX:USDCAD",        "USD/CAD",           "Forex",      5),
    Instrument("usdmxn",    "OANDA:USDMXN",     "USD/MXN",           "Forex",      6),
    Instrument("us02y",     "TVC:US02Y",        "US 2Y Yield",       "Rates",      1),
    Instrument("us05y",     "TVC:US05Y",        "US 5Y Yield",       "Rates",      2),
    Instrument("us10y",     "TVC:US10Y",        "US 10Y Yield",      "Rates",      3),
    Instrument("us30y",     "TVC:US30Y",        "US 30Y Yield",      "Rates",      4),
    Instrument("dxy",       "TVC:DXY",          "US Dollar Index",   "FX/Index",   5),
    Instrument("sp500",     "OANDA:SPX500USD",  "S&P 500 Cash CFD",  "Index",      6),
)

WATCHLIST_NOTES = {
    "realtime": "spot metals and cash index - real time, no exchange delay",
    "futures": "COMEX/CME contracts - 10 min delayed without a data subscription",
    "gold": "gold plus the high-priority silver, AUDUSD and 2Y/5Y yield candidates; check price age and usable flags",
    "markets15": "unified 15-minute commodities and financials price proxies; not Kalshi settlement",
}

WATCHLIST = REALTIME       # default
WATCHLISTS = {"realtime": REALTIME, "futures": FUTURES, "gold": GOLD, "markets15": MARKETS15}

BY_SYMBOL = {i.symbol: i for i in WATCHLIST}
BY_KEY = {i.key: i for i in WATCHLIST}
SYMBOLS = [i.symbol for i in WATCHLIST]
