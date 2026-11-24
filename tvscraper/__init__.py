"""Near-real-time market data from TradingView's chart WebSocket feed."""

from .analytics import MarketState
from .bars import Bar, BarStream
from .client import ANONYMOUS_TOKEN, TradingViewClient
from .feed import MarketDataFeed
from .instruments import GOLD, WATCHLIST, WATCHLISTS, Instrument
from .quotes import DEFAULT_FIELDS, Quote, QuoteStream
from .sinks import CsvSink, FanoutSink, JsonlSink, LineSink, TableSink

__all__ = [
    "Bar", "BarStream",
    "TradingViewClient", "ANONYMOUS_TOKEN", "MarketDataFeed",
    "Quote", "QuoteStream", "DEFAULT_FIELDS",
    "CsvSink", "JsonlSink", "LineSink", "TableSink", "FanoutSink",
    "MarketState", "Instrument", "WATCHLIST", "WATCHLISTS", "GOLD",
]
__version__ = "0.2.0"
