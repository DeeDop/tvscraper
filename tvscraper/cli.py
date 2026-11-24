"""Command line entry point: ``python -m tvscraper quotes|bars``."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading

from .bars import VALID_RESOLUTIONS, Bar, BarStream
from .client import ANONYMOUS_TOKEN, TradingViewClient
from .quotes import QuoteStream
from .sinks import CsvSink, FanoutSink, JsonlSink, LineSink, Sink, TableSink

QUOTE_COLUMNS = [
    "symbol", "received_utc", "exchange_utc", "last", "change", "change_pct",
    "volume", "bid", "ask", "open", "high", "low", "prev_close", "update_mode",
    "last_price_received_utc", "price_age_s", "usable_short_term",
]
BAR_COLUMNS = ["symbol", "resolution", "time_utc", "open", "high", "low", "close", "volume", "closed"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tvscraper",
        description="Stream near-real-time quotes and OHLCV bars from TradingView.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="log protocol detail")
    parser.add_argument(
        "--token",
        default=os.environ.get("TV_AUTH_TOKEN", ANONYMOUS_TOKEN),
        help="TradingView auth token (default: anonymous, or $TV_AUTH_TOKEN)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("symbols", nargs="+", help="EXCHANGE:TICKER, e.g. BINANCE:BTCUSDT NASDAQ:AAPL")
    common.add_argument("--csv", metavar="PATH", help="append rows to a CSV file")
    common.add_argument("--jsonl", metavar="PATH", help="append rows to a JSONL file")
    common.add_argument("--quiet", action="store_true", help="suppress console output")
    common.add_argument("--duration", type=float, metavar="SEC", help="stop after N seconds")

    quotes = sub.add_parser("quotes", parents=[common], help="stream tick quotes")
    quotes.add_argument("--plain", action="store_true", help="scrolling lines instead of a live table")

    bars = sub.add_parser("bars", parents=[common], help="stream OHLCV bars")
    bars.add_argument("--resolution", default="1", help=f"one of: {', '.join(sorted(VALID_RESOLUTIONS))}")
    bars.add_argument("--history", type=int, default=300, help="bars of history to replay on connect")
    bars.add_argument("--closed-only", action="store_true", help="emit only completed bars")
    bars.add_argument("--extended-hours", action="store_true", help="include pre/post market sessions")

    dash = sub.add_parser("dashboard", help="serve the live dashboard for the tracked watchlist")
    dash.add_argument("--host", default="127.0.0.1")
    dash.add_argument("--port", type=int, default=8787)
    dash.add_argument(
        "--watchlist", choices=["realtime", "futures", "gold"], default="realtime",
        help="realtime: spot metals + cash index (default); "
             "futures: delayed COMEX/CME; gold: five original inputs plus silver, AUDUSD, 2Y/5Y",
    )
    dash.add_argument("--resolution", default="1", help=f"bar size; one of: {', '.join(sorted(VALID_RESOLUTIONS))}")
    dash.add_argument("--history", type=int, default=360, help="bars of history to load on connect")
    dash.add_argument("--no-browser", action="store_true", help="do not open a browser window")

    serve = sub.add_parser("serve", help="headless read-only JSON/SSE endpoint for another program")
    serve.add_argument("--host", default="127.0.0.1", help="loopback only (default: 127.0.0.1)")
    serve.add_argument("--port", type=int, default=8788, help="HTTP API port (default: 8788)")
    serve.add_argument(
        "--watchlist", choices=["gold", "realtime", "futures"], default="gold",
        help="gold (default): original five plus only high-priority correlation candidates",
    )
    return parser


def _make_sink(args: argparse.Namespace, columns: list[str], console: Sink | None) -> Sink:
    sinks: list[Sink] = []
    if console is not None and not args.quiet:
        sinks.append(console)
    if args.csv:
        sinks.append(CsvSink(args.csv, columns))
    if args.jsonl:
        sinks.append(JsonlSink(args.jsonl))
    return FanoutSink(*sinks)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.command == "serve":
        from .api import run_api
        from .instruments import WATCHLISTS

        run_api(
            host=args.host,
            port=args.port,
            instruments=WATCHLISTS[args.watchlist],
            name=args.watchlist,
            token=args.token,
        )
        return 0

    if args.command == "dashboard":
        from .dashboard import run_dashboard
        from .instruments import WATCHLISTS

        run_dashboard(
            host=args.host,
            port=args.port,
            instruments=WATCHLISTS[args.watchlist],
            resolution=args.resolution,
            history=args.history,
            token=args.token,
            open_browser=not args.no_browser,
        )
        return 0

    client = TradingViewClient(auth_token=args.token)
    done = threading.Event()

    if args.command == "quotes":
        console = LineSink() if args.plain else TableSink()
        sink = _make_sink(args, QUOTE_COLUMNS, console)
        QuoteStream(client, args.symbols, on_update=lambda q: sink.write(q.as_row()))
    else:
        sink = _make_sink(args, BAR_COLUMNS, LineSink())

        def emit(bar: Bar) -> None:
            if args.closed_only and not bar.closed:
                return
            sink.write(bar.as_row())

        BarStream(
            client,
            args.symbols,
            resolution=args.resolution,
            history=args.history,
            on_bar=emit,
            include_forming=not args.closed_only,
            extended_hours=args.extended_hours,
        )

    signal.signal(signal.SIGINT, lambda *_: done.set())
    client.start()
    if not client.wait_connected(timeout=20):
        print("could not connect to TradingView", file=sys.stderr)
        client.stop()
        return 1

    try:
        done.wait(timeout=args.duration)
    finally:
        client.stop()
        sink.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
