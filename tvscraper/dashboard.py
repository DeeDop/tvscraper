"""A local dashboard server for watching and verifying the live feed.

Runs the quote and bar streams, folds them into a :class:`MarketState`, and
serves a single page that renders from it. Updates reach the browser over
Server-Sent Events on two cadences:

``tick`` (1s)
    Quotes, verification checks, and counters. A few KB, so it can be frequent.
``full`` (5s)
    Adds the bar arrays, the indexed series, and the correlation matrix, which
    together are ~50x larger and only need to move at bar speed.

Standard library only - no web framework, no CDN, nothing to install.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .analytics import MarketState
from .bars import BarStream
from .client import TradingViewClient
from .instruments import WATCHLIST, Instrument
from .quotes import QuoteStream

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
TICK_INTERVAL = 1.0
FULL_INTERVAL = 5.0


class DashboardHandler(BaseHTTPRequestHandler):
    """Routes: ``/`` the page, ``/api/snapshot`` one-shot JSON, ``/api/stream`` SSE."""

    protocol_version = "HTTP/1.1"
    server_version = "tvscraper"

    def __init__(self, state: MarketState, stop: threading.Event, *args: Any, **kwargs: Any) -> None:
        self.state = state
        self.stop = stop
        super().__init__(*args, **kwargs)

    def log_message(self, fmt: str, *args: Any) -> None:
        log.debug("%s - %s", self.address_string(), fmt % args)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's naming
        route = self.path.split("?", 1)[0]
        if route in ("/", "/index.html"):
            self._serve_file(STATIC_DIR / "dashboard.html", "text/html; charset=utf-8")
        elif route == "/api/snapshot":
            self._serve_json(self.state.snapshot(with_series=True))
        elif route == "/api/stream":
            self._serve_stream()
        else:
            self.send_error(404, "not found")

    # -- responses -------------------------------------------------------

    def _serve_file(self, path: Path, content_type: str) -> None:
        try:
            body = path.read_bytes()
        except OSError:
            self.send_error(404, f"missing {path.name}")
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_stream(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        last_full = 0.0
        try:
            while not self.stop.is_set():
                now = time.monotonic()
                if now - last_full >= FULL_INTERVAL:
                    self._emit("full", self.state.snapshot(with_series=True))
                    last_full = now
                else:
                    self._emit("tick", self.state.snapshot(with_series=False))
                if self.stop.wait(TICK_INTERVAL):
                    break
        except (BrokenPipeError, ConnectionResetError, OSError):
            log.debug("stream client disconnected")

    def _emit(self, event: str, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, default=str)
        self.wfile.write(f"event: {event}\ndata: {body}\n\n".encode("utf-8"))
        self.wfile.flush()


def run_dashboard(
    host: str = "127.0.0.1",
    port: int = 8787,
    instruments: tuple[Instrument, ...] = WATCHLIST,
    resolution: str = "1",
    history: int = 360,
    token: str | None = None,
    open_browser: bool = True,
) -> None:
    """Start the streams and serve the dashboard until interrupted."""
    resolution_seconds = _resolution_seconds(resolution)
    state = MarketState(
        instruments,
        window_minutes=max(60, history) * resolution_seconds // 60,
        resolution_seconds=resolution_seconds,
    )

    client = TradingViewClient(auth_token=token) if token else TradingViewClient()
    symbols = [i.symbol for i in instruments]
    QuoteStream(client, symbols, on_update=state.on_quote)
    BarStream(client, symbols, resolution=resolution, history=history, on_bar=state.on_bar)

    stop = threading.Event()
    handler = partial(DashboardHandler, state, stop)
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True

    client.start()
    if not client.wait_connected(timeout=20):
        server.server_close()
        raise ConnectionError("could not connect to TradingView")

    url = f"http://{host}:{port}/"
    print(f"dashboard  {url}")
    print(f"streaming  {', '.join(symbols)}")
    print("press Ctrl+C to stop")

    if open_browser:
        import webbrowser

        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        client.stop()


def _resolution_seconds(resolution: str) -> int:
    if resolution.isdigit():
        return int(resolution) * 60
    if resolution.endswith("S") and resolution[:-1].isdigit():
        return int(resolution[:-1])
    return {"1D": 86_400, "1W": 604_800, "1M": 2_592_000}.get(resolution, 60)
