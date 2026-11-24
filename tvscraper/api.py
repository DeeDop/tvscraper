"""Loopback-only, read-only JSON/SSE endpoint for the headless quote library.

Start with: python -m tvscraper serve --watchlist gold
GET http://127.0.0.1:8788/api/quotes
GET http://127.0.0.1:8788/api/stream  (SSE "quotes" events, <= 4 Hz)
GET http://127.0.0.1:8788/api/health

No order execution or trade APIs; no accounts or app framework required.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .client import TradingViewClient
from .feed import MarketDataFeed
from .instruments import GOLD, Instrument

log = logging.getLogger(__name__)


class FeedHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "tvscraper-feed"

    def __init__(
        self,
        feed: MarketDataFeed,
        stop: threading.Event,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.feed = feed
        self.stop = stop
        super().__init__(*args, **kwargs)

    def log_message(self, fmt: str, *args: Any) -> None:
        log.debug("%s - %s", self.address_string(), fmt % args)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path in ("/api/quotes", "/"):
            self._json(self.feed.snapshot())
        elif path == "/api/health":
            snapshot = self.feed.snapshot()
            self._json({
                "connected": snapshot["connected"],
                "watchlist": snapshot["watchlist"],
                "sequence": snapshot["sequence"],
                "ready": sum(q["usable_short_term"] for q in snapshot["quotes"].values()),
                "total": len(snapshot["quotes"]),
            })
        elif path == "/api/stream":
            self._sse()
        else:
            self.send_error(404, "unknown endpoint")

    def _json(self, body: dict[str, Any]) -> None:
        data = json.dumps(body, separators=(",", ":"), allow_nan=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def _sse(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        try:
            snapshot = self.feed.snapshot()
            self._emit(snapshot)
            sequence = snapshot["sequence"]
            last_sent = time.monotonic()
            last_ping = last_sent
            while not self.stop.is_set():
                sequence_now = self.feed.wait_for_update(sequence, timeout=1.0)
                now = time.monotonic()
                if sequence_now != sequence and now - last_sent >= 0.25:
                    snapshot = self.feed.snapshot()
                    self._emit(snapshot)
                    sequence = snapshot["sequence"]
                    last_sent = now
                    last_ping = now
                elif sequence_now != sequence:
                    # High-throughput symbols need a short rate limit so one
                    # local consumer does not spin at CPU speed on every tick.
                    self.stop.wait(min(0.25, max(0.01, 0.25 - (now - last_sent))))
                elif now - last_ping >= 15.0:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    last_ping = now
        except (BrokenPipeError, ConnectionResetError, OSError):
            log.debug("SSE client disconnected")

    def _emit(self, snapshot: dict[str, Any]) -> None:
        payload = json.dumps(snapshot, separators=(",", ":"), allow_nan=False)
        self.wfile.write(f"event: quotes\ndata: {payload}\n\n".encode("utf-8"))
        self.wfile.flush()


def run_api(
    *,
    host: str = "127.0.0.1",
    port: int = 8788,
    instruments: tuple[Instrument, ...] = GOLD,
    name: str = "gold",
    token: str | None = None,
) -> None:
    """Run a headless price endpoint for other local processes.

    Binding is intentionally restricted to loopback: this unauthenticated
    research interface must not accidentally become public on the LAN.
    """
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("headless API has no authentication; use a loopback host")
    client = TradingViewClient(auth_token=token) if token else TradingViewClient()
    feed = MarketDataFeed(instruments, client=client, name=name)
    stop = threading.Event()
    server = ThreadingHTTPServer((host, port), partial(FeedHandler, feed, stop))
    server.daemon_threads = True
    try:
        feed.start()
        if not feed.wait_connected(20):
            raise ConnectionError("could not connect to TradingView")
        print(f"read-only quote API  http://{host}:{port}/api/quotes")
        print(f"live SSE updates     http://{host}:{port}/api/stream")
        print(f"health               http://{host}:{port}/api/health")
        print(f"watchlist {name}: {', '.join(i.symbol for i in instruments)}")
        print("press Ctrl+C to stop")
        try:
            server.serve_forever(poll_interval=0.25)
        except KeyboardInterrupt:
            print("\nstopping")
    finally:
        stop.set()
        server.server_close()
        feed.stop()
