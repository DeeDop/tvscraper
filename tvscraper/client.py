"""Connection handling: socket lifecycle, dispatch, and reconnect."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from typing import Any, Callable

import websocket

from . import protocol

log = logging.getLogger(__name__)

WS_URL = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart"
ORIGIN = "https://www.tradingview.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Free/anonymous access. Exchanges that mandate a subscription return delayed
# data under this token; crypto and FX are real time. See README for how to use
# your own account token instead.
ANONYMOUS_TOKEN = "unauthorized_user_token"

Handler = Callable[[list[Any]], None]


class TradingViewClient:
    """A single WebSocket connection, shared by any number of streams.

    Streams register handlers for protocol methods (``qsd``, ``du``, ...) and an
    ``on_connect`` hook that (re)builds their server-side session. The hook runs
    again after every reconnect, so subscriptions survive dropped connections.
    """

    def __init__(
        self,
        auth_token: str = ANONYMOUS_TOKEN,
        url: str = WS_URL,
        reconnect: bool = True,
        max_backoff: float = 60.0,
    ) -> None:
        self.auth_token = auth_token
        self.url = url
        self.reconnect = reconnect
        self.max_backoff = max_backoff

        self._ws: websocket.WebSocket | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._send_lock = threading.Lock()
        self._handlers: dict[str, list[Handler]] = defaultdict(list)
        self._on_connect: list[Callable[[], None]] = []

    # -- registration ----------------------------------------------------

    def on(self, method: str, handler: Handler) -> None:
        """Call *handler* with the param list of every ``method`` message."""
        self._handlers[method].append(handler)

    def on_connect(self, hook: Callable[[], None]) -> None:
        """Run *hook* after each successful connect, to (re)create sessions."""
        self._on_connect.append(hook)

    # -- lifecycle -------------------------------------------------------

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="tv-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:  # already tearing down
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)

    def wait_connected(self, timeout: float = 15.0) -> bool:
        return self._connected.wait(timeout)

    def send(self, func: str, params: list[Any]) -> None:
        self._send_raw(protocol.message(func, params))

    def _send_raw(self, framed: str) -> None:
        with self._send_lock:
            ws = self._ws
            if ws is None:
                raise ConnectionError("not connected")
            ws.send(framed)

    # -- internals -------------------------------------------------------

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                self._connect()
                backoff = 1.0  # a clean connect resets the penalty
                self._read_loop()
            except Exception as exc:
                if self._stop.is_set():
                    break
                log.warning("connection lost (%s: %s)", type(exc).__name__, exc)
            finally:
                self._connected.clear()
                self._ws = None

            if self._stop.is_set() or not self.reconnect:
                break
            log.info("reconnecting in %.0fs", backoff)
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, self.max_backoff)

    def _connect(self) -> None:
        log.info("connecting to %s", self.url)
        self._ws = websocket.create_connection(
            self.url,
            timeout=30,
            origin=ORIGIN,
            header=[f"User-Agent: {USER_AGENT}"],
            suppress_origin=False,
        )
        self.send("set_auth_token", [self.auth_token])
        for hook in self._on_connect:
            hook()
        self._connected.set()
        log.info("connected")

    def _read_loop(self) -> None:
        buffer = b""
        while not self._stop.is_set():
            try:
                data = self._ws.recv()  # type: ignore[union-attr]
            except websocket.WebSocketTimeoutException:
                continue  # idle socket; heartbeats will resume
            if data == "":
                raise ConnectionError("server closed the connection")
            buffer += data.encode("utf-8") if isinstance(data, str) else data
            payloads, buffer = protocol.parse_frames(buffer)
            for payload in payloads:
                self._dispatch(payload)

    def _dispatch(self, payload: str) -> None:
        if protocol.is_heartbeat(payload):
            self._send_raw(protocol.frame(payload))  # echo to stay alive
            return
        try:
            msg = json.loads(payload)
        except ValueError:
            log.debug("non-JSON payload: %.120s", payload)
            return

        method = msg.get("m")
        if method is None:
            return  # session_id / protocol handshake blobs
        if method in ("critical_error", "protocol_error"):
            log.error("server error: %s", msg.get("p"))
            return
        params = msg.get("p", [])
        for handler in self._handlers.get(method, ()):
            try:
                handler(params)
            except Exception:
                log.exception("handler for %r failed", method)
