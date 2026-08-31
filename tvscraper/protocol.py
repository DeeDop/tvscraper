"""Wire protocol for TradingView's socket.io-flavoured WebSocket feed.

Messages are length-prefixed frames::

    ~m~{byte_length}~m~{payload}

The payload is either a JSON object ``{"m": <method>, "p": [<params>]}`` or a
heartbeat token ``~h~{n}`` which must be echoed back verbatim to keep the
connection alive.
"""

from __future__ import annotations

import json
import random
import re
import string
from typing import Any, Iterable

_HEADER_RE = re.compile(rb"~m~(\d+)~m~")
_HEARTBEAT_RE = re.compile(r"^~h~\d+$")


def frame(payload: str) -> str:
    """Wrap a payload in the ``~m~len~m~`` header (length is in bytes)."""
    return f"~m~{len(payload.encode('utf-8'))}~m~{payload}"


def message(func: str, params: Iterable[Any]) -> str:
    """Build a framed protocol call, e.g. ``quote_add_symbols``."""
    body = json.dumps({"m": func, "p": list(params)}, separators=(",", ":"))
    return frame(body)


def parse_frames(buffer: bytes) -> tuple[list[str], bytes]:
    """Split a byte buffer into complete payloads plus the trailing remainder.

    A single WebSocket message often carries several frames, and a frame can
    straddle two messages, so the caller keeps the remainder and feeds it back.
    """
    payloads: list[str] = []
    pos = 0
    while True:
        header = _HEADER_RE.match(buffer, pos)
        if header is None:
            break
        end = header.end() + int(header.group(1))
        if end > len(buffer):
            break  # frame is still in flight
        payloads.append(buffer[header.end():end].decode("utf-8", "replace"))
        pos = end
    return payloads, buffer[pos:]


def is_heartbeat(payload: str) -> bool:
    return bool(_HEARTBEAT_RE.match(payload))


def session_id(prefix: str) -> str:
    """TradingView expects session names like ``qs_a1b2c3d4e5f6``."""
    tail = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"{prefix}_{tail}"


def symbol_spec(symbol: str, adjustment: str = "splits", session: str | None = None) -> str:
    """Serialise the ``={"symbol": ...}`` blob used by ``resolve_symbol``."""
    spec: dict[str, str] = {"symbol": symbol, "adjustment": adjustment}
    if session:
        spec["session"] = session  # "regular" or "extended"
    return "=" + json.dumps(spec, separators=(",", ":"))
