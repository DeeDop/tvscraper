"""Output targets: live console table, CSV, and JSONL."""

from __future__ import annotations

import csv
import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def enable_ansi() -> None:
    """Turn on virtual-terminal processing so escape codes work on Windows."""
    if os.name == "nt":
        os.system("")


class Sink:
    def write(self, row: dict[str, Any]) -> None: ...
    def close(self) -> None: ...


class CsvSink(Sink):
    """Append rows to a CSV, writing the header on first use.

    Flushes every row so the file is tailable while the scraper runs.
    """

    def __init__(self, path: str | Path, columns: Iterable[str]) -> None:
        self.path = Path(path)
        self.columns = list(columns)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not self.path.exists() or self.path.stat().st_size == 0
        self._fh = self.path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=self.columns, extrasaction="ignore")
        self._lock = threading.Lock()
        if is_new:
            self._writer.writeheader()
            self._fh.flush()

    def write(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._writer.writerow(row)
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            self._fh.close()


class JsonlSink(Sink):
    """One JSON object per line — convenient for replay and pandas."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._fh.write(json.dumps(row, default=str) + "\n")
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            self._fh.close()


class LineSink(Sink):
    """Plain scrolling output — one line per update."""

    def write(self, row: dict[str, Any]) -> None:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        body = "  ".join(f"{k}={_fmt(v)}" for k, v in row.items() if v not in (None, ""))
        print(f"[{stamp}] {body}", flush=True)


class TableSink(Sink):
    """In-place quote table, redrawn on each update.

    Keeps one row per symbol and repaints by rewinding the cursor, so a long
    watchlist stays readable instead of scrolling away.
    """

    COLUMNS = [
        ("symbol", 18, "<"),
        ("last", 14, ">"),
        ("change", 12, ">"),
        ("change_pct", 9, ">"),
        ("bid", 14, ">"),
        ("ask", 14, ">"),
        ("volume", 16, ">"),
        ("exchange_utc", 14, ">"),
    ]

    def __init__(self) -> None:
        enable_ansi()
        self._rows: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._painted = 0

    def write(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._rows[row["symbol"]] = row
            self._render()

    def _render(self) -> None:
        if self._painted:
            sys.stdout.write(f"\033[{self._painted}A")  # rewind to the top

        header = "  ".join(f"{name:{align}{width}}" for name, width, align in self.COLUMNS)
        lines = [f"\033[1m{header}\033[0m\033[K", "-" * len(header) + "\033[K"]
        for symbol in sorted(self._rows):
            row = self._rows[symbol]
            cells = []
            for name, width, align in self.COLUMNS:
                value = row.get(name)
                if name == "exchange_utc" and value:
                    value = str(value)[11:19]
                cells.append(f"{_fmt(value):{align}{width}}")
            line = "  ".join(cells)
            pct = row.get("change_pct")
            if isinstance(pct, (int, float)) and pct:
                line = f"\033[{32 if pct > 0 else 31}m{line}\033[0m"
            lines.append(line + "\033[K")

        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()
        self._painted = len(lines)

    def close(self) -> None:
        sys.stdout.write("\n")
        sys.stdout.flush()


class FanoutSink(Sink):
    """Send every row to several sinks (e.g. console plus CSV)."""

    def __init__(self, *sinks: Sink) -> None:
        self.sinks = [s for s in sinks if s is not None]

    def write(self, row: dict[str, Any]) -> None:
        for sink in self.sinks:
            sink.write(row)

    def close(self) -> None:
        for sink in self.sinks:
            sink.close()


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        if abs(value) >= 1_000_000:
            return f"{value:,.0f}"
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        return f"{value:.6g}"
    return str(value)
