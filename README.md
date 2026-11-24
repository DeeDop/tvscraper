# tvscraper

Near-real-time market data from TradingView, pulled from the same WebSocket feed
their charts use (`wss://data.tradingview.com`) rather than by scraping the
rendered page. Data is **pushed** on every tick — no polling, no browser, no
Selenium — so latency is a network hop rather than a refresh interval.

This started life as the live feed of a private intraday trading system, where
it paper-traded a book of strategies in real time before the system moved its
price series to a paid vendor. It is released standalone so that anyone who
wants a live market-data feed can have one for free: anonymous access streams
crypto, FX, spot metals, cash indices and the real VIX index in real time —
no account, no API key, no bill. One dependency (`websocket-client`),
standard library for everything else.

## Install

```bash
git clone https://github.com/Drew4311/tvscraper.git
cd tvscraper
pip install -r requirements.txt
```

Run the commands below from the repo root, or make the package importable
from anywhere with:

```bash
pip install -e .
```

which also installs a `tvscraper` console command equivalent to
`python -m tvscraper`.

## Quick start

Live quote table for a watchlist:

```bash
python -m tvscraper quotes BINANCE:BTCUSDT NASDAQ:AAPL FX:EURUSD
```

Stream 1-minute OHLCV bars and append them to a CSV:

```bash
python -m tvscraper bars BINANCE:BTCUSDT --resolution 1 --csv data/bars.csv
```

Record only completed bars (the usual choice for building a dataset):

```bash
python -m tvscraper bars NASDAQ:AAPL --resolution 5 --closed-only --csv data/aapl.csv
```

Stop with `Ctrl+C`, or pass `--duration 60` to run for a fixed number of seconds.

## Headless endpoint and Python library for a separate Gold bot

**This repository supplies market data; the separate Gold bot owns its
strategies, correlation measurements, probabilities, and any trading logic.**
The headless service does **not** open the dashboard, place orders, or require
an API account. It uses one anonymous TradingView WebSocket quote session.

After installing the project, run this in the tvscraper directory:

```powershell
.\.venv\Scripts\python.exe -m tvscraper serve --watchlist gold
```

By default the API listens **only on this computer** at
`127.0.0.1:8788` and exposes:

| Read-only endpoint | Purpose |
|---|---|
| `GET http://127.0.0.1:8788/api/quotes` | Latest validated JSON for all nine high-priority gold-research inputs |
| `GET http://127.0.0.1:8788/api/stream` | Server-Sent Events (`quotes`), pushed on updates, throttled to 4 messages/second |
| `GET http://127.0.0.1:8788/api/health` | Connection status and count of currently usable inputs |

Example bot-side consumer (standard library):

```python
import json
from urllib.request import urlopen

with urlopen("http://127.0.0.1:8788/api/quotes", timeout=2) as response:
    feed = json.load(response)

gold = feed["quotes"]["gold"]
if feed["connected"] and gold["usable_short_term"]:
    print(gold["last"], gold["exchange_utc"], gold["price_age_s"])
```

Each instrument is present even if it has no quote. The record includes
`symbol`, `last`, `exchange_utc`, `received_utc`, `price_age_s`,
`update_mode`, `status` and `usable_short_term`. **The receiving bot must
reject missing, stale, delayed, invalid, and disconnected records** rather than
treating them as neutral votes. A live connection alone is not enough.
`OANDA:XAUUSD` is a gold spot proxy, not Kalshi's settlement price.

The same feed can be used **directly as a Python library** without HTTP:

```python
from tvscraper import MarketDataFeed, GOLD

feed = MarketDataFeed(GOLD)
feed.start()
try:
    if not feed.wait_connected(20):
        raise RuntimeError("TradingView unavailable")
    result = feed.snapshot()
    print(result["quotes"]["silver"])
finally:
    feed.stop()
```

For the lowest integration latency, supply an `on_quote` callback to
`MarketDataFeed(GOLD, on_quote=...)`; it receives updates on the WebSocket
thread. Use the original exchange timestamp to distinguish a new print from
a quote-field update.

The service intentionally refuses binding to public interfaces because it is
**unauthenticated**. Avoid exposing it through a tunnel. The feed is an
unofficial TradingView interface; use of its market data is subject to
TradingView and exchange data licenses.

### Focused 15-minute Gold + USD currency price library

This headless endpoint supports **one Kalshi commodity target (GOLD) and six
USD currency targets**. High-priority gold context is retained for research,
but is *not* treated as another Kalshi betting target.

```powershell
.\.venv\Scripts\python.exe -m tvscraper serve --watchlist markets15
```

The 12 unique quote keys are:

| Type | Keys |
|---|---|
| Bet targets | `gold`, `eurusd`, `gbpusd`, `usdjpy`, `audusd`, `usdcad`, `usdmxn` |
| Gold context only | `silver`, `dxy`, `us02y`, `us05y`, `us10y` |

The `gold` preset remains separate with its nine high-priority Gold research
inputs. The `markets15` preset is a 12-symbol superset of those nine, adding
the other three currencies only. Commodities such as oil/platinum and unrelated
market targets are **not subscribed**. The independent BTC/crypto project is
unchanged.

Verify `/api/quotes` timestamps and `usable_short_term` before using any
symbol. In particular, Treasury quotes can be stale out of hours. These are
spot, CFD or reference-market proxies; **none replaces Kalshi's exact contract
settlement price**. The feed itself has no trading functionality.

## Gold research: original five plus high-priority correlation candidates

The `--watchlist gold` dashboard now includes the original five and **only**
the four additional tickers marked High or Very high in the proposed research
shortlist. The labels are *research hypotheses*, **not** verified predictive
correlations for 15-minute Kalshi markets.

| Input | TradingView subscription | Why included | Limitation |
|---|---|---|---|
| Gold spot | `OANDA:XAUUSD` | Reference gold return series | Spot/CFD proxy, not Kalshi settlement or COMEX |
| US Dollar Index | `TVC:DXY` | Often inverse macro relationship | Index; no executable book/volume |
| EUR/USD | `FX:EURUSD` | USD-related confirmation | No centrally reported spot-FX volume |
| USD/JPY | `FX:USDJPY` | USD and risk regime | Relationship varies by regime |
| US 10-year yield | `TVC:US10Y` | Rates confirmation | May be stale outside active hours |
| **Silver spot (very high)** | **`OANDA:XAGUSD`** | Closely related precious metal | Correlation is not a lead signal |
| **AUD/USD (high)** | **`FX:AUDUSD`** | Commodity currency / USD channel | USD exposure overlaps with EURUSD |
| **US 2-year yield (high)** | **`TVC:US02Y`** | Short-rate expectations | May be stale; nominal is not real yield |
| **US 5-year yield (high)** | **`TVC:US05Y`** | Medium-rate expectations | May be stale; overlaps with other yields |

The 2Y/5Y ticker names are candidate TradingView identifiers, **not yet live
verified with the anonymous feed**. The same applies to their short-term update
cadences. If the server rejects a symbol, `symbol_error` is logged; if a quote
is old, `usable_short_term=False`. That ticker should have **no live voting
weight** until its feed quality and predictive value are established.

From the project root, run the dashboard (live prices, freshness checks,
1-minute bars and historical return-correlation matrix):

```bash
python -m tvscraper dashboard --watchlist gold
```

Or inspect live updates in the terminal:

```bash
python -m tvscraper quotes TVC:DXY FX:EURUSD FX:USDJPY TVC:US10Y OANDA:XAUUSD OANDA:XAGUSD FX:AUDUSD TVC:US02Y TVC:US05Y
```

To collect timestamped quotes for later analysis:

```bash
python -m tvscraper quotes TVC:DXY FX:EURUSD FX:USDJPY TVC:US10Y OANDA:XAUUSD OANDA:XAGUSD FX:AUDUSD TVC:US02Y TVC:US05Y --plain --csv gold_market_ticks.csv
```

CSV output includes `price_age_s` (age of underlying printed price, not the
latest WS message), `last_price_received_utc` and `usable_short_term`.
The terminal table displays price age and eligibility. A false flag means **do
not use that instrument as a live short-term trading vote**, not that the
symbol is necessarily wrong. `TVC:` index/yield feeds do not supply valid
exchange-traded volume or executable bid/ask: those are exported as blank.

The dashboard's Freshness check uses `lp_time` and the locally aggregated
30-second candles use only fresh exchange-timestamped prints. An active socket
does **not** prove current market prices. The correlation matrix is based on
aligned 1-minute *returns*, not price levels. It describes historical
co-movement, **not** predictive lead/lag or demonstrated Kalshi edge. Avoid
assigning signal weights without forward validation.

Run the offline regression tests:

```bash
python -m unittest discover -s tests -v
```

This is an unofficial research feed, not an official TradingView API.
TradingView and market-data licensing terms apply, and anonymous live
availability is not guaranteed.

## Dashboard

```bash
python -m tvscraper dashboard
```

Opens `http://127.0.0.1:8787/` and streams copper, silver, gold, Bitcoin,
Ethereum, Nasdaq-100, S&P 500 and VIX into a live page — **real time, no exchange
delay**. Standard library only — no web framework and nothing loaded from a CDN.

| | |
|---|---|
| **Stat tiles** | Last price, session change, and a trend line per instrument. |
| **Extraction checks** | Six tests per instrument — the verification panel, below. |
| **Relative performance** | All instruments rebased to 100 at the window start, on a shared time base, so they compare on one axis. |
| **Return correlation** | Pearson correlation of log returns over bars each pair shares. |
| **Detail chart** | Full-width candlesticks with volume, VWAP/SMA overlays, crosshair, and a 21-field readout scoped to whatever is on screen. |
| **Table view** | Every plotted number as text. |

### The candlestick chart

Candles are **hollow when the bar closed up and filled when it closed down**, on
top of the usual green/red. That is not decoration: measured with the palette
validator, green against red is only ΔE 4.1 apart under deuteranopia — far below
the ΔE 8 floor — so colour alone would leave red-green colour-blind readers unable
to tell direction. The hollow/filled fill carries it independently, and it happens
to be the traditional candlestick form anyway.

Because that distinction is the accessible channel, the chart refuses to draw
candles it cannot draw honestly: below ~6px per bar the hollow interior stops
being visible, so it falls back to a line and says why. At the full-width card
that ceiling is around 190 bars — 3h of 1-minute bars draws, 6h does not.

Controls on the card:

| | |
|---|---|
| **Chart** | Candles or line. |
| **VWAP** | Volume-weighted average price, cumulative over the visible bars. Blank for feeds with no volume. |
| **SMA 20** | 20-bar simple moving average of closes. |
| **Session H/L** | Reference lines at the session high and low from the quote feed. |

Hovering gives a crosshair with a price chip on the axis and a full readout for
the bar under the pointer: open, high, low, close, change and %, range, volume,
and whether that bar is closed or still forming. The last price is marked with a
hairline and a chip on the price axis at all times.

### Timeframes

The **Bars** row offers 30s, 1m, 5m and 15m. Only two of those are fetched:

- **1m** is the live stream from TradingView.
- **30s** is aggregated locally from quote ticks (see below).
- **5m and 15m** are rolled up in the browser from the 1-minute bars — exact, and
  cheaper than opening 8 instruments × 4 timeframes = 32 extra chart sessions for
  data already arriving.

Combine with the **Window** row (5m–6h) and drag-zoom for the actual time filter.

### Zooming and drilling in

- **Drag across either chart** to zoom to that time range. Both charts and the
  readout follow the selection.
- **Double-click** a chart, or press **Reset zoom**, to go back.
- **Click a line or a legend entry** on the comparison chart to load that
  instrument into the detail chart below.

The readout under the detail chart recomputes for the visible range: bar count,
first/last time, open, high and low with the times they printed, close, change and
change %, range, volume, VWAP, live bid/ask and spread in bps, annualised realised
volatility, session high/low, and tick count.

Flags: `--port`, `--host`, `--watchlist`, `--resolution`, `--history`, `--no-browser`.

The **Bars** control sets the timeframe (30s, 1m, 5m, 15m); the
**Window** control sets how much history is shown (5m through 6h). Picking 30s
pulls the window in to 15m automatically, since 30-second candles cover minutes,
not hours.

The page updates over Server-Sent Events on two cadences — a ~4KB heartbeat every
second for prices and checks, and the full bar history every five seconds. The
JSON behind it is also available directly:

```bash
curl -s http://127.0.0.1:8787/api/snapshot
```

### Verifying the data is right

The dashboard's reason for existing is the **Extraction checks** panel. Each
instrument runs six tests:

| Check | What a failure would mean |
|---|---|
| **Feed** | Real-time vs exchange-delayed. Not a fault — disclosure (see below). |
| **Freshness** | Seconds since the last tick. Degrades rather than alarming, since a quiet feed is normal outside session hours. |
| **Quote vs bar** | **The load-bearing one.** Quotes and bars arrive over two *independent* server sessions. Their agreement, in basis points, is the strongest available evidence that both decode paths are correct. In practice this sits at 0.0 bps. |
| **Range sanity** | The last price must lie inside the session high–low it is reported against. |
| **Spread** | A crossed book (bid > ask) means fields are being misread. |
| **Bar continuity** | Whether bars are still landing, allowing for the feed's delay. Minutes with no trades are counted and reported as information — a thin contract legitimately prints no bar for a minute in which nothing traded. |

A useful independent sanity check is the correlation matrix. Measured live it
reproduces the relationships you would expect: S&P↔Nasdaq +0.79, BTC↔ETH +0.76,
gold↔silver +0.72, **VIX↔S&P −0.44** with VIX negative against everything, and
metals-vs-crypto near zero. That structure only appears if the whole pipeline is
decoding correctly.

### Why these symbols

| | Real time (default) | Futures (`--watchlist futures`) |
|---|---|---|
| Copper | `OANDA:XCUUSD` | `COMEX:HG1!` |
| Silver | `OANDA:XAGUSD` | `COMEX:SI1!` |
| Gold | `OANDA:XAUUSD` | `COMEX:GC1!` |
| Bitcoin | `BINANCE:BTCUSDT` | same |
| Ethereum | `BINANCE:ETHUSDT` | same |
| Nasdaq-100 | `OANDA:NAS100USD` | `CME_MINI:NQ1!` |
| S&P 500 | `OANDA:SPX500USD` | `CME_MINI:ES1!` |
| VIX | `TVC:VIX` | `TVC:VIX` |

The COMEX and CME contracts are **10-minute delayed** for anonymous users
(`delayed_streaming_600`) — exchange entitlement, not a scraper limitation. The
default watchlist therefore uses OANDA's spot metals and cash index instead, which
stream with no delay and track the same underlying.

The trade-off is real and worth knowing: spot is not the futures contract. Gold
spot runs tens of dollars below front-month `GC1!` (the basis — carry and time to
expiry), and `NAS100USD` is the cash index rather than the NQ contract. Direction
and percentage moves track closely; absolute levels do not match. If you need the
exact contract price, use `--watchlist futures` and accept the delay, or supply
`TV_AUTH_TOKEN` from an account entitled to CME/COMEX data to get those contracts
live.

### VIX is different from the rest

`TVC:VIX` is the real VIX index and streams in real time — `CBOE:VIX` is 15-minute
delayed, and the CFD versions track VIX *futures*, which sit well away from spot
(18.0 against 15.2 when checked). The trade-off is that the index only updates
during the US cash session, and being an index it has **no volume and no order
book**. Outside 09:30–16:15 ET you will therefore see:

- an empty volume subplot and `—` for volume, VWAP, bid, ask and spread;
- a **Bar continuity** warning giving the age of the newest bar;
- VIX dropped from the comparison chart, with a note naming it, because its last
  bars predate the window. It stays in the tiles and the detail chart.

These are accurate reports of a closed session, not extraction faults. There is no
free real-time VIX futures feed, so `--watchlist futures` uses the same symbol.

### 30-second bars

TradingView refuses sub-minute bars to unentitled accounts — a `30S` series comes
back `seconds_not_entitled`. The 30s candles in the dashboard are therefore
aggregated locally in [analytics.py](tvscraper/analytics.py) from the real-time
quote ticks, bucketed on wall-clock 30-second boundaries with volume taken as the
increment in the session's cumulative figure.

The consequence is that **30s candles have no history to backfill**: they start
empty and fill in as the process runs, about 30 per instrument per 15 minutes. The
1-minute view loads its history immediately, as before. The correlation matrix
always uses 1-minute bars regardless of the Bars setting, since it needs enough
history to be meaningful.

## Symbols

Use TradingView's `EXCHANGE:TICKER` form — `BINANCE:BTCUSDT`, `NASDAQ:AAPL`,
`AMEX:SPY`, `FX:EURUSD`, `COMEX:GC1!`. If a symbol is wrong the server replies
with a `symbol_error`, which the scraper logs. The fastest way to find the exact
string is the symbol search on tradingview.com.

## Commands

Both subcommands take `--csv PATH`, `--jsonl PATH`, `--quiet`, and `--duration SEC`.

| | |
|---|---|
| `quotes SYMBOLS...` | Tick quotes: last, change, bid/ask, session OHLC, volume. `--plain` swaps the live table for scrolling lines (better when piping to a file). |
| `bars SYMBOLS...` | OHLCV bars. `--resolution` (`1`,`3`,`5`,`15`,`30`,`45`,`60`,`120`,`180`,`240`,`1D`,`1W`,`1M`), `--history N` bars replayed on connect, `--closed-only` to skip the forming bar, `--extended-hours` for pre/post market. |

## Data freshness

The `update_mode` column tells you what you're actually getting:

- `streaming` — real time. Crypto, FX, and most futures are real time for
  anonymous users, as are US equities via the consolidated tape.
- `delayed_streaming` — exchange-mandated delay (typically 15 minutes). Still
  pushed live, just offset.

Exchanges that require a paid data subscription only return real time to an
account entitled to them. To use your own entitlements, pass an auth token:

```bash
export TV_AUTH_TOKEN="..."      # or --token on the command line
```

You can read that token from a logged-in browser session on tradingview.com —
it is the string in the `set_auth_token` WebSocket frame (DevTools → Network →
WS), or `window.user.auth_token` in the console. It is a credential, so treat it
like a password and keep it out of version control. Without it the scraper
connects anonymously, which is what the examples above do.

## Bars: forming vs closed

Each bar is emitted repeatedly while it is open, then once more with
`closed=True` when the next bar begins:

```
time_utc=2026-08-18T00:15:00+00:00  close=64,465.81  closed=no    <- forming
time_utc=2026-08-18T00:15:00+00:00  close=64,470.74  closed=no    <- forming
time_utc=2026-08-18T00:15:00+00:00  close=64,470.74  closed=yes   <- final
time_utc=2026-08-18T00:16:00+00:00  close=64,470.75  closed=no    <- next bar
```

Filter on `closed` if you only want settled candles — a forming bar's high, low,
and close all still move. `--closed-only` does this for you.

## Use as a library

```python
from tvscraper import TradingViewClient, QuoteStream

client = TradingViewClient()
QuoteStream(client, ["BINANCE:BTCUSDT"], on_update=lambda q: print(q.symbol, q.last))
client.start()
client.wait_connected()
```

`client.start()` runs the socket on a background thread and your callback fires
per tick. See [examples/price_alert.py](examples/price_alert.py) for a threshold
alert that also writes to CSV. `BarStream` has the same shape with an `on_bar`
callback, and both can share one client — a single connection carries any number
of quote and chart sessions.

## Layout

| File | |
|---|---|
| [protocol.py](tvscraper/protocol.py) | Frame encoding/decoding (`~m~len~m~payload`), heartbeats, session ids |
| [client.py](tvscraper/client.py) | Socket lifecycle, message dispatch, reconnect with backoff |
| [quotes.py](tvscraper/quotes.py) | Quote session; merges partial updates into per-symbol snapshots |
| [bars.py](tvscraper/bars.py) | Chart session; history replay and forming/closed bar tracking |
| [sinks.py](tvscraper/sinks.py) | Live table, scrolling lines, CSV, JSONL, fan-out |
| [instruments.py](tvscraper/instruments.py) | The tracked watchlist and its fixed colour slots |
| [analytics.py](tvscraper/analytics.py) | Rolling state, derived series, the six checks |
| [dashboard.py](tvscraper/dashboard.py) | HTTP + SSE server |
| [static/dashboard.html](tvscraper/static/dashboard.html) | The page: charts, checks, table view |
| [cli.py](tvscraper/cli.py) | Argument parsing and wiring |

## Behaviour notes

- **Reconnects are automatic.** Dropped connections retry with exponential
  backoff up to 60s, and every stream rebuilds its server-side session on
  reconnect, so subscriptions survive. Bar history replays after a reconnect;
  already-seen bars are deduplicated, but if you are appending to CSV across a
  reconnect, deduplicate on `(symbol, time_utc)` when you load the file.
- **Partial updates are merged.** The server sends only changed fields, so
  `QuoteStream` keeps a running snapshot per symbol rather than passing the raw
  fragments through. Quotes are held back until a last price arrives.
- **CSV and JSONL are appended and flushed per row**, so you can tail them while
  the scraper runs.

## Fair use

This is an unofficial client for personal, non-commercial use. TradingView's
Terms of Service restrict automated access and prohibit redistributing their
market data, and exchange data carries its own licensing terms — so keep what
you collect to yourself. Subscribe to a sensible number of symbols and don't
hammer reconnects; the defaults here are deliberately gentle.
