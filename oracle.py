# oracle.py
"""Binance aggTrade WebSocket oracle — streams BTC, ETH, SOL prices.

Start-up sequence (per symbol):
  1. Calculate the exact open timestamp of the current 15-minute candle.
  2. Fetch that candle's true open price from the Binance REST Kline API.
  3. Seed LATEST_PRICES[symbol]["open_price"] with the real historical value.
  4. Open the aggTrade WebSocket and stream live ticks normally.

This eliminates the mid-epoch start bug where the bot would incorrectly treat
the very first live tick as the period open price, corrupting all C1 math.
"""

import asyncio
import json
import time

import aiohttp
import websockets
from colorama import Fore, Style, init

from config import Config

# Initialize colorama
init(autoreset=True)

PING_INTERVAL_SECONDS   = 20
STALE_THRESHOLD_SECONDS = 120
RECONNECT_DELAY_SECONDS = 3

# Binance REST endpoint for historical klines
_BINANCE_KLINE_URL = "https://api.binance.com/api/v3/klines"

# Max attempts to seed open_price from REST before falling back to first tick
_SEED_MAX_RETRIES = 3
_SEED_RETRY_DELAY = 2.0   # seconds between REST retries

_BINANCE_WS_URLS: dict[str, str] = {
    "btc/usd": "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
    "eth/usd": "wss://stream.binance.com:9443/ws/ethusdt@aggTrade",
    "sol/usd": "wss://stream.binance.com:9443/ws/solusdt@aggTrade",
}

# REST symbol mapping  (oracle key → Binance USDT pair)
_BINANCE_REST_SYMBOLS: dict[str, str] = {
    "btc/usd": "BTCUSDT",
    "eth/usd": "ETHUSDT",
    "sol/usd": "SOLUSDT",
}

LATEST_PRICES: dict[str, dict] = {
    "btc/usd": {"price": 0.0, "timestamp": 0, "open_price": 0.0},
    "eth/usd": {"price": 0.0, "timestamp": 0, "open_price": 0.0},
    "sol/usd": {"price": 0.0, "timestamp": 0, "open_price": 0.0},
}

_TRACKED = set(LATEST_PRICES.keys())

# Tracks which 15-min period each symbol's open_price belongs to.
# Driven by Binance server timestamps (ms) — never local system time.
_PERIOD_START: dict[str, int] = {sym: 0 for sym in _TRACKED}

# Throttle prints to avoid terminal flooding
_last_printed_move: dict[str, float] = {sym: 0.0 for sym in _TRACKED}
_PRINT_MOVE_THRESHOLD = 0.00005  # 0.005%

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_price_move_pct(symbol: str) -> float:
    """Return percentage move from open_price for the given symbol.

    Returns 0.0 if open_price is not yet set or symbol is unknown.
    """
    symbol = symbol.lower()
    entry = LATEST_PRICES.get(symbol)
    if entry is None or entry["open_price"] == 0.0:
        return 0.0
    return (entry["price"] - entry["open_price"]) / entry["open_price"]


# ---------------------------------------------------------------------------
# REST seed — fetch the true period-open price from Binance Kline API
# ---------------------------------------------------------------------------

def _current_period_open_ms() -> int:
    """Return the Unix timestamp (ms) of the start of the current 15-min candle.

    Example: called at 15:51:23 UTC → returns timestamp for 15:45:00.000 UTC.
    Uses local system UTC time only to compute the boundary; all subsequent
    period-change detection still uses Binance server event timestamps.
    """
    now_ms = int(time.time() * 1000)
    # Floor to the nearest 15-minute boundary (900 000 ms)
    return (now_ms // 900_000) * 900_000


async def _fetch_period_open_price(symbol: str) -> float:
    """Query Binance REST for the open price of the current 15-min candle.

    Returns the float open price on success, or 0.0 if all retries fail.

    Binance klines response layout (each candle is a list):
      [0]  Open time (ms)
      [1]  Open price  ← we want this
      [2]  High price
      [3]  Low price
      [4]  Close price
      ...
    """
    binance_sym = _BINANCE_REST_SYMBOLS.get(symbol)
    if not binance_sym:
        return 0.0

    open_time_ms = _current_period_open_ms()
    label = symbol.split("/")[0].upper()

    params = {
        "symbol":    binance_sym,
        "interval":  "15m",
        "startTime": open_time_ms,
        "endTime":   open_time_ms + 899_999,   # end just before the next boundary
        "limit":     1,
    }

    for attempt in range(1, _SEED_MAX_RETRIES + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    _BINANCE_KLINE_URL,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        print(
                            f"{Fore.YELLOW}[ORACLE] [{label}] Kline REST HTTP {resp.status} "
                            f"(attempt {attempt}/{_SEED_MAX_RETRIES})"
                        )
                        await asyncio.sleep(_SEED_RETRY_DELAY)
                        continue

                    data = await resp.json(content_type=None)

            if not isinstance(data, list) or not data:
                print(
                    f"{Fore.YELLOW}[ORACLE] [{label}] Kline REST returned empty list "
                    f"(attempt {attempt}/{_SEED_MAX_RETRIES})"
                )
                await asyncio.sleep(_SEED_RETRY_DELAY)
                continue

            candle = data[0]
            open_price = float(candle[1])

            if open_price <= 0.0:
                print(
                    f"{Fore.YELLOW}[ORACLE] [{label}] Kline REST returned open_price=0 "
                    f"(attempt {attempt}/{_SEED_MAX_RETRIES})"
                )
                await asyncio.sleep(_SEED_RETRY_DELAY)
                continue

            print(
                f"{Fore.GREEN}[ORACLE] [{label}] ✓ Seeded period open_price "
                f"= ${open_price:,.4f}  "
                f"(candle open_time={candle[0]})"
            )
            return open_price

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            print(
                f"{Fore.YELLOW}[ORACLE] [{label}] Kline REST error: "
                f"{type(exc).__name__} (attempt {attempt}/{_SEED_MAX_RETRIES})"
            )
            await asyncio.sleep(_SEED_RETRY_DELAY)
        except (ValueError, TypeError, IndexError) as exc:
            print(
                f"{Fore.YELLOW}[ORACLE] [{label}] Kline REST parse error: "
                f"{exc} (attempt {attempt}/{_SEED_MAX_RETRIES})"
            )
            await asyncio.sleep(_SEED_RETRY_DELAY)

    print(
        f"{Fore.RED}[ORACLE] [{label}] All {_SEED_MAX_RETRIES} REST seed attempts failed — "
        f"open_price will be set from first live tick (degraded accuracy)."
    )
    return 0.0


# ---------------------------------------------------------------------------
# Internal price update
# ---------------------------------------------------------------------------

def _update_price(symbol: str, price: float, event_time_ms: int) -> None:
    """Check for period reset using Binance server time, write price, and print status."""
    entry = LATEST_PRICES[symbol]

    # Detect period change using Binance event time (15-min boundary in seconds)
    period_start_s = (event_time_ms // 900_000) * 900
    if _PERIOD_START[symbol] != period_start_s:
        _PERIOD_START[symbol] = period_start_s
        entry["open_price"] = price
        label = symbol.split("/")[0].upper()
        print(
            f"{Fore.CYAN}[ORACLE] [{label}] New 15-min period — "
            f"open_price reset to ${price:,.4f}"
        )

    # Fallback: if REST seed failed and this is the first live tick ever,
    # use it as the open_price.  This path is a degraded fallback only.
    if entry["open_price"] == 0.0:
        entry["open_price"] = price
        label = symbol.split("/")[0].upper()
        print(
            f"{Fore.YELLOW}[ORACLE] [{label}] ⚠ REST seed unavailable — "
            f"open_price set from first live tick: ${price:,.4f}"
        )

    entry["price"] = price
    entry["timestamp"] = time.time()

    move_pct = get_price_move_pct(symbol)
    last_move = _last_printed_move.get(symbol, 0.0)
    if abs(move_pct - last_move) > _PRINT_MOVE_THRESHOLD:
        _last_printed_move[symbol] = move_pct
        sign = "+" if move_pct >= 0 else ""
        label = symbol.split("/")[0].upper()
        print(
            f"{Fore.YELLOW}[ORACLE] {label}: ${price:,.4f}  "
            f"| move: {sign}{move_pct * 100:.3f}%  "
            f"(open=${entry['open_price']:,.4f})"
        )


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

def _parse_message(symbol: str, raw: str) -> bool:
    """Parse a Binance aggTrade WS message and update price. Returns True if extracted."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return False

    if not isinstance(data, dict):
        return False

    try:
        latest_price = float(data["p"])
        # Binance server event time (ms) — never local system time
        event_time_ms = int(data.get("E", data.get("T", 0)))
    except (KeyError, ValueError, TypeError):
        return False

    if event_time_ms == 0:
        return False

    _update_price(symbol, latest_price, event_time_ms)
    return True


# ---------------------------------------------------------------------------
# Per-symbol WebSocket connection
# ---------------------------------------------------------------------------

async def _ping_loop(ws) -> None:
    """Send a standard WebSocket ping frame every 20s (Binance requirement)."""
    while True:
        await asyncio.sleep(PING_INTERVAL_SECONDS)
        try:
            await ws.ping()
        except websockets.exceptions.ConnectionClosed:
            break


async def _listen_symbol(symbol: str) -> None:
    """Connect and stream aggTrade prices for one symbol.

    Raises asyncio.TimeoutError if no message received within STALE_THRESHOLD_SECONDS.
    """
    label = symbol.split("/")[0].upper()
    url = _BINANCE_WS_URLS[symbol]

    print(f"{Fore.GREEN}[ORACLE] [{label}] Connecting to {url}")

    async with websockets.connect(
        url,
        ping_interval=None,
        ping_timeout=None,
    ) as ws:
        print(f"{Fore.GREEN}[ORACLE] [{label}] Connected")

        ping_task = asyncio.create_task(_ping_loop(ws))
        try:
            while True:
                message = await asyncio.wait_for(
                    ws.recv(), timeout=STALE_THRESHOLD_SECONDS
                )
                _parse_message(symbol, message)
        finally:
            ping_task.cancel()


async def _run_symbol(symbol: str) -> None:
    """Seed period open_price from REST, then run WebSocket with auto-reconnect."""
    label = symbol.split("/")[0].upper()

    # ── Step 1: Seed true period open_price from Binance REST Kline API ───────
    # This happens once per start-up (and after each WS reconnect in case the
    # bot has been offline long enough that a new period has started).
    open_price = await _fetch_period_open_price(symbol)
    if open_price > 0.0:
        # Pre-set the open_price before any WS tick arrives.
        # Also set the period boundary so _update_price() does not overwrite it
        # with the first live tick when the period start matches.
        LATEST_PRICES[symbol]["open_price"] = open_price
        open_time_ms = _current_period_open_ms()
        _PERIOD_START[symbol] = open_time_ms // 1000  # convert ms → s for comparison
        print(
            f"{Fore.GREEN}[ORACLE] [{label}] open_price seeded = ${open_price:,.4f}  "
            f"(period boundary = {open_time_ms})"
        )
    # ──────────────────────────────────────────────────────────────────────────

    # ── Step 2: WebSocket stream with auto-reconnect ──────────────────────────
    while True:
        try:
            await _listen_symbol(symbol)
        except asyncio.TimeoutError:
            print(
                f"{Fore.YELLOW}[ORACLE] [{label}] No message in "
                f"{STALE_THRESHOLD_SECONDS}s — reconnecting..."
            )
        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException,
            OSError,
        ) as exc:
            print(
                f"{Fore.RED}[ORACLE] [{label}] Disconnected: {exc} "
                f"— reconnecting in {RECONNECT_DELAY_SECONDS}s..."
            )
        except Exception as exc:  # pylint: disable=broad-except
            print(
                f"{Fore.RED}[ORACLE] [{label}] Error: {exc} "
                f"— reconnecting in {RECONNECT_DELAY_SECONDS}s..."
            )
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)

        # Re-seed on reconnect: the bot may have been offline long enough that
        # a new 15-min candle has started.  A stale open_price is worse than
        # a fresh one fetched here.
        open_price = await _fetch_period_open_price(symbol)
        if open_price > 0.0:
            LATEST_PRICES[symbol]["open_price"] = open_price
            open_time_ms = _current_period_open_ms()
            _PERIOD_START[symbol] = open_time_ms // 1000
            print(
                f"{Fore.GREEN}[ORACLE] [{label}] open_price re-seeded after reconnect "
                f"= ${open_price:,.4f}"
            )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run_oracle() -> None:
    """Seed all symbols from Binance REST, then stream via aggTrade WebSocket."""
    await asyncio.gather(*[_run_symbol(sym) for sym in _TRACKED])


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_oracle())
