# oracle.py
"""Binance aggTrade WebSocket oracle — streams BTC, ETH, SOL prices."""

import asyncio
import json
import time

import websockets
from colorama import Fore, Style, init

from config import Config

# Initialize colorama
init(autoreset=True)

PING_INTERVAL_SECONDS = 20
STALE_THRESHOLD_SECONDS = 120
RECONNECT_DELAY_SECONDS = 3

_BINANCE_WS_URLS: dict[str, str] = {
    "btc/usd": "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
    "eth/usd": "wss://stream.binance.com:9443/ws/ethusdt@aggTrade",
    "sol/usd": "wss://stream.binance.com:9443/ws/solusdt@aggTrade",
}

LATEST_PRICES: dict[str, dict] = {
    "btc/usd": {"price": 0.0, "timestamp": 0, "open_price": 0.0},
    "eth/usd": {"price": 0.0, "timestamp": 0, "open_price": 0.0},
    "sol/usd": {"price": 0.0, "timestamp": 0, "open_price": 0.0},
}

_TRACKED = set(LATEST_PRICES.keys())

# Tracks which 15-min period each symbol's open_price belongs to
# Driven by Binance server timestamps (ms) — never local system time
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
# Period helpers
# ---------------------------------------------------------------------------

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
            f"{Fore.CYAN}[ORACLE] [{label}] New period — open_price reset to ${price:,.2f}"
        )

    # First price ever → set open_price
    if entry["open_price"] == 0.0:
        entry["open_price"] = price

    entry["price"] = price
    entry["timestamp"] = time.time()

    move_pct = get_price_move_pct(symbol)
    last_move = _last_printed_move.get(symbol, 0.0)
    if abs(move_pct - last_move) > _PRINT_MOVE_THRESHOLD:
        _last_printed_move[symbol] = move_pct
        sign = "+" if move_pct >= 0 else ""
        label = symbol.split("/")[0].upper()
        print(
            f"{Fore.YELLOW}[ORACLE] {label}: ${price:,.2f}  "
            f"| move: {sign}{move_pct * 100:.3f}% from open (open=${entry['open_price']:,.2f})"
        )


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

def _parse_message(symbol: str, raw: str) -> bool:
    """Parse a Binance aggTrade WS message and update price. Returns True if price extracted."""
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
    """Run a single-symbol connection with automatic reconnection."""
    label = symbol.split("/")[0].upper()
    while True:
        try:
            await _listen_symbol(symbol)
        except asyncio.TimeoutError:
            print(
                f"{Fore.YELLOW}[ORACLE] [{label}] No message in "
                f"{STALE_THRESHOLD_SECONDS}s — reconnecting..."
            )
        except (websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException,
                OSError) as exc:
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run_oracle() -> None:
    """Stream all 3 symbols concurrently via Binance aggTrade WebSocket."""
    await asyncio.gather(*[_run_symbol(sym) for sym in _TRACKED])


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_oracle())
