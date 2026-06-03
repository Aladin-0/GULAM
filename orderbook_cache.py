# orderbook_cache.py
"""Polymarket CLOB in-memory order book cache via WebSocket + REST snapshot."""

import asyncio
import json
from collections import defaultdict

import aiohttp
import websockets
from colorama import Fore, Style, init

from config import Config
from scanner import get_active_markets

init(autoreset=True)

CLOB_REST_URL = Config.POLYMARKET_HOST
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RECONNECT_DELAY_SECONDS = 3
SNAPSHOT_TIMEOUT_SECONDS = 10

# In-memory order book cache:
#   _ORDERBOOKS[token_id] = {"bids": {price_str: size}, "asks": {price_str: size}, "timestamp": float}
_ORDERBOOKS: dict[str, dict] = defaultdict(lambda: {
    "bids": {},
    "asks": {},
    "timestamp": 0.0,
    "ready": False,
})

# Set of token IDs currently subscribed (managed by background worker)
_SUBSCRIBED_TOKENS: set[str] = set()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_orderbook(token_id: str) -> dict | None:
    """Return the cached order book for a token_id, or None if not tracked."""
    entry = _ORDERBOOKS.get(token_id)
    if entry is None or not entry.get("ready", False):
        return None
    return entry


def validate_liquidity(
    token_id: str, target_side: str, target_price: float, required_capital: float
) -> tuple[bool, float]:
    """Validate liquidity in sub-millisecond time from local RAM cache.

    Returns (is_liquid, available_value) where available_value is the
    total target_price * shares at or better than target_price.
    """
    entry = get_orderbook(token_id)
    if entry is None:
        return False, 0.0

    book_side = "asks" if target_side.upper() == "BUY" else "bids"
    levels = entry.get(book_side, {})
    if not levels:
        return False, 0.0

    available_value = 0.0
    if target_side.upper() == "BUY":
        # Buying: we need asks <= target_price
        for price_str, size in levels.items():
            price = float(price_str)
            if price <= target_price:
                available_value += price * size
    else:
        # Selling: we need bids >= target_price
        for price_str, size in levels.items():
            price = float(price_str)
            if price >= target_price:
                available_value += price * size

    return available_value >= required_capital, available_value


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _token_ids_from_markets() -> set[str]:
    """Collect all up/down token IDs from active markets."""
    tokens = set()
    for mkt in get_active_markets().values():
        tokens.add(mkt.get("up_token_id", ""))
        tokens.add(mkt.get("down_token_id", ""))
    return {t for t in tokens if t}


async def _fetch_snapshot(session: aiohttp.ClientSession, token_id: str) -> dict | None:
    """Fetch initial depth snapshot via REST."""
    url = f"{CLOB_REST_URL}/book"
    try:
        async with session.get(url, params={"token_id": token_id}) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data
    except Exception as exc:
        print(
            f"{Fore.RED}[ORDERBOOK] Snapshot fetch failed for "
            f"{token_id[:12]}...: {exc}"
        )
        return None


def _apply_snapshot(token_id: str, data: dict) -> None:
    """Seed the local cache with a REST snapshot."""
    entry = _ORDERBOOKS[token_id]
    bids: dict[str, float] = {}
    asks: dict[str, float] = {}
    for b in data.get("bids", []):
        try:
            price_str = str(b.get("price", ""))
            size = float(b.get("size", 0))
            if price_str and float(price_str) > 0 and size > 0:
                bids[price_str] = size
        except (ValueError, TypeError):
            continue
    for a in data.get("asks", []):
        try:
            price_str = str(a.get("price", ""))
            size = float(a.get("size", 0))
            if price_str and float(price_str) > 0 and size > 0:
                asks[price_str] = size
        except (ValueError, TypeError):
            continue

    entry["bids"] = bids
    entry["asks"] = asks
    entry["ready"] = True
    entry["timestamp"] = asyncio.get_event_loop().time()
    print(
        f"{Fore.CYAN}[ORDERBOOK] Snapshot seeded for "
        f"{token_id[:12]}...  "
        f"bids={len(bids)}  asks={len(asks)}"
    )


def _apply_delta(token_id: str, delta: dict) -> None:
    """Mutate local cache with a single delta packet."""
    entry = _ORDERBOOKS[token_id]
    side_key = "bids" if delta.get("side") == "B" else "asks"
    side = entry.get(side_key, {})

    try:
        price_str = str(delta.get("price", ""))
        size = float(delta.get("size", 0))
        price_val = float(price_str)
    except (ValueError, TypeError):
        return

    if not price_str or price_val <= 0:
        return

    if size == 0:
        side.pop(price_str, None)
    else:
        side[price_str] = size

    entry["timestamp"] = asyncio.get_event_loop().time()


async def _seed_snapshots(token_ids: set[str]) -> None:
    """Fetch REST snapshots for all token_ids before WS connection."""
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_snapshot(session, tid) for tid in token_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for tid, result in zip(token_ids, results):
            if isinstance(result, dict):
                _apply_snapshot(tid, result)
            elif isinstance(result, Exception):
                print(
                    f"{Fore.RED}[ORDERBOOK] Snapshot exception for "
                    f"{tid[:12]}...: {result}"
                )


# ---------------------------------------------------------------------------
# WebSocket worker
# ---------------------------------------------------------------------------

async def _maintenance_loop(ws) -> None:
    """Background loop inside open WS: dynamic subscriptions + stale cache cleanup."""
    while True:
        await asyncio.sleep(5)

        # 1. Dynamic subscriptions for newly-spawned tokens
        target_tokens = _token_ids_from_markets()
        new_tokens = target_tokens - _SUBSCRIBED_TOKENS
        if new_tokens:
            await _seed_snapshots(new_tokens)
            sub_msg = {"assets": list(new_tokens), "type": "market"}
            try:
                await ws.send(json.dumps(sub_msg))
                _SUBSCRIBED_TOKENS.update(new_tokens)
                print(
                    f"{Fore.GREEN}[ORDERBOOK] Dynamically subscribed to "
                    f"{len(new_tokens)} new token(s)."
                )
            except Exception as exc:
                print(
                    f"{Fore.RED}[ORDERBOOK] Dynamic subscribe failed: {exc}"
                )

        # 2. Prune stale cache entries no longer in active markets
        stale = set(_ORDERBOOKS.keys()) - target_tokens
        if stale:
            for tid in stale:
                _ORDERBOOKS.pop(tid, None)
                _SUBSCRIBED_TOKENS.discard(tid)
            print(
                f"{Fore.YELLOW}[ORDERBOOK] Pruned {len(stale)} stale token(s) "
                f"from cache."
            )


async def _ws_receive_loop(ws) -> None:
    """Read messages from the WebSocket and apply deltas."""
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        msg_type = msg.get("type", "").lower()
        if msg_type == "orderbook_snapshot":
            tid = msg.get("asset_id", msg.get("token_id", ""))
            if tid:
                _apply_snapshot(tid, msg.get("data", {}))
        elif msg_type == "orderbook_delta":
            tid = msg.get("asset_id", msg.get("token_id", ""))
            deltas = msg.get("changes", [])
            for d in deltas:
                _apply_delta(tid, d)
        elif msg_type == "error":
            print(
                f"{Fore.RED}[ORDERBOOK] WS error msg: {msg.get('msg', msg)}"
            )


async def _ws_worker() -> None:
    """Persistent WebSocket connection with auto-reconnect and dynamic subscriptions."""
    global _SUBSCRIBED_TOKENS

    while True:
        # Determine target token IDs from current active markets
        target_tokens = _token_ids_from_markets()
        if target_tokens:
            # Seed snapshots before connecting
            await _seed_snapshots(target_tokens)

        try:
            print(
                f"{Fore.GREEN}[ORDERBOOK] Connecting to Polymarket CLOB WS..."
            )
            async with websockets.connect(CLOB_WS_URL, ping_interval=10, ping_timeout=10) as ws:
                await asyncio.sleep(0.5)  # let handshake stabilize
                print(
                    f"{Fore.GREEN}[ORDERBOOK] WS connected."
                )

                # Subscribe to all token IDs
                if target_tokens:
                    sub_msg = {"assets": list(target_tokens), "type": "market"}
                    await ws.send(json.dumps(sub_msg))
                    _SUBSCRIBED_TOKENS = set(target_tokens)
                    print(
                        f"{Fore.GREEN}[ORDERBOOK] Subscribed to "
                        f"{len(target_tokens)} token(s)."
                    )

                await asyncio.gather(
                    _ws_receive_loop(ws),
                    _maintenance_loop(ws),
                )

        except websockets.exceptions.ConnectionClosed as exc:
            print(
                f"{Fore.YELLOW}[ORDERBOOK] WS closed: {exc} "
                f"— reconnecting in {RECONNECT_DELAY_SECONDS}s..."
            )
        except Exception as exc:
            print(
                f"{Fore.RED}[ORDERBOOK] WS error: {exc} "
                f"— reconnecting in {RECONNECT_DELAY_SECONDS}s..."
            )

        await asyncio.sleep(RECONNECT_DELAY_SECONDS)


async def run_orderbook_cache() -> None:
    """Public coroutine: run the order book cache worker indefinitely."""
    await _ws_worker()
