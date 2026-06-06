# orderbook_cache.py
"""Polymarket CLOB in-memory order book cache via WebSocket + REST snapshot.

Network resilience layer:
  • WS 1001 "Going Away" interceptor with exponential backoff (1→2→4→8→60s cap).
  • Active keep-alive heartbeat: ping sent every 15s; pong timeout triggers recycle.
  • Forced cache invalidation on any disconnect — prevents signal_engine from
    reading stale/"ghost" data while reconnecting.
"""

import asyncio
import json
import time
import random
from collections import defaultdict

import aiohttp
import websockets
import websockets.exceptions
from colorama import Fore, Style, init

from config import Config
from scanner import get_active_markets

init(autoreset=True)

CLOB_REST_URL = Config.POLYMARKET_HOST
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SNAPSHOT_TIMEOUT_SECONDS = 10

# ---------------------------------------------------------------------------
# Keep-alive / backoff constants
# ---------------------------------------------------------------------------
HEARTBEAT_INTERVAL_SECONDS: int = 15       # ping cadence
HEARTBEAT_PONG_TIMEOUT_SECONDS: int = 10   # max wait for pong before recycle
BACKOFF_BASE_SECONDS: float = 2.0          # first retry delay
BACKOFF_MAX_SECONDS: float = 60.0          # ceiling for exponential backoff

# ---------------------------------------------------------------------------
# In-memory order book cache:
#   _ORDERBOOKS[token_id] = {"bids": {price_str: size}, "asks": {price_str: size},
#                             "timestamp": float, "ready": bool}
# ---------------------------------------------------------------------------
_ORDERBOOKS: dict[str, dict] = defaultdict(lambda: {
    "bids": {},
    "asks": {},
    "timestamp": 0.0,
    "ready": False,
})

# Set of token IDs currently subscribed (managed by background worker)
_SUBSCRIBED_TOKENS: set[str] = set()

# Tracks whether the WebSocket is currently open and receiving data.
# Set True only after a successful connect + subscription; False on any close/error.
_WS_CONNECTED: bool = False


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def is_connected() -> bool:
    """Return True ONLY if the WebSocket is actively open and receiving data.

    Used by signal_engine as a strict gate: if this returns False, no signal
    should ever be generated — the cache may be a frozen REST snapshot.
    """
    return _WS_CONNECTED


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
        async with session.get(
            url, 
            params={"token_id": token_id},
            timeout=aiohttp.ClientTimeout(total=10.0)
        ) as resp:
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
# Cache invalidation hook  (Feature #3)
# ---------------------------------------------------------------------------

def _invalidate_cache() -> None:
    """Purge all cached order book data on disconnect.

    Sets every entry's bids/asks to empty dicts and marks ready=False so that
    get_orderbook() and validate_liquidity() return None/False immediately.
    Upstream components (signal_engine) are therefore hard-blocked from reading
    stale or ghost data during any reconnection window.

    This mutates the existing dict objects in-place rather than replacing
    _ORDERBOOKS itself, so there is no risk of a reference becoming orphaned.
    """
    count = len(_ORDERBOOKS)
    for entry in _ORDERBOOKS.values():
        entry["bids"] = {}
        entry["asks"] = {}
        entry["ready"] = False
        entry["timestamp"] = 0.0
    if count:
        print(
            f"{Fore.YELLOW}[ORDERBOOK] 🧹 Cache INVALIDATED — {count} token(s) "
            f"zeroed out. Signal engine blocked until reconnect + re-snapshot."
        )


# ---------------------------------------------------------------------------
# WebSocket workers
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


async def _heartbeat_loop(ws) -> None:
    """Active keep-alive: send a WebSocket ping every 15 seconds.  (Feature #2)

    Waits for the corresponding pong within HEARTBEAT_PONG_TIMEOUT_SECONDS.
    If the exchange does not acknowledge within that window, raises an
    asyncio.TimeoutError so that the outer gather() tears down all tasks and
    triggers the reconnection cycle.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        try:
            # ws.ping() returns an asyncio.Future that resolves to None when
            # the pong frame is received.  We must await the Future itself
            # (not pass the unawaited call) inside asyncio.wait_for so the
            # timeout fires correctly.  The resolved value is None — there is
            # no RTT float available from the websockets library.
            pong_waiter = ws.ping()
            await asyncio.wait_for(pong_waiter, timeout=HEARTBEAT_PONG_TIMEOUT_SECONDS)
            print(
                f"{Fore.CYAN}[ORDERBOOK] 💓 Heartbeat OK — pong received."
            )
        except asyncio.TimeoutError:
            print(
                f"{Fore.RED}[ORDERBOOK] 💔 Heartbeat TIMEOUT — "
                f"no pong within {HEARTBEAT_PONG_TIMEOUT_SECONDS}s. "
                f"Recycling connection..."
            )
            # Close the WebSocket to force a reconnect in the outer loop.
            await ws.close()
            raise  # propagate so gather() terminates all sibling tasks


async def _ws_worker() -> None:
    """Persistent WebSocket connection with auto-reconnect, heartbeat and dynamic subs.

    Reconnection schedule (exponential backoff, Feature #1):
        attempt 1 → 1s wait
        attempt 2 → 2s
        attempt 3 → 4s
        attempt 4 → 8s
        attempt 5+ → 60s (cap)

    Boot handshake sequence (aggressive pre-fetch fix):
        1. Poll scanner every second until at least one token is available.
        2. Seed ALL token snapshots from REST before opening the WebSocket.
        3. Connect WebSocket and subscribe to all tokens.
        4. Re-seed any tokens that the scanner added between step 1 and step 3.
        5. Mark _WS_CONNECTED = True — signal engine is now unblocked.
    """
    global _SUBSCRIBED_TOKENS, _WS_CONNECTED

    attempt: int = 0

    while True:
        # ── Step 1: Wait for scanner to publish at least one token ───────────
        # At process startup the scanner may not have completed its first REST
        # poll yet.  We loop here until there is something to subscribe to,
        # rather than connecting with an empty subscription list and relying on
        # the lazy _maintenance_loop (which only runs every 5 seconds) to pick
        # them up later.
        target_tokens: set[str] = set()
        _seed_wait_logged = False
        while not target_tokens:
            target_tokens = _token_ids_from_markets()
            if not target_tokens:
                if not _seed_wait_logged:
                    print(
                        f"{Fore.YELLOW}[ORDERBOOK] ⏳ Waiting for scanner to publish "
                        f"token IDs before seeding snapshots..."
                    )
                    _seed_wait_logged = True
                await asyncio.sleep(1)
        print(
            f"{Fore.GREEN}[ORDERBOOK] 🔍 Scanner ready — "
            f"{len(target_tokens)} token(s) found. Pre-seeding all snapshots..."
        )

        # ── Step 2: Seed ALL snapshots aggressively before WS connect ────────
        # Fires all REST snapshot fetches concurrently so the cache is fully
        # populated before the first signal evaluation can ever run.
        await _seed_snapshots(target_tokens)

        # ── Exponential backoff calculation ─────────────────
        reconnect_delay = min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * (2 ** attempt)) + random.uniform(0, 1)

        try:
            print(
                f"{Fore.GREEN}[ORDERBOOK] Connecting to Polymarket CLOB WS "
                f"(next backoff if fail: {reconnect_delay:.2f}s)..."
            )
            # Disable the built-in ping_interval so our explicit heartbeat loop
            # is the sole keep-alive mechanism — prevents double-ping conflicts.
            async with websockets.connect(
                CLOB_WS_URL,
                ping_interval=None,   # heartbeat managed by _heartbeat_loop
                ping_timeout=None,
            ) as ws:
                await asyncio.sleep(0.5)  # let handshake stabilize
                print(f"{Fore.GREEN}[ORDERBOOK] ✅ WS connected.")

                # ── Step 3: Subscribe to all known tokens ─────────────────────
                if target_tokens:
                    sub_msg = {"assets": list(target_tokens), "type": "market"}
                    await ws.send(json.dumps(sub_msg))
                    _SUBSCRIBED_TOKENS = set(target_tokens)
                    print(
                        f"{Fore.GREEN}[ORDERBOOK] Subscribed to "
                        f"{len(target_tokens)} token(s)."
                    )

                # ── Step 4: Re-seed any tokens added during WS handshake ──────
                # Between step 1 and step 3 the scanner may have added new
                # markets.  Catch them now so nothing is missed before the
                # _maintenance_loop takes over.
                fresh_tokens = _token_ids_from_markets() - _SUBSCRIBED_TOKENS
                if fresh_tokens:
                    print(
                        f"{Fore.GREEN}[ORDERBOOK] Post-connect re-seed: "
                        f"{len(fresh_tokens)} new token(s) found during handshake."
                    )
                    await _seed_snapshots(fresh_tokens)
                    extra_sub = {"assets": list(fresh_tokens), "type": "market"}
                    try:
                        await ws.send(json.dumps(extra_sub))
                        _SUBSCRIBED_TOKENS.update(fresh_tokens)
                    except Exception:
                        pass  # _maintenance_loop will retry in 5s

                # ── Step 5: Mark WS live — signal engine unblocked ────────────
                _WS_CONNECTED = True
                attempt = 0  # reset backoff on success

                # Run all three concurrent tasks under this connection.
                # Any one raising cancels the rest → falls through to reconnect.
                await asyncio.gather(
                    _ws_receive_loop(ws),
                    _maintenance_loop(ws),
                    _heartbeat_loop(ws),
                )

        # ── Feature #1: WS 1001 "Going Away" interceptor ────────────────────
        except websockets.exceptions.ConnectionClosedOK as exc:
            _WS_CONNECTED = False
            _invalidate_cache()  # Feature #3: hard-purge stale data immediately
            if exc.rcvd is not None and exc.rcvd.code == 1001:
                print(
                    f"{Fore.YELLOW}[ORDERBOOK] ⚡ WS 1001 'Going Away' — "
                    f"server evicted this connection (Cloudflare/load-balancer recycle). "
                    f"Reconnecting in {reconnect_delay:.0f}s with exponential backoff..."
                )
            else:
                print(
                    f"{Fore.YELLOW}[ORDERBOOK] WS closed cleanly (code="
                    f"{exc.rcvd.code if exc.rcvd else '?'}) — "
                    f"reconnecting in {reconnect_delay:.0f}s..."
                )

        except websockets.exceptions.ConnectionClosedError as exc:
            _WS_CONNECTED = False
            _invalidate_cache()  # Feature #3
            print(
                f"{Fore.RED}[ORDERBOOK] WS closed with error: {exc} "
                f"— reconnecting in {reconnect_delay:.0f}s..."
            )

        except websockets.exceptions.ConnectionClosed as exc:
            # Catch-all for any other ConnectionClosed subclass
            _WS_CONNECTED = False
            _invalidate_cache()  # Feature #3
            print(
                f"{Fore.YELLOW}[ORDERBOOK] WS connection closed: {exc} "
                f"— reconnecting in {reconnect_delay:.0f}s..."
            )

        except asyncio.TimeoutError:
            # Raised by _heartbeat_loop when pong is not received in time
            _WS_CONNECTED = False
            _invalidate_cache()  # Feature #3
            print(
                f"{Fore.RED}[ORDERBOOK] Heartbeat pong timeout — "
                f"recycling connection in {reconnect_delay:.0f}s..."
            )

        except Exception as exc:
            _WS_CONNECTED = False
            _invalidate_cache()  # Feature #3
            print(
                f"{Fore.RED}[ORDERBOOK] WS unexpected error: {exc} "
                f"— reconnecting in {reconnect_delay:.0f}s..."
            )

        # ── Exponential backoff: 1 → 2 → 4 → 8 → 60s (cap) ─────────────────
        reconnect_delay = min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * (2 ** attempt)) + random.uniform(0, 1)
        print(
            f"{Fore.YELLOW}[ORDERBOOK] ⏳ Waiting {reconnect_delay:.2f}s before reconnect (attempt {attempt})..."
        )
        await asyncio.sleep(reconnect_delay)
        attempt += 1


async def run_orderbook_cache() -> None:
    """Public coroutine: run the order book cache worker indefinitely."""
    await _ws_worker()
