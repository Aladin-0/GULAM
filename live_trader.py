# live_trader.py
"""Live execution layer — routes signals to the Polymarket CLOB on Polygon Mainnet.

Only active when PAPER_TRADING=False. Uses py-clob-client-v2 to construct,
sign (ECDSA via private key), and broadcast real limit orders.
"""

import asyncio
import time
from datetime import date

import aiohttp
from colorama import Fore, Style, init
from eth_account import Account
# V2 SDK — replaces archived py-clob-client
from py_clob_client_v2 import (
    ClobClient,
    ApiCreds,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
)

from config import Config
from signal_engine import SIGNAL_HISTORY
import orderbook_cache as clob_cache
# SQLite persistence — re-uses paper_trader's DB layer (separate logical namespace)
from paper_trader import (
    _save_position as _db_save_position,
    _delete_position as _db_delete_position,
    _persist_full_state as _db_persist_paper,  # wraps PAPER_STATE — we call selectively
    _load_all_positions,
    _get_db,
    _save_scalar,
    _load_scalar,
)

init(autoreset=True)

# ---------------------------------------------------------------------------
# CLOB client — constructed once at import time (credentials from Config)
# ---------------------------------------------------------------------------

def _build_clob_client() -> ClobClient:
    """Instantiate and return a V2-authenticated ClobClient.

    Reads two .env keys:
      POLYMARKET_SIG_TYPE     — 1=POLY_PROXY, 3=POLY_1271 (default: 3)
      POLYMARKET_PROXY_WALLET — deposit/proxy wallet address (funder)

    For Polymarket V2:
      - Funds deposited via website live in a deposit wallet smart contract.
      - signature_type=3 (POLY_1271): EOA signs; deposit contract verifies via
        EIP-1271 isValidSignature(). Required when maker = deposit wallet.
      - signature_type=1 (POLY_PROXY): for old V1-style proxy wallets.
    """
    import os as _os
    proxy_wallet: str = _os.getenv("POLYMARKET_PROXY_WALLET", "").strip()
    sig_type: int = int(_os.getenv("POLYMARKET_SIG_TYPE", "3"))

    sig_name = {1: "POLY_PROXY", 3: "POLY_1271"}.get(sig_type, str(sig_type))
    print(f"{Fore.CYAN}[LIVE] Signature type : {sig_type} ({sig_name})")

    if proxy_wallet:
        print(f"{Fore.CYAN}[LIVE] Deposit wallet : {proxy_wallet}")
    else:
        print(
            f"{Fore.YELLOW}[LIVE] ⚠️  POLYMARKET_PROXY_WALLET not set in .env.\n"
            f"         Open polygonscan.com/address/<your-EOA> in browser,\n"
            f"         find the deposit wallet contract, paste it in .env,\n"
            f"         then restart the bot."
        )

    creds = ApiCreds(
        api_key=Config.POLYMARKET_API_KEY,
        api_secret=Config.POLYMARKET_API_SECRET,
        api_passphrase=Config.POLYMARKET_API_PASSPHRASE,
    )
    client = ClobClient(
        host=Config.POLYMARKET_HOST,
        chain_id=Config.CHAIN_ID,
        key=Config.PRIVATE_KEY,
        creds=creds,
        signature_type=sig_type,
        funder=proxy_wallet if proxy_wallet else None,
    )
    return client


# Lazy singleton — initialised on first use so import-time failures are clear
_clob_client: ClobClient | None = None


def _get_client() -> ClobClient:
    global _clob_client
    if _clob_client is None:
        _clob_client = _build_clob_client()
    return _clob_client


# ---------------------------------------------------------------------------
# Live position state  (in-memory mirror; backed by SQLite via state_store)
# ---------------------------------------------------------------------------

LIVE_STATE: dict = {
    "capital": Config.INITIAL_CAPITAL,
    "available_capital": Config.INITIAL_CAPITAL,
    "total_profit": 0.0,
    "total_trades": 0,
    "winning_trades": 0,
    "losing_trades": 0,
    "loss_count": 0,
    "total_lost_usd": 0.0,
    "daily_profit": 0.0,
    "daily_trades": 0,
    "open_positions": {},   # condition_id → position dict
    "trade_history": [],
}

_last_processed_index: int = 0
_trading_halted: bool = False
_today: date = date.today()

MONITOR_INTERVAL_SECONDS = 5
SUMMARY_INTERVAL_SECONDS = 300

# ---------------------------------------------------------------------------
# State persistence — SQLite helpers imported above from paper_trader
# Live trader uses the same bot_state.db but writes under a distinct key prefix
# so paper and live sessions never corrupt each other's scalars.
# ---------------------------------------------------------------------------

_LIVE_PREFIX = "live_"   # prefix for all live-mode scalar keys in paper_state table


def _live_persist_state() -> None:
    """Persist all LIVE_STATE scalars to SQLite under the live_ prefix."""
    conn = _get_db()
    scalars = {
        f"{_LIVE_PREFIX}capital": LIVE_STATE["capital"],
        f"{_LIVE_PREFIX}available_capital": LIVE_STATE["available_capital"],
        f"{_LIVE_PREFIX}total_profit": LIVE_STATE["total_profit"],
        f"{_LIVE_PREFIX}total_trades": LIVE_STATE["total_trades"],
        f"{_LIVE_PREFIX}winning_trades": LIVE_STATE["winning_trades"],
        f"{_LIVE_PREFIX}losing_trades": LIVE_STATE["losing_trades"],
        f"{_LIVE_PREFIX}loss_count": LIVE_STATE["loss_count"],
        f"{_LIVE_PREFIX}total_lost_usd": LIVE_STATE["total_lost_usd"],
        f"{_LIVE_PREFIX}daily_profit": LIVE_STATE["daily_profit"],
        f"{_LIVE_PREFIX}daily_trades": LIVE_STATE["daily_trades"],
    }
    import json
    conn.executemany(
        "INSERT INTO paper_state(key, value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [(k, json.dumps(v)) for k, v in scalars.items()],
    )
    conn.commit()


def _live_load_state() -> None:
    """Hydrate LIVE_STATE scalars and open positions from SQLite on startup."""
    LIVE_STATE["capital"] = _load_scalar(f"{_LIVE_PREFIX}capital", Config.INITIAL_CAPITAL)
    LIVE_STATE["available_capital"] = _load_scalar(f"{_LIVE_PREFIX}available_capital", Config.INITIAL_CAPITAL)
    LIVE_STATE["total_profit"] = _load_scalar(f"{_LIVE_PREFIX}total_profit", 0.0)
    LIVE_STATE["total_trades"] = _load_scalar(f"{_LIVE_PREFIX}total_trades", 0)
    LIVE_STATE["winning_trades"] = _load_scalar(f"{_LIVE_PREFIX}winning_trades", 0)
    LIVE_STATE["losing_trades"] = _load_scalar(f"{_LIVE_PREFIX}losing_trades", 0)
    LIVE_STATE["loss_count"] = _load_scalar(f"{_LIVE_PREFIX}loss_count", 0)
    LIVE_STATE["total_lost_usd"] = _load_scalar(f"{_LIVE_PREFIX}total_lost_usd", 0.0)
    LIVE_STATE["daily_profit"] = _load_scalar(f"{_LIVE_PREFIX}daily_profit", 0.0)
    LIVE_STATE["daily_trades"] = _load_scalar(f"{_LIVE_PREFIX}daily_trades", 0)
    recovered = _load_all_positions()
    if recovered:
        LIVE_STATE["open_positions"].update(recovered)
        print(
            f"{Fore.YELLOW}[LIVE] Recovered {len(recovered)} open position(s) from DB — "
            f"verify these are still active on Polymarket before continuing."
        )


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_live_state() -> dict:
    return LIVE_STATE


def get_open_positions() -> dict:
    """Return the open positions dict (condition_id -> position) for the hedge loop."""
    return LIVE_STATE["open_positions"]


def get_live_capital() -> float:
    """Return current live capital (USD).  Called by signal_engine via callback."""
    return LIVE_STATE["capital"]


def get_performance_summary() -> dict:
    total = LIVE_STATE["total_trades"]
    win_rate = LIVE_STATE["winning_trades"] / total if total > 0 else 0.0
    total_return_pct = (
        LIVE_STATE["total_profit"] / Config.INITIAL_CAPITAL
        if Config.INITIAL_CAPITAL > 0 else 0.0
    )
    return {
        "capital": LIVE_STATE["capital"],
        "total_profit": LIVE_STATE["total_profit"],
        "total_return_pct": total_return_pct,
        "win_rate": win_rate,
        "total_trades": total,
        "daily_profit": LIVE_STATE["daily_profit"],
        "daily_trades": LIVE_STATE["daily_trades"],
        "loss_count": LIVE_STATE["loss_count"],
        "total_lost_usd": LIVE_STATE["total_lost_usd"],
        "open_positions": len(LIVE_STATE["open_positions"]),
    }


# ---------------------------------------------------------------------------
# Risk guards
# ---------------------------------------------------------------------------

def _maybe_daily_reset() -> None:
    global _today, _trading_halted
    today = date.today()
    if today != _today:
        _today = today
        LIVE_STATE["daily_profit"] = 0.0
        LIVE_STATE["daily_trades"] = 0
        _trading_halted = False
        print(f"{Fore.CYAN}[LIVE] Midnight reset — daily counters cleared.")


def _check_daily_loss_limit() -> bool:
    global _trading_halted
    limit = -(Config.INITIAL_CAPITAL * Config.DAILY_LOSS_LIMIT_PCT)
    if LIVE_STATE["daily_profit"] < limit:
        if not _trading_halted:
            _trading_halted = True
            print(
                f"\n{Fore.RED}{Style.BRIGHT}[LIVE] *** DAILY LOSS LIMIT HIT ***  "
                f"daily_profit={LIVE_STATE['daily_profit']:.4f}  "
                f"limit={limit:.4f}  Trading halted for the rest of the day."
            )
        return True
    return False


# ---------------------------------------------------------------------------
# Order construction & broadcast
# ---------------------------------------------------------------------------

def _compute_position_size() -> float:
    """Calculate USD position size from available equity."""
    total_equity = LIVE_STATE["available_capital"] + sum(
        p["cost"] for p in LIVE_STATE["open_positions"].values()
    )
    return total_equity * Config.MAX_POSITION_SIZE_PCT


async def _verify_order_filled(order_id: str, token_id: str, max_wait: float = 30.0) -> float:
    """
    Poll the CLOB to confirm an order was actually FILLED.

    Returns the filled size (shares) if confirmed filled, or 0.0 if the order
    was cancelled, expired, or still open after max_wait seconds.
    This is the KEY guard: we only record a position after Polymarket confirms the fill.
    """
    client = _get_client()
    deadline = time.time() + max_wait
    poll_interval = 3.0

    while time.time() < deadline:
        try:
            order_resp = await asyncio.wait_for(
                asyncio.to_thread(client.get_order, order_id),
                timeout=10.0,
            )
            if isinstance(order_resp, dict):
                status = order_resp.get("status", "").upper()
                size_matched = float(order_resp.get("size_matched", 0) or 0)
                if status in ("MATCHED", "FILLED") or size_matched > 0:
                    return size_matched
                if status in ("CANCELLED", "EXPIRED", "UNMATCHED"):
                    print(
                        f"{Fore.YELLOW}[LIVE] Order {order_id[:16]}... status={status} — NOT filled. "
                        f"No position recorded. Capital returned."
                    )
                    return 0.0
        except Exception as exc:  # pylint: disable=broad-except
            print(f"{Fore.YELLOW}[LIVE] Fill check error: {type(exc).__name__} — retrying...")

        await asyncio.sleep(poll_interval)

    print(
        f"{Fore.YELLOW}[LIVE] Order {order_id[:16]}... fill unconfirmed after {max_wait:.0f}s — "
        f"treating as NOT filled. Check Polymarket dashboard."
    )
    return 0.0


async def _place_order(signal: dict, size_usd: float) -> dict | None:
    """
    Construct, sign and POST a GTC limit order to the Polymarket CLOB.

    Returns the API response dict on success, or None on any failure.
    A None return means NO order was sent — capital is safe.

    Fix #2 — Aggressive Taker Execution:
      Instead of placing at the Gamma REST snapshot price (maker order that
      waits for a counterparty), we cross the spread by targeting the live
      best-ask from the CLOB WebSocket cache, then adding a small slippage
      buffer (+0.005) so our order sits above all resting asks and executes
      immediately as a taker.  The result is capped at MAX_TOKEN_PRICE.
    """
    token_id: str = signal["token_id"]

    # ── Live taker price (Fix #2) ─────────────────────────────────────────────
    _snapshot_price: float = signal["entry_price"]  # Gamma REST fallback
    _taker_price: float = _snapshot_price
    book = clob_cache.get_orderbook(token_id)
    if book is not None:
        asks = book.get("asks", {})
        if asks:
            try:
                best_ask = min(float(p) for p in asks)
                if 0.0 < best_ask < 1.0:
                    _taker_price = best_ask + 0.005  # cross the spread aggressively
            except (ValueError, TypeError):
                pass
    entry_price: float = round(
        min(_taker_price, Config.MAX_TOKEN_PRICE),
        4,
    )
    # ─────────────────────────────────────────────────────────────────────────

    shares: float = round(size_usd / entry_price, 4)

    if shares <= 0 or entry_price <= 0:
        print(f"{Fore.RED}[LIVE] Invalid order params: shares={shares} price={entry_price}")
        return None

    order_args = OrderArgs(
        token_id=token_id,
        price=entry_price,
        size=shares,
        side=Side.BUY,
    )

    try:
        client = _get_client()

        # V2: single call signs + broadcasts; 10-second timeout
        resp = await asyncio.wait_for(
            asyncio.to_thread(
                client.create_and_post_order,
                order_args,
                PartialCreateOrderOptions(tick_size="0.01"),
                OrderType.GTC,
            ),
            timeout=10.0,
        )

    except asyncio.TimeoutError:
        print(
            f"{Fore.RED}{Style.BRIGHT}[LIVE] BROADCAST TIMEOUT — "
            f"order for {signal['symbol'].upper()} {signal['side']} "
            f"did not confirm within 10s. Position state UNKNOWN. "
            f"Check Polymarket dashboard immediately."
        )
        return None

    except (ConnectionError, aiohttp.ClientError) as exc:
        # Log type only — aiohttp errors may embed request headers with API keys
        print(f"{Fore.RED}[LIVE] Network error during broadcast: {type(exc).__name__}")
        return None

    except Exception as exc:  # pylint: disable=broad-except
        # Expose full exception string to allow diagnosis of CLOB rejection reasons
        print(f"{Fore.RED}[LIVE] Order placement failed: {type(exc).__name__}: {exc}")
        return None

    # ---- Validate API response ----
    if not isinstance(resp, dict):
        safe_resp = str(resp)[:120] if resp is not None else "None"
        print(f"{Fore.RED}[LIVE] Unexpected response type from CLOB: {safe_resp}...")
        return None

    if not resp.get("success", False):
        error_msg = resp.get("errorMsg", resp.get("error", "unknown"))
        print(
            f"{Fore.RED}[LIVE] Order REJECTED by CLOB: {error_msg}  "
            f"(token={token_id[:12]}...  price={entry_price}  size={shares})"
        )
        if "insufficient" in str(error_msg).lower():
            print(
                f"{Fore.RED}[LIVE] Insufficient USDC balance or liquidity. "
                f"Check wallet balance and order-book depth."
            )
        return None

    order_id = resp.get("orderID", resp.get("order_id", "N/A"))
    print(
        f"{Fore.GREEN}{Style.BRIGHT}[LIVE] ✓ Order SUBMITTED — "
        f"orderID={order_id}  "
        f"token={token_id[:12]}...  "
        f"price={entry_price}  shares={shares:.4f}  "
        f"size_usd=${size_usd:.2f}  (awaiting fill confirmation...)"
    )
    return resp


# ---------------------------------------------------------------------------
# Position tracking
# ---------------------------------------------------------------------------

async def _open_live_position(signal: dict) -> None:
    """Attempt to place a live order and record the position ONLY after fill is confirmed."""
    condition_id = signal["condition_id"]

    if condition_id in LIVE_STATE["open_positions"]:
        return  # already in this market

    size_usd = _compute_position_size()
    if size_usd < Config.MIN_ORDER_SIZE_USD:
        print(
            f"{Fore.YELLOW}[LIVE] Position size ${size_usd:.2f} below minimum "
            f"${Config.MIN_ORDER_SIZE_USD:.2f} — skipping."
        )
        return

    entry_price: float = signal["entry_price"]
    if entry_price <= 0:
        return

    resp = await _place_order(signal, size_usd)
    if resp is None:
        return  # order failed — no state change

    order_id = resp.get("orderID", resp.get("order_id", ""))

    # ── FILL VERIFICATION (the critical fix) ─────────────────────────────────
    # Do NOT record a position until Polymarket confirms the order was matched.
    # This prevents the bot from counting unfilled orders as real positions.
    filled_shares = await _verify_order_filled(order_id, signal["token_id"], max_wait=60.0)
    if filled_shares <= 0:
        print(
            f"{Fore.RED}[LIVE] ✗ Order NOT FILLED — {signal['symbol'].upper()} {signal['side']} "
            f"skipped. No position recorded. Capital unchanged."
        )
        return
    # ─────────────────────────────────────────────────────────────────────────

    # Use actual filled shares (may differ from requested due to partial fills)
    shares = filled_shares
    cost = shares * entry_price
    clob_entry_fee = cost * Config.CLOB_FEE_PCT

    position = {
        "condition_id": condition_id,
        "question": signal["question"],
        "symbol": signal["symbol"],
        "side": signal["side"],
        "token_id": signal["token_id"],
        "slug": signal.get("slug", ""),
        "entry_price": entry_price,
        "price_to_beat": signal.get("price_to_beat", 0.0),
        "shares": shares,
        "cost": cost,
        "clob_entry_fee": clob_entry_fee,
        "entry_time": time.time(),
        "time_remaining": signal["time_remaining"],
        "order_id": order_id,
    }

    LIVE_STATE["open_positions"][condition_id] = position
    LIVE_STATE["available_capital"] -= cost + clob_entry_fee
    LIVE_STATE["capital"] = (
        LIVE_STATE["available_capital"]
        + sum(p["cost"] for p in LIVE_STATE["open_positions"].values())
    )

    # --- Persist to SQLite immediately ---
    _db_save_position(condition_id, position)
    _live_persist_state()

    print(
        f"{Fore.GREEN}[LIVE] ✅ Position CONFIRMED FILLED: "
        f"[{signal['symbol'].upper()}] {signal['side']}  "
        f"shares={shares:.4f}  entry={entry_price:.4f}  cost=${cost:.2f}  "
        f"clob_fee=${clob_entry_fee:.4f}  "
        f"avail=${LIVE_STATE['available_capital']:.2f}"
    )


async def execute_hedge_dump(condition_id: str) -> None:
    """Emergency liquidation: aggressively sell the entire live position into the orderbook.

    Phase 3 Escape Hatch — called by signal_engine when a baseline breach is detected.

    Execution model:
      1. Fetch the best available bid from the in-memory CLOB orderbook cache.
      2. Construct a GTC sell limit order at best_bid (aggressive taker price).
      3. Sign and broadcast the order via py-clob-client with a 10-second timeout.
      4. Call _record_settlement() with exact fee accounting regardless of broadcast
         status (conservatively marks as closed to prevent double-dump).

    Security: all exception messages suppressed (may embed signing key material).
    """
    position = LIVE_STATE["open_positions"].get(condition_id)
    if position is None:
        return  # Already closed or hedged

    sym = position.get("symbol", "?").upper()
    side = position.get("side", "?")
    token_id: str = position.get("token_id", "")
    entry_price: float = position.get("entry_price", 0.0)
    shares: float = position.get("shares", 0.0)

    if shares <= 0 or not token_id:
        print(f"{Fore.RED}[HEDGE] [LIVE] Invalid position state for {condition_id[:8]}... — skipping.")
        return

    # ── Determine exit price: best bid from CLOB cache (aggressive taker sell) ───
    exit_price: float = 0.0
    book = clob_cache.get_orderbook(token_id)
    if book:
        bids = book.get("bids", {})
        if bids:
            exit_price = max(float(p) for p in bids)

    if exit_price == 0.0:
        exit_price = max(0.0, entry_price * 0.99)
        print(
            f"{Fore.YELLOW}[HEDGE] [LIVE] No live book for {sym} {side} — "
            f"using conservative fallback exit: ${exit_price:.4f}"
        )

    # ── Fee math ─────────────────────────────────────────────────────────────────
    clob_exit_fee: float = (exit_price * shares) * Config.CLOB_FEE_PCT
    spread_loss: float = (entry_price - exit_price) * shares
    net_exit_cost: float = spread_loss + clob_exit_fee

    print(
        f"{Fore.RED}{Style.BRIGHT}"
        f"[HEDGE] 🚨 LIVE EMERGENCY DUMP: {sym} {side}\n"
        f"[HEDGE]   Entry price  : ${entry_price:.4f}\n"
        f"[HEDGE]   Best bid     : ${exit_price:.4f}  (aggressive taker fill)\n"
        f"[HEDGE]   Spread loss  : ${spread_loss:.4f}\n"
        f"[HEDGE]   CLOB fee     : ${clob_exit_fee:.4f}\n"
        f"[HEDGE]   Net exit cost: ${net_exit_cost:.4f}\n"
        f"[HEDGE]   Shares       : {shares:.4f}\n"
        f"[HEDGE]   Broadcasting SELL order to Polymarket CLOB..."
    )

    # ── Broadcast sell order ──────────────────────────────────────────────────────
    order_placed = False
    try:
        sell_args = OrderArgs(
            token_id=token_id,
            price=round(exit_price, 4),
            size=round(shares, 4),
            side=Side.SELL,
        )
        client = _get_client()
        # V2: single call signs + broadcasts
        resp = await asyncio.wait_for(
            asyncio.to_thread(
                client.create_and_post_order,
                sell_args,
                PartialCreateOrderOptions(tick_size="0.01"),
                OrderType.GTC,
            ),
            timeout=10.0,
        )
        if isinstance(resp, dict) and resp.get("success", False):
            order_id = resp.get("orderID", resp.get("order_id", "N/A"))
            print(
                f"{Fore.GREEN}[HEDGE] [LIVE] ✅ Sell order accepted — "
                f"orderID={order_id}  price={exit_price:.4f}  shares={shares:.4f}"
            )
            order_placed = True
        else:
            error_msg = resp.get("errorMsg", "unknown") if isinstance(resp, dict) else "?"
            print(
                f"{Fore.RED}[HEDGE] [LIVE] Sell order REJECTED by CLOB: {error_msg} — "
                f"recording as closed conservatively to prevent double-dump."
            )
    except asyncio.TimeoutError:
        print(
            f"{Fore.RED}[HEDGE] [LIVE] SELL BROADCAST TIMEOUT — "
            f"state unknown. Marking closed conservatively. "
            f"Verify on Polymarket dashboard immediately."
        )
    except Exception as exc:  # pylint: disable=broad-except
        print(
            f"{Fore.RED}[HEDGE] [LIVE] Sell order failed: {type(exc).__name__}: {exc}. "
            f"Marking position closed conservatively."
        )

    # ── Record settlement regardless of broadcast status ─────────────────────────
    # Conservative close: if order status is unknown, use exit_price to avoid
    # a double-sell if the order DID fill.  SQLite write happens here.
    _record_settlement(condition_id, exit_price, "hedge_dump")

    result_tag = "order confirmed" if order_placed else "conservative close"
    print(
        f"{Fore.GREEN}[HEDGE] [LIVE] ✅ Position {condition_id[:8]}... hedged ({result_tag}). "
        f"Capital: ${LIVE_STATE['capital']:.2f}"
    )


def _record_settlement(condition_id: str, exit_price: float, reason: str) -> None:
    """Record a settled position in LIVE_STATE (no on-chain action needed — market resolves)."""
    position = LIVE_STATE["open_positions"].pop(condition_id, None)
    if position is None:
        return

    shares = position["shares"]
    cost = position["cost"]
    gross_profit = (exit_price - position["entry_price"]) * shares

    # CLOB round-trip fee: entry fee already debited from capital on open;
    # we deduct exit fill fee here (applied on settlement proceeds by Polymarket).
    clob_entry_fee: float = position.get("clob_entry_fee", cost * Config.CLOB_FEE_PCT)
    clob_exit_fee: float = (exit_price * shares) * Config.CLOB_FEE_PCT
    round_trip_fee: float = clob_entry_fee + clob_exit_fee
    net_profit = gross_profit - clob_exit_fee   # entry fee already left capital on open
    pnl_pct = net_profit / cost if cost > 0 else 0.0
    is_win = net_profit >= 0

    # Return cost + gross to available capital; entry fee already out, exit fee debited now
    LIVE_STATE["available_capital"] += cost + gross_profit - clob_exit_fee
    LIVE_STATE["capital"] = (
        LIVE_STATE["available_capital"]
        + sum(p["cost"] for p in LIVE_STATE["open_positions"].values())
    )
    # ── P&L Accounting Identity ─────────────────────────────────────────────
    # Use explicit branches so the dashboard ALWAYS moves in the correct
    # direction: wins add a positive amount; losses subtract a positive amount.
    # This eliminates any sign-confusion in the += operator path.
    if is_win:
        LIVE_STATE["total_profit"] += net_profit          # net_profit > 0
        LIVE_STATE["daily_profit"] += net_profit          # net_profit > 0
    else:
        loss_magnitude = abs(net_profit)                  # always positive
        LIVE_STATE["total_profit"] -= loss_magnitude      # guaranteed decrease
        LIVE_STATE["daily_profit"] -= loss_magnitude      # guaranteed decrease
    # ─────────────────────────────────────────────────────────────────────────
    LIVE_STATE["total_trades"] += 1
    LIVE_STATE["daily_trades"] += 1

    if is_win:
        LIVE_STATE["winning_trades"] += 1
    else:
        LIVE_STATE["losing_trades"] += 1
        LIVE_STATE["loss_count"] += 1
        LIVE_STATE["total_lost_usd"] += abs(net_profit)

    trade_record = {
        "condition_id": condition_id,
        "question": position["question"],
        "symbol": position["symbol"],
        "side": position["side"],
        "entry_price": position["entry_price"],
        "exit_price": exit_price,
        "shares": shares,
        "cost": cost,
        "gross_profit": gross_profit,
        "clob_round_trip_fee": round_trip_fee,
        "profit": net_profit,
        "pnl_pct": pnl_pct,
        "reason": reason,
        "entry_time": position["entry_time"],
        "exit_time": time.time(),
        "order_id": position.get("order_id", ""),
    }
    LIVE_STATE["trade_history"].append(trade_record)
    if len(LIVE_STATE["trade_history"]) > 100:
        del LIVE_STATE["trade_history"][0]

    # --- Persist to SQLite immediately ---
    _db_delete_position(condition_id)
    _live_persist_state()

    color = Fore.GREEN if is_win else Fore.RED
    result = "WIN" if is_win else "LOSS"
    pnl_str = (
        f"profit=+${net_profit:.4f} (after ${round_trip_fee:.4f} round-trip CLOB fee)"
        if is_win
        else f"loss=${net_profit:.4f} (after ${round_trip_fee:.4f} round-trip CLOB fee)"
    )
    print(
        f"{color}{Style.BRIGHT}[LIVE] SETTLED {result}: "
        f"{position['symbol'].upper()} {position['side']} | "
        f"entry={position['entry_price']:.4f} → exit={exit_price:.2f} | "
        f"{pnl_str}  capital=${LIVE_STATE['capital']:.2f}"
    )


# ---------------------------------------------------------------------------
# Position monitor
# ---------------------------------------------------------------------------

async def _fetch_actual_payout(token_id: str, order_id: str, position: dict) -> float | None:
    """
    Determine the REAL outcome of a settled Polymarket position.

    Strategy (in order of reliability):
      1. Oracle price vs price_to_beat — fast, accurate once market ends.
      2. CLOB get_trades(TradeParams) — confirms via on-chain trade records.
      3. CLOB get_order(order_id)    — last resort status check.

    Returns 1.0 (win), 0.0 (loss), or None (not yet settled — defer).
    """
    from oracle import LATEST_PRICES

    # ── Method 1: Oracle price vs price_to_beat (most reliable) ──────────────
    # Once the 15-min market closes, oracle IS the settlement price.
    oracle_key = f"{position['symbol'].lower()}/usd"
    oracle_entry = LATEST_PRICES.get(oracle_key, {})
    oracle_price: float = oracle_entry.get("price", 0.0)
    ptb: float = position.get("price_to_beat", 0.0)

    if oracle_price > 0 and ptb > 0:
        if position["side"] == "UP":
            is_win = oracle_price > ptb
        else:
            is_win = oracle_price < ptb
        result = 1.0 if is_win else 0.0
        print(
            f"{Fore.CYAN}[LIVE] Oracle settlement: {position['symbol'].upper()} "
            f"{position['side']} | price={oracle_price:.2f} ptb={ptb:.2f} "
            f"→ {'WIN ✅' if is_win else 'LOSS ❌'}"
        )
        return result

    # ── Method 2: CLOB get_trades with correct TradeParams ───────────────────
    client = _get_client()
    try:
        from py_clob_client_v2.clob_types import TradeParams
        params = TradeParams(asset_id=token_id)
        trades_resp = await asyncio.wait_for(
            asyncio.to_thread(client.get_trades, params),
            timeout=10.0,
        )
        if isinstance(trades_resp, list) and trades_resp:
            for trade in trades_resp:
                trade_type = str(trade.get("type", "")).upper()
                if trade_type in ("REDEMPTION", "SETTLEMENT", "REDEEM"):
                    price = float(trade.get("price", -1))
                    if price >= 0:
                        result = 1.0 if price >= 0.99 else 0.0
                        print(
                            f"{Fore.CYAN}[LIVE] Trade API: type={trade_type} "
                            f"price={price:.4f} → {'WIN ✅' if result >= 0.99 else 'LOSS ❌'}"
                        )
                        return result
    except Exception as exc:  # pylint: disable=broad-except
        print(f"{Fore.YELLOW}[LIVE] Trade API error: {type(exc).__name__} — trying order status...")

    # ── Method 3: get_order status ────────────────────────────────────────────
    try:
        order_resp = await asyncio.wait_for(
            asyncio.to_thread(client.get_order, order_id),
            timeout=10.0,
        )
        if isinstance(order_resp, dict):
            status = order_resp.get("status", "").upper()
            if status in ("REDEEMED", "SETTLED"):
                outcome = order_resp.get("outcome", "").upper()
                return 1.0 if outcome == "WIN" else 0.0
    except Exception as exc:  # pylint: disable=broad-except
        print(f"{Fore.YELLOW}[LIVE] Order status error: {type(exc).__name__}")

    return None  # Not settled yet — defer


async def _monitor_live_positions() -> None:
    """Check age of open positions; settle only after verifying actual Polymarket payout."""
    from scanner import get_active_markets

    active_markets = get_active_markets()
    to_settle: list[tuple[str, float, str]] = []

    for condition_id, position in list(LIVE_STATE["open_positions"].items()):
        elapsed = time.time() - position["entry_time"]
        sym = position.get("symbol", "?").upper()
        cid_short = condition_id[:8]

        # --- Hard age limit (safety net only) ---
        if elapsed > Config.MAX_POSITION_AGE_SECONDS:
            print(
                f"{Fore.RED}[LIVE] FORCE EXPIRED: {sym} held "
                f"{int(elapsed // 60)}min — settling conservatively as LOSS."
            )
            to_settle.append((condition_id, 0.0, "expired_unresolved"))
            continue

        # Market is no longer active — check if Polymarket settled it
        if condition_id not in active_markets:
            order_id = position.get("order_id", "")
            token_id = position.get("token_id", "")

            if not order_id:
                # No order_id stored (legacy position) — fall back to conservative loss
                print(f"{Fore.YELLOW}[LIVE] {sym} {position['side']} (cid={cid_short}...) "
                      f"no order_id stored — settling conservatively.")
                to_settle.append((condition_id, 0.0, "settled_loss"))
                continue

            # ── REAL PAYOUT CHECK (the core fix) ─────────────────────────────
            # Ask Polymarket directly: did this position win or lose?
            # This is the ONLY source of truth — not oracle, not token price.
            actual_payout = await _fetch_actual_payout(token_id, order_id, position)

            if actual_payout is not None:
                exit_price = actual_payout   # 1.0 = win, 0.0 = loss
                reason = "settled_win" if actual_payout >= 0.99 else "settled_loss"
                print(
                    f"{Fore.CYAN}[LIVE] {sym} {position['side']} "
                    f"— Polymarket confirmed: {'WIN ✅' if actual_payout >= 0.99 else 'LOSS ❌'}"
                )
                to_settle.append((condition_id, exit_price, reason))
            else:
                # Settlement not confirmed yet — defer with timeout
                uncertain_at: float | None = position.get("uncertain_resolve_at")
                if uncertain_at is None:
                    print(
                        f"{Fore.YELLOW}[LIVE] {sym} {position['side']} "
                        f"(cid={cid_short}...) market ended, awaiting Polymarket settlement..."
                    )
                    position["uncertain_resolve_at"] = time.time() + 120  # wait up to 2 min
                    _db_save_position(condition_id, position)
                elif time.time() >= uncertain_at:
                    print(
                        f"{Fore.RED}[LIVE] {sym} {position['side']} "
                        f"(cid={cid_short}...) settlement unconfirmed after 2min — recording as LOSS"
                    )
                    to_settle.append((condition_id, 0.0, "settled_loss"))
            # ─────────────────────────────────────────────────────────────────

    for condition_id, exit_price, reason in to_settle:
        _record_settlement(condition_id, exit_price, reason)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_live_trader(queue: asyncio.Queue) -> None:
    """Live execution loop. Public coroutine for main.py."""
    global _last_processed_index

    # --- Recover persisted state from SQLite before doing anything else ---
    _live_load_state()

    # --- Patch A: Force-settle any DB-recovered positions already past age limit ---
    # Mirrors paper_trader.py startup_cleanup block (L539-561)
    _stale_on_start = [
        cid for cid, pos in LIVE_STATE["open_positions"].items()
        if time.time() - pos["entry_time"] > Config.MAX_POSITION_AGE_SECONDS
    ]
    for _cid in _stale_on_start:
        _sym = LIVE_STATE["open_positions"][_cid].get("symbol", "?").upper()
        _side = LIVE_STATE["open_positions"][_cid].get("side", "?")
        print(
            f"{Fore.RED}[LIVE] STARTUP CLEANUP: {_sym} {_side} "
            f"— position already expired at recovery, force-settling conservatively."
        )
        _record_settlement(_cid, 0.0, "startup_cleanup")

    print(
        f"\n{Fore.RED}{Style.BRIGHT}{'!' * 56}\n"
        f"[LIVE] *** LIVE MAINNET TRADING ACTIVE ***\n"
        f"[LIVE] Chain ID  : {Config.CHAIN_ID}\n"
        f"[LIVE] Host      : {Config.POLYMARKET_HOST}\n"
        f"[LIVE] Capital   : ${LIVE_STATE['capital']:.2f} USDC\n"
        f"[LIVE] Open pos. : {len(LIVE_STATE['open_positions'])} recovered from DB\n"
        f"{'!' * 56}\n"
    )

    # Validate client connectivity at startup
    try:
        client = _get_client()
        ok = await asyncio.to_thread(client.get_ok)
        print(f"{Fore.GREEN}[LIVE] CLOB health check: {ok}")
    except Exception as exc:  # pylint: disable=broad-except
        print(
            f"{Fore.RED}{Style.BRIGHT}[LIVE] CLOB connectivity check FAILED: {exc}\n"
            f"[LIVE] Verify POLYMARKET_HOST, API credentials, and network. Aborting."
        )
        return

    # ── Pre-flight: balance check (CLOB API + on-chain fallback) ─────────────
    # The CLOB get_balance_allowance endpoint may return 0 for EIP-1967 deposit
    # wallets even when funds exist. We therefore also check on-chain directly.
    import os as _pre_os
    _deposit_wallet = _pre_os.getenv("POLYMARKET_PROXY_WALLET", "").strip()
    _PUSD_ADDR = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
    _DECIMALS  = 1_000_000
    internal_balance_usd: float = 0.0

    try:
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        bal_resp = await asyncio.to_thread(
            client.get_balance_allowance,
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
        )
        raw_balance = float((bal_resp or {}).get("balance", 0) or 0)
        internal_balance_usd = raw_balance / _DECIMALS
        print(
            f"{Fore.CYAN}[LIVE] CLOB reported balance  : ${internal_balance_usd:.4f} pUSD"
        )
    except Exception as exc:  # pylint: disable=broad-except
        print(
            f"{Fore.YELLOW}[LIVE] CLOB balance API error: {type(exc).__name__} — "
            f"falling back to on-chain check."
        )

    # On-chain fallback: query pUSD balance at the deposit wallet directly
    if internal_balance_usd == 0 and _deposit_wallet:
        try:
            from web3 import Web3 as _W3
            _w3 = _W3(_W3.HTTPProvider("https://polygon-bor-rpc.publicnode.com",
                                        request_kwargs={"timeout": 10}))
            _erc20_abi = [{"inputs":[{"name":"a","type":"address"}],"name":"balanceOf",
                           "outputs":[{"type":"uint256"}],"stateMutability":"view",
                           "type":"function"}]
            _pusd = _w3.eth.contract(
                address=_W3.to_checksum_address(_PUSD_ADDR), abi=_erc20_abi)
            _raw = _pusd.functions.balanceOf(
                _W3.to_checksum_address(_deposit_wallet)).call()
            internal_balance_usd = _raw / _DECIMALS
            print(
                f"{Fore.CYAN}[LIVE] On-chain deposit wallet : ${internal_balance_usd:.4f} pUSD  "
                f"({_deposit_wallet[:16]}...)"
            )
        except Exception as exc2:  # pylint: disable=broad-except
            print(
                f"{Fore.YELLOW}[LIVE] On-chain balance check failed: "
                f"{type(exc2).__name__} — proceeding with caution."
            )

    if internal_balance_usd == 0:
        print(
            f"\n{Fore.RED}{Style.BRIGHT}"
            f"[LIVE] ⛔ Deposit wallet has 0 pUSD — no funds to trade.\n"
            f"[LIVE] Go to https://polymarket.com → Deposit to fund your account.\n"
            f"[LIVE] Deposit wallet: {_deposit_wallet}\n"
            f"[LIVE] Halting bot — no orders will be placed."
        )
        return

    # Sync LIVE_STATE capital with actual on-chain balance
    if internal_balance_usd > 0 and internal_balance_usd != LIVE_STATE["capital"]:
        print(
            f"{Fore.CYAN}[LIVE] Syncing capital to on-chain balance: "
            f"${LIVE_STATE['capital']:.2f} → ${internal_balance_usd:.2f}"
        )
        LIVE_STATE["capital"]           = internal_balance_usd
        LIVE_STATE["available_capital"] = internal_balance_usd - sum(
            p["cost"] for p in LIVE_STATE["open_positions"].values()
        )
        _live_persist_state()
    # ─────────────────────────────────────────────────────────────────────────

    last_summary_ts = time.time()

    # ── Fix #1: Queue-based consumer — replaces 5-second polling loop ─────────
    # Two concurrent tasks share this function:
    #   1. _signal_consumer: blocks on queue.get(), processes signals with zero lag
    #   2. _position_monitor: periodic settlement checks + daily summary
    # Both are gathered here inside run_live_trader so they share all local state.

    async def _signal_consumer() -> None:
        """Consume signals from the shared asyncio.Queue as they arrive."""
        while True:
            signal = await queue.get()
            try:
                _maybe_daily_reset()
                if not _trading_halted and not _check_daily_loss_limit():
                    await _open_live_position(signal)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"{Fore.RED}[LIVE] Error processing signal: {type(exc).__name__} (details suppressed)")
            finally:
                queue.task_done()

    async def _position_monitor() -> None:
        """Background task: settle positions and print periodic summaries."""
        nonlocal last_summary_ts
        while True:
            try:
                _maybe_daily_reset()
                if LIVE_STATE["open_positions"]:
                    await _monitor_live_positions()
                _check_daily_loss_limit()

                if time.time() - last_summary_ts >= SUMMARY_INTERVAL_SECONDS:
                    summary = get_performance_summary()
                    sign = "+" if summary["total_profit"] >= 0 else ""
                    print(
                        f"\n{Fore.CYAN}{Style.BRIGHT}[LIVE] ══ LIVE PERFORMANCE ══\n"
                        f"{Fore.CYAN}  Capital    : ${summary['capital']:.2f}\n"
                        f"{Fore.CYAN}  Total P&L  : {sign}{summary['total_profit']:.4f}\n"
                        f"{Fore.CYAN}  Win Rate   : {summary['win_rate'] * 100:.1f}% "
                        f"({summary['total_trades']} trades)\n"
                        f"{Fore.CYAN}  Open       : {summary['open_positions']} position(s)\n"
                    )
                    last_summary_ts = time.time()
            except Exception as exc:  # pylint: disable=broad-except
                print(f"{Fore.RED}[LIVE] Monitor error: {type(exc).__name__} (details suppressed)")
            await asyncio.sleep(MONITOR_INTERVAL_SECONDS)

    await asyncio.gather(
        _signal_consumer(),
        _position_monitor(),
    )


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_live_trader(asyncio.Queue()))
