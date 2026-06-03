# live_trader.py
"""Live execution layer — routes signals to the Polymarket CLOB on Polygon Mainnet.

Only active when PAPER_TRADING=False. Uses py-clob-client to construct,
sign (ECDSA via private key), and broadcast real limit orders.
"""

import asyncio
import time
from datetime import date

import aiohttp
from colorama import Fore, Style, init
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from py_clob_client.constants import BUY

from config import Config
from signal_engine import SIGNAL_HISTORY
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
    """Instantiate and return an authenticated ClobClient."""
    creds = ApiCreds(
        api_key=Config.POLYMARKET_API_KEY,
        api_secret=Config.POLYMARKET_API_SECRET,
        api_passphrase=Config.POLYMARKET_API_PASSPHRASE,
    )
    # Derive wallet address from private key — never trust a hardcoded string
    derived_address: str = Account.from_key(Config.PRIVATE_KEY).address
    client = ClobClient(
        host=Config.POLYMARKET_HOST,
        chain_id=Config.CHAIN_ID,
        key=Config.PRIVATE_KEY,
        creds=creds,
        signature_type=0,       # EOA (Externally Owned Account) signing
        funder=derived_address,
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


async def _place_order(signal: dict, size_usd: float) -> dict | None:
    """
    Construct, sign and POST a GTC limit order to the Polymarket CLOB.

    Returns the API response dict on success, or None on any failure.
    A None return means NO order was sent — capital is safe.
    """
    token_id: str = signal["token_id"]
    entry_price: float = round(signal["entry_price"], 4)
    shares: float = round(size_usd / entry_price, 4)

    if shares <= 0 or entry_price <= 0:
        print(f"{Fore.RED}[LIVE] Invalid order params: shares={shares} price={entry_price}")
        return None

    order_args = OrderArgs(
        token_id=token_id,
        price=entry_price,
        size=shares,
        side=BUY,
    )

    try:
        client = _get_client()

        # Sign the order — pure CPU, no network
        signed_order = await asyncio.to_thread(client.create_order, order_args)

        # Broadcast with a hard 10-second timeout
        resp = await asyncio.wait_for(
            asyncio.to_thread(client.post_order, signed_order, OrderType.GTC),
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
        # Suppress message — py-clob-client exceptions may embed signing key material
        print(f"{Fore.RED}[LIVE] Order placement failed: {type(exc).__name__} (details suppressed for security)")
        return None

    # ---- Validate API response ----
    if not isinstance(resp, dict):
        # Truncate repr — raw CLOB responses may echo auth tokens in error payloads
        safe_resp = str(resp)[:120] if resp is not None else "None"
        print(f"{Fore.RED}[LIVE] Unexpected response type from CLOB: {safe_resp}...")
        return None

    if not resp.get("success", False):
        error_msg = resp.get("errorMsg", resp.get("error", "unknown"))
        print(
            f"{Fore.RED}[LIVE] Order REJECTED by CLOB: {error_msg}  "
            f"(token={token_id[:12]}...  price={entry_price}  size={shares})"
        )
        # Surface specific actionable errors
        if "insufficient" in str(error_msg).lower():
            print(
                f"{Fore.RED}[LIVE] Insufficient USDC balance or liquidity. "
                f"Check wallet balance and order-book depth."
            )
        return None

    order_id = resp.get("orderID", resp.get("order_id", "N/A"))
    print(
        f"{Fore.GREEN}{Style.BRIGHT}[LIVE] ✓ Order ACCEPTED — "
        f"orderID={order_id}  "
        f"token={token_id[:12]}...  "
        f"price={entry_price}  shares={shares:.4f}  "
        f"size_usd=${size_usd:.2f}"
    )
    return resp


# ---------------------------------------------------------------------------
# Position tracking
# ---------------------------------------------------------------------------

async def _open_live_position(signal: dict) -> None:
    """Attempt to place a live order and record the position on success."""
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

    shares = size_usd / entry_price
    cost = shares * entry_price
    clob_entry_fee = cost * Config.CLOB_FEE_PCT   # Polymarket maker/taker fee on entry fill

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
        "clob_entry_fee": clob_entry_fee,          # persisted so recovery sees the real cost basis
        "entry_time": time.time(),
        "time_remaining": signal["time_remaining"],
        "order_id": resp.get("orderID", ""),
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
        f"{Fore.YELLOW}[LIVE] Position recorded: "
        f"[{signal['symbol'].upper()}] {signal['side']}  "
        f"shares={shares:.4f}  entry={entry_price:.4f}  cost=${cost:.2f}  "
        f"clob_fee=${clob_entry_fee:.4f}  "
        f"avail=${LIVE_STATE['available_capital']:.2f}"
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

async def _monitor_live_positions() -> None:
    """Check age of open positions; force-settle expired ones conservatively."""
    from oracle import LATEST_PRICES
    from scanner import get_active_markets

    active_markets = get_active_markets()
    to_settle: list[tuple[str, float, str]] = []

    for condition_id, position in LIVE_STATE["open_positions"].items():
        elapsed = time.time() - position["entry_time"]

        # --- Hard age limit ---
        if elapsed > Config.MAX_POSITION_AGE_SECONDS:
            sym = position.get("symbol", "?").upper()
            print(
                f"{Fore.RED}[LIVE] FORCE EXPIRED: {sym} held "
                f"{int(elapsed // 60)}min — settling conservatively."
            )
            to_settle.append((condition_id, 0.0, "expired_unresolved"))
            continue

        if condition_id not in active_markets:
            oracle_key = f"{position['symbol'].lower()}/usd"
            oracle_entry = LATEST_PRICES.get(oracle_key, {})
            oracle_price: float = oracle_entry.get("price", 0.0)
            ptb: float = position.get("price_to_beat", 0.0)

            if oracle_price > 0 and ptb > 0:
                # Oracle data available — resolve mathematically (mirrors paper_trader)
                if position["side"] == "UP":
                    is_win = oracle_price > ptb
                else:
                    is_win = oracle_price < ptb
                exit_price = 1.0 if is_win else 0.0
                reason = "settled_win" if is_win else "settled_loss"
                to_settle.append((condition_id, exit_price, reason))
            else:
                # --- Oracle not ready: 60-second deferral (mirrors paper_trader exactly) ---
                uncertain_at: float | None = position.get("uncertain_resolve_at")
                cid_short = condition_id[:8]
                sym = position.get("symbol", "?").upper()
                if uncertain_at is None:
                    print(
                        f"{Fore.YELLOW}[LIVE] {sym} {position['side']} "
                        f"(cid={cid_short}...) market gone, "
                        f"oracle/PTB unavailable → deferring 60s..."
                    )
                    position["uncertain_resolve_at"] = time.time() + 60
                    _db_save_position(condition_id, position)  # persist deferral timestamp
                elif time.time() >= uncertain_at:
                    print(
                        f"{Fore.RED}[LIVE] {sym} {position['side']} "
                        f"(cid={cid_short}...) force-resolved (no oracle data) → LOSS"
                    )
                    to_settle.append((condition_id, 0.0, "settled_loss"))

    for condition_id, exit_price, reason in to_settle:
        _record_settlement(condition_id, exit_price, reason)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_live_trader() -> None:
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

    last_summary_ts = time.time()

    while True:
        try:
            _maybe_daily_reset()

            if not _trading_halted:
                new_signals = SIGNAL_HISTORY[_last_processed_index:]
                _last_processed_index = len(SIGNAL_HISTORY)

                if not _check_daily_loss_limit():
                    for signal in new_signals:
                        await _open_live_position(signal)

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
            # Suppress exc message — library exceptions may embed key material
            print(f"{Fore.RED}[LIVE] Unexpected error in main loop: {type(exc).__name__} (details suppressed)")

        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_live_trader())
