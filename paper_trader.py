# paper_trader.py
"""Paper trader: simulates trade execution against real market data, zero real money."""

import asyncio
import time
from datetime import date

import aiohttp
from colorama import Fore, Style, init

from config import Config
from oracle import LATEST_PRICES
from scanner import get_active_markets
from signal_engine import SIGNAL_HISTORY

GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"
RESOLUTION_CHECK_DELAY = 60  # seconds to wait before re-checking unresolved markets

# Initialize colorama
init(autoreset=True)

MONITOR_INTERVAL_SECONDS = 5
SUMMARY_INTERVAL_SECONDS = 300     # 5 minutes
MAX_POSITION_AGE_SECONDS = 900     # force-close after 15 minutes unresolved

# ---------------------------------------------------------------------------
# Paper trading state
# ---------------------------------------------------------------------------

PAPER_STATE: dict = {
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
    "open_positions": {},
    "trade_history": [],
}

# Internal bookkeeping
_last_processed_index: int = 0   # index into SIGNAL_HISTORY up to which we have acted
_trading_halted: bool = False    # True when daily loss limit is hit
_today: date = date.today()
_last_summary_ts: float = time.time()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_paper_state() -> dict:
    """Return the full PAPER_STATE dict."""
    return PAPER_STATE


def get_performance_summary() -> dict:
    """Return a concise performance snapshot."""
    total_trades = PAPER_STATE["total_trades"]
    win_rate = (
        PAPER_STATE["winning_trades"] / total_trades if total_trades > 0 else 0.0
    )
    total_return_pct = (
        PAPER_STATE["total_profit"] / Config.INITIAL_CAPITAL
        if Config.INITIAL_CAPITAL > 0
        else 0.0
    )
    return {
        "capital": PAPER_STATE["capital"],
        "total_profit": PAPER_STATE["total_profit"],
        "total_return_pct": total_return_pct,
        "win_rate": win_rate,
        "total_trades": total_trades,
        "daily_profit": PAPER_STATE["daily_profit"],
        "daily_trades": PAPER_STATE["daily_trades"],
        "loss_count": PAPER_STATE["loss_count"],
        "total_lost_usd": PAPER_STATE["total_lost_usd"],
        "open_positions": len(PAPER_STATE["open_positions"]),
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _maybe_daily_reset() -> None:
    """Reset daily counters at midnight."""
    global _today, _trading_halted
    today = date.today()
    if today != _today:
        _today = today
        PAPER_STATE["daily_profit"] = 0.0
        PAPER_STATE["daily_trades"] = 0
        _trading_halted = False
        print(f"{Fore.CYAN}[PAPER] Midnight reset — daily counters cleared.")


def _check_daily_loss_limit() -> bool:
    """Return True (and halt trading) if daily loss limit is breached."""
    global _trading_halted
    limit = -(Config.INITIAL_CAPITAL * Config.DAILY_LOSS_LIMIT_PCT)
    if PAPER_STATE["daily_profit"] < limit:
        if not _trading_halted:
            _trading_halted = True
            print(
                f"\n{Fore.RED}{Style.BRIGHT}[PAPER] *** DAILY LOSS LIMIT HIT ***  "
                f"daily_profit={PAPER_STATE['daily_profit']:.4f}  "
                f"limit={limit:.4f}  "
                f"Trading halted for the rest of the day."
            )
        return True
    return False


def _open_position(signal: dict) -> None:
    """Open a simulated limit order for the given signal."""
    condition_id = signal["condition_id"]

    if condition_id in PAPER_STATE["open_positions"]:
        return  # already have a position for this exact market

    total_equity = PAPER_STATE["available_capital"] + sum(
        p["cost"] for p in PAPER_STATE["open_positions"].values()
    )
    size = total_equity * Config.MAX_POSITION_SIZE_PCT
    if size < 1.0:
        return  # not enough capital

    entry_price: float = signal["entry_price"]
    if entry_price <= 0:
        return

    shares = size / entry_price
    cost = shares * entry_price  # == size

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
        "entry_time": time.time(),
        "time_remaining": signal["time_remaining"],
    }

    PAPER_STATE["open_positions"][condition_id] = position
    PAPER_STATE["available_capital"] -= cost + Config.SIMULATED_GAS_FEE_USD

    print(
        f"{Fore.YELLOW}[PAPER] Position opened: "
        f"[{signal['symbol'].upper()}] {signal['side']}  "
        f"shares={shares:.4f}  entry={entry_price:.4f}  cost=${cost:.2f}  "
        f"gas=${Config.SIMULATED_GAS_FEE_USD:.2f}  "
        f"t_left={signal.get('time_remaining', 0.0):.1f}s  "
        f"avail=${PAPER_STATE['available_capital']:.2f}"
    )


def _close_position(condition_id: str, exit_price: float, reason: str) -> None:
    """Close an open position, record the trade and update state."""
    position = PAPER_STATE["open_positions"].pop(condition_id, None)
    if position is None:
        return

    shares: float = position["shares"]
    cost: float = position["cost"]
    gross_profit = (exit_price - position["entry_price"]) * shares
    round_trip_gas = 2 * Config.SIMULATED_GAS_FEE_USD
    net_profit = gross_profit - round_trip_gas
    pnl_pct = net_profit / cost if cost > 0 else 0.0
    is_win = net_profit >= 0

    # Update capital (return cost + gross_profit minus exit gas)
    PAPER_STATE["available_capital"] += cost + gross_profit - Config.SIMULATED_GAS_FEE_USD
    PAPER_STATE["capital"] = (
        PAPER_STATE["available_capital"]
        + sum(p["cost"] for p in PAPER_STATE["open_positions"].values())
    )
    PAPER_STATE["total_profit"] += net_profit
    PAPER_STATE["daily_profit"] += net_profit
    PAPER_STATE["total_trades"] += 1
    PAPER_STATE["daily_trades"] += 1

    if is_win:
        PAPER_STATE["winning_trades"] += 1
    else:
        PAPER_STATE["losing_trades"] += 1
        PAPER_STATE["loss_count"] += 1
        PAPER_STATE["total_lost_usd"] += abs(net_profit)

    trade_record = {
        "condition_id": condition_id,
        "question": position["question"],
        "symbol": position["symbol"],
        "side": position["side"],
        "entry_price": position["entry_price"],
        "exit_price": exit_price,
        "shares": shares,
        "cost": cost,
        "profit": net_profit,
        "pnl_pct": pnl_pct,
        "reason": reason,
        "entry_time": position["entry_time"],
        "exit_time": time.time(),
    }

    PAPER_STATE["trade_history"].append(trade_record)
    if len(PAPER_STATE["trade_history"]) > 100:
        del PAPER_STATE["trade_history"][0]

    color = Fore.GREEN if is_win else Fore.RED
    result = "WIN" if is_win else "LOSS"
    sym_label = f"{position['symbol'].upper()} {position['side']}"
    pnl_str = (
        f"profit=+${net_profit:.2f} (after ${round_trip_gas:.2f} round-trip gas)"
        if is_win
        else f"loss=${net_profit:.2f} (after ${round_trip_gas:.2f} round-trip gas)"
    )
    print(
        f"{color}{Style.BRIGHT}[PAPER] SETTLED {result}: {sym_label} | "
        f"entry={position['entry_price']:.2f} \u2192 resolved ${exit_price:.2f} | "
        f"{pnl_str}"
    )
    print(
        f"{Fore.CYAN}[PAPER] New capital: ${PAPER_STATE['capital']:.2f}  "
        f"(open positions: {len(PAPER_STATE['open_positions'])})"
    )


async def _fetch_resolution(slug: str) -> dict | None:
    """Fetch market resolution from Gamma API. Returns dict with resolution info or None."""
    if not slug:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(GAMMA_API_URL, params={"slug": slug}) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if isinstance(data, list) and data:
                    return data[0]
                elif isinstance(data, dict):
                    return data
    except (aiohttp.ClientError, ValueError, TypeError):
        pass
    return None


async def _monitor_positions() -> None:
    """Check open positions; exit on settlement, force-expire after 15min."""
    active_markets = get_active_markets()
    to_close: list[tuple[str, float, str]] = []

    open_count = len(PAPER_STATE["open_positions"])
    if open_count:
        print(f"{Fore.CYAN}[PAPER] Monitoring {open_count} positions...")

    for condition_id, position in PAPER_STATE["open_positions"].items():
        elapsed = time.time() - position["entry_time"]

        # --- Hard age limit: force-close after 15 minutes ---
        if elapsed > MAX_POSITION_AGE_SECONDS:
            sym = position.get("symbol", "?").upper()
            print(
                f"{Fore.RED}[PAPER] FORCE EXPIRED: {sym} held "
                f"{int(elapsed // 60)}min without resolution"
            )
            to_close.append((condition_id, 0.0, "expired_unresolved"))
            continue

        market = active_markets.get(condition_id)

        if market is not None:
            # Market still live — refresh last known token price
            if position["side"] == "UP":
                position["last_price"] = float(
                    market.get("up_price", position.get("last_price", position["entry_price"]))
                )
            else:
                position["last_price"] = float(
                    market.get("down_price", position.get("last_price", position["entry_price"]))
                )
            position.pop("uncertain_resolve_at", None)
            continue

        # --- Market has disappeared — resolve using oracle vs PTB ---
        sym = position.get("symbol", "?").upper()
        cid_short = condition_id[:8]
        oracle_key = f"{position['symbol'].lower()}/usd"
        oracle_entry = LATEST_PRICES.get(oracle_key, {})
        oracle_price: float = oracle_entry.get("price", 0.0)
        ptb: float = position.get("price_to_beat", 0.0)

        uncertain_at: float | None = position.get("uncertain_resolve_at")

        if oracle_price == 0.0 or ptb == 0.0:
            # Oracle not ready — defer resolution
            if uncertain_at is None:
                print(
                    f"{Fore.YELLOW}[PAPER] {sym} {position['side']} "
                    f"(cid={cid_short}...) market gone, "
                    f"oracle/PTB unavailable → deferring 60s..."
                )
                position["uncertain_resolve_at"] = time.time() + 60
            elif time.time() >= uncertain_at:
                # Force-close after deferral with no oracle data
                print(
                    f"{Fore.RED}[PAPER] {sym} {position['side']} "
                    f"(cid={cid_short}...) force-resolved (no oracle data) → LOSS"
                )
                to_close.append((condition_id, 0.0, "settled_loss"))
            continue

        # Oracle data available — mathematically resolve
        if position["side"] == "UP":
            is_win = oracle_price > ptb
        else:
            is_win = oracle_price < ptb

        if is_win:
            print(
                f"{Fore.GREEN}[PAPER] {sym} {position['side']} "
                f"(cid={cid_short}...) market gone, "
                f"oracle=${oracle_price:,.2f} vs PTB=${ptb:,.2f} \u2192 WIN"
            )
            to_close.append((condition_id, 1.0, "settled_win"))
        else:
            print(
                f"{Fore.RED}[PAPER] {sym} {position['side']} "
                f"(cid={cid_short}...) market gone, "
                f"oracle=${oracle_price:,.2f} vs PTB=${ptb:,.2f} \u2192 LOSS"
            )
            to_close.append((condition_id, 0.0, "settled_loss"))

    for condition_id, exit_price, reason in to_close:
        _close_position(condition_id, exit_price, reason)


def _print_performance_summary() -> None:
    """Print a cyan performance snapshot to the terminal."""
    summary = get_performance_summary()
    sign = "+" if summary["total_profit"] >= 0 else ""
    dsign = "+" if summary["daily_profit"] >= 0 else ""
    print(
        f"\n{Fore.CYAN}{Style.BRIGHT}[PAPER] ══ PERFORMANCE SUMMARY ══\n"
        f"{Fore.CYAN}  Capital       : ${summary['capital']:.2f}\n"
        f"{Fore.CYAN}  Total P&L     : {sign}{summary['total_profit']:.4f} "
        f"({sign}{summary['total_return_pct'] * 100:.2f}%)\n"
        f"{Fore.CYAN}  Daily P&L     : {dsign}{summary['daily_profit']:.4f}\n"
        f"{Fore.CYAN}  Win Rate      : {summary['win_rate'] * 100:.1f}%  "
        f"({summary['total_trades']} trades total, {summary['daily_trades']} today)\n"
        f"{Fore.CYAN}  Losing trades : {summary['loss_count']}\n"
        f"{Fore.CYAN}  Total lost    : -${summary['total_lost_usd']:.2f}\n"
        f"{Fore.CYAN}  Open Positions: {summary['open_positions']}\n"
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

_OPEN_POS_PRINT_INTERVAL = 30
_last_open_pos_ts: float = 0.0


async def run_paper_trader() -> None:
    """Monitor signals and positions continuously. Public coroutine for main.py."""
    global _last_processed_index, _last_summary_ts, _last_open_pos_ts

    print(
        f"{Fore.CYAN}[PAPER] Paper trader started — "
        f"capital=${PAPER_STATE['capital']:.2f}  "
        f"paper_trading={Config.PAPER_TRADING}"
    )

    # --- Force-close stale positions on startup ---
    stale_ids = []
    for cid, pos in PAPER_STATE["open_positions"].items():
        elapsed = time.time() - pos["entry_time"]
        if elapsed > 900:  # older than 15 minutes
            stale_ids.append(cid)

    for cid in stale_ids:
        pos = PAPER_STATE["open_positions"][cid]
        oracle_key = f"{pos['symbol'].lower()}/usd"
        oracle_entry = LATEST_PRICES.get(oracle_key, {})
        oracle_price = oracle_entry.get("price", 0.0)
        ptb = pos.get("price_to_beat", 0.0)

        if oracle_price == 0.0 or ptb == 0.0:
            exit_price = 0.0  # conservative fallback
        elif pos["side"] == "UP":
            exit_price = 1.0 if oracle_price > ptb else 0.0
        else:
            exit_price = 1.0 if oracle_price < ptb else 0.0

        _close_position(cid, exit_price, "startup_cleanup")
        print(
            f"[PAPER] STARTUP CLEANUP: closed stale {pos['symbol']} {pos['side']}"
        )

    while True:
        try:
            _maybe_daily_reset()

            # --- Process ALL new signals since last cycle ---
            if not _trading_halted:
                new_signals = SIGNAL_HISTORY[_last_processed_index:]
                _last_processed_index = len(SIGNAL_HISTORY)
                if not _check_daily_loss_limit():
                    for signal in new_signals:
                        _open_position(signal)

            # --- Monitor open positions ---
            if PAPER_STATE["open_positions"]:
                await _monitor_positions()

                # Print open positions status every 30 seconds
                if time.time() - _last_open_pos_ts >= _OPEN_POS_PRINT_INTERVAL:
                    parts = []
                    for pos in PAPER_STATE["open_positions"].values():
                        lp = pos.get("last_price", pos["entry_price"])
                        parts.append(
                            f"[{pos['symbol'].upper()} {pos['side']}] "
                            f"entry={pos['entry_price']:.3f} last_price={lp:.3f}"
                        )
                    print(f"{Fore.CYAN}[PAPER] Open: {' | '.join(parts)}")
                    _last_open_pos_ts = time.time()

            # --- Daily loss limit check (always, even without new signal) ---
            _check_daily_loss_limit()

            # --- Periodic performance summary every 10 minutes ---
            if time.time() - _last_summary_ts >= SUMMARY_INTERVAL_SECONDS:
                _print_performance_summary()
                _last_summary_ts = time.time()

        except Exception as exc:  # pylint: disable=broad-except
            print(f"{Fore.RED}[PAPER] Unexpected error: {exc}")

        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_paper_trader())
