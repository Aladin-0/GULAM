# paper_trader.py
"""Paper trader: simulates trade execution against real market data, zero real money.

State persistence: open positions and performance metrics are written to SQLite
on every mutation. On restart, state is fully recovered from disk so no position
is ever silently orphaned.
"""

import asyncio
import json
import sqlite3
import time
from datetime import date
from pathlib import Path

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

# ---------------------------------------------------------------------------
# SQLite persistence layer
# ---------------------------------------------------------------------------

_DB_PATH = Path("bot_state.db")
_db_conn: sqlite3.Connection | None = None


def _get_db() -> sqlite3.Connection:
    """Return (and lazily create) the SQLite connection."""
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        _db_conn.row_factory = sqlite3.Row
        _init_db(_db_conn)
    return _db_conn


def _init_db(conn: sqlite3.Connection) -> None:
    """Create tables if they do not exist."""
    # WAL mode: reads never block writes; NORMAL sync: fsync only on checkpoint
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS paper_state (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS open_positions (
            condition_id TEXT PRIMARY KEY,
            data         TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trade_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            condition_id TEXT NOT NULL,
            data         TEXT NOT NULL,
            exit_time    REAL NOT NULL
        );
    """)
    conn.commit()


def _save_scalar(key: str, value) -> None:
    """Upsert a single scalar value into paper_state."""
    conn = _get_db()
    conn.execute(
        "INSERT INTO paper_state(key, value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value)),
    )
    conn.commit()


def _load_scalar(key: str, default):
    """Load a scalar from paper_state, returning default if absent."""
    conn = _get_db()
    row = conn.execute("SELECT value FROM paper_state WHERE key=?", (key,)).fetchone()
    return json.loads(row["value"]) if row else default


def _save_position(condition_id: str, position: dict) -> None:
    conn = _get_db()
    conn.execute(
        "INSERT INTO open_positions(condition_id, data) VALUES(?,?) "
        "ON CONFLICT(condition_id) DO UPDATE SET data=excluded.data",
        (condition_id, json.dumps(position)),
    )
    conn.commit()


def _delete_position(condition_id: str) -> None:
    conn = _get_db()
    conn.execute("DELETE FROM open_positions WHERE condition_id=?", (condition_id,))
    conn.commit()


def _save_trade_record(record: dict) -> None:
    conn = _get_db()
    conn.execute(
        "INSERT INTO trade_history(condition_id, data, exit_time) VALUES(?,?,?)",
        (record["condition_id"], json.dumps(record), record["exit_time"]),
    )
    # Keep history table lean — keep last 500 rows
    conn.execute(
        "DELETE FROM trade_history WHERE id NOT IN "
        "(SELECT id FROM trade_history ORDER BY id DESC LIMIT 500)"
    )
    conn.commit()


def _load_all_positions() -> dict:
    """Return all open positions from DB as {condition_id: dict}."""
    conn = _get_db()
    rows = conn.execute("SELECT condition_id, data FROM open_positions").fetchall()
    return {row["condition_id"]: json.loads(row["data"]) for row in rows}


def _persist_full_state() -> None:
    """Write all mutable PAPER_STATE scalars to DB in one transaction."""
    conn = _get_db()
    scalars = {
        "capital": PAPER_STATE["capital"],
        "available_capital": PAPER_STATE["available_capital"],
        "total_profit": PAPER_STATE["total_profit"],
        "total_trades": PAPER_STATE["total_trades"],
        "winning_trades": PAPER_STATE["winning_trades"],
        "losing_trades": PAPER_STATE["losing_trades"],
        "loss_count": PAPER_STATE["loss_count"],
        "total_lost_usd": PAPER_STATE["total_lost_usd"],
        "daily_profit": PAPER_STATE["daily_profit"],
        "daily_trades": PAPER_STATE["daily_trades"],
        "state_date": str(_today),
    }
    conn.executemany(
        "INSERT INTO paper_state(key, value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [(k, json.dumps(v)) for k, v in scalars.items()],
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Paper trading state — loaded from DB on startup
# ---------------------------------------------------------------------------

def _build_initial_state() -> dict:
    """Bootstrap PAPER_STATE from DB (first run → use defaults)."""
    return {
        "capital": _load_scalar("capital", Config.INITIAL_CAPITAL),
        "available_capital": _load_scalar("available_capital", Config.INITIAL_CAPITAL),
        "total_profit": _load_scalar("total_profit", 0.0),
        "total_trades": _load_scalar("total_trades", 0),
        "winning_trades": _load_scalar("winning_trades", 0),
        "losing_trades": _load_scalar("losing_trades", 0),
        "loss_count": _load_scalar("loss_count", 0),
        "total_lost_usd": _load_scalar("total_lost_usd", 0.0),
        "daily_profit": _load_scalar("daily_profit", 0.0),
        "daily_trades": _load_scalar("daily_trades", 0),
        "open_positions": _load_all_positions(),
        "trade_history": [],
    }


# Force DB init at import time so tables exist before any write
_get_db()
PAPER_STATE: dict = _build_initial_state()

# Internal bookkeeping
_last_processed_index: int = 0
_trading_halted: bool = False
_today: date = date.fromisoformat(
    _load_scalar("state_date", str(date.today()))
)
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
        _persist_full_state()
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
    if size < Config.MIN_ORDER_SIZE_USD:
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

    # --- Persist immediately ---
    _save_position(condition_id, position)
    _persist_full_state()

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

    # ── P&L Accounting Identity ─────────────────────────────────────────────
    # Use explicit branches so the dashboard ALWAYS moves in the correct
    # direction: wins add a positive amount; losses subtract a positive amount.
    # This eliminates any sign-confusion in the += operator path.
    if is_win:
        PAPER_STATE["total_profit"] += net_profit          # net_profit > 0
        PAPER_STATE["daily_profit"] += net_profit          # net_profit > 0
    else:
        loss_magnitude = abs(net_profit)                   # always positive
        PAPER_STATE["total_profit"] -= loss_magnitude      # guaranteed decrease
        PAPER_STATE["daily_profit"] -= loss_magnitude      # guaranteed decrease
    # ───────────────────────────────────────────────────────────────────

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

    # --- Persist immediately: delete position, save trade, update scalars ---
    _delete_position(condition_id)
    _save_trade_record(trade_record)
    _persist_full_state()

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
        f"entry={position['entry_price']:.2f} → resolved ${exit_price:.2f} | "
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
    """Check open positions; exit on settlement, force-expire after MAX_POSITION_AGE_SECONDS."""
    active_markets = get_active_markets()
    to_close: list[tuple[str, float, str]] = []

    open_count = len(PAPER_STATE["open_positions"])
    if open_count:
        print(f"{Fore.CYAN}[PAPER] Monitoring {open_count} positions...")

    for condition_id, position in PAPER_STATE["open_positions"].items():
        elapsed = time.time() - position["entry_time"]

        # --- Hard age limit ---
        if elapsed > Config.MAX_POSITION_AGE_SECONDS:
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
            # Persist the updated last_price
            _save_position(condition_id, position)
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
            if uncertain_at is None:
                print(
                    f"{Fore.YELLOW}[PAPER] {sym} {position['side']} "
                    f"(cid={cid_short}...) market gone, "
                    f"oracle/PTB unavailable → deferring 60s..."
                )
                position["uncertain_resolve_at"] = time.time() + 60
                _save_position(condition_id, position)
            elif time.time() >= uncertain_at:
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
                f"oracle=${oracle_price:,.2f} vs PTB=${ptb:,.2f} → WIN"
            )
            to_close.append((condition_id, 1.0, "settled_win"))
        else:
            print(
                f"{Fore.RED}[PAPER] {sym} {position['side']} "
                f"(cid={cid_short}...) market gone, "
                f"oracle=${oracle_price:,.2f} vs PTB=${ptb:,.2f} → LOSS"
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
        f"{Fore.CYAN}  State DB      : {_DB_PATH.resolve()}\n"
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

_OPEN_POS_PRINT_INTERVAL = 30
_last_open_pos_ts: float = 0.0


async def run_paper_trader() -> None:
    """Monitor signals and positions continuously. Public coroutine for main.py."""
    global _last_processed_index, _last_summary_ts, _last_open_pos_ts

    recovered = len(PAPER_STATE["open_positions"])
    print(
        f"{Fore.CYAN}[PAPER] Paper trader started — "
        f"capital=${PAPER_STATE['capital']:.2f}  "
        f"paper_trading={Config.PAPER_TRADING}  "
        f"recovered={recovered} open position(s) from DB ({_DB_PATH})"
    )

    # --- Force-close stale positions recovered from DB on startup ---
    stale_ids = [
        cid for cid, pos in PAPER_STATE["open_positions"].items()
        if time.time() - pos["entry_time"] > Config.MAX_POSITION_AGE_SECONDS
    ]

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

            # --- Periodic performance summary every 5 minutes ---
            if time.time() - _last_summary_ts >= SUMMARY_INTERVAL_SECONDS:
                _print_performance_summary()
                _last_summary_ts = time.time()

        except Exception as exc:  # pylint: disable=broad-except
            print(f"{Fore.RED}[PAPER] Unexpected error: {exc}")

        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_paper_trader())
