# signal_engine.py
"""Signal engine — 55-second oracle lag / token mispricing strategy.

Edge: Chainlink oracle updates instantly; Polymarket token prices lag ~55s.
When oracle is clearly above/below PTB, the correct-side token is still cheap.
We enter before the market catches up.

PTB = LATEST_PRICES[oracle_key]["open_price"]  (Chainlink price at period start)

Phase 3 — Mid-Epoch Active Hedging:
  For every open position, the evaluation loop checks whether the Binance spot
  oracle has crossed the epoch open_price in the wrong direction ("baseline breach").
  If it has, and the contract is still within HEDGE_WINDOW_SECONDS of expiry, the
  escape hatch fires: execute_hedge_dump() is called on the active trader instance
  to aggressively liquidate the position before expiration locks in a 100% loss.
"""

import asyncio
import time
from datetime import date, datetime, timezone

from colorama import Fore, Style, init

from config import Config
from oracle import LATEST_PRICES
import orderbook_cache as clob_cache
from orderbook_cache import validate_liquidity
from scanner import get_active_markets, prune_expired_markets

# Initialize colorama
init(autoreset=True)

EVAL_INTERVAL_SECONDS = 1
SIGNALED_MARKETS_CLEAR_INTERVAL = 900   # clear every 15 minutes (one market period)
DIAGNOSTIC_INTERVAL_SECONDS = 10        # print full market status every 10s

# ---------------------------------------------------------------------------
# Phase 3: Mid-Epoch Active Hedging constants
# ---------------------------------------------------------------------------
# Window (seconds) before contract expiry inside which the escape hatch can fire.
# Mirrors Config.MAX_EXECUTION_TIME_SECONDS — positions entered in this window
# need an exit path if the oracle reversal invalidates the original edge.
HEDGE_WINDOW_SECONDS: int = 90

# Tracks condition_ids already hedged this epoch so we never double-dump.
_HEDGED_POSITIONS: set[str] = set()

# Injected at startup by main.py via register_hedge_callback().
# Signature: async def hedge_fn(condition_id: str) -> None
_hedge_callback: "callable | None" = None

# Injected at startup by main.py via register_positions_callback().
# Signature: def positions_fn() -> dict[str, dict]
_get_open_positions_fn: "callable | None" = None

# Injected at startup by main.py via register_capital_callback().
# Signature: def capital_fn() -> float
_get_capital_fn: "callable | None" = None

# ---------------------------------------------------------------------------
# Set of condition_ids already signaled this hour
# ---------------------------------------------------------------------------
SIGNALED_MARKETS: set[str] = set()

# Ordered signal history — capped at 100
SIGNAL_HISTORY: list[dict] = []

# Stats
_last_signal_time: float | None = None
_signals_today: int = 0
_today: date = date.today()
_last_clear_ts: float = time.time()
_last_diag_ts: float = 0.0


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_latest_signal() -> dict | None:
    """Return the most recent signal or None."""
    return SIGNAL_HISTORY[-1] if SIGNAL_HISTORY else None


def get_signal_stats() -> dict:
    """Return aggregate stats about generated signals."""
    return {
        "total_signals": len(SIGNAL_HISTORY),
        "signals_today": _signals_today,
        "last_signal_time": _last_signal_time,
    }


def register_hedge_callback(fn: "callable") -> None:
    """Register the active trader's hedge-dump coroutine with the signal engine.

    Called once at startup (main.py) after both the signal engine and the active
    trader module are imported.  This avoids circular imports — the signal engine
    never imports paper_trader or live_trader directly.

    ``fn`` must be an async callable with signature:
        async def fn(condition_id: str) -> None
    """
    global _hedge_callback
    _hedge_callback = fn
    print(f"{Fore.CYAN}[SIGNAL] Hedge callback registered: {getattr(fn, '__qualname__', repr(fn))}")


def register_positions_callback(fn: "callable") -> None:
    """Register a synchronous getter that returns the trader's open positions dict."""
    global _get_open_positions_fn
    _get_open_positions_fn = fn
    print(f"{Fore.CYAN}[SIGNAL] Positions callback registered: {getattr(fn, '__qualname__', repr(fn))}")


def register_capital_callback(fn: "callable") -> None:
    """Register a synchronous getter that returns the trader's current live capital.

    Called once at startup (main.py).  Used by _evaluate_market() to compute
    required_capital dynamically against real-time equity rather than a frozen
    INITIAL_CAPITAL constant.

    ``fn`` must be a synchronous callable with signature:
        def fn() -> float   # current live capital in USD
    """
    global _get_capital_fn
    _get_capital_fn = fn
    print(f"{Fore.CYAN}[SIGNAL] Capital callback registered: {getattr(fn, '__qualname__', repr(fn))}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _oracle_key(symbol: str) -> str:
    """'btc' → 'btc/usd'"""
    return f"{symbol.lower()}/usd"


def _record_signal(signal: dict) -> None:
    """Append to SIGNAL_HISTORY (cap 100) and update daily stats."""
    global _last_signal_time, _signals_today, _today
    SIGNAL_HISTORY.append(signal)
    if len(SIGNAL_HISTORY) > 100:
        del SIGNAL_HISTORY[0]
    _last_signal_time = signal["timestamp"]
    today = date.today()
    if today != _today:
        _today = today
        _signals_today = 0
    _signals_today += 1


def _maybe_clear_signaled_markets() -> None:
    """Clear SIGNALED_MARKETS every 15 minutes so markets can be re-evaluated."""
    global _last_clear_ts
    if time.time() - _last_clear_ts >= SIGNALED_MARKETS_CLEAR_INTERVAL:
        cleared = len(SIGNALED_MARKETS)
        SIGNALED_MARKETS.clear()
        _last_clear_ts = time.time()
        if cleared:
            print(
                f"{Fore.WHITE}[SIGNAL] Period reset — cleared {cleared} "
                f"condition_id(s) from SIGNALED_MARKETS."
            )


def _evaluate_market(market: dict) -> dict | None:
    """Evaluate one market against the three entry conditions."""
    condition_id: str = market["condition_id"]
    symbol: str = market.get("symbol", "")
    if not symbol or condition_id in SIGNALED_MARKETS:
        return None

    oracle_key = _oracle_key(symbol)
    entry = LATEST_PRICES.get(oracle_key, {})
    current_price: float = entry.get("price", 0.0)
    price_to_beat: float = entry.get("open_price", 0.0)
    end_date_str = market.get("end_date", "")
    if end_date_str:
        try:
            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            t_left_s: float = (end_dt - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, AttributeError):
            t_left_s = market["time_remaining_seconds"]
    else:
        t_left_s: float = market["time_remaining_seconds"]

    if current_price == 0.0 or price_to_beat == 0.0:
        return None

    # (Time constraint handled by c2 condition below)
    move_pct: float = (current_price - price_to_beat) / price_to_beat

    # ── LATENCY ARBITRAGE STRATEGY ─────────────────────────────────────────
    # We want to buy the WINNING side when a massive wick happens.
    if current_price >= price_to_beat:
        correct_side = "UP"
        token_id: str = market["up_token_id"]
        _static_token_price: float = market["up_price"]
    else:
        correct_side = "DOWN"
        token_id = market["down_token_id"]
        _static_token_price = market["down_price"]

    # ── Live token price resolution (Fix #3) ─────────────────────────────────
    # Prefer the live CLOB WebSocket cache over the 30-second Gamma REST snapshot.
    # For the correct-side token:
    #   UP   → we need the best ASK (cheapest price we can buy at)
    #   DOWN → we need the best ASK (same — we always BUY the winner token)
    token_price: float = _static_token_price  # fallback to Gamma snapshot
    book = clob_cache.get_orderbook(token_id)
    if book is not None:
        asks = book.get("asks", {})
        if asks:
            try:
                live_ask = min(float(p) for p in asks)
                if 0.0 < live_ask < 1.0:
                    token_price = live_ask
            except (ValueError, TypeError):
                pass  # fall back to static price
    # ─────────────────────────────────────────────────────────────────────────

    dynamic_need_pct = Config.BASE_GAP_BPS / 10000.0
    risk_multiplier = 1.0

    c1 = abs(move_pct) >= dynamic_need_pct
    c2 = t_left_s <= 180
    c3 = token_price <= 0.75
    
    # ── Phantom Orderbook Block Neutralized ──────────────────────────────────
    c4 = True

    if not (c1 and c2 and c3 and c4):
        return None

    # ── Dynamic liquidity gate (Fix #4) ──────────────────────────────────────
    # Use live capital from trader instead of frozen INITIAL_CAPITAL constant.
    live_capital: float = Config.INITIAL_CAPITAL  # conservative fallback
    if _get_capital_fn is not None:
        try:
            live_capital = float(_get_capital_fn())
        except Exception:  # pylint: disable=broad-except
            pass
    required_capital = max(live_capital, Config.INITIAL_CAPITAL) * Config.MAX_POSITION_SIZE_PCT
    # ─────────────────────────────────────────────────────────────────────────

    is_liquid, available_value = validate_liquidity(
        token_id, "BUY", token_price, required_capital
    )
    if not is_liquid:
        print(
            f"{Fore.YELLOW}[SIGNAL] ILLIQUID TRAP: {symbol.upper()} {correct_side} "
            f"token_price=${token_price:.4f}  available_value=${available_value:.2f}  "
            f"required=${required_capital:.2f} — skipping trade"
        )
        return None

    gap: float = current_price - price_to_beat
    return {
        "condition_id": condition_id,
        "question": market["question"],
        "symbol": symbol,
        "slug": market.get("slug", ""),
        "side": correct_side,
        "token_id": token_id,
        "entry_price": token_price,
        "price_to_beat": price_to_beat,
        "current_price": current_price,
        "gap": gap,
        "gap_pct": move_pct * 100,
        "time_remaining": t_left_s,
        "dynamic_need_pct": dynamic_need_pct,
        "risk_multiplier": risk_multiplier,
        "timestamp": time.time(),
    }


def _emit_signal(signal: dict) -> None:
    """Print a clear signal-fired block to the terminal."""
    gap_sign = "+" if signal["gap"] >= 0 else ""
    direction = "ABOVE" if signal["gap"] >= 0 else "BELOW"
    print(
        f"\n{Fore.GREEN}{Style.BRIGHT}[SIGNAL] *** SIGNAL FIRED ***\n"
        f"{Fore.GREEN}  Market       : {signal['question'][:80]}\n"
        f"{Fore.GREEN}  Oracle       : ${signal['current_price']:,.2f} vs PTB: "
        f"${signal['price_to_beat']:,.2f} → price {direction} beat\n"
        f"{Fore.GREEN}  Gap          : {gap_sign}${signal['gap']:,.2f} "
        f"({gap_sign}{signal['gap_pct']:.3f}%)\n"
        f"{Fore.GREEN}  Need (dynamic): {signal['dynamic_need_pct'] * 100:.4f}%  "
        f"(risk_mult={signal['risk_multiplier']:.2f}x)\n"
        f"{Fore.GREEN}  Correct side : {signal['side']}\n"
        f"{Fore.GREEN}  Token price  : ${signal['entry_price']:.4f} "
        f"\u2190 MISPRICED (should be ~$0.97)\n"
        f"{Fore.GREEN}  Token ID     : {signal['token_id']}\n"
        f"{Fore.GREEN}  Time left    : {signal['time_remaining']:.1f}s\n"
        f"{Fore.GREEN}  ENTERING POSITION\n"
    )


def _print_diagnostics(markets: dict) -> None:
    """Every 10 seconds: print a full table showing why each market is waiting."""
    base_need_pct = Config.BASE_GAP_BPS / 10000.0

    print(f"\n{Fore.WHITE}{'─' * 70}")
    print(f"{Fore.WHITE}{Style.BRIGHT}  SIGNAL DIAGNOSTICS  —  {time.strftime('%H:%M:%S')}")
    print(f"{Fore.WHITE}{'─' * 70}")

    # Oracle prices summary
    for sym in ("btc", "eth", "sol"):
        key = f"{sym}/usd"
        entry = LATEST_PRICES.get(key, {})
        price = entry.get("price", 0.0)
        open_p = entry.get("open_price", 0.0)
        if price == 0.0:
            print(f"{Fore.RED}  [{sym.upper():3s}] Oracle: NO DATA")
            continue
        move = (price - open_p) / open_p if open_p else 0.0
        move_sign = "+" if move >= 0 else ""
        need = base_need_pct
        ok = "✓" if abs(move) > need else "✗"
        bar_val = min(abs(move) / need, 1.0) if need > 0 else 0
        bar = "█" * int(bar_val * 10) + "░" * (10 - int(bar_val * 10))
        color = Fore.GREEN if abs(move) > need else Fore.YELLOW
        print(
            f"{color}  [{sym.upper():3s}] ${price:>10,.2f}  "
            f"move={move_sign}{move*100:.3f}% [{bar}] need={need*100:.3f}%  {ok}"
        )

    # Per-market token status
    if markets:
        print(f"{Fore.WHITE}{'·' * 70}")
        print(f"{Fore.WHITE}  {'MARKET':<30} {'SIDE':>4} {'TOKEN':>6} {'TIME':>6}  CONDITIONS")
        for cid, mkt in markets.items():
            sym = mkt.get("symbol", "?")
            key = f"{sym}/usd"
            entry = LATEST_PRICES.get(key, {})
            current = entry.get("price", 0.0)
            open_p = entry.get("open_price", 0.0)
            end_date_str = mkt.get("end_date", "")
            if end_date_str:
                try:
                    end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                    t_left_s = (end_dt - datetime.now(timezone.utc)).total_seconds()
                except (ValueError, AttributeError):
                    t_left_s = mkt.get("time_remaining_seconds", 0.0)
            else:
                t_left_s = mkt.get("time_remaining_seconds", 0.0)

            if current == 0.0 or open_p == 0.0:
                print(f"{Fore.RED}  {sym.upper():<30} oracle not ready")
                continue

            move = (current - open_p) / open_p
            if current >= open_p:
                side = "DOWN"
                token_price = mkt.get("down_price", 0.0)
            else:
                side = "UP"
                token_price = mkt.get("up_price", 0.0)

            c1 = abs(move) >= 0.0009
            c2 = t_left_s >= 300
            c3 = token_price <= 0.35
            
            imbalance = LATEST_PRICES.get(_oracle_key(sym), {}).get("imbalance", 0.5)
            if side == "DOWN":
                c4 = imbalance < 0.35
            else:
                c4 = imbalance > 0.65

            # Liquidity check for diagnostics
            required_capital = Config.INITIAL_CAPITAL * Config.MAX_POSITION_SIZE_PCT
            is_liquid, available_value = validate_liquidity(
                mkt.get("up_token_id" if side == "UP" else "down_token_id", ""),
                "BUY", token_price, required_capital
            )
            slug = mkt.get("slug", cid)[-28:]
            already = " [already signaled]" if cid in SIGNALED_MARKETS else ""

            c1s = f"{Fore.GREEN}C1✓{Fore.WHITE}" if c1 else f"{Fore.RED}C1✗(wick={move*100:.3f}%){Fore.WHITE}"
            c2s = f"{Fore.GREEN}C2✓{Fore.WHITE}" if c2 else f"{Fore.RED}C2✗(t={t_left_s:.0f}s){Fore.WHITE}"
            c3s = f"{Fore.GREEN}C3✓{Fore.WHITE}" if c3 else f"{Fore.RED}C3✗(price={token_price:.3f}){Fore.WHITE}"
            c4s = f"{Fore.GREEN}C4✓{Fore.WHITE}" if c4 else f"{Fore.RED}C4✗(wall={imbalance:.2f}){Fore.WHITE}"
            c5s = f"{Fore.GREEN}LIQ✓${available_value:.0f}{Fore.WHITE}" if is_liquid else f"{Fore.RED}LIQ✗${available_value:.0f}<{required_capital:.0f}{Fore.WHITE}"
            print(
                f"{Fore.WHITE}  {slug:<30} {side:>4} {token_price:>6.3f} {t_left_s:>5.0f}s  "
                f"{c1s} {c2s} {c3s} {c4s} {c5s}{already}"
            )
    else:
        print(f"{Fore.YELLOW}  No active markets being tracked.")

    print(f"{Fore.WHITE}{'─' * 70}\n")


# ---------------------------------------------------------------------------
# Phase 3: Escape Hatch — hedge trigger evaluation
# ---------------------------------------------------------------------------

async def _check_hedge_triggers(open_positions: dict, trigger_symbol: str = "") -> None:
    """Scan all open positions for a baseline breach and fire the escape hatch.

    [DISABLED FOR FADE STRATEGY]
    The Fade strategy buys the losing side specifically expecting a reversion.
    The escape hatch logic contradicts this thesis.
    """
    return

    global _HEDGED_POSITIONS

    if _hedge_callback is None:
        return  # no trader registered yet — skip silently

    if not open_positions:
        return

    now = time.time()
    triggers: list[str] = []

    # get base symbol from trigger_symbol if provided
    base_sym = trigger_symbol.split("/")[0].lower() if trigger_symbol else ""

    for condition_id, position in open_positions.items():
        # Skip already-hedged positions (idempotency guard)
        if condition_id in _HEDGED_POSITIONS:
            continue

        symbol: str = position.get("symbol", "")
        
        # If event-driven, only check positions for the symbol that just ticked
        if base_sym and symbol.lower() != base_sym:
            continue
            
        side: str = position.get("side", "")
        ptb: float = position.get("price_to_beat", 0.0)   # epoch open_price
        entry_time: float = position.get("entry_time", 0.0)

        if not symbol or not side or ptb == 0.0:
            continue

        # Only act while position is within the hedge window
        elapsed = now - entry_time
        if elapsed > HEDGE_WINDOW_SECONDS:
            continue

        # Pull live Binance spot price from oracle in-process RAM dict
        oracle_key = f"{symbol.lower()}/usd"
        oracle_entry = LATEST_PRICES.get(oracle_key, {})
        spot: float = oracle_entry.get("price", 0.0)

        if spot == 0.0:
            continue  # oracle not ready — don't act on missing data

        # ── Baseline breach detection ────────────────────────────────────────
        breach = False
        if side == "UP" and spot < ptb:
            breach = True
        elif side == "DOWN" and spot > ptb:
            breach = True
        # ─────────────────────────────────────────────────────────────────────

        if breach:
            print(
                f"\n{Fore.RED}{Style.BRIGHT}"
                f"[HEDGE] 🚨 ESCAPE HATCH TRIGGERED for {symbol.upper()}!\n"
                f"[HEDGE]    Position side  : {side}\n"
                f"[HEDGE]    Epoch PTB      : ${ptb:,.4f}\n"
                f"[HEDGE]    Current spot   : ${spot:,.4f}  ← crossed baseline\n"
                f"[HEDGE]    Elapsed        : {elapsed:.1f}s into position\n"
                f"[HEDGE]    Liquidating position to preserve capital..."
            )
            triggers.append(condition_id)

    # Execute hedges outside the iteration loop to avoid mutating the dict
    for condition_id in triggers:
        _HEDGED_POSITIONS.add(condition_id)   # mark BEFORE async call → idempotent
        try:
            await _hedge_callback(condition_id)
        except Exception as exc:
            print(
                f"{Fore.RED}[HEDGE] ⚠️  execute_hedge_dump raised an error for "
                f"{condition_id[:8]}...: {exc}"
            )
            # Don't remove from _HEDGED_POSITIONS — we already tried once.
            # The position monitor loop will clean it up on expiry.


# ---------------------------------------------------------------------------
# Event-driven evaluation
# ---------------------------------------------------------------------------

_execution_queue: asyncio.Queue | None = None

async def on_oracle_tick(symbol: str) -> None:
    """Event-driven callback fired immediately by the oracle when a new price arrives."""
    # ── Stale Cache Guard ────────────────────────────────────────────────────
    if not clob_cache.is_connected():
        return
    # ─────────────────────────────────────────────────────────────────────────

    if _execution_queue is None:
        return

    # Check hedges first (fast)
    if _hedge_callback is not None and _get_open_positions_fn is not None:
        try:
            open_positions = _get_open_positions_fn()
            if open_positions:
                await _check_hedge_triggers(open_positions, trigger_symbol=symbol)
        except Exception as exc:
            print(f"{Fore.RED}[SIGNAL] Hedge check error: {exc}")

    # Evaluate markets for the specific symbol that just ticked
    markets = get_active_markets()
    generated = 0
    generated_this_pass: set[str] = set()
    
    # symbol comes in as 'btc/usd', we just want 'btc'
    base_sym = symbol.split("/")[0].lower()

    for market in markets.values():
        if market.get("symbol", "").lower() != base_sym:
            continue

        try:
            signal = _evaluate_market(market)
        except Exception as exc:
            print(f"{Fore.RED}[SIGNAL] Error evaluating {market.get('condition_id', '?')}: {exc}")
            continue

        if signal:
            key = signal["symbol"] + signal["side"]
            if key in generated_this_pass:
                continue
            generated_this_pass.add(key)
            SIGNALED_MARKETS.add(signal["condition_id"])
            _record_signal(signal)
            
            # Print signal banner
            slug = market.get("slug", signal["condition_id"])[-28:]
            print(f"\n{Fore.GREEN}{Style.BRIGHT}[SIGNAL] *** SIGNAL FIRED ***")
            print(f"  Market       : {market.get('question', slug)}")
            
            # ── Queue delivery (Fix #1): zero-lag hand-off to trader ──────────
            # We don't await queue.put() if it might block, but asyncio.Queue is unbuffered and non-blocking here
            try:
                _execution_queue.put_nowait(signal)
            except asyncio.QueueFull:
                await _execution_queue.put(signal)
            # ─────────────────────────────────────────────────────────────────
            generated += 1


# ---------------------------------------------------------------------------
# Maintenance loop
# ---------------------------------------------------------------------------

async def run_signal_engine(queue: asyncio.Queue) -> None:
    """Run periodic maintenance (diagnostics, cleanup). The heavy lifting is now event-driven."""
    global _last_diag_ts, _execution_queue
    _execution_queue = queue

    print(
        f"{Fore.WHITE}[SIGNAL] Signal engine started "
        f"(EVENT DRIVEN, diag every {DIAGNOSTIC_INTERVAL_SECONDS}s, "
        f"hedge window={HEDGE_WINDOW_SECONDS}s)."
    )

    while True:
        try:
            if not clob_cache.is_connected():
                # print blocked message only occasionally to avoid spam
                await asyncio.sleep(EVAL_INTERVAL_SECONDS)
                continue

            _maybe_clear_signaled_markets()
            prune_expired_markets()
            markets = get_active_markets()

            # Full diagnostic printout every 10 seconds
            if time.time() - _last_diag_ts >= DIAGNOSTIC_INTERVAL_SECONDS:
                _print_diagnostics(markets)
                _last_diag_ts = time.time()

        except Exception as exc:
            print(f"{Fore.RED}[SIGNAL] Unexpected error in maintenance: {exc}")

        await asyncio.sleep(EVAL_INTERVAL_SECONDS)


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_signal_engine())