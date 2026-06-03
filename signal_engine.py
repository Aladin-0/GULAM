# signal_engine.py
"""Signal engine — 55-second oracle lag / token mispricing strategy.

Edge: Chainlink oracle updates instantly; Polymarket token prices lag ~55s.
When oracle is clearly above/below PTB, the correct-side token is still cheap.
We enter before the market catches up.

PTB = LATEST_PRICES[oracle_key]["open_price"]  (Chainlink price at period start)
"""

import asyncio
import time
from datetime import date, datetime, timezone

from colorama import Fore, Style, init

from config import Config
from oracle import LATEST_PRICES
from orderbook_cache import validate_liquidity
from scanner import get_active_markets, prune_expired_markets

# Initialize colorama
init(autoreset=True)

EVAL_INTERVAL_SECONDS = 1
SIGNALED_MARKETS_CLEAR_INTERVAL = 900   # clear every 15 minutes (one market period)
DIAGNOSTIC_INTERVAL_SECONDS = 10        # print full market status every 10s

# Set of condition_ids already signaled this hour
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

    # Only trade in the final MAX_EXECUTION_TIME_SECONDS window
    if not (0 < t_left_s <= Config.MAX_EXECUTION_TIME_SECONDS):
        return None

    move_pct: float = (current_price - price_to_beat) / price_to_beat

    # Mispricing edge: oracle says one direction but token price is still cheap
    if current_price >= price_to_beat:
        correct_side = "UP"
        token_id: str = market["up_token_id"]
        token_price: float = market["up_price"]
    else:
        correct_side = "DOWN"
        token_id = market["down_token_id"]
        token_price = market["down_price"]

    # Dynamic risk engine: scales with token price, bounded [0.06%, 0.12%]
    risk_multiplier = 1.0 + (token_price * 0.4)
    base = Config.BASE_GAP_BPS / 10000.0
    dynamic_need_pct: float = base * risk_multiplier
    dynamic_need_pct = max(0.0006, min(dynamic_need_pct, 0.0012))

    c1 = abs(move_pct) > dynamic_need_pct
    c2 = 0 < t_left_s <= Config.MAX_EXECUTION_TIME_SECONDS
    c3 = token_price < Config.MAX_TOKEN_PRICE  # market disagrees with oracle — strong edge

    if not (c1 and c2 and c3):
        return None

    # --- Microsecond liquidity gate: RAM-only order book validation ---
    required_capital = Config.INITIAL_CAPITAL * Config.MAX_POSITION_SIZE_PCT
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
                side = "UP"
                token_price = mkt.get("up_price", 0.0)
            else:
                side = "DOWN"
                token_price = mkt.get("down_price", 0.0)

            risk_multiplier = 1.0 + (token_price * 0.4)
            base = Config.BASE_GAP_BPS / 10000.0
            dynamic_need_pct: float = base * risk_multiplier
            dynamic_need_pct = max(0.0006, min(dynamic_need_pct, 0.0012))

            c1 = abs(move) > dynamic_need_pct
            c2 = 0 < t_left_s <= Config.MAX_EXECUTION_TIME_SECONDS
            c3 = token_price < Config.MAX_TOKEN_PRICE

            # Liquidity check for diagnostics
            required_capital = Config.INITIAL_CAPITAL * Config.MAX_POSITION_SIZE_PCT
            is_liquid, available_value = validate_liquidity(
                mkt.get("up_token_id" if side == "UP" else "down_token_id", ""),
                "BUY", token_price, required_capital
            )
            c4 = is_liquid

            c1s = f"{Fore.GREEN}C1✓{Fore.WHITE}" if c1 else f"{Fore.RED}C1✗(move={move*100:.3f}%<need={dynamic_need_pct*100:.4f}% r={risk_multiplier:.1f}x){Fore.WHITE}"
            c2s = f"{Fore.GREEN}C2✓{Fore.WHITE}" if c2 else f"{Fore.RED}C2✗(t={t_left_s:.0f}s not in 0-{Config.MAX_EXECUTION_TIME_SECONDS}s){Fore.WHITE}"
            c3s = f"{Fore.GREEN}C3✓{Fore.WHITE}" if c3 else f"{Fore.RED}C3✗(token={token_price:.3f}>{Config.MAX_TOKEN_PRICE}){Fore.WHITE}"
            c4s = f"{Fore.GREEN}LIQ✓${available_value:.0f}{Fore.WHITE}" if c4 else f"{Fore.RED}LIQ✗${available_value:.0f}<{required_capital:.0f}{Fore.WHITE}"

            slug = mkt.get("slug", cid)[-28:]
            already = " [already signaled]" if cid in SIGNALED_MARKETS else ""
            print(
                f"{Fore.WHITE}  {slug:<30} {side:>4} {token_price:>6.3f} {t_left_s:>5.0f}s  "
                f"{c1s} {c2s} {c3s} {c4s}{already}"
            )
    else:
        print(f"{Fore.YELLOW}  No active markets being tracked.")

    print(f"{Fore.WHITE}{'─' * 70}\n")


# ---------------------------------------------------------------------------
# Evaluation loop
# ---------------------------------------------------------------------------

async def _evaluate_once(markets: dict) -> int:
    """One evaluation pass. Returns signals generated."""
    generated = 0
    generated_this_pass: set[str] = set()
    for market in markets.values():
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
            _emit_signal(signal)
            generated += 1
    return generated


async def run_signal_engine() -> None:
    """Evaluate all active markets every second. Public coroutine for main.py."""
    global _last_diag_ts

    print(f"{Fore.WHITE}[SIGNAL] Signal engine started (eval every {EVAL_INTERVAL_SECONDS}s, diag every {DIAGNOSTIC_INTERVAL_SECONDS}s).")

    while True:
        try:
            _maybe_clear_signaled_markets()
            prune_expired_markets()
            markets = get_active_markets()

            # Full diagnostic printout every 10 seconds
            if time.time() - _last_diag_ts >= DIAGNOSTIC_INTERVAL_SECONDS:
                _print_diagnostics(markets)
                _last_diag_ts = time.time()

            generated = await _evaluate_once(markets)

        except Exception as exc:
            print(f"{Fore.RED}[SIGNAL] Unexpected error: {exc}")

        await asyncio.sleep(EVAL_INTERVAL_SECONDS)


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_signal_engine())