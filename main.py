# main.py
"""Gulam — Polymarket 15-Min Oracle Bot entry point."""

import asyncio
import signal
import time
from datetime import datetime

from colorama import Fore, Style, init

from config import Config
from orderbook_cache import run_orderbook_cache
from oracle import run_oracle
from scanner import get_market_count, run_scanner
from signal_engine import (
    get_signal_stats,
    run_signal_engine,
    register_hedge_callback,
    register_positions_callback,
)

init(autoreset=True)

BANNER = r"""
 ██████╗ ██╗   ██╗██╗      █████╗ ███╗   ███╗
██╔════╝ ██║   ██║██║     ██╔══██╗████╗ ████║
██║  ███╗██║   ██║██║     ███████║██╔████╔██║
██║   ██║██║   ██║██║     ██╔══██║██║╚██╔╝██║
╚██████╔╝╚██████╔╝███████╗██║  ██║██║ ╚═╝ ██║
 ╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝
"""

STATS_INTERVAL_SECONDS = 60

# ---------------------------------------------------------------------------
# Mode-aware import: select trader module based on PAPER_TRADING flag
# ---------------------------------------------------------------------------

if Config.PAPER_TRADING:
    from paper_trader import (
        get_performance_summary,
        run_paper_trader as _run_trader,
        execute_hedge_dump as _trader_hedge_dump,
        get_open_positions as _trader_get_positions,
    )
    _TRADER_NAME = "paper_trader"
else:
    from live_trader import (  # type: ignore[no-redef]
        get_performance_summary,
        run_live_trader as _run_trader,
        execute_hedge_dump as _trader_hedge_dump,
        get_open_positions as _trader_get_positions,
    )
    _TRADER_NAME = "live_trader"

# Maps a task name to its coroutine factory for the Execution Engine group
_TASK_FACTORIES: dict[str, callable] = {
    "oracle": run_oracle,
    "scanner": run_scanner,
    "signal_engine": run_signal_engine,
    _TRADER_NAME: _run_trader,
}

# Register the escape-hatch callbacks with the signal engine.
# Done here (post-import) to avoid circular imports between signal_engine
# and the trader modules.  Both callbacks are injected once at process start.
register_hedge_callback(_trader_hedge_dump)
register_positions_callback(_trader_get_positions)



# ---------------------------------------------------------------------------
# Startup banner
# ---------------------------------------------------------------------------

def _print_banner() -> None:
    mode = "Paper Trading Mode" if Config.PAPER_TRADING else "*** LIVE MAINNET MODE ***"
    mode_color = Fore.GREEN if Config.PAPER_TRADING else Fore.RED
    print(f"{Fore.GREEN}{Style.BRIGHT}{BANNER}")
    print(f"{mode_color}{Style.BRIGHT}  Polymarket 15-Min Oracle Bot | {mode}")
    print(f"{Fore.GREEN}  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    Config.summary()


# ---------------------------------------------------------------------------
# Live stats display
# ---------------------------------------------------------------------------

def _print_live_stats() -> None:
    perf = get_performance_summary()
    stats = get_signal_stats()
    market_count = get_market_count()

    sign = "+" if perf["daily_profit"] >= 0 else ""
    now = datetime.now().strftime("%H:%M:%S")
    mode_tag = "[PAPER]" if Config.PAPER_TRADING else "[LIVE ★]"

    print(
        f"\n{Fore.CYAN}{'═' * 54}\n"
        f"{Fore.CYAN}{Style.BRIGHT}  GULAM {mode_tag}  —  {now}\n"
        f"{Fore.CYAN}{'─' * 54}\n"
        f"{Fore.CYAN}  Markets tracked   : {market_count}\n"
        f"{Fore.CYAN}  Signals today     : {stats['signals_today']}\n"
        f"{Fore.CYAN}  Capital           : ${perf['capital']:.2f}\n"
        f"{Fore.CYAN}  Daily P&L         : {sign}{perf['daily_profit']:.4f}\n"
        f"{Fore.CYAN}  Win rate          : {perf['win_rate'] * 100:.1f}%  "
        f"({perf['total_trades']} total trades)\n"
        f"{Fore.CYAN}  Losing trades     : {perf['loss_count']}\n"
        f"{Fore.CYAN}  Total lost        : -${perf['total_lost_usd']:.2f}\n"
        f"{Fore.CYAN}  Open positions    : {perf['open_positions']}\n"
        f"{Fore.CYAN}{'═' * 54}\n"
    )


# ---------------------------------------------------------------------------
# Supervised task wrapper — restarts a single crashed task
# ---------------------------------------------------------------------------

async def _supervised(name: str, factory: callable) -> None:
    """Run a coroutine factory and restart it on any exception."""
    while True:
        try:
            await factory()
        except asyncio.CancelledError:
            raise   # propagate cancellation — do not restart
        except Exception as exc:  # pylint: disable=broad-except
            print(
                f"\n{Fore.RED}{Style.BRIGHT}[MAIN] Task '{name}' crashed: "
                f"{type(exc).__name__} (message suppressed for security)\n"
                f"{Fore.RED}[MAIN] Restarting '{name}' in 5 seconds..."
            )
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Periodic stats task
# ---------------------------------------------------------------------------

async def _stats_loop() -> None:
    """Print live stats every 60 seconds."""
    while True:
        await asyncio.sleep(STATS_INTERVAL_SECONDS)
        try:
            _print_live_stats()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"{Fore.RED}[MAIN] Stats display error: {exc}")


# ---------------------------------------------------------------------------
# Shutdown display
# ---------------------------------------------------------------------------

def _print_shutdown_summary() -> None:
    try:
        perf = get_performance_summary()
        sign = "+" if perf["total_profit"] >= 0 else ""
        dsign = "+" if perf["daily_profit"] >= 0 else ""
        print(
            f"\n{Fore.CYAN}{Style.BRIGHT}{'═' * 54}\n"
            f"{Fore.CYAN}{Style.BRIGHT}  GULAM FINAL PERFORMANCE SUMMARY\n"
            f"{Fore.CYAN}{'─' * 54}\n"
            f"{Fore.CYAN}  Capital           : ${perf['capital']:.2f}\n"
            f"{Fore.CYAN}  Total P&L         : {sign}{perf['total_profit']:.4f} "
            f"({sign}{perf['total_return_pct'] * 100:.2f}%)\n"
            f"{Fore.CYAN}  Daily P&L         : {dsign}{perf['daily_profit']:.4f}\n"
            f"{Fore.CYAN}  Win Rate          : {perf['win_rate'] * 100:.1f}%\n"
            f"{Fore.CYAN}  Total Trades      : {perf['total_trades']} "
            f"({perf['daily_trades']} today)\n"
            f"{Fore.CYAN}  Losing trades     : {perf['loss_count']}\n"
            f"{Fore.CYAN}  Total lost        : -${perf['total_lost_usd']:.2f}\n"
            f"{Fore.CYAN}  Open Positions    : {perf['open_positions']}\n"
            f"{Fore.CYAN}{'═' * 54}\n"
        )
    except Exception:  # pylint: disable=broad-except
        pass


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def _run_orderbook_synchronizer() -> None:
    """Group A: Real-time Order Book Synchronizer via WebSocket."""
    await _supervised("orderbook_cache", run_orderbook_cache)


async def _run_execution_engine() -> None:
    """Group B: High-speed Signal / Execution Engine."""
    tasks = [
        asyncio.create_task(_supervised(name, factory), name=name)
        for name, factory in _TASK_FACTORIES.items()
    ]
    tasks.append(asyncio.create_task(_stats_loop(), name="stats"))
    await asyncio.gather(*tasks)


async def main() -> None:
    _print_banner()

    all_tasks = [
        asyncio.create_task(_run_orderbook_synchronizer(), name="orderbook_sync"),
        asyncio.create_task(_run_execution_engine(), name="execution_engine"),
    ]

    # Register SIGTERM handler so process managers (systemd, Docker) trigger
    # clean task cancellation instead of instant process kill
    def _on_sigterm() -> None:
        print(f"\n{Fore.YELLOW}[MAIN] SIGTERM received — initiating graceful shutdown...")
        for t in all_tasks:
            t.cancel()

    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
    except (NotImplementedError, OSError):
        pass  # Windows does not support add_signal_handler

    try:
        await asyncio.gather(*all_tasks)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n{Fore.CYAN}{Style.BRIGHT}[MAIN] Gulam shutting down...")
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
        _print_shutdown_summary()

        # Checkpoint and close the SQLite WAL journal cleanly
        # Prevents leaving a dangling -wal sidecar file that can confuse
        # future process starts or backup tools
        try:
            from paper_trader import _get_db
            _db = _get_db()
            _db.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            _db.close()
            print(f"{Fore.CYAN}[MAIN] SQLite WAL checkpointed and connection closed.")
        except Exception:  # pylint: disable=broad-except
            pass  # Never let DB cleanup prevent exit

        print(f"{Fore.CYAN}[MAIN] Goodbye.\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
