# main.py
"""Gulam — Polymarket 15-Min Oracle Bot entry point."""

import asyncio
import time
from datetime import datetime

from colorama import Fore, Style, init

from config import Config
from orderbook_cache import run_orderbook_cache
from oracle import run_oracle
from paper_trader import get_performance_summary, run_paper_trader
from scanner import get_market_count, run_scanner
from signal_engine import get_signal_stats, run_signal_engine

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

# Maps a task name to its coroutine factory for the Execution Engine group
_TASK_FACTORIES: dict[str, callable] = {
    "oracle": run_oracle,
    "scanner": run_scanner,
    "signal_engine": run_signal_engine,
    "paper_trader": run_paper_trader,
}


# ---------------------------------------------------------------------------
# Startup banner
# ---------------------------------------------------------------------------

def _print_banner() -> None:
    print(f"{Fore.GREEN}{Style.BRIGHT}{BANNER}")
    print(
        f"{Fore.GREEN}{Style.BRIGHT}"
        "  Polymarket 15-Min Oracle Bot | Paper Trading Mode"
    )
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

    print(
        f"\n{Fore.CYAN}{'═' * 54}\n"
        f"{Fore.CYAN}{Style.BRIGHT}  GULAM LIVE STATS  —  {now}\n"
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
                f"{type(exc).__name__}: {exc}\n"
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
    """Group B: High-speed Signal / Execution Engine (all existing bots)."""
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
        print(f"{Fore.CYAN}[MAIN] Goodbye.\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
