# config.py
"""Application configuration loaded from environment variables."""
import os
import sys

from colorama import Fore, Style, init
from dotenv import load_dotenv

# Initialize colorama
init(autoreset=True)

# Load environment variables from .env file
load_dotenv()


def _str_to_bool(value: str | None) -> bool:
    """Convert string value to boolean."""
    if value is None:
        return False
    return value.lower() in ("true", "1", "yes", "on")


class _Config:
    """Application configuration singleton."""

    # --- Credentials & Network ---
    # .strip() on all secrets: trailing whitespace from clipboard paste causes
    # silent HMAC-SHA256 signature mismatch → every CLOB order rejected with 401
    PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "").strip()
    POLYMARKET_HOST: str = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com").strip()
    CHAIN_ID: int = int(os.getenv("CHAIN_ID", "137"))

    # --- Polymarket Builder API ---
    POLYMARKET_API_KEY: str = os.getenv("POLYMARKET_API_KEY", "").strip()
    POLYMARKET_API_SECRET: str = os.getenv("POLYMARKET_API_SECRET", "").strip()
    POLYMARKET_API_PASSPHRASE: str = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()

    # --- Capital & Position Sizing ---
    INITIAL_CAPITAL: float = float(os.getenv("INITIAL_CAPITAL", "10.0"))
    MAX_POSITION_SIZE_PCT: float = float(os.getenv("MAX_POSITION_SIZE_PCT", "0.20"))
    MIN_ORDER_SIZE_USD: float = float(os.getenv("MIN_ORDER_SIZE_USD", "1.0"))

    # --- Execution Timing ---
    MAX_EXECUTION_TIME_SECONDS: int = int(os.getenv("MAX_EXECUTION_TIME_SECONDS", "90"))
    MAX_POSITION_AGE_SECONDS: int = int(os.getenv("MAX_POSITION_AGE_SECONDS", "900"))

    # --- Signal Filter ---
    BASE_GAP_BPS: float = float(os.getenv("BASE_GAP_BPS", "100.0"))
    MAX_TOKEN_PRICE: float = float(os.getenv("MAX_TOKEN_PRICE", "0.75"))
    MIN_PRICE_MOVE_PCT: float = float(os.getenv("MIN_PRICE_MOVE_PCT", "0.25"))
    MIN_TIME_REMAINING_MINUTES: int = int(os.getenv("MIN_TIME_REMAINING_MINUTES", "5"))

    # --- Dynamic Risk Gate Bounds ---
    # NOTE: bounds must be >= BASE_GAP_BPS/10000 or the cap will override it.
    # With BASE_GAP_BPS=100 (1.0%), raw dynamic_need peaks at ~1.4%.
    # Ceiling raised to 0.02 (2%) so the gap calculation is never capped out.
    MIN_DYNAMIC_NEED_PCT: float = float(os.getenv("MIN_DYNAMIC_NEED_PCT", "0.0025"))
    MAX_DYNAMIC_NEED_PCT: float = float(os.getenv("MAX_DYNAMIC_NEED_PCT", "0.0200"))

    # --- Risk Management ---
    DAILY_LOSS_LIMIT_PCT: float = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.02"))

    # --- Gas / Fees ---
    SIMULATED_GAS_FEE_USD: float = float(os.getenv("SIMULATED_GAS_FEE_USD", "0.05"))
    # Polymarket CLOB maker/taker fee applied on order fill (both sides of round-trip)
    CLOB_FEE_PCT: float = float(os.getenv("CLOB_FEE_PCT", "0.0002"))

    # --- Mode ---
    PAPER_TRADING: bool = _str_to_bool(os.getenv("PAPER_TRADING"))

    def summary(self) -> None:
        """Print all config values to terminal, masking secrets."""
        print("\n" + "=" * 56)
        print(f"{Style.BRIGHT}CONFIGURATION SUMMARY")
        print("=" * 56)

        # ---- Credentials ----
        if not self.PRIVATE_KEY:
            print(f"{Fore.RED}PRIVATE_KEY         : MISSING (required)")
            print("\nExiting: PRIVATE_KEY is required but not set.")
            sys.exit(1)
        else:
            masked_key = "*" * (len(self.PRIVATE_KEY) - 4) + self.PRIVATE_KEY[-4:]
            print(f"{Fore.GREEN}PRIVATE_KEY         : {masked_key}")

        # Polymarket API creds
        if self.POLYMARKET_API_KEY:
            print(f"{Fore.GREEN}API_KEY             : {self.POLYMARKET_API_KEY[:8]}...")
        else:
            print(f"{Fore.YELLOW}API_KEY             : NOT SET (live trading will fail)")

        # ---- Network ----
        print(f"{Fore.GREEN}POLYMARKET_HOST     : {self.POLYMARKET_HOST}")
        print(f"{Fore.GREEN}CHAIN_ID            : {self.CHAIN_ID}")

        # ---- Capital ----
        print(f"{Fore.GREEN}INITIAL_CAPITAL     : ${self.INITIAL_CAPITAL:.2f}")
        print(f"{Fore.GREEN}MAX_POSITION_SIZE   : {self.MAX_POSITION_SIZE_PCT * 100:.1f}%")
        print(f"{Fore.GREEN}MIN_ORDER_SIZE_USD  : ${self.MIN_ORDER_SIZE_USD:.2f}")

        # ---- Execution ----
        print(f"{Fore.GREEN}MAX_EXEC_TIME       : {self.MAX_EXECUTION_TIME_SECONDS}s")
        print(f"{Fore.GREEN}MAX_POSITION_AGE    : {self.MAX_POSITION_AGE_SECONDS}s")

        # ---- Signal ----
        print(f"{Fore.GREEN}BASE_GAP_BPS        : {self.BASE_GAP_BPS} bps")
        print(f"{Fore.GREEN}MAX_TOKEN_PRICE     : {self.MAX_TOKEN_PRICE}")
        print(f"{Fore.GREEN}MIN_PRICE_MOVE_PCT  : {self.MIN_PRICE_MOVE_PCT}%")
        print(f"{Fore.GREEN}MIN_TIME_LEFT       : {self.MIN_TIME_REMAINING_MINUTES} min")
        print(
            f"{Fore.GREEN}DYNAMIC_NEED_RANGE  : "
            f"{self.MIN_DYNAMIC_NEED_PCT * 100:.4f}% – "
            f"{self.MAX_DYNAMIC_NEED_PCT * 100:.4f}%"
        )

        # ---- Risk ----
        print(f"{Fore.GREEN}DAILY_LOSS_LIMIT    : {self.DAILY_LOSS_LIMIT_PCT * 100:.1f}%")
        print(f"{Fore.GREEN}SIMULATED_GAS_FEE   : ${self.SIMULATED_GAS_FEE_USD:.3f}")
        print(f"{Fore.GREEN}CLOB_FEE_PCT        : {self.CLOB_FEE_PCT * 100:.4f}% (live round-trip fee)")

        # ---- Mode ----
        mode_color = Fore.YELLOW if self.PAPER_TRADING else Fore.RED
        mode_label = "PAPER TRADING" if self.PAPER_TRADING else "*** LIVE MAINNET ***"
        print(f"{mode_color}{Style.BRIGHT}MODE                : {mode_label}")

        print("=" * 56 + "\n")


# Single Config instance for import across the project
Config = _Config()