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

    PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "")
    POLYMARKET_HOST: str = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    CHAIN_ID: int = int(os.getenv("CHAIN_ID", "137"))
    INITIAL_CAPITAL: float = float(os.getenv("INITIAL_CAPITAL", "10.0"))
    MAX_POSITION_SIZE_PCT: float = float(os.getenv("MAX_POSITION_SIZE_PCT", "0.25"))
    MAX_EXECUTION_TIME_SECONDS: int = int(os.getenv("MAX_EXECUTION_TIME_SECONDS", "90"))
    BASE_GAP_BPS: float = float(os.getenv("BASE_GAP_BPS", "6.0"))
    MAX_TOKEN_PRICE: float = float(os.getenv("MAX_TOKEN_PRICE", "0.985"))
    DAILY_LOSS_LIMIT_PCT: float = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.02"))
    PAPER_TRADING: bool = _str_to_bool(os.getenv("PAPER_TRADING"))
    SIMULATED_GAS_FEE_USD: float = 0.05

    def summary(self) -> None:
        """Print all config values to terminal, masking the private key."""
        print("\n" + "=" * 50)
        print(f"{Style.BRIGHT}CONFIGURATION SUMMARY")
        print("=" * 50)

        # Validate and display PRIVATE_KEY
        if not self.PRIVATE_KEY:
            print(f"{Fore.RED}PRIVATE_KEY: MISSING (required)")
            print("\nExiting: PRIVATE_KEY is required but not set.")
            sys.exit(1)
        else:
            masked_key = "*" * (len(self.PRIVATE_KEY) - 4) + self.PRIVATE_KEY[-4:]
            print(f"{Fore.GREEN}PRIVATE_KEY: {masked_key}")

        print(f"{Fore.GREEN}POLYMARKET_HOST: {self.POLYMARKET_HOST}")
        print(f"{Fore.GREEN}CHAIN_ID: {self.CHAIN_ID}")
        print(f"{Fore.GREEN}INITIAL_CAPITAL: {self.INITIAL_CAPITAL}")
        print(f"{Fore.GREEN}MAX_POSITION_SIZE_PCT: {self.MAX_POSITION_SIZE_PCT}")
        print(f"{Fore.GREEN}MAX_EXECUTION_TIME_SECONDS: {self.MAX_EXECUTION_TIME_SECONDS}")
        print(f"{Fore.GREEN}BASE_GAP_BPS: {self.BASE_GAP_BPS}")
        print(f"{Fore.GREEN}MAX_TOKEN_PRICE: {self.MAX_TOKEN_PRICE}")
        print(f"{Fore.GREEN}DAILY_LOSS_LIMIT_PCT: {self.DAILY_LOSS_LIMIT_PCT}")
        print(f"{Fore.GREEN}PAPER_TRADING: {self.PAPER_TRADING}")
        print(f"{Fore.GREEN}SIMULATED_GAS_FEE_USD: {self.SIMULATED_GAS_FEE_USD}")
        print("=" * 50 + "\n")


# Single Config instance for import across the project
Config = _Config()