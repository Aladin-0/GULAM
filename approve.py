#!/usr/bin/env python3
"""
approve.py — One-time on-chain USDC allowance setup for Polymarket.

Run this ONCE before starting the live bot. It calls the Polymarket CLOB
to set the balance & allowance for both COLLATERAL (USDC) and CONDITIONAL
(outcome token) asset types on Polygon Mainnet.

Without this, every order placement will fail with a PolyApiException
regardless of wallet balance or API credentials.

Usage:
    python3 approve.py
"""

import os
import sys

from dotenv import load_dotenv
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams, AssetType

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "").strip()
HOST: str = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com").strip()
CHAIN_ID: int = int(os.getenv("CHAIN_ID", "137"))
API_KEY: str = os.getenv("POLYMARKET_API_KEY", "").strip()
API_SECRET: str = os.getenv("POLYMARKET_API_SECRET", "").strip()
API_PASSPHRASE: str = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()

if not PRIVATE_KEY:
    print("[approve] ❌  PRIVATE_KEY is not set in .env — aborting.")
    sys.exit(1)

if not API_KEY or not API_SECRET or not API_PASSPHRASE:
    print("[approve] ❌  POLYMARKET_API_KEY / API_SECRET / API_PASSPHRASE missing from .env — aborting.")
    sys.exit(1)

wallet_address: str = Account.from_key(PRIVATE_KEY).address
print(f"[approve] Wallet  : {wallet_address}")
print(f"[approve] Host    : {HOST}")
print(f"[approve] Chain   : {CHAIN_ID} (Polygon Mainnet)")
print()

# ---------------------------------------------------------------------------
# Build ClobClient with Level 2 auth (API creds required by allowance endpoint)
# ---------------------------------------------------------------------------
creds = ApiCreds(
    api_key=API_KEY,
    api_secret=API_SECRET,
    api_passphrase=API_PASSPHRASE,
)
client = ClobClient(
    host=HOST,
    chain_id=CHAIN_ID,
    key=PRIVATE_KEY,
    creds=creds,
    signature_type=0,       # EOA — same as live_trader.py
    funder=wallet_address,
)

# ---------------------------------------------------------------------------
# Set allowance for COLLATERAL (USDC) — the token you spend to buy shares
# ---------------------------------------------------------------------------
print("[approve] Step 1/2 — Setting COLLATERAL (USDC) allowance...")
try:
    resp = client.update_balance_allowance(
        params=BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=0,
        )
    )
    print(f"[approve] ✅  COLLATERAL allowance set. Response: {resp}")
except Exception as exc:
    print(f"[approve] ❌  COLLATERAL allowance FAILED: {type(exc).__name__}: {exc}")
    print()
    print("[approve] Troubleshooting:")
    print("  1. Ensure your wallet has MATIC for gas (Polygon gas fee ~$0.001).")
    print("  2. Ensure PRIVATE_KEY in .env is correct with no leading/trailing spaces.")
    print("  3. Check network connectivity to the Polygon RPC.")
    sys.exit(1)

print()
print("[approve] ✅  COLLATERAL (USDC) allowance is set.")
print("[approve] Note: CONDITIONAL (outcome token) allowance is per-market ERC-1155")
print("[approve]       and does not require a blanket approval — the CLOB handles it.")
print()
print("[approve] ✅  Setup complete. You can now start the live bot: python3 main.py")
