#!/usr/bin/env python3
"""
approve.py — V2 Pre-flight: on-chain allowance + internal balance sync.

Run this ONCE before starting the live bot. It:
  1. Checks on-chain pUSD token allowance for the V2 exchange contracts.
  2. Calls update_balance_allowance (COLLATERAL + CONDITIONAL) to sync
     the CLOB's internal ledger with your proxy wallet balance.
  3. Reports your internal exchange balance.
  4. Prints clear instructions if manual deposit action is still needed.

Polymarket V2 uses a PROXY WALLET flow:
  • Your EOA (private key) owns a Polymarket Proxy wallet on-chain.
  • Funds deposited via the website live in that proxy address.
  • Orders are signed by the EOA but executed from the proxy balance.
  • This script syncs the exchange's internal ledger to reflect that balance.

Usage:
    python3 approve.py
"""

import os
import sys

from dotenv import load_dotenv
from eth_account import Account
from py_clob_client_v2 import ClobClient, ApiCreds
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "").strip()
HOST: str        = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com").strip()
CHAIN_ID: int    = int(os.getenv("CHAIN_ID", "137"))
API_KEY: str     = os.getenv("POLYMARKET_API_KEY", "").strip()
API_SECRET: str  = os.getenv("POLYMARKET_API_SECRET", "").strip()
API_PASS: str    = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()

if not PRIVATE_KEY:
    print("[approve] ❌  PRIVATE_KEY is not set in .env — aborting.")
    sys.exit(1)

if not API_KEY or not API_SECRET or not API_PASS:
    print("[approve] ❌  API credentials missing from .env — aborting.")
    sys.exit(1)

wallet_address: str = Account.from_key(PRIVATE_KEY).address
print(f"\n[approve] Wallet  : {wallet_address}")
print(f"[approve] Host    : {HOST}")
print(f"[approve] Chain   : {CHAIN_ID} (Polygon Mainnet)\n")

# ---------------------------------------------------------------------------
# Build V2 ClobClient — POLY_PROXY (signature_type=1)
# The EOA signs on behalf of the Polymarket Proxy deposit wallet.
# ---------------------------------------------------------------------------
creds = ApiCreds(
    api_key=API_KEY,
    api_secret=API_SECRET,
    api_passphrase=API_PASS,
)
client = ClobClient(
    host=HOST,
    chain_id=CHAIN_ID,
    key=PRIVATE_KEY,
    creds=creds,
    signature_type=1,   # POLY_PROXY
)

# ---------------------------------------------------------------------------
# Step 1 — Connectivity check
# ---------------------------------------------------------------------------
print("[approve] ── Step 1: CLOB connectivity ──────────────────────────────")
try:
    ok = client.get_ok()
    print(f"[approve]   CLOB health: {ok}")
except Exception as e:
    print(f"[approve] ❌  Cannot reach CLOB: {e}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Step 2 — Check + sync COLLATERAL (pUSD) balance
# ---------------------------------------------------------------------------
print("\n[approve] ── Step 2: Collateral (pUSD) balance sync ─────────────────")
try:
    before = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    raw_bal    = float(before.get("balance",   0) or 0)
    raw_allow  = float(before.get("allowance", 0) or 0)
    DECIMALS   = 1_000_000
    bal_usd    = raw_bal   / DECIMALS
    allow_usd  = raw_allow / DECIMALS
    print(f"[approve]   Before sync — balance: ${bal_usd:.4f}  allowance: ${allow_usd:.4f}")
except Exception as e:
    print(f"[approve] ⚠️  Could not read COLLATERAL balance: {e}")
    bal_usd = -1.0

try:
    result = client.update_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    after_bal   = float((result or {}).get("balance",   0) or 0) / DECIMALS
    after_allow = float((result or {}).get("allowance", 0) or 0) / DECIMALS
    print(f"[approve]   After  sync — balance: ${after_bal:.4f}  allowance: ${after_allow:.4f}")
    if after_bal > 0:
        print(f"[approve]   ✅ COLLATERAL balance confirmed: ${after_bal:.4f} pUSD available for trading.")
    else:
        print(f"[approve]   ⚠️  COLLATERAL balance is 0 after sync.")
except Exception as e:
    print(f"[approve]   ⚠️  update_balance_allowance (COLLATERAL) returned: {e}")
    after_bal = 0.0

# ---------------------------------------------------------------------------
# Step 3 — Sync CONDITIONAL (outcome token) allowance
# ---------------------------------------------------------------------------
print("\n[approve] ── Step 3: Conditional token allowance sync ───────────────")
try:
    result_cond = client.update_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL)
    )
    print(f"[approve]   ✅ CONDITIONAL allowance synced: {result_cond}")
except Exception as e:
    print(f"[approve]   ⚠️  update_balance_allowance (CONDITIONAL) returned: {e}")

# ---------------------------------------------------------------------------
# Step 4 — Final status and operator instructions
# ---------------------------------------------------------------------------
print("\n[approve] ── Step 4: Final status ───────────────────────────────────")

if after_bal > 0:
    print(f"""
[approve] ✅  ALL SYSTEMS GO — internal balance: ${after_bal:.4f} pUSD

  Your proxy wallet has funds and allowances are set.
  You can now start the bot:

      python3 main.py
""")
else:
    print(f"""
[approve] ⛔  INTERNAL BALANCE IS 0 — bot will not execute orders.

  Your wallet {wallet_address} has 0 pUSD in the Polymarket exchange ledger.

  The Polymarket V2 "deposit wallet flow" requires:
  ┌──────────────────────────────────────────────────────────────────┐
  │  MANUAL STEP REQUIRED (one-time, done via the website)           │
  │                                                                  │
  │  1. Go to https://polymarket.com                                 │
  │  2. Connect your wallet ({wallet_address[:20]}...)    │
  │  3. Click "Deposit" (top right button)                           │
  │  4. Deposit USDC — it will be automatically converted to pUSD    │
  │     and credited to your internal trading balance.               │
  │  5. Wait ~30 seconds, then re-run this script:                   │
  │       python3 approve.py                                         │
  │  6. Once balance > 0 appears, start the bot:                     │
  │       python3 main.py                                            │
  └──────────────────────────────────────────────────────────────────┘

  Note: Your on-chain EOA wallet may have USDC/POL but those are NOT
  the same as your Polymarket internal trading balance. You must
  deposit through the Polymarket interface to fund the proxy wallet.
""")
    sys.exit(1)
