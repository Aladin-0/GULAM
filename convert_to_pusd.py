#!/usr/bin/env python3
"""
convert_to_pusd.py
------------------
One-time script: converts your entire USDC.e balance to pUSD
via the Polymarket Collateral Onramp contract on Polygon.

Contracts (Polygon Mainnet, chain_id=137):
  USDC.e (old collateral)  : 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
  pUSD  (new collateral V2): 0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB
  Collateral Onramp        : 0x93070a84521F5B8ee40348705f15c1363677b102

Flow:
  1. Check USDC.e balance
  2. Approve Onramp to spend your USDC.e  (if not already approved)
  3. Call onramp.wrap(amount)  → receive pUSD 1:1
  4. Print final pUSD balance
"""

import os
import sys
from dotenv import load_dotenv
from web3 import Web3
from eth_account import Account

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
load_dotenv()

PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
if not PRIVATE_KEY:
    sys.exit("ERROR: PRIVATE_KEY not found in .env")

# Try multiple RPCs — first one to connect wins
POLYGON_RPCS = [
    os.getenv("POLYGON_RPC_URL", ""),               # override via .env
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
    "https://polygon-mainnet.public.blastapi.io",
    "https://endpoints.omniatech.io/v1/matic/mainnet/public",
    "https://polygon-rpc.com",
    "https://1rpc.io/matic",
]

# Polygon contract addresses
USDC_E_ADDRESS   = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
PUSD_ADDRESS     = Web3.to_checksum_address("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")
ONRAMP_ADDRESS   = Web3.to_checksum_address("0x93070a84521F5B8ee40348705f15c1363677b102")

# Minimal ABIs — only the functions we need
ERC20_ABI = [
    {"inputs": [{"name": "account", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [],
     "name": "decimals", "outputs": [{"name": "", "type": "uint8"}],
     "stateMutability": "view", "type": "function"},
]

ONRAMP_ABI = [
    # wrap(uint256 amount) — deposit USDC.e, receive pUSD 1:1
    {"inputs": [{"name": "amount", "type": "uint256"}],
     "name": "wrap", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
]

# --------------------------------------------------------------------------
# Connect — try each RPC until one works
# --------------------------------------------------------------------------
print(f"\nConnecting to Polygon...")
w3 = None
for rpc_url in POLYGON_RPCS:
    if not rpc_url:
        continue
    try:
        _w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
        if _w3.is_connected():
            block = _w3.eth.block_number
            print(f"  ✓ {rpc_url}  (block {block})")
            w3 = _w3
            break
        else:
            print(f"  ✗ {rpc_url}")
    except Exception as e:
        print(f"  ✗ {rpc_url} — {str(e)[:70]}")

if w3 is None:
    print("""
ERROR: All Polygon RPCs failed. Your server may be restricting outbound connections.

Fix: Add a private RPC key to .env:
  Get a free key at https://alchemy.com → Create App → Polygon Mainnet
  Then add to .env:
    POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY

Then re-run: python3 convert_to_pusd.py
""")
    sys.exit(1)

acct = Account.from_key(PRIVATE_KEY)
wallet = acct.address
print(f"Wallet     : {wallet}")

# --------------------------------------------------------------------------
# Check balances
# --------------------------------------------------------------------------
usdc_e  = w3.eth.contract(address=USDC_E_ADDRESS, abi=ERC20_ABI)
pusd    = w3.eth.contract(address=PUSD_ADDRESS,   abi=ERC20_ABI)
onramp  = w3.eth.contract(address=ONRAMP_ADDRESS, abi=ONRAMP_ABI)

usdc_balance_raw  = usdc_e.functions.balanceOf(wallet).call()
pusd_balance_raw  = pusd.functions.balanceOf(wallet).call()
pol_balance_wei   = w3.eth.get_balance(wallet)

DECIMALS = 6
usdc_balance = usdc_balance_raw / 10**DECIMALS
pusd_balance = pusd_balance_raw / 10**DECIMALS
pol_balance  = pol_balance_wei  / 10**18

print(f"\nCurrent balances:")
print(f"  USDC.e : ${usdc_balance:.6f}")
print(f"  pUSD   : ${pusd_balance:.6f}")
print(f"  POL    : {pol_balance:.6f}  (gas token)")

if usdc_balance_raw == 0:
    print("\nNo USDC.e balance to convert.")
    print(f"Current pUSD balance: ${pusd_balance:.6f}")
    sys.exit(0)

if pol_balance < 0.01:
    print(f"\nWARNING: Very low POL balance ({pol_balance:.6f}). You may not have enough for gas.")
    print("Continuing anyway — if it fails, send some POL to your wallet first.")

amount_to_wrap = usdc_balance_raw  # convert 100% of USDC.e
print(f"\nWill convert: ${usdc_balance:.6f} USDC.e → ${usdc_balance:.6f} pUSD (1:1)")
print()

# --------------------------------------------------------------------------
# Step 1: Approve Onramp to spend USDC.e
# --------------------------------------------------------------------------
current_allowance = usdc_e.functions.allowance(wallet, ONRAMP_ADDRESS).call()

if current_allowance < amount_to_wrap:
    print(f"[1/3] Approving Onramp to spend {usdc_balance:.6f} USDC.e ...")
    nonce = w3.eth.get_transaction_count(wallet)
    gas_price = w3.eth.gas_price

    approve_tx = usdc_e.functions.approve(
        ONRAMP_ADDRESS,
        amount_to_wrap
    ).build_transaction({
        "from":     wallet,
        "nonce":    nonce,
        "gasPrice": int(gas_price * 1.2),   # 20% tip to ensure fast inclusion
        "chainId":  137,
    })

    # Estimate gas
    try:
        approve_tx["gas"] = w3.eth.estimate_gas(approve_tx)
    except Exception as e:
        approve_tx["gas"] = 80_000  # safe fallback

    signed_approve = acct.sign_transaction(approve_tx)
    tx_hash = w3.eth.send_raw_transaction(signed_approve.raw_transaction)
    print(f"    Approve TX sent: {tx_hash.hex()}")
    print(f"    Waiting for confirmation...")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt["status"] != 1:
        sys.exit(f"ERROR: Approve transaction failed! Hash: {tx_hash.hex()}")
    print(f"    ✓ Approved  (block {receipt['blockNumber']})")
else:
    print(f"[1/3] Allowance already sufficient (${current_allowance / 10**DECIMALS:.6f}) — skipping approve.")

# --------------------------------------------------------------------------
# Step 2: Call onramp.wrap(amount)
# --------------------------------------------------------------------------
print(f"\n[2/3] Wrapping {usdc_balance:.6f} USDC.e → pUSD ...")
nonce = w3.eth.get_transaction_count(wallet)
gas_price = w3.eth.gas_price

wrap_tx = onramp.functions.wrap(amount_to_wrap).build_transaction({
    "from":     wallet,
    "nonce":    nonce,
    "gasPrice": int(gas_price * 1.2),
    "chainId":  137,
})

try:
    wrap_tx["gas"] = w3.eth.estimate_gas(wrap_tx)
except Exception as e:
    print(f"    Gas estimate failed ({e}) — using 150,000 fallback")
    wrap_tx["gas"] = 150_000

signed_wrap = acct.sign_transaction(wrap_tx)
tx_hash = w3.eth.send_raw_transaction(signed_wrap.raw_transaction)
print(f"    Wrap TX sent: {tx_hash.hex()}")
print(f"    Polygonscan : https://polygonscan.com/tx/{tx_hash.hex()}")
print(f"    Waiting for confirmation (up to 2 min)...")

receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
if receipt["status"] != 1:
    sys.exit(f"ERROR: Wrap transaction failed! Hash: {tx_hash.hex()}")
print(f"    ✓ Wrapped!  (block {receipt['blockNumber']}, gas used: {receipt['gasUsed']})")

# --------------------------------------------------------------------------
# Step 3: Print final balances
# --------------------------------------------------------------------------
print(f"\n[3/3] Final balances:")
usdc_after = usdc_e.functions.balanceOf(wallet).call() / 10**DECIMALS
pusd_after = pusd.functions.balanceOf(wallet).call()  / 10**DECIMALS
print(f"  USDC.e : ${usdc_after:.6f}  (was ${usdc_balance:.6f})")
print(f"  pUSD   : ${pusd_after:.6f}  (was ${pusd_balance:.6f})")

print(f"""
══════════════════════════════════════════════════════
✓  Conversion complete.
   Your wallet now has ${pusd_after:.2f} pUSD — ready for V2 trading.

   Next steps:
   1. Run Phase 3 code upgrade (V2 SDK in live_trader.py)
   2. Re-derive API credentials with V2 client
   3. Restart GULAM bot
══════════════════════════════════════════════════════
""")
