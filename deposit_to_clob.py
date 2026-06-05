#!/usr/bin/env python3
"""
deposit_to_clob.py
──────────────────
One-time setup: converts your EOA's native USDC into pUSD and deposits
it directly into your Polymarket V2 proxy wallet (counterfactual address),
then syncs the CLOB internal ledger so the bot can trade.

Steps performed:
  1. Derive your proxy wallet address from the V2 Exchange contract
  2. Approve native USDC for the pUSD wrapper contract
  3. Call pUSD.depositFor(proxy_wallet, amount) to mint pUSD there directly
  4. Set POLYMARKET_PROXY_WALLET in .env
  5. Call CLOB update_balance_allowance to sync the ledger
  6. Verify the CLOB now reports balance > 0

Usage:
    python3 deposit_to_clob.py
"""
import os, sys
from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
if not PRIVATE_KEY:
    sys.exit("ERROR: PRIVATE_KEY not set in .env")

EOA  = Web3.to_checksum_address(Account.from_key(PRIVATE_KEY).address)
PUSD = Web3.to_checksum_address("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")
USDC = Web3.to_checksum_address("0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359")  # native
EX_V2  = Web3.to_checksum_address("0xE111180000d2663C0091e4f400237545B87B996B")
NEG_EX = Web3.to_checksum_address("0xe2222d279d744050d28e00520010520000310F59")
MAX_UINT = 2**256 - 1

RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://rpc.ankr.com/polygon",
]

# ── Connect ──────────────────────────────────────────────────────────────────
print(f"\nEOA: {EOA}\n")
w3 = None
for rpc in RPCS:
    try:
        _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 12}))
        if _w3.is_connected():
            print(f"Connected: {rpc}  (block {_w3.eth.block_number})\n")
            w3 = _w3
            break
    except Exception:
        pass
if not w3:
    sys.exit("Cannot connect to Polygon RPC")

# ── ABIs ─────────────────────────────────────────────────────────────────────
ERC20_ABI = [
    {"inputs":[{"name":"a","type":"address"}],"name":"balanceOf","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"name":"s","type":"address"},{"name":"a","type":"uint256"}],"name":"approve","outputs":[{"type":"bool"}],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"name":"o","type":"address"},{"name":"s","type":"address"}],"name":"allowance","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"},
]
PUSD_ABI = ERC20_ABI + [
    # pUSD wrapper: takes USDC, mints pUSD 1:1
    {"inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"name":"depositFor","outputs":[{"type":"bool"}],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"name":"amount","type":"uint256"}],"name":"deposit","outputs":[{"type":"bool"}],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[],"name":"underlying","outputs":[{"type":"address"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"asset","outputs":[{"type":"address"}],"stateMutability":"view","type":"function"},
]
EX_ABI = [
    {"inputs":[{"name":"_addr","type":"address"}],"name":"getProxyWalletAddress","outputs":[{"type":"address"}],"stateMutability":"view","type":"function"},
]

usdc = w3.eth.contract(address=USDC, abi=ERC20_ABI)
pusd = w3.eth.contract(address=PUSD, abi=PUSD_ABI)
ex   = w3.eth.contract(address=EX_V2, abi=EX_ABI)

# ── Step 1: Get proxy wallet address ─────────────────────────────────────────
print("── Step 1: Proxy wallet address ─────────────────────────────────────")
proxy = ex.functions.getProxyWalletAddress(EOA).call()
print(f"  Proxy wallet : {proxy}\n")

# ── Step 2: Check current balances ───────────────────────────────────────────
print("── Step 2: Current balances ─────────────────────────────────────────")
usdc_bal = usdc.functions.balanceOf(EOA).call()
pusd_at_proxy = pusd.functions.balanceOf(proxy).call()
pol_bal = w3.eth.get_balance(EOA)
print(f"  EOA USDC         : ${usdc_bal/1e6:.4f}")
print(f"  Proxy pUSD       : ${pusd_at_proxy/1e6:.4f}")
print(f"  POL (gas)        : {pol_bal/1e18:.4f}")

if usdc_bal == 0:
    sys.exit("\n❌  No USDC in EOA. Deposit USDC to your MetaMask wallet first.")

# How much to deposit — leave $1 buffer, use at most $19
deposit_usdc = min(usdc_bal - 1_000_000, usdc_bal)  # keep $1 spare
if deposit_usdc <= 0:
    sys.exit("Not enough USDC (need > $1)")

print(f"\n  Will deposit : ${deposit_usdc/1e6:.4f} USDC → pUSD → proxy wallet")

# ── Build tx helper ───────────────────────────────────────────────────────────
nonce = w3.eth.get_transaction_count(EOA)
gas_price = w3.eth.gas_price

def send_tx(built_tx, label):
    global nonce
    signed = w3.eth.account.sign_transaction(built_tx, PRIVATE_KEY)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  {label} TX: {tx_hash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt.status != 1:
        sys.exit(f"❌  {label} transaction FAILED")
    print(f"  ✅ {label} confirmed (block {receipt.blockNumber})")
    nonce += 1
    return receipt

# ── Step 3: Approve USDC for pUSD contract ───────────────────────────────────
print("\n── Step 3: Approve USDC for pUSD contract ───────────────────────────")
current_allowance = usdc.functions.allowance(EOA, PUSD).call()
if current_allowance < deposit_usdc:
    approve_tx = usdc.functions.approve(PUSD, MAX_UINT).build_transaction({
        "from": EOA, "nonce": nonce,
        "gas": 80_000, "gasPrice": gas_price,
        "chainId": 137,
    })
    send_tx(approve_tx, "USDC→pUSD approve")
else:
    print(f"  Already approved (${current_allowance/1e6:.2f}) — skipping")

# ── Step 4: Mint pUSD directly to proxy wallet ───────────────────────────────
print("\n── Step 4: depositFor(proxy_wallet, amount) — mint pUSD to proxy ───")
try:
    deposit_tx = pusd.functions.depositFor(proxy, deposit_usdc).build_transaction({
        "from": EOA, "nonce": nonce,
        "gas": 150_000, "gasPrice": gas_price,
        "chainId": 137,
    })
    send_tx(deposit_tx, "pUSD.depositFor")
except Exception as e:
    # Fallback: deposit to EOA, then transfer to proxy
    print(f"  depositFor failed ({e}) — falling back to deposit + transfer")
    dep_tx = pusd.functions.deposit(deposit_usdc).build_transaction({
        "from": EOA, "nonce": nonce,
        "gas": 120_000, "gasPrice": gas_price,
        "chainId": 137,
    })
    send_tx(dep_tx, "pUSD.deposit")

    # Transfer pUSD to proxy
    TRANSFER_ABI = [{"inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"type":"bool"}],"stateMutability":"nonpayable","type":"function"}]
    pusd2 = w3.eth.contract(address=PUSD, abi=TRANSFER_ABI)
    xfer_tx = pusd2.functions.transfer(proxy, deposit_usdc).build_transaction({
        "from": EOA, "nonce": nonce,
        "gas": 80_000, "gasPrice": gas_price,
        "chainId": 137,
    })
    send_tx(xfer_tx, "pUSD transfer to proxy")

# ── Step 5: Verify on-chain pUSD at proxy ────────────────────────────────────
print("\n── Step 5: Verify on-chain balance ──────────────────────────────────")
proxy_pusd = pusd.functions.balanceOf(proxy).call()
print(f"  Proxy wallet pUSD balance : ${proxy_pusd/1e6:.4f}")

# ── Step 6: Update .env with proxy wallet ────────────────────────────────────
print("\n── Step 6: Writing POLYMARKET_PROXY_WALLET to .env ─────────────────")
env_path = os.path.join(os.path.dirname(__file__), ".env")
with open(env_path) as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    if line.startswith("POLYMARKET_PROXY_WALLET="):
        new_lines.append(f"POLYMARKET_PROXY_WALLET={proxy}\n")
    elif line.startswith("POLYMARKET_SIG_TYPE="):
        new_lines.append("POLYMARKET_SIG_TYPE=1\n")
    else:
        new_lines.append(line)

with open(env_path, "w") as f:
    f.writelines(new_lines)
print(f"  POLYMARKET_PROXY_WALLET={proxy}")
print(f"  POLYMARKET_SIG_TYPE=1 (POLY_PROXY)")

# ── Step 7: Sync CLOB ledger ─────────────────────────────────────────────────
print("\n── Step 7: Sync CLOB internal ledger ───────────────────────────────")
try:
    from py_clob_client_v2 import ClobClient, ApiCreds
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

    creds = ApiCreds(
        api_key=os.getenv("POLYMARKET_API_KEY",""),
        api_secret=os.getenv("POLYMARKET_API_SECRET",""),
        api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE",""),
    )
    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=PRIVATE_KEY,
        creds=creds,
        signature_type=1,
        funder=proxy,
    )
    r = client.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    bal = float((r or {}).get("balance", 0) or 0) / 1_000_000
    print(f"  CLOB balance after sync : ${bal:.4f}")
    if bal > 0:
        print("\n✅  ALL DONE — bot is funded and ready.")
        print("    Run:  python3 main.py")
    else:
        print("\n⚠️  CLOB still shows $0 — try running approve.py then main.py")
        print("    The CLOB may need a few seconds to propagate.")
except Exception as e:
    print(f"  CLOB sync error: {e}")
    print("  Run approve.py manually to sync.")
