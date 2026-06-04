#!/usr/bin/env python3
"""
find_proxy.py
-------------
Discovers the Polymarket proxy/deposit wallet address for this bot's EOA
by scanning on-chain USDC and pUSD transfer history on Polygon.

Once found, writes POLYMARKET_PROXY_WALLET=<address> to .env so live_trader.py
can use it as the `funder` parameter with signature_type=POLY_PROXY (1).

Usage:
    python3 find_proxy.py
"""
import os, sys
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

# ---------------------------------------------------------------------------
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
if not PRIVATE_KEY:
    sys.exit("ERROR: PRIVATE_KEY not set in .env")

from eth_account import Account
EOA = Web3.to_checksum_address(Account.from_key(PRIVATE_KEY).address)

# Token addresses on Polygon Mainnet
TOKENS = {
    "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    "USDC":   "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
    "pUSD":   "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
}
PUSD_ADDR = Web3.to_checksum_address(TOKENS["pUSD"])

ERC20_ABI = [
    {"inputs":[{"name":"account","type":"address"}],"name":"balanceOf",
     "outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
]

RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://rpc.ankr.com/polygon",
    "https://polygon-mainnet.public.blastapi.io",
]

# ---------------------------------------------------------------------------
print(f"\nEOA: {EOA}\n")
print("Connecting to Polygon...")
w3 = None
for rpc in RPCS:
    try:
        _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        if _w3.is_connected():
            print(f"  ✓ {rpc}  (block {_w3.eth.block_number})\n")
            w3 = _w3
            break
    except Exception as e:
        print(f"  ✗ {rpc}")
if not w3:
    sys.exit("Cannot connect to Polygon RPC")

TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()
EOA_topic = "0x000000000000000000000000" + EOA[2:].lower()

pusd_contract = w3.eth.contract(address=PUSD_ADDR, abi=ERC20_ABI)
latest = w3.eth.block_number
CHUNK  = 9000
# Scan last ~60 days (≈2,592,000 blocks at 2s/block → 288 chunks max)
MAX_CHUNKS = 288

print(f"Scanning last ~60 days for token outflows from {EOA[:10]}...\n")

found: dict = {}

for i in range(MAX_CHUNKS):
    to_block   = latest - i * CHUNK
    from_block = to_block - CHUNK
    if from_block < 0:
        break

    for token_name, raw_addr in TOKENS.items():
        token_addr = Web3.to_checksum_address(raw_addr)
        try:
            logs = w3.eth.get_logs({
                "fromBlock": hex(from_block),
                "toBlock":   hex(to_block),
                "address":   token_addr,
                "topics":    [TRANSFER_TOPIC, EOA_topic],   # FROM EOA
            })
            for log in logs:
                dest = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
                raw_val = bytes.fromhex(log["data"].hex().lstrip("0x").zfill(64))
                val = int.from_bytes(raw_val, "big") / 1_000_000
                print(f"  [{token_name}]  {EOA[:8]}... → {dest}  ${val:.4f}  block={log['blockNumber']}")
                if dest not in found:
                    found[dest] = []
                found[dest].append((token_name, val, log["blockNumber"]))
        except Exception as e:
            err = str(e)
            if "exceed maximum" in err or "invalid argument" in err:
                pass   # RPC limit — keep going
            # else silently skip

    if i % 20 == 0:
        print(f"  ... chunk {i}/{MAX_CHUNKS} (block {from_block})")

    if found:
        # Stop once we found at least one outflow
        print(f"\n  Found {len(found)} destination(s) — stopping scan.\n")
        break

# ---------------------------------------------------------------------------
if not found:
    print("""
No outgoing USDC/pUSD transfers detected in the last 60 days.

This likely means the proxy wallet is COUNTERFACTUAL:
  • The address exists and holds funds, but the contract bytecode hasn't
    been deployed on-chain yet (zero-gas "lazy" deployment pattern).
  • The CLOB balance API returns 0 for counterfactual wallets.

ACTION REQUIRED:
  1. Go to https://polymarket.com
  2. Make a $1 manual trade (buy anything) using your connected MetaMask
  3. MetaMask will ask you to sign TWO transactions — approve both
  4. Re-run this script:  python3 find_proxy.py
  5. The proxy address will appear and be written to .env automatically
""")
    sys.exit(0)

# ---------------------------------------------------------------------------
print("Checking pUSD balance at discovered addresses:\n")
proxy_wallet = None

for addr, events in found.items():
    try:
        bal = pusd_contract.functions.balanceOf(addr).call() / 1_000_000
        code = w3.eth.get_code(addr)
        is_contract = len(code) > 2
        print(f"  {addr}")
        print(f"    pUSD balance : ${bal:.4f}")
        print(f"    Is contract  : {is_contract}  (bytecode len={len(code)})")
        print(f"    Transfers    : {events}")
        if is_contract:
            proxy_wallet = addr
            print(f"    ✅ PROXY WALLET — will use as funder")
        elif bal > 0:
            proxy_wallet = addr
            print(f"    ✅ EOA PROXY — will use as funder")
        print()
    except Exception as e:
        print(f"  {addr}: error checking balance — {e}\n")

# ---------------------------------------------------------------------------
if not proxy_wallet:
    print("""
No proxy wallet with pUSD balance found.
Your $10 may be in a counterfactual proxy (no bytecode deployed yet).
Follow the ACTION REQUIRED steps above (make a $1 manual trade first).
""")
    sys.exit(0)

# Write to .env
env_path = os.path.join(os.path.dirname(__file__), ".env")
with open(env_path, "r") as f:
    env_lines = f.readlines()

key = "POLYMARKET_PROXY_WALLET"
new_line = f"{key}={proxy_wallet}\n"
replaced = False
new_lines = []
for line in env_lines:
    if line.startswith(key):
        new_lines.append(new_line)
        replaced = True
    else:
        new_lines.append(line)
if not replaced:
    new_lines.append(new_line)

with open(env_path, "w") as f:
    f.writelines(new_lines)

print(f"""
══════════════════════════════════════════════════════
✅  Proxy wallet found and saved to .env:

    POLYMARKET_PROXY_WALLET={proxy_wallet}

Next steps:
  1. git pull on the server (live_trader.py reads this from .env)
  2. python3 approve.py
  3. python3 main.py
══════════════════════════════════════════════════════
""")
