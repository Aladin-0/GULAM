# Building a Polymarket Trading Bot in Rust: The Definitive Guide (V2 Proxy Wallets & EIP-7739)

Building a high-frequency trading bot for Polymarket in pure Rust provides massive latency and performance benefits. However, Polymarket's matching engine relies on L2 smart contracts (Polygon) and uses highly specific EIP-712 and EIP-7739 (POLY_1271) signature architectures that are poorly documented. 

When using a **"Deposit Wallet"** (Proxy Wallet / Smart Wallet), standard EOA (Externally Owned Account) signatures will fail with a `400 Bad Request: signature does not match order hash` error. 

This guide explains how to properly authenticate, construct orders, sign them, and format the EIP-7739 payload in pure Rust without relying on the official Python/JS SDKs.

---

## 1. API Keys and Authentication Headers
When you create API keys on Polymarket using a Proxy Wallet, you are provided with an API Key, an API Secret, and an API Passphrase. Your account consists of two addresses:
1. **EOA (Externally Owned Account)**: The private key address that controls your account.
2. **Proxy / Deposit Wallet**: The smart contract wallet where your USDC funds actually reside.

### The Authentication Trap
To authenticate WebSocket and REST requests (e.g., placing orders or fetching `GET /auth`), you must generate a timestamped Level 0 / Level 1 signature.
* **Header Rule:** You MUST set the `POLY_ADDRESS` header to the **EOA** address (the private key address) that owns the API key, NOT the proxy deposit wallet address. 
* **Payload Rule:** However, when submitting a live order JSON payload, the order itself must reflect the proxy wallet architecture.

---

## 2. The JSON Order Payload Structure for Proxy Wallets
For deposit wallets, the JSON payload must instruct the matching engine to use the EIP-7739 signature recovery path.
* `signatureType`: Must be `3` (POLY_1271).
* `maker`: Must be the **Proxy Deposit Wallet** address.
* `signer`: Must be the **Proxy Deposit Wallet** address.
* `owner`: Must be your UUID API Key string.

```json
{
  "owner": "YOUR_API_KEY_UUID",
  "order": {
    "salt": 1781212077385977453,
    "maker": "0xYourProxyDepositWalletAddress",
    "signer": "0xYourProxyDepositWalletAddress",
    "tokenId": "356460752088127968500574...",
    "makerAmount": "10000000",
    "takerAmount": "20000000",
    "side": "BUY",
    "expiration": "0", 
    "signatureType": 3,
    "timestamp": "1781212077",
    "metadata": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "builder": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "signature": "0x..." // See Section 3
  },
  "orderType": "GTC"
}
```
*(Note: `expiration` is often sent as `"0"` for GTC limits, and relies on standard orderbook matching behavior, but is NOT part of the signed hash.)*

---

## 3. EIP-7739 (POLY_1271) Signature Construction in Rust

The most complex part of placing an order is generating the signature. A `signatureType: 3` signature is not a standard ECDSA 65-byte string. It is a 358-character hex blob that contains the ECDSA signature tightly wrapped with validation metadata.

### Step 3.1: The Base Order Struct Hash
First, ABI-encode the order data and compute the Keccak256 hash. The order struct must perfectly align with the expected EVM memory layout:
`[salt, maker, signer, tokenId, makerAmount, takerAmount, side, signatureType, timestamp, metadata, builder]`

**Important Notes for Rust:**
* Strings like `tokenId` and `salt` must be parsed as `U256` big-endian representations.
* `maker` and `signer` addresses must be padded to 32 bytes.
* `side` (0 = BUY, 1 = SELL) and `signatureType` (3) are `uint8` types, meaning they occupy the final byte of their respective 32-byte slots in ABI encoding.

### Step 3.2: The App Domain Separator (The V2 Contract Trap)
This is the most critical and widely undocumented trap. The Polymarket V2 API requires the EIP-712 App Domain Separator to be built using the **V2 CTF Exchange Smart Contract Address**.
Many developers accidentally use the older V1 address (`0x4bFb4...`), causing an immediate signature rejection.

The strictly correct App Domain Separator for Polygon (Chain 137) is built using the V2 Contract (`0xE111180000d2663C0091e4f400237545B87B996B`) and the `"2"` version string:

```rust
// keccak256(abi.encode(DOMAIN_TYPEHASH, keccak256("Polymarket CTF Exchange"), keccak256("2"), 137, 0xE111180000d2663C0091e4f400237545B87B996B))
pub const POLYMARKET_DOMAIN_SEPARATOR: [u8; 32] = [
    0x32, 0x64, 0xe1, 0x59, 0x34, 0x62, 0x53, 0xe2, 0x6a, 0x64, 0xe0, 0x0b, 0x69, 0x03, 
    0x2d, 0xb0, 0xe7, 0xd3, 0x2f, 0x94, 0x62, 0x8d, 0xe3, 0xe6, 0xee, 0xcb, 0x50, 0x30, 
    0x4d, 0x7a, 0xf3, 0xd2
];
```

### Step 3.3: The Solady Wrapper (Typed Data Hash)
Instead of signing the base order hash directly, Proxy orders wrap the hash in a Solady EIP-7739 typed data container.

```rust
// keccak256("TypedDataSign(bytes32 contents,bytes1 nameHash,bytes1 versionHash,uint256 chainId,bytes1 verifyingContract,bytes32 salt)")
pub const SOLADY_TYPE_HASH: [u8; 32] = hex!("810842217cda43bbdfa74d28df528256d0d297a7a7bb628bd6ee0090f7d5c7dd");
```
ABI encode and hash the following:
`keccak256(abi.encode(SOLADY_TYPE_HASH, base_struct_hash, empty_name_hash, empty_version_hash, chainId_137, proxy_signer_address, zero_salt))`
*(Note: `empty_name_hash` and `empty_version_hash` are just the Keccak256 hashes of `""` empty strings).*

### Step 3.4: The Digest and the `v` Value Bug
Generate the final EIP-712 digest: 
`keccak256("\x19\x01" + POLYMARKET_DOMAIN_SEPARATOR + typed_data_sign_struct_hash)`

Sign this digest using your EOA's Private Key using standard ECDSA.

**CRITICAL FIX:** For EIP-7739 `POLY_1271`, the final trailing byte of the `inner_ecdsa_signature` (the `v` recovery byte) **MUST BE** exactly `27` or `28`. 
If your Rust crypto library (like `ethers-rs` using standard normalization) subtracts `27` to produce a `v` of `0` or `1`, the smart contract's `ecrecover` precompile will silently fail, causing a 400 rejection.
```rust
let mut inner_sig = signature.to_vec();
if inner_sig[64] < 27 { inner_sig[64] += 27; } // POLY_1271 strictly requires 27 or 28
```

### Step 3.5: Appending the Payload Suffix
The final `signature` string sent in the JSON payload is a direct concatenation of multiple cryptographic components that allows the L2 node to deterministically prove the signature path.

```
Final Signature String = 
  "0x" + 
  inner_ecdsa_signature (65 bytes) + 
  app_domain_separator (32 bytes) + 
  base_struct_hash (32 bytes) + 
  order_type_string_hex (171 bytes) + 
  string_length_uint16_hex (2 bytes)
```
**Type String Reference:**
`"Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)"`

The fully concatenated string will exactly equal **358 characters** (`0x` + 356 hex characters).

---

## 4. Final Debugging Checklist
If you receive `400 Bad Request: invalid POLY_1271 signature`:
1. [ ] Check if `POLYMARKET_DOMAIN_SEPARATOR` is using the V2 Contract (`0xE111...`), not the V1 Contract.
2. [ ] Check if the `v` byte of your ECDSA signature is exactly `27` or `28` (usually `1b` or `1c` in hex).
3. [ ] Verify `maker` and `signer` in the payload JSON match your Proxy Deposit Wallet Address.
4. [ ] Verify your `POLY_ADDRESS` HTTP header matches your EOA Private Key Address.
5. [ ] Verify the final signature string is exactly 358 characters.
