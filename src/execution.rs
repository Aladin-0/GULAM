use base64::Engine as _;
use chrono::Utc;
use ethers::signers::LocalWallet;
use ethers::types::H160;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::Sha256;

use std::str::FromStr;
use ethers::signers::Signer;

type HmacSha256 = Hmac<Sha256>;



// keccak256(abi.encode(DOMAIN_TYPEHASH, keccak256("Polymarket CTF Exchange"), keccak256("2"), 137, 0xE111180000d2663C0091e4f400237545B87B996B))
pub const POLYMARKET_DOMAIN_SEPARATOR: [u8; 32] = [
    0x32, 0x64, 0xe1, 0x59, 0x34, 0x62, 0x53, 0xe2, 0x6a, 0x64, 0xe0, 0x0b, 0x69, 0x03, 0x2d, 0xb0, 0xe7, 0xd3, 0x2f, 0x94, 0x62, 0x8d, 0xe3, 0xe6, 0xee, 0xcb, 0x50, 0x30, 0x4d, 0x7a, 0xf3, 0xd2
];

// keccak256("Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)")
pub const POLYMARKET_ORDER_TYPEHASH: [u8; 32] = [
    0xbb, 0x86, 0x31, 0x8a, 0x21, 0x38, 0xf5, 0xfa, 0x8a, 0xe3, 0x2f, 0xbe, 0x8e, 0x65, 0x9f, 0x8f,
    0xcf, 0x13, 0xcc, 0x6a, 0xe4, 0x01, 0x4a, 0x70, 0x78, 0x93, 0x05, 0x54, 0x33, 0x81, 0x85, 0x89,
];

pub const SOLADY_TYPE_HASH: [u8; 32] = [
    0x6b, 0xa0, 0x28, 0x56, 0x5c, 0xb3, 0x24, 0xc2, 0xaa, 0x02, 0xbb, 0x71, 0x4b, 0x98, 0x16, 0xd0, 0xbd, 0xdd, 0x55, 0x7a, 0x2f, 0x33, 0xbb, 0x36, 0xcf, 0x13, 0x27, 0x2a, 0x42, 0x56, 0xbd, 0x42
];

pub const DEPOSIT_WALLET_NAME_HASH: [u8; 32] = [
    0xd6, 0x82, 0xb5, 0x29, 0xa1, 0x7c, 0xda, 0x19, 0xaa, 0x27, 0x5f, 0x3a, 0x05, 0x06, 0x08, 0xf9, 0xe9, 0x40, 0x1f, 0xad, 0xd1, 0xb0, 0xd2, 0x33, 0xd8, 0x15, 0x19, 0x97, 0x22, 0x95, 0x82, 0x8b
];

pub const DEPOSIT_WALLET_VERSION_HASH: [u8; 32] = [
    0xc8, 0x9e, 0xfd, 0xaa, 0x54, 0xc0, 0xf2, 0x0c, 0x7a, 0xdf, 0x61, 0x28, 0x82, 0xdf, 0x09, 0x50, 0xf5, 0xa9, 0x51, 0x63, 0x7e, 0x03, 0x07, 0xcd, 0xcb, 0x4c, 0x67, 0x2f, 0x29, 0x8b, 0x8b, 0xc6
];

pub struct PolymarketCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

pub struct ExecutionContext {
    pub creds: PolymarketCredentials,
    pub wallet: LocalWallet,
    pub proxy_addr_str: String,
    pub eoa_signer_address: String,
    pub domain_separator: [u8; 32],
    pub api_secret_bytes: Vec<u8>,
    pub order_typehash: [u8; 32],
}

impl ExecutionContext {
    pub fn new(config: &crate::config::Config) -> Self {
        let wallet = LocalWallet::from_str(&config.private_key).expect("Invalid private key");
        // Use checksummed EIP-55 addresses — Polymarket compares signer address as a case-sensitive string
        let eoa = ethers::utils::to_checksum(&wallet.address(), None);
        let proxy_addr = H160::from_str(&config.polymarket_proxy_wallet).unwrap_or(wallet.address());
        let proxy_checksum = ethers::utils::to_checksum(&proxy_addr, None);

        let api_secret_bytes = base64::engine::general_purpose::URL_SAFE
            .decode(&config.polymarket_api_secret)
            .unwrap_or_else(|_| base64::engine::general_purpose::STANDARD.decode(&config.polymarket_api_secret).unwrap_or_default());

        let ctx = Self {
            creds: PolymarketCredentials {
                api_key: config.polymarket_api_key.clone(),
                api_secret: config.polymarket_api_secret.clone(),
                api_passphrase: config.polymarket_api_passphrase.clone(),
            },
            wallet,
            proxy_addr_str: proxy_checksum.clone(),
            eoa_signer_address: eoa.clone(),
            domain_separator: POLYMARKET_DOMAIN_SEPARATOR,
            api_secret_bytes,
            order_typehash: POLYMARKET_ORDER_TYPEHASH,
        };
        println!("[EXEC] Signer address (checksummed): {}", eoa);
        println!("[EXEC] Proxy address  (checksummed): {}", proxy_checksum);
        println!("[EXEC] API secret decoded: {} bytes (want 32)", ctx.api_secret_bytes.len());
        ctx
    }
}

pub fn generate_level_1_headers(
    method: &str,
    request_path: &str,
    body: &str,
    creds: &PolymarketCredentials,
    signer_address: &str,
    api_secret_bytes: &[u8],
) -> HeaderMap {
    let timestamp_seconds_str = Utc::now().timestamp().to_string();
    let message = format!("{}{}{}{}", timestamp_seconds_str, method, request_path, body);
    let mut mac = HmacSha256::new_from_slice(api_secret_bytes).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    let signature_base64 = base64::engine::general_purpose::URL_SAFE.encode(mac.finalize().into_bytes());

    let mut headers = HeaderMap::new();
    headers.insert("POLY_ADDRESS".parse::<HeaderName>().unwrap(), HeaderValue::from_str(signer_address).unwrap());
    headers.insert("POLY_SIGNATURE".parse::<HeaderName>().unwrap(), HeaderValue::from_str(&signature_base64).unwrap());
    headers.insert("POLY_TIMESTAMP".parse::<HeaderName>().unwrap(), HeaderValue::from_str(&timestamp_seconds_str).unwrap());
    headers.insert("POLY_API_KEY".parse::<HeaderName>().unwrap(), HeaderValue::from_str(&creds.api_key).unwrap());
    headers.insert("POLY_PASSPHRASE".parse::<HeaderName>().unwrap(), HeaderValue::from_str(&creds.api_passphrase).unwrap());
    headers.insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers
}

// In the Python bot, order placing returns an Order ID, and fill verification polls `get_order`.
// For parity we will simulate the exact REST responses expected by Polmarket API.
// Since actual signing requires exact domain separators matching python `py_clob_client_v2`,
// we will submit to CLOB API if configured, else stub.
// To achieve STRICT behavior, we will return an order ID if successful.
