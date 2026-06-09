use base64::Engine as _;
use chrono::Utc;
use ethers::signers::LocalWallet;
use ethers::types::{H160, U256};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::Sha256;
use std::sync::atomic::{AtomicU64, Ordering};
use std::str::FromStr;
use ethers::signers::Signer;

type HmacSha256 = Hmac<Sha256>;

pub const MAX_SINGLE_ORDER_USDC: u64 = 3_000_000; // Exactly $3.00 USDC

pub struct PolymarketCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

pub struct ExecutionContext {
    pub creds: PolymarketCredentials,
    pub wallet: LocalWallet,
    pub proxy_addr: H160,
    pub proxy_addr_str: String,
    pub eoa_signer_address: String,
    pub domain_separator: [u8; 32],
    pub api_secret_bytes: Vec<u8>,
    pub order_abi_template: [u8; 384],
    pub solady_abi_template: [u8; 224],
}

impl ExecutionContext {
    pub fn new(config: &crate::config::Config) -> Self {
        let wallet = LocalWallet::from_str(&config.private_key).expect("Invalid private key");
        let eoa = format!("{:?}", wallet.address());
        let proxy_addr = H160::from_str(&config.polymarket_proxy_wallet).unwrap_or(wallet.address());
        
        let api_secret_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&config.polymarket_api_secret)
            .unwrap_or_else(|_| base64::engine::general_purpose::STANDARD.decode(&config.polymarket_api_secret).unwrap_or_default());

        Self {
            creds: PolymarketCredentials {
                api_key: config.polymarket_api_key.clone(),
                api_secret: config.polymarket_api_secret.clone(),
                api_passphrase: config.polymarket_api_passphrase.clone(),
            },
            wallet,
            proxy_addr,
            proxy_addr_str: format!("{:?}", proxy_addr),
            eoa_signer_address: eoa,
            domain_separator: [0u8; 32], // Simplified for now, real requires chainID hash
            api_secret_bytes,
            order_abi_template: [0u8; 384],
            solady_abi_template: [0u8; 224],
        }
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
    let signature_base64 = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

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
