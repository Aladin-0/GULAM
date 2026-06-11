use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub paper_trading: bool,
    pub max_execution_time_seconds: f64,
    pub base_gap_bps: f64,
    pub min_dynamic_need_pct: f64,
    pub max_dynamic_need_pct: f64,
    pub max_token_price: f64,
    pub initial_capital: f64,
    pub max_position_size_pct: f64,
    pub min_order_size_usd: f64,
    pub clob_fee_pct: f64,
    pub daily_loss_limit_pct: f64,
    pub max_position_age_seconds: f64,
    pub polymarket_api_key: String,
    pub polymarket_api_secret: String,
    pub polymarket_api_passphrase: String,
    pub polymarket_host: String,

    pub private_key: String,
    pub polymarket_proxy_wallet: String,
    pub polymarket_sig_type: u64,
    pub auth_identity_mode: String,
    pub order_identity_mode: String,
}

impl Config {
    pub fn load() -> Self {
        dotenv::dotenv().ok();
        Self {
            paper_trading: env::var("PAPER_TRADING").unwrap_or_else(|_| "false".to_string()) == "true",
            max_execution_time_seconds: env::var("MAX_EXECUTION_TIME_SECONDS").unwrap_or_else(|_| "90.0".to_string()).parse().unwrap_or(90.0),
            base_gap_bps: env::var("BASE_GAP_BPS").unwrap_or_else(|_| "6.0".to_string()).parse().unwrap_or(6.0),
            min_dynamic_need_pct: env::var("MIN_DYNAMIC_NEED_PCT").unwrap_or_else(|_| "0.0006".to_string()).parse().unwrap_or(0.0006),
            max_dynamic_need_pct: env::var("MAX_DYNAMIC_NEED_PCT").unwrap_or_else(|_| "0.0012".to_string()).parse().unwrap_or(0.0012),
            max_token_price: env::var("MAX_TOKEN_PRICE").unwrap_or_else(|_| "0.999".to_string()).parse().unwrap_or(0.999),
            initial_capital: env::var("INITIAL_CAPITAL").unwrap_or_else(|_| "6.0".to_string()).parse().unwrap_or(6.0),
            max_position_size_pct: env::var("MAX_POSITION_SIZE_PCT").unwrap_or_else(|_| "0.90".to_string()).parse().unwrap_or(0.90),
            min_order_size_usd: env::var("MIN_ORDER_SIZE_USD").unwrap_or_else(|_| "5.0".to_string()).parse().unwrap_or(5.0),
            clob_fee_pct: env::var("CLOB_FEE_PCT").unwrap_or_else(|_| "0.02".to_string()).parse().unwrap_or(0.02),
            daily_loss_limit_pct: env::var("DAILY_LOSS_LIMIT_PCT").unwrap_or_else(|_| "0.5".to_string()).parse().unwrap_or(0.5),
            max_position_age_seconds: env::var("MAX_POSITION_AGE_SECONDS").unwrap_or_else(|_| "120.0".to_string()).parse().unwrap_or(120.0),
            polymarket_api_key: env::var("POLYMARKET_API_KEY").unwrap_or_default(),
            polymarket_api_secret: env::var("POLYMARKET_API_SECRET").unwrap_or_default(),
            polymarket_api_passphrase: env::var("POLYMARKET_API_PASSPHRASE").unwrap_or_default(),
            polymarket_host: env::var("POLYMARKET_HOST").unwrap_or_else(|_| "https://clob.polymarket.com".to_string()),

            private_key: env::var("PRIVATE_KEY").unwrap_or_default(),
            polymarket_proxy_wallet: env::var("POLYMARKET_PROXY_WALLET").or_else(|_| env::var("PROXY_WALLET")).unwrap_or_default(),
            polymarket_sig_type: env::var("POLYMARKET_SIG_TYPE").unwrap_or_else(|_| "3".to_string()).parse().unwrap_or(3),
            auth_identity_mode: env::var("POLYMARKET_L1_ADDRESS_MODE").unwrap_or_else(|_| "PROXY".to_string()),
            order_identity_mode: env::var("POLYMARKET_ORDER_IDENTITY_MODE").unwrap_or_else(|_| "PROXY".to_string()),
        }
    }
}
