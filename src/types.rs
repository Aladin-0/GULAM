use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceTick {
    pub symbol: String,
    pub price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub condition_id: String,
    pub question: String,
    pub symbol: String,
    pub slug: String,
    pub side: String,
    pub token_id: String,
    pub entry_price: f64,
    pub price_to_beat: f64,
    pub current_price: f64,
    pub gap: f64,
    pub gap_pct: f64,
    pub time_remaining: f64,
    pub dynamic_need_pct: f64,
    pub risk_multiplier: f64,
    pub timestamp: f64,
    #[serde(skip)]
    #[serde(default = "default_latency_metrics")]
    pub latency: Option<LatencyMetrics>,
}

#[derive(Debug, Clone)]
pub struct LatencyMetrics {
    pub binance_tick_received_at: std::time::Instant,
    pub signal_decision_started_at: std::time::Instant,
    pub signal_decision_finished_at: std::time::Instant,
    pub order_build_started_at: Option<std::time::Instant>,
    pub order_build_finished_at: Option<std::time::Instant>,
    pub http_send_started_at: Option<std::time::Instant>,
    pub http_ack_received_at: Option<std::time::Instant>,
    pub user_ws_fill_received_at: Option<std::time::Instant>,
    pub position_recorded_at: Option<std::time::Instant>,
}

impl Default for LatencyMetrics {
    fn default() -> Self {
        Self {
            binance_tick_received_at: std::time::Instant::now(),
            signal_decision_started_at: std::time::Instant::now(),
            signal_decision_finished_at: std::time::Instant::now(),
            order_build_started_at: None,
            order_build_finished_at: None,
            http_send_started_at: None,
            http_ack_received_at: None,
            user_ws_fill_received_at: None,
            position_recorded_at: None,
        }
    }
}

fn default_latency_metrics() -> Option<LatencyMetrics> {
    None
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingOrder {
    pub order_id: String,
    pub signal: Signal,
    pub approx_shares: f64,
    pub order_price: f64,
    pub submitted_at: f64,
    #[serde(skip)]
    #[serde(default = "default_latency_metrics")]
    pub latency: Option<LatencyMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingHedge {
    pub order_id: String,
    pub position: Position,
    pub approx_shares: f64,
    pub order_price: f64,
    pub submitted_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserWsMessage {
    pub event_type: Option<String>,
    pub event: Option<String>,
    pub status: Option<String>,
    pub order_id: Option<String>,
    #[serde(rename = "orderID")]
    pub order_id_alt: Option<String>,
    pub size_matched: Option<String>,
    pub average_price: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub question: String,
    pub symbol: String,
    pub slug: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub up_price: f64,
    pub down_price: f64,
    pub end_date: Option<String>,
    pub time_remaining_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub condition_id: String,
    pub question: String,
    pub symbol: String,
    pub side: String,
    pub token_id: String,
    pub slug: String,
    pub entry_price: f64,
    pub price_to_beat: f64,
    pub shares: f64,
    pub cost: f64,
    pub clob_entry_fee: f64,
    pub entry_time: f64,
    pub time_remaining: f64,
    pub order_id: String,
    pub uncertain_resolve_at: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub condition_id: String,
    pub question: String,
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub shares: f64,
    pub cost: f64,
    pub gross_profit: f64,
    pub clob_round_trip_fee: f64,
    pub profit: f64,
    pub pnl_pct: f64,
    pub reason: String,
    pub entry_time: f64,
    pub exit_time: f64,
    pub order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveState {
    pub capital: f64,
    pub available_capital: f64,
    pub total_profit: f64,
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub loss_count: u64,
    pub total_lost_usd: f64,
    pub daily_profit: f64,
    pub daily_trades: u64,
    pub open_positions: std::collections::HashMap<String, Position>,
    pub pending_orders: std::collections::HashMap<String, PendingOrder>,
    pub pending_hedges: std::collections::HashMap<String, PendingHedge>,
    pub trade_history: Vec<TradeRecord>,
}

impl Default for LiveState {
    fn default() -> Self {
        Self {
            capital: 0.0,
            available_capital: 0.0,
            total_profit: 0.0,
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            loss_count: 0,
            total_lost_usd: 0.0,
            daily_profit: 0.0,
            daily_trades: 0,
            open_positions: std::collections::HashMap::new(),
            pending_orders: std::collections::HashMap::new(),
            pending_hedges: std::collections::HashMap::new(),
            trade_history: Vec::new(),
        }
    }
}
