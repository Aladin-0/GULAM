use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Orderbook {
    pub asks: HashMap<String, String>,
    pub bids: HashMap<String, String>,
    pub last_updated: std::time::Instant,
}

impl Default for Orderbook {
    fn default() -> Self {
        Self {
            asks: HashMap::new(),
            bids: HashMap::new(),
            last_updated: std::time::Instant::now(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WsLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Deserialize)]
pub struct WsBookEvent {
    #[serde(rename = "event", default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub asset_id: String,
    pub bids: Option<Vec<WsLevel>>,
    pub asks: Option<Vec<WsLevel>>,
    #[serde(default)]
    pub timestamp: String,
}

#[derive(Debug, Serialize)]
pub struct WsSubscribe {
    pub assets_ids: Vec<String>,
    pub r#type: String,
}

pub type OrderbookCacheMap = Arc<RwLock<HashMap<String, Orderbook>>>;
pub type ConnectionStatus = Arc<std::sync::atomic::AtomicBool>;

pub enum SubChange {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

#[derive(Clone)]
pub struct OrderbookCache {
    pub cache: OrderbookCacheMap,
    pub connected: ConnectionStatus,
    pub active_tokens: Arc<RwLock<HashSet<String>>>,
    pub sub_tx: mpsc::Sender<SubChange>,
}

impl OrderbookCache {
    pub fn new() -> (Self, mpsc::Receiver<SubChange>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                cache: Arc::new(RwLock::new(HashMap::new())),
                connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                active_tokens: Arc::new(RwLock::new(HashSet::new())),
                sub_tx: tx,
            },
            rx
        )
    }

    pub async fn get_orderbook(&self, token_id: &str) -> Option<Orderbook> {
        let r = self.cache.read().await;
        r.get(token_id).cloned()
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn validate_liquidity(
        &self,
        token_id: &str,
        side: &str,
        price: f64,
        required_capital: f64,
    ) -> (bool, f64) {
        let book_opt = self.get_orderbook(token_id).await;
        let mut available_value = 0.0;

        if let Some(book) = book_opt {
            let levels = if side == "BUY" { &book.asks } else { &book.bids };
            for (p_str, size_str) in levels {
                if let (Ok(p), Ok(s)) = (p_str.parse::<f64>(), size_str.parse::<f64>()) {
                    if side == "BUY" && p <= price {
                        available_value += p * s;
                    } else if side == "SELL" && p >= price {
                        available_value += p * s;
                    }
                }
            }
        }

        (available_value >= required_capital, available_value)
    }

    pub async fn update_subscriptions(&self, desired_tokens: HashSet<String>) {
        let mut current = self.active_tokens.write().await;
        let mut added = Vec::new();
        let mut removed = Vec::new();
        
        for t in desired_tokens.iter() {
            if !current.contains(t) {
                current.insert(t.clone());
                added.push(t.clone());
            }
        }
        
        let current_keys: Vec<String> = current.iter().cloned().collect();
        for t in current_keys {
            if !desired_tokens.contains(&t) {
                current.remove(&t);
                removed.push(t);
            }
        }

        if !added.is_empty() {
            let _ = self.sub_tx.send(SubChange::Subscribe(added)).await;
        }
        if !removed.is_empty() {
            let _ = self.sub_tx.send(SubChange::Unsubscribe(removed)).await;
        }
    }

    pub async fn is_fresh(&self, asset_id: &str, max_age_secs: u64) -> bool {
        let r = self.cache.read().await;
        if let Some(ob) = r.get(asset_id) {
            ob.last_updated.elapsed().as_secs() <= max_age_secs
        } else {
            false
        }
    }

    pub async fn calculate_sweep_price(&self, token_id: &str, side: &str, target_size: f64) -> Option<f64> {
        let book_opt = self.get_orderbook(token_id).await;
        if let Some(book) = book_opt {
            let mut levels = Vec::new();
            let source_map = if side == "BUY" { &book.asks } else { &book.bids };
            for (p_str, size_str) in source_map {
                if let (Ok(p), Ok(s)) = (p_str.parse::<f64>(), size_str.parse::<f64>()) {
                    levels.push((p, s));
                }
            }
            if side == "BUY" {
                levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            } else {
                levels.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
            }

            let mut accumulated_size = 0.0;
            let mut sweep_price = None;
            for (p, s) in levels {
                accumulated_size += s;
                sweep_price = Some(p);
                if accumulated_size >= target_size {
                    break;
                }
            }
            return sweep_price;
        }
        None
    }
}

pub async fn run_orderbook_cache(ob_cache: OrderbookCache, mut sub_rx: mpsc::Receiver<SubChange>) {
    println!("[ORDERBOOK] Connecting to Polymarket CLOB WS...");
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let mut backoff = 1u64;
    let max_backoff = 30u64;

    loop {
        if backoff > 1 {
            println!("[ORDERBOOK-RECONNECT] Waiting {}s before reconnecting...", backoff);
            tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
        }

        ob_cache.connected.store(false, std::sync::atomic::Ordering::Relaxed);
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                println!("[ORDERBOOK] WS Connected.");
                ob_cache.connected.store(true, std::sync::atomic::Ordering::Relaxed);
                let (mut write, mut read) = ws_stream.split();

                // Re-subscribe to all existing tokens upon connection
                let existing_tokens: Vec<String> = {
                    let set = ob_cache.active_tokens.read().await;
                    set.iter().cloned().collect()
                };

                if !existing_tokens.is_empty() {
                    for chunk in existing_tokens.chunks(100) {
                        let sub_msg = WsSubscribe {
                            assets_ids: chunk.to_vec(),
                            r#type: "market".to_string(),
                        };
                        if let Ok(json) = serde_json::to_string(&sub_msg) {
                            let _ = write.send(Message::Text(json)).await;
                        }
                    }
                }

                let connected_at = std::time::Instant::now();
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
                ping_interval.tick().await;

                let mut last_parse_err = std::time::Instant::now();
                let mut parse_err_count = 0;

                loop {
                    tokio::select! {
                        Some(change) = sub_rx.recv() => {
                            match change {
                                SubChange::Subscribe(tokens) => {
                                    for chunk in tokens.chunks(100) {
                                        let sub_msg = WsSubscribe {
                                            assets_ids: chunk.to_vec(),
                                            r#type: "market".to_string(),
                                        };
                                        if let Ok(json) = serde_json::to_string(&sub_msg) {
                                            let _ = write.send(Message::Text(json)).await;
                                        }
                                    }
                                }
                                SubChange::Unsubscribe(tokens) => {
                                    // Gamma API or CLOB WS unsubscribe? We just omit them.
                                    // (If Polymarket WS supports "unsubscribe" type, we can send it here).
                                    // For now, we just removed them from `active_tokens` which handles it implicitly on reconnects.
                                }
                            }
                        }
                        _ = ping_interval.tick() => {
                            let _ = write.send(Message::Ping(vec![])).await;
                        }
                        msg_res = tokio::time::timeout(tokio::time::Duration::from_secs(30), read.next()) => {
                            match msg_res {
                                Ok(Some(Ok(Message::Text(text)))) => {
                                    // Stable connection check
                                    if connected_at.elapsed().as_secs() > 10 {
                                        backoff = 1; // Reset backoff
                                    }
                                    if text.contains("\"message\":\"Successfully subscribed\"") {
                                        continue;
                                    }
                                    if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                        let events = if parsed_json.is_array() {
                                            parsed_json.as_array().unwrap().clone()
                                        } else {
                                            vec![parsed_json]
                                        };
                                        for event_val in events {
                                            if let Ok(book_event) = serde_json::from_value::<WsBookEvent>(event_val) {
                                                if book_event.asset_id.is_empty() { continue; }
                                                let mut w = ob_cache.cache.write().await;
                                                let entry = w.entry(book_event.asset_id.clone()).or_default();
                                                entry.last_updated = std::time::Instant::now();
                                                
                                                if let Some(bids) = book_event.bids {
                                                    for b in bids {
                                                        if b.size == "0" { entry.bids.remove(&b.price); }
                                                        else { entry.bids.insert(b.price, b.size); }
                                                    }
                                                }
                                                if let Some(asks) = book_event.asks {
                                                    for a in asks {
                                                        if a.size == "0" { entry.asks.remove(&a.price); }
                                                        else { entry.asks.insert(a.price, a.size); }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        parse_err_count += 1;
                                        if last_parse_err.elapsed().as_secs() >= 60 {
                                            let snippet = if text.len() > 100 { &text[..100] } else { &text };
                                            println!("[ORDERBOOK-ERROR] WS Parse failed ({} times in 60s): {}", parse_err_count, snippet);
                                            parse_err_count = 0;
                                            last_parse_err = std::time::Instant::now();
                                        }
                                    }
                                }
                                Ok(Some(Ok(Message::Close(_)))) => {
                                    println!("[ORDERBOOK] WS Closed by remote.");
                                    break;
                                }
                                Ok(Some(Err(e))) => {
                                    println!("[ORDERBOOK] WS Error: {}", e);
                                    break;
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    println!("[ORDERBOOK] WS silent drop detected (no messages/pings for 30s). Reconnecting...");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                println!("[ORDERBOOK-RECONNECT] WS connection failed: {}", e);
            }
        }
        
        // Disconnected state cleanup
        ob_cache.connected.store(false, std::sync::atomic::Ordering::Relaxed);
        backoff = (backoff * 2).min(max_backoff);
    }
}
