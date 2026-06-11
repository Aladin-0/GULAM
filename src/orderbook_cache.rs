use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Orderbook {
    pub asks: Vec<(f64, f64)>, // Sorted ascending (lowest price first)
    pub bids: Vec<(f64, f64)>, // Sorted descending (highest price first)
    pub last_updated: std::time::Instant,
}

impl Default for Orderbook {
    fn default() -> Self {
        Self {
            asks: Vec::new(),
            bids: Vec::new(),
            last_updated: std::time::Instant::now(),
        }
    }
}

/// Single entry inside a price_change event from Polymarket market WS
#[derive(Debug, Deserialize)]
pub struct WsPriceChangeEntry {
    pub asset_id: String,
    pub best_ask: Option<String>,
    pub best_bid: Option<String>,
}

/// Top-level message from wss://ws-subscriptions-clob.polymarket.com/ws/market
#[derive(Debug, Deserialize)]
pub struct WsPriceChangeEvent {

    pub price_changes: Option<Vec<WsPriceChangeEntry>>,
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

    pub async fn validate_liquidity(&self, token_id: &str, side: &str, price: f64, required_capital: f64) -> (bool, f64) {
        let mut available_value = 0.0;
        let book_opt = self.get_orderbook(token_id).await;
        if let Some(book) = book_opt {
            let levels = if side == "BUY" { &book.asks } else { &book.bids };
            for &(p, s) in levels {
                if (side == "BUY" && p <= price) || (side == "SELL" && p >= price) {
                    available_value += p * s;
                } else {
                    break;
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
            let levels = if side == "BUY" { &book.asks } else { &book.bids };

            let mut accumulated_size = 0.0;
            let mut sweep_price = None;
            for &(p, s) in levels {
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

fn update_level(levels: &mut Vec<(f64, f64)>, price: f64, size: f64, is_ask: bool) {
    let search = levels.binary_search_by(|&(p, _)| {
        if is_ask {
            p.partial_cmp(&price).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            price.partial_cmp(&p).unwrap_or(std::cmp::Ordering::Equal)
        }
    });

    match search {
        Ok(idx) => {
            if size == 0.0 {
                levels.remove(idx);
            } else {
                levels[idx].1 = size;
            }
        }
        Err(idx) => {
            if size > 0.0 {
                levels.insert(idx, (price, size));
            }
        }
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

                // Track tokens subscribed in THIS connection session to avoid re-subscribing (INVALID OPERATION)
                let mut session_subscribed: std::collections::HashSet<String> = std::collections::HashSet::new();

                // Subscribe to all existing tokens on fresh connection
                let existing_tokens: Vec<String> = {
                    let set = ob_cache.active_tokens.read().await;
                    set.iter().cloned().collect()
                };

                if !existing_tokens.is_empty() {
                    for chunk in existing_tokens.chunks(100) {
                        if chunk.is_empty() { continue; }
                        let sub_msg = WsSubscribe {
                            assets_ids: chunk.to_vec(),
                            r#type: "market".to_string(),
                        };
                        if let Ok(json) = serde_json::to_string(&sub_msg) {
                            let _ = write.send(Message::Text(json)).await;
                            for t in chunk { session_subscribed.insert(t.clone()); }
                        }
                    }
                }

                let connected_at = std::time::Instant::now();
                // Send text PING every 20s to keep connection alive (binary ping frames not always honoured)
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(20));
                ping_interval.tick().await; // skip first immediate tick

                let mut last_parse_err = std::time::Instant::now();
                let mut parse_err_count = 0;

                loop {
                    tokio::select! {
                        Some(change) = sub_rx.recv() => {
                            match change {
                                SubChange::Subscribe(tokens) => {
                                    if tokens.is_empty() { continue; }
                                    // Only subscribe to tokens NOT already subscribed in this session
                                    let new_tokens: Vec<String> = tokens.into_iter()
                                        .filter(|t| !session_subscribed.contains(t))
                                        .collect();
                                    if new_tokens.is_empty() { continue; }
                                    for chunk in new_tokens.chunks(100) {
                                        if chunk.is_empty() { continue; }
                                        let sub_msg = WsSubscribe {
                                            assets_ids: chunk.to_vec(),
                                            r#type: "market".to_string(),
                                        };
                                        if let Ok(json) = serde_json::to_string(&sub_msg) {
                                            let _ = write.send(Message::Text(json)).await;
                                            for t in chunk { session_subscribed.insert(t.clone()); }
                                        }
                                    }
                                }
                                SubChange::Unsubscribe(tokens) => {
                                    // Remove from session tracking so reconnect won't skip them
                                    for t in tokens { session_subscribed.remove(&t); }
                                }
                            }
                        }
                        _ = ping_interval.tick() => {
                            // Text PING — Polymarket echoes "PONG" which keeps connection alive
                            let _ = write.send(Message::Text("PING".to_string())).await;
                        }
                        // Shorten silence timeout to 45s — Polymarket drops at ~60s
                        msg_res = tokio::time::timeout(tokio::time::Duration::from_secs(45), read.next()) => {
                            match msg_res {
                                Ok(Some(Ok(Message::Text(text)))) => {
                                    // Stable connection check
                                    if connected_at.elapsed().as_secs() > 10 {
                                        backoff = 1; // Reset backoff
                                    }
                                    // Skip PONG replies and subscription acks
                                    if text == "PONG" || text.contains("\"message\":\"Successfully subscribed\"") {
                                        continue;
                                    }
                                    if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                        // Skip empty arrays (subscription ack)
                                        if parsed_json.is_array() && parsed_json.as_array().unwrap().is_empty() {
                                            continue;
                                        }
                                        let events = if parsed_json.is_array() {
                                            parsed_json.as_array().unwrap().clone()
                                        } else {
                                            vec![parsed_json]
                                        };
                                        for event_val in events {
                                            // Polymarket sends price_change events with price_changes array
                                            if let Ok(evt) = serde_json::from_value::<WsPriceChangeEvent>(event_val) {
                                                if let Some(changes) = evt.price_changes {
                                                    let mut w = ob_cache.cache.write().await;
                                                    for change in changes {
                                                        if change.asset_id.is_empty() { continue; }
                                                        let entry = w.entry(change.asset_id.clone()).or_default();
                                                        entry.last_updated = std::time::Instant::now();
                                                        // Update best bid
                                                        if let Some(bid_str) = change.best_bid {
                                                            if let Ok(bid_price) = bid_str.parse::<f64>() {
                                                                if bid_price > 0.0 {
                                                                    update_level(&mut entry.bids, bid_price, 1.0, false);
                                                                }
                                                            }
                                                        }
                                                        // Update best ask
                                                        if let Some(ask_str) = change.best_ask {
                                                            if let Ok(ask_price) = ask_str.parse::<f64>() {
                                                                if ask_price > 0.0 {
                                                                    update_level(&mut entry.asks, ask_price, 1.0, true);
                                                                }
                                                            }
                                                        }
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
                                    println!("[ORDERBOOK] WS silent drop detected (no messages/pings for 120s). Reconnecting...");
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
