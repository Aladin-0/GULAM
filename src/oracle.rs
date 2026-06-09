use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct OracleEntry {
    pub price: f64,
    pub open_price: f64,
}

pub type OracleCache = Arc<RwLock<HashMap<String, OracleEntry>>>;

#[derive(Deserialize, Debug)]
pub struct BinanceTicker {
    pub symbol: String,
    pub price: String,
}

pub async fn run_oracle(cache: OracleCache, tick_tx: tokio::sync::mpsc::Sender<crate::types::PriceTick>) {
    println!("[ORACLE] Oracle started. Initializing...");

    let symbols = vec![
        ("BTCUSDT", "btc/usd"),
        ("ETHUSDT", "eth/usd"),
        ("SOLUSDT", "sol/usd"),
    ];

    let cache_for_kline = cache.clone();
    let symbols_for_kline = symbols.clone();

    // Spawn a background task to refresh the 15m kline open_price every minute
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            for (binance_sym, cache_key) in &symbols_for_kline {
                let kline_url = format!("https://api.binance.com/api/v3/klines?symbol={}&interval=15m&limit=1", binance_sym);
                if let Ok(k_res) = client.get(&kline_url).send().await {
                    if let Ok(klines) = k_res.json::<Vec<Vec<serde_json::Value>>>().await {
                        if let Some(kline) = klines.first() {
                            if let Some(o_str) = kline.get(1).and_then(|v| v.as_str()) {
                                if let Ok(open_price) = o_str.parse::<f64>() {
                                    let mut w = cache_for_kline.write().await;
                                    let entry = w.entry(cache_key.to_string()).or_insert(OracleEntry { price: 0.0, open_price });
                                    entry.open_price = open_price;
                                }
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    });

    use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
    use futures_util::StreamExt;
    
    #[derive(Deserialize)]
    struct WsAggTrade {
        s: String, // Symbol
        p: String, // Price
    }

    let ws_url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade";

    loop {
        println!("[ORACLE] Connecting to Binance WS...");
        match connect_async(ws_url).await {
            Ok((ws_stream, _)) => {
                println!("[ORACLE] WS Connected.");
                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            // HFT Fast-Path: Zero-allocation string scanning instead of serde_json
                            if let (Some(s_idx), Some(p_idx)) = (text.find("\"s\":\""), text.find("\"p\":\"")) {
                                let s_start = s_idx + 5;
                                let p_start = p_idx + 5;
                                if let (Some(s_end), Some(p_end)) = (text[s_start..].find('"'), text[p_start..].find('"')) {
                                    let symbol = &text[s_start..s_start + s_end];
                                    let price_str = &text[p_start..p_start + p_end];
                                    if let Ok(price) = price_str.parse::<f64>() {
                                        let cache_key = match symbol {
                                            "BTCUSDT" => "btc/usd",
                                            "ETHUSDT" => "eth/usd",
                                            "SOLUSDT" => "sol/usd",
                                            _ => continue,
                                        };
                                        {
                                            let mut w = cache.write().await;
                                            let entry = w.entry(cache_key.to_string()).or_insert(OracleEntry { price, open_price: price });
                                            entry.price = price;
                                        }
                                        let _ = tick_tx.send(crate::types::PriceTick {
                                            symbol: cache_key.to_string(),
                                            price,
                                        }).await;
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            println!("[ORACLE] WS Closed by remote.");
                            break;
                        }
                        Err(e) => {
                            println!("[ORACLE] WS Error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
                println!("[ORACLE] WS Disconnected. Reconnecting in 5s...");
            }
            Err(e) => {
                println!("[ORACLE] WS Connect failed: {}. Retrying in 5s...", e);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
