use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use std::time::Duration;
use tokio::sync::mpsc;
use crate::config::Config;
use crate::types::UserWsMessage;

pub async fn run_user_ws(config: Config, user_ws_tx: mpsc::Sender<UserWsMessage>) {
    let ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
    let mut backoff = 1;

    loop {
        println!("[USER_WS] Connecting to {}...", ws_url);
        match connect_async(ws_url).await {
            Ok((mut ws_stream, _)) => {
                println!("[USER_WS] Connected.");
                backoff = 1;

                let auth_msg = serde_json::json!({
                    "type": "user",
                    "auth": {
                        "apiKey": config.polymarket_api_key,
                        "secret": config.polymarket_api_secret,
                        "passphrase": config.polymarket_api_passphrase
                    }
                });

                if let Err(e) = ws_stream.send(Message::Text(serde_json::to_string(&auth_msg).unwrap())).await {
                    println!("[USER_WS] ❌ Failed to send auth msg: {}", e);
                    continue;
                }

                let (mut ws_tx, mut ws_rx) = ws_stream.split();
                let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(10));
                
                // Do first tick immediately so we don't send heartbeat instantly
                heartbeat_interval.tick().await;

                loop {
                    tokio::select! {
                        _ = heartbeat_interval.tick() => {
                            // Send empty object `{}` as heartbeat every 10 seconds
                            if let Err(e) = ws_tx.send(Message::Text("{}".to_string())).await {
                                println!("[USER_WS] ❌ Failed to send heartbeat: {}", e);
                                break;
                            }
                        }
                        msg_opt = ws_rx.next() => {
                            match msg_opt {
                                Some(Ok(Message::Text(text))) => {
                                    // Ignore heartbeat responses or empty objects
                                    if text.trim() == "{}" { continue; }
                                    
                                    // Try parsing as array
                                    if let Ok(msgs) = serde_json::from_str::<Vec<UserWsMessage>>(&text) {
                                        for msg in msgs {
                                            let _ = user_ws_tx.send(msg).await;
                                        }
                                    } else if let Ok(msg) = serde_json::from_str::<UserWsMessage>(&text) {
                                        let _ = user_ws_tx.send(msg).await;
                                    } else if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if val.is_array() {
                                            for v in val.as_array().unwrap() {
                                                if let Ok(msg) = serde_json::from_value::<UserWsMessage>(v.clone()) {
                                                    let _ = user_ws_tx.send(msg).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(ping))) => {
                                    let _ = ws_tx.send(Message::Pong(ping)).await;
                                }
                                Some(Err(e)) => {
                                    println!("[USER_WS] ❌ Stream error: {}", e);
                                    break;
                                }
                                None => {
                                    println!("[USER_WS] ⚠️ Connection closed by remote.");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                println!("[USER_WS] ❌ Connection failed: {}", e);
            }
        }

        println!("[USER_WS] Waiting {}s before reconnecting...", backoff);
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = std::cmp::min(backoff * 2, 30);
    }
}
