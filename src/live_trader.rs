use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use std::time::SystemTime;
use chrono::Utc;
use reqwest::Client;

use crate::config::Config;
use crate::types::{Signal, Position, LiveState, TradeRecord};
use crate::state_store::StateStore;
use crate::orderbook_cache::OrderbookCache;
use crate::oracle::OracleCache;
use crate::execution::{ExecutionContext, generate_level_1_headers};

pub struct LiveTrader {
    pub config: Config,
    pub state_store: Arc<StateStore>,
    pub live_state: Arc<RwLock<LiveState>>,
    pub orderbook: OrderbookCache,
    pub oracle: OracleCache,
    pub exec_ctx: ExecutionContext,
    pub client: Client,
}

impl LiveTrader {
    pub fn new(
        config: Config,
        state_store: Arc<StateStore>,
        live_state: Arc<RwLock<LiveState>>,
        orderbook: OrderbookCache,
        oracle: OracleCache,
    ) -> Self {
        let exec_ctx = ExecutionContext::new(&config);
        Self {
            config,
            state_store,
            live_state,
            orderbook,
            oracle,
            exec_ctx,
            client: Client::new(),
        }
    }

    pub async fn startup_checks(&self) {
        if self.config.paper_trading {
            println!("[TRADER] 📝 PAPER TRADING: Skipping real balance sync. Using initial capital.");
            return;
        }
        println!("[TRADER] Running startup balance sync...");
        let headers = generate_level_1_headers("GET", "/balance-allowance", "", &self.exec_ctx.creds, &self.exec_ctx.eoa_signer_address, &self.exec_ctx.api_secret_bytes);
        let url = format!("{}/balance-allowance?asset_type=COLLATERAL&signature_type={}", self.config.polymarket_host, self.config.polymarket_sig_type);
        
        if let Ok(res) = self.client.get(&url).headers(headers).send().await {
            if let Ok(json) = res.json::<serde_json::Value>().await {
                if let Some(balance_str) = json.get("balance").and_then(|v| v.as_str()) {
                    if let Ok(bal) = balance_str.parse::<f64>() {
                        let actual_balance = bal / 1_000_000.0;
                        let mut state = self.live_state.write().await;
                        // Deduct open positions cost from actual balance to get available
                        let reserved: f64 = state.open_positions.values().map(|p| p.cost).sum();
                        state.capital = actual_balance;
                        state.available_capital = actual_balance - reserved;
                        self.state_store.save_scalar("live_available_capital", &state.available_capital);
                        println!("[TRADER] Startup synced live balance: ${:.2} (Available: ${:.2})", actual_balance, state.available_capital);
                    }
                }
            } else {
                println!("[TRADER] ⚠️ Failed to parse balance-allowance.");
            }
        } else {
            println!("[TRADER] ⚠️ Network error fetching startup balance.");
        }
    }

    pub async fn process_signal(&self, signal: Signal) {
        let (mut available, mut total_equity) = {
            let state = self.live_state.read().await;
            let reserved: f64 = state.open_positions.values().map(|p| p.cost).sum();
            (state.available_capital, state.available_capital + reserved)
        };

        let mut size_usd = total_equity * self.config.max_position_size_pct;
        if size_usd > available { size_usd = available; }

        if size_usd < self.config.min_order_size_usd {
            println!("[TRADER] Skipping. Size ${:.2} < Min ${:.2}", size_usd, self.config.min_order_size_usd);
            return;
        }

        let approx_shares = size_usd / signal.entry_price;
        let mut order_price = if let Some(sweep_price) = self.orderbook.calculate_sweep_price(&signal.token_id, "BUY", approx_shares).await {
            sweep_price
        } else {
            signal.entry_price + 0.005 // fallback
        };

        if order_price > self.config.max_token_price {
            order_price = self.config.max_token_price;
        }

        let shares = size_usd / order_price;
        let cost = size_usd;
        
        // --- REAL LIVE EXECUTION PATH ---
        println!("[TRADER] Executing live BUY: {} shares of {} at ${:.4} (Cost: ${:.2})", shares, signal.symbol, order_price, cost);
        
        let maker_amount = (shares * order_price * 1_000_000.0) as u64;
        let taker_amount = (shares * 1_000_000.0) as u64;

        let salt = ethers::types::U256::from(Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64);
        let order_timestamp = ethers::types::U256::from(Utc::now().timestamp());

        let mut order_abi = self.exec_ctx.order_abi_template;
        salt.to_big_endian(&mut order_abi[32..64]);
        let token_id_u256 = ethers::types::U256::from_str_radix(&signal.token_id, 10).unwrap_or_default();
        token_id_u256.to_big_endian(&mut order_abi[128..160]);
        ethers::types::U256::from(maker_amount).to_big_endian(&mut order_abi[160..192]);
        ethers::types::U256::from(taker_amount).to_big_endian(&mut order_abi[192..224]);
        order_timestamp.to_big_endian(&mut order_abi[288..320]);
        
        let struct_hash = ethers::utils::keccak256(order_abi);

        let mut solady_abi = self.exec_ctx.solady_abi_template;
        solady_abi[32..64].copy_from_slice(&struct_hash);
        let typed_data_sign_struct_hash = ethers::utils::keccak256(solady_abi);
        
        let mut digest_input = [0u8; 66];
        digest_input[0] = 0x19;
        digest_input[1] = 0x01;
        digest_input[2..34].copy_from_slice(&self.exec_ctx.domain_separator);
        digest_input[34..66].copy_from_slice(&typed_data_sign_struct_hash);
        
        let digest = ethers::utils::keccak256(digest_input);
        let signature = self.exec_ctx.wallet.sign_hash(digest.into()).expect("Failed to sign digest");
        let mut signature_bytes = signature.to_vec();
        
        let mut final_signature = String::with_capacity(634);
        final_signature.push_str("0x");
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        for &b in &signature_bytes {
            final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
            final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
        }
        for &b in &self.exec_ctx.domain_separator {
            final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
            final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
        }
        for &b in &struct_hash {
            final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
            final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
        }
        let order_type_string = b"Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
        for &b in order_type_string {
            final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
            final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
        }
        let len_u16 = order_type_string.len() as u16;
        for &b in &len_u16.to_be_bytes() {
            final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
            final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
        }

        let expiration = Utc::now().timestamp() + 5;
        let json_body = format!(
            r#"{{"owner":"{}","order":{{"salt":{},"maker":"{}","signer":"{}","tokenId":"{}","makerAmount":"{}","takerAmount":"{}","side":"BUY","expiration":"{}","signatureType":{},"timestamp":"{}","metadata":"0x0000000000000000000000000000000000000000000000000000000000000000","builder":"0x0000000000000000000000000000000000000000000000000000000000000000","signature":"{}"}},"orderType":"GTC","deferExec":false,"postOnly":false}}"#,
            self.config.polymarket_proxy_wallet,
            salt.low_u64(),
            self.exec_ctx.proxy_addr_str,
            self.exec_ctx.proxy_addr_str,
            signal.token_id,
            maker_amount,
            taker_amount,
            expiration,
            self.config.polymarket_sig_type,
            order_timestamp,
            final_signature
        );
        
        let mut signal_mut = signal.clone();
        if let Some(ref mut lat) = signal_mut.latency {
            lat.order_build_finished_at = Some(std::time::Instant::now());
            lat.http_send_started_at = Some(std::time::Instant::now());
        }

        let mut order_id = String::new();
        let mut filled = false;
        let mut fill_price = order_price;
        let mut fill_shares = shares;

        if self.config.paper_trading {
            if let Some(ref mut lat) = signal_mut.latency {
                lat.http_ack_received_at = Some(std::time::Instant::now());
                lat.user_ws_fill_received_at = Some(std::time::Instant::now());
            }

            println!("[TRADER] 📝 PAPER TRADING: Simulating instant fill for {} shares of {} at ${:.4}", shares, signal_mut.symbol, order_price);
            order_id = format!("paper_order_{}", Utc::now().timestamp());
            filled = true;
        } else {
            let url = format!("{}/order", self.config.polymarket_host);
            let headers = generate_level_1_headers("POST", "/order", &json_body, &self.exec_ctx.creds, &self.exec_ctx.eoa_signer_address, &self.exec_ctx.api_secret_bytes);
            let post_result = self.client.post(&url).headers(headers).body(json_body).send().await;
            
            if let Some(ref mut lat) = signal_mut.latency {
                lat.http_ack_received_at = Some(std::time::Instant::now());
            }

            if let Ok(res) = post_result {
                if let Ok(json) = res.json::<serde_json::Value>().await {
                    if let Some(oid) = json.get("orderID").and_then(|v| v.as_str()) {
                        order_id = oid.to_string();
                    } else if let Some(oid) = json.get("orderId").and_then(|v| v.as_str()) {
                        order_id = oid.to_string();
                    }
                }
            }

            if order_id.is_empty() {
                println!("[TRADER] 🚫 Order submission failed or no orderID returned. Aborting.");
                return;
            }

            // Instead of polling, we now register the pending order and let the WS loop handle it.
            let pending = crate::types::PendingOrder {
                order_id: order_id.clone(),
                signal: signal_mut.clone(),
                approx_shares: shares,
                order_price,
                submitted_at: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
                latency: signal_mut.latency.clone(),
            };
            {
                let mut state = self.live_state.write().await;
                state.pending_orders.insert(order_id.clone(), pending);
            }
            println!("[TRADER] ⏳ Order {} submitted via REST. Awaiting User WS fill confirmation...", order_id);
            return; // Exit process_signal without mutating capital or open_positions!
        }
        
        // This is only reached in PAPER TRADING mode.
        if !filled {
            return;
        }

        let actual_cost = fill_price * fill_shares;
        let clob_entry_fee = actual_cost * self.config.clob_fee_pct;

        let position = Position {
            condition_id: signal_mut.condition_id.clone(),
            question: signal_mut.question.clone(),
            symbol: signal_mut.symbol.clone(),
            side: signal_mut.side.clone(),
            token_id: signal_mut.token_id.clone(),
            slug: signal_mut.slug.clone(),
            entry_price: fill_price,
            price_to_beat: signal_mut.price_to_beat,
            shares: fill_shares,
            cost: actual_cost,
            clob_entry_fee,
            entry_time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
            time_remaining: signal_mut.time_remaining,
            order_id,
            uncertain_resolve_at: None,
        };

        if let Some(ref mut lat) = signal_mut.latency {
            lat.position_recorded_at = Some(std::time::Instant::now());
        }
        self.print_latency(&signal_mut, &signal_mut.latency);

        {
            let mut state = self.live_state.write().await;
            state.available_capital -= actual_cost;
            state.open_positions.insert(signal_mut.condition_id.clone(), position.clone());
            self.state_store.save_scalar("live_available_capital", &state.available_capital);
        }
        let store = self.state_store.clone();
        let cid = signal_mut.condition_id.clone();
        let pos_clone = position.clone();
        tokio::task::spawn_blocking(move || {
            store.save_position(&cid, &pos_clone);
        });
        println!("[TRADER] ✅ Position recorded: {} (Cost: ${:.2})", signal_mut.symbol, actual_cost);
    }

    pub async fn process_hedge(&self, condition_id: String) {
        let pos = {
            let state = self.live_state.read().await;
            state.open_positions.get(&condition_id).cloned()
        };

        if let Some(pos) = pos {
            println!("[TRADER] Hedging position {}...", pos.symbol);
            
            let mut exit_price = 0.0;
            if let Some(book) = self.orderbook.get_orderbook(&pos.token_id).await {
                let mut best_bid = 0.0;
                for p_str in book.bids.keys() {
                    if let Ok(p) = p_str.parse::<f64>() {
                        if p > best_bid { best_bid = p; }
                    }
                }
                exit_price = best_bid;
            }
            if exit_price <= 0.0 { exit_price = pos.entry_price * 0.99; }
            if exit_price < 0.0 { exit_price = 0.0; }

            let mut order_id = format!("hedge_order_{}", Utc::now().timestamp());
            let mut filled = false;
            let mut fill_price = exit_price;
            let fill_shares = pos.shares;

            if self.config.paper_trading {
                println!("[TRADER] 📝 PAPER TRADING: Simulating instant fill for {} shares of {} at ${:.4} (SELL)", pos.shares, pos.symbol, exit_price);
                filled = true;
            } else {
                let maker_amount = (pos.shares * 1_000_000.0) as u64;
                let taker_amount = (pos.shares * exit_price * 1_000_000.0) as u64;

                let salt = ethers::types::U256::from(Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64);
                let order_timestamp = ethers::types::U256::from(Utc::now().timestamp());

                let mut order_abi = self.exec_ctx.order_abi_template;
                salt.to_big_endian(&mut order_abi[32..64]);
                let token_id_u256 = ethers::types::U256::from_str_radix(&pos.token_id, 10).unwrap_or_default();
                token_id_u256.to_big_endian(&mut order_abi[128..160]);
                ethers::types::U256::from(maker_amount).to_big_endian(&mut order_abi[160..192]);
                ethers::types::U256::from(taker_amount).to_big_endian(&mut order_abi[192..224]);
                ethers::types::U256::from(1).to_big_endian(&mut order_abi[224..256]); // SELL is 1
                order_timestamp.to_big_endian(&mut order_abi[288..320]);
                
                let struct_hash = ethers::utils::keccak256(order_abi);

                let mut solady_abi = self.exec_ctx.solady_abi_template;
                solady_abi[32..64].copy_from_slice(&struct_hash);
                let typed_data_sign_struct_hash = ethers::utils::keccak256(solady_abi);
                
                let mut digest_input = [0u8; 66];
                digest_input[0] = 0x19;
                digest_input[1] = 0x01;
                digest_input[2..34].copy_from_slice(&self.exec_ctx.domain_separator);
                digest_input[34..66].copy_from_slice(&typed_data_sign_struct_hash);
                
                let digest = ethers::utils::keccak256(digest_input);
                let signature = self.exec_ctx.wallet.sign_hash(digest.into()).expect("Failed to sign digest");
                let signature_bytes = signature.to_vec();
                
                let mut final_signature = String::with_capacity(634);
                final_signature.push_str("0x");
                const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
                for &b in &signature_bytes {
                    final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
                    final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
                }
                for &b in &self.exec_ctx.domain_separator {
                    final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
                    final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
                }
                for &b in &struct_hash {
                    final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
                    final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
                }
                let order_type_string = b"Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
                for &b in order_type_string {
                    final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
                    final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
                }
                let len_u16 = order_type_string.len() as u16;
                for &b in &len_u16.to_be_bytes() {
                    final_signature.push(HEX_CHARS[(b >> 4) as usize] as char);
                    final_signature.push(HEX_CHARS[(b & 0x0F) as usize] as char);
                }

                let order_struct = serde_json::json!({
                    "salt": salt.low_u64(),
                    "maker": self.exec_ctx.proxy_addr_str,
                    "signer": self.exec_ctx.proxy_addr_str,
                    "tokenId": pos.token_id,
                    "makerAmount": maker_amount.to_string(),
                    "takerAmount": taker_amount.to_string(),
                    "side": "SELL",
                    "expiration": (Utc::now().timestamp() + 5).to_string(),
                    "signatureType": self.config.polymarket_sig_type,
                    "timestamp": order_timestamp.to_string(),
                    "metadata": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "builder": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "signature": final_signature
                });

                let final_payload = serde_json::json!({
                    "owner": self.config.polymarket_proxy_wallet,
                    "order": order_struct,
                    "orderType": "GTC",
                    "deferExec": false,
                    "postOnly": false
                });

                let json_body = serde_json::to_string(&final_payload).unwrap();
                let url = format!("{}/order", self.config.polymarket_host);
                
                let post_result = {
                    let headers = generate_level_1_headers("POST", "/order", &json_body, &self.exec_ctx.creds, &self.exec_ctx.eoa_signer_address, &self.exec_ctx.api_secret_bytes);
                    self.client.post(&url).headers(headers).body(json_body).send().await
                };

                if let Ok(res) = post_result {
                    if let Ok(json) = res.json::<serde_json::Value>().await {
                        if let Some(oid) = json.get("orderID").and_then(|v| v.as_str()) {
                            order_id = oid.to_string();
                        } else if let Some(oid) = json.get("orderId").and_then(|v| v.as_str()) {
                            order_id = oid.to_string();
                        }
                    }
                }

                if order_id.is_empty() || order_id.starts_with("hedge_order_") {
                    println!("[TRADER] 🚫 Hedge submission failed or no orderID returned. Aborting hedge.");
                    return;
                }

                // Instead of polling, register pending hedge
                let pending = crate::types::PendingHedge {
                    order_id: order_id.clone(),
                    position: pos.clone(),
                    approx_shares: pos.shares,
                    order_price: exit_price,
                    submitted_at: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
                };
                {
                    let mut state = self.live_state.write().await;
                    state.open_positions.remove(&condition_id);
                    state.pending_hedges.insert(order_id.clone(), pending);
                }
                println!("[TRADER] ⏳ Hedge order {} submitted via REST. Awaiting User WS fill confirmation...", order_id);
                return;
            }

            // This is only reached in PAPER TRADING mode.
            if !filled {
                println!("[TRADER] 🚫 Hedge order {} not filled. Aborting hedge state updates.", order_id);
                return;
            }
            exit_price = fill_price;

            let clob_exit_fee = (exit_price * pos.shares) * self.config.clob_fee_pct;
            let gross_profit = (exit_price - pos.entry_price) * pos.shares;
            let net_profit = gross_profit - pos.clob_entry_fee - clob_exit_fee;

            let mut state = self.live_state.write().await;
            state.available_capital += pos.cost + net_profit;
            state.total_profit += net_profit;
            state.daily_profit += net_profit;
            state.total_trades += 1;
            state.daily_trades += 1;

            if net_profit > 0.0 { state.winning_trades += 1; }
            else { state.losing_trades += 1; state.loss_count += 1; state.total_lost_usd += net_profit.abs(); }

            let record = TradeRecord {
                condition_id: pos.condition_id.clone(),
                question: pos.question.clone(),
                symbol: pos.symbol.clone(),
                side: pos.side.clone(),
                entry_price: pos.entry_price,
                exit_price,
                shares: pos.shares,
                cost: pos.cost,
                gross_profit,
                clob_round_trip_fee: pos.clob_entry_fee + clob_exit_fee,
                profit: net_profit,
                pnl_pct: (net_profit / pos.cost) * 100.0,
                reason: "hedge_dump".to_string(),
                entry_time: pos.entry_time,
                exit_time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
                order_id: format!("hedge_order_{}", Utc::now().timestamp()),
            };

            state.trade_history.push(record.clone());
            let store = self.state_store.clone();
            let rec_clone = record.clone();
            let cid_clone = condition_id.clone();
            let state_cap = state.available_capital;
            tokio::task::spawn_blocking(move || {
                store.save_trade_record(&rec_clone);
                store.save_scalar("live_available_capital", &state_cap);
                store.delete_position(&cid_clone);
            });
            state.open_positions.remove(&condition_id);
        }
    }

    pub async fn check_settlements(&self) {
        let condition_ids: Vec<String> = {
            let state = self.live_state.read().await;
            state.open_positions.keys().cloned().collect()
        };

        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();

        for cid in condition_ids {
            let mut remove = false;
            let mut record_opt = None;
            let mut net_profit = 0.0;
            let mut pos_cost = 0.0;

            {
                let mut state = self.live_state.write().await;
                if let Some(pos) = state.open_positions.get_mut(&cid) {
                    if now - pos.entry_time > self.config.max_position_age_seconds {
                        remove = true;
                        
                        // Verification Chain
                        let mut exit_price = 0.0;
                        let mut resolved = false;

                        // 1. Check Trade API (Order Status)
                        let path = format!("/order/{}", pos.order_id);
                        let headers = generate_level_1_headers("GET", &path, "", &self.exec_ctx.creds, &self.exec_ctx.eoa_signer_address, &self.exec_ctx.api_secret_bytes);
                        let url = format!("{}{}", self.config.polymarket_host, path);
                        
                        if let Ok(res) = self.client.get(&url).headers(headers).send().await {
                            if let Ok(json) = res.json::<serde_json::Value>().await {
                                let status = json.get("status").and_then(|v| v.as_str()).unwrap_or("");
                                if status == "CANCELED" || status == "EXPIRED" {
                                    resolved = true;
                                    exit_price = 0.0;
                                }
                            }
                        }

                        // 2. Check Oracle if Trade API didn't conclusively resolve it
                        if !resolved {
                            let sym_upper = format!("{}USDT", pos.symbol.to_uppercase());
                            let sym_lower = format!("{}usdt", pos.symbol.to_lowercase());
                            let sym_old = format!("{}/usd", pos.symbol.to_lowercase());
                            
                            let oracle = self.oracle.read().await;
                            let entry_opt = oracle.get(&sym_upper)
                                .or_else(|| oracle.get(&sym_lower))
                                .or_else(|| oracle.get(&sym_old));

                            if let Some(entry) = entry_opt {
                                if entry.price > 0.0 {
                                    if (pos.side == "UP" && entry.price >= pos.price_to_beat) || 
                                       (pos.side == "DOWN" && entry.price < pos.price_to_beat) {
                                        resolved = true;
                                        exit_price = 1.0; // Payout token worth $1 if won
                                    } else {
                                        resolved = true;
                                        exit_price = 0.0; // Worth $0 if lost
                                    }
                                }
                            }
                        }

                        if !resolved {
                            // Fallback conservative exit
                            exit_price = 0.0;
                        }

                        let clob_exit_fee = (exit_price * pos.shares) * self.config.clob_fee_pct;
                        let gross_profit = (exit_price - pos.entry_price) * pos.shares;
                        net_profit = gross_profit - pos.clob_entry_fee - clob_exit_fee;
                        pos_cost = pos.cost;

                        record_opt = Some(TradeRecord {
                            condition_id: pos.condition_id.clone(),
                            question: pos.question.clone(),
                            symbol: pos.symbol.clone(),
                            side: pos.side.clone(),
                            entry_price: pos.entry_price,
                            exit_price,
                            shares: pos.shares,
                            cost: pos.cost,
                            gross_profit,
                            clob_round_trip_fee: pos.clob_entry_fee + clob_exit_fee,
                            profit: net_profit,
                            pnl_pct: (net_profit / pos.cost) * 100.0,
                            reason: "expired_unresolved".to_string(),
                            entry_time: pos.entry_time,
                            exit_time: now,
                            order_id: format!("expire_{}_{}", pos.symbol, Utc::now().timestamp()),
                        });
                    }
                }
            }

            if remove {
                let mut state = self.live_state.write().await;
                state.open_positions.remove(&cid);
                if let Some(rec) = record_opt {
                    state.trade_history.push(rec.clone());
                    let store = self.state_store.clone();
                    let rec_clone = rec.clone();
                    let cid_clone = cid.clone();
                    let state_cap = state.available_capital;
                    tokio::task::spawn_blocking(move || {
                        store.save_trade_record(&rec_clone);
                        store.save_scalar("live_available_capital", &state_cap);
                        store.delete_position(&cid_clone);
                    });
                    println!("[SETTLEMENT] Position expired: {}", cid);
                }
            }
        }
    }
    pub async fn finalize_pending_order(&self, order_id: &str, filled: bool, mut fill_price: f64, mut fill_shares: f64) {
        let pending_opt = {
            let mut state = self.live_state.write().await;
            state.pending_orders.remove(order_id)
        };

        if let Some(mut pending) = pending_opt {
            if !filled {
                println!("[TRADER] 🚫 Pending Order {} canceled/failed. Dropping without mutating state.", order_id);
                return;
            }

            if let Some(ref mut lat) = pending.latency {
                lat.user_ws_fill_received_at = Some(std::time::Instant::now());
            }

            if fill_price <= 0.0 { fill_price = pending.order_price; }
            if fill_shares <= 0.0 { fill_shares = pending.approx_shares; }

            let actual_cost = fill_price * fill_shares;
            let clob_entry_fee = actual_cost * self.config.clob_fee_pct;

            let position = Position {
                condition_id: pending.signal.condition_id.clone(),
                question: pending.signal.question.clone(),
                symbol: pending.signal.symbol.clone(),
                side: pending.signal.side.clone(),
                token_id: pending.signal.token_id.clone(),
                slug: pending.signal.slug.clone(),
                entry_price: fill_price,
                price_to_beat: pending.signal.price_to_beat,
                shares: fill_shares,
                cost: actual_cost,
                clob_entry_fee,
                entry_time: pending.submitted_at,
                time_remaining: pending.signal.time_remaining,
                order_id: order_id.to_string(),
                uncertain_resolve_at: None,
            };

            if let Some(ref mut lat) = pending.latency {
                lat.position_recorded_at = Some(std::time::Instant::now());
            }
            self.print_latency(&pending.signal, &pending.latency);

            {
                let mut state = self.live_state.write().await;
                state.available_capital -= actual_cost;
                state.open_positions.insert(pending.signal.condition_id.clone(), position.clone());
                self.state_store.save_scalar("live_available_capital", &state.available_capital);
            }
            let store = self.state_store.clone();
            let cid = pending.signal.condition_id.clone();
            let pos_clone = position.clone();
            tokio::task::spawn_blocking(move || {
                store.save_position(&cid, &pos_clone);
            });
            println!("[TRADER] ✅ Position recorded via WS: {} (Cost: ${:.2})", pending.signal.symbol, actual_cost);
        }
    }

    pub async fn finalize_pending_hedge(&self, order_id: &str, filled: bool, mut fill_price: f64) {
        let pending_opt = {
            let mut state = self.live_state.write().await;
            state.pending_hedges.remove(order_id)
        };

        if let Some(pending) = pending_opt {
            if !filled {
                println!("[TRADER] 🚫 Pending Hedge {} canceled/failed. Restoring position to open state.", order_id);
                let mut state = self.live_state.write().await;
                state.open_positions.insert(pending.position.condition_id.clone(), pending.position);
                return;
            }

            if fill_price <= 0.0 { fill_price = pending.order_price; }
            let pos = pending.position;
            let exit_price = fill_price;

            let clob_exit_fee = (exit_price * pos.shares) * self.config.clob_fee_pct;
            let gross_profit = (exit_price - pos.entry_price) * pos.shares;
            let net_profit = gross_profit - pos.clob_entry_fee - clob_exit_fee;

            let mut state = self.live_state.write().await;
            state.available_capital += pos.cost + net_profit;
            state.total_profit += net_profit;
            state.daily_profit += net_profit;
            state.total_trades += 1;
            state.daily_trades += 1;

            if net_profit > 0.0 { state.winning_trades += 1; }
            else { state.losing_trades += 1; state.loss_count += 1; state.total_lost_usd += net_profit.abs(); }

            let record = TradeRecord {
                condition_id: pos.condition_id.clone(),
                question: pos.question.clone(),
                symbol: pos.symbol.clone(),
                side: pos.side.clone(),
                entry_price: pos.entry_price,
                exit_price,
                shares: pos.shares,
                cost: pos.cost,
                gross_profit,
                clob_round_trip_fee: pos.clob_entry_fee + clob_exit_fee,
                profit: net_profit,
                pnl_pct: (net_profit / pos.cost) * 100.0,
                reason: "hedge_dump".to_string(),
                entry_time: pos.entry_time,
                exit_time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
                order_id: order_id.to_string(),
            };

            state.trade_history.push(record.clone());
            self.state_store.save_scalar("live_available_capital", &state.available_capital);
            
            let store = self.state_store.clone();
            let rec_clone = record.clone();
            let cid_clone = pos.condition_id.clone();
            tokio::task::spawn_blocking(move || {
                store.save_trade_record(&rec_clone);
                store.delete_position(&cid_clone);
            });
            println!("[SETTLEMENT] ✅ Hedge exit completed via WS: {} (Profit: ${:.2})", pos.symbol, net_profit);
        }
    }

    pub async fn reconcile_pending_orders(&self) {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();
        let pending_ids: Vec<(String, f64)> = {
            let state = self.live_state.read().await;
            state.pending_orders.iter().map(|(k, v)| (k.clone(), v.submitted_at)).collect()
        };
        let pending_hedge_ids: Vec<(String, f64)> = {
            let state = self.live_state.read().await;
            state.pending_hedges.iter().map(|(k, v)| (k.clone(), v.submitted_at)).collect()
        };

        for (oid, sub_time) in pending_ids {
            if now - sub_time > 6.0 {
                let path = format!("/order/{}", oid);
                let headers = generate_level_1_headers("GET", &path, "", &self.exec_ctx.creds, &self.exec_ctx.eoa_signer_address, &self.exec_ctx.api_secret_bytes);
                let url = format!("{}{}", self.config.polymarket_host, path);
                
                if let Ok(res) = self.client.get(&url).headers(headers).send().await {
                    if let Ok(json) = res.json::<serde_json::Value>().await {
                        let status = json.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        if status == "MATCHED" || status == "FILLED" {
                            let mut fill_price = 0.0;
                            let mut fill_shares = 0.0;
                            if let Some(ep_str) = json.get("average_price").and_then(|v| v.as_str()) {
                                if let Ok(ep) = ep_str.parse::<f64>() { fill_price = ep; }
                            }
                            if let Some(sh_str) = json.get("size_matched").and_then(|v| v.as_str()) {
                                if let Ok(sh) = sh_str.parse::<f64>() { fill_shares = sh; }
                            }
                            println!("[TRADER] ⚠️ WS missed fill. Reconciled via REST for {}", oid);
                            self.finalize_pending_order(&oid, true, fill_price, fill_shares).await;
                        } else if status == "CANCELED" || status == "EXPIRED" || status == "REJECTED" || status == "FAILED" {
                            self.finalize_pending_order(&oid, false, 0.0, 0.0).await;
                        }
                    }
                }
            }
        }

        for (oid, sub_time) in pending_hedge_ids {
            if now - sub_time > 6.0 {
                let path = format!("/order/{}", oid);
                let headers = generate_level_1_headers("GET", &path, "", &self.exec_ctx.creds, &self.exec_ctx.eoa_signer_address, &self.exec_ctx.api_secret_bytes);
                let url = format!("{}{}", self.config.polymarket_host, path);
                
                if let Ok(res) = self.client.get(&url).headers(headers).send().await {
                    if let Ok(json) = res.json::<serde_json::Value>().await {
                        let status = json.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        if status == "MATCHED" || status == "FILLED" {
                            let mut fill_price = 0.0;
                            if let Some(ep_str) = json.get("average_price").and_then(|v| v.as_str()) {
                                if let Ok(ep) = ep_str.parse::<f64>() { fill_price = ep; }
                            }
                            println!("[TRADER] ⚠️ WS missed fill. Reconciled hedge via REST for {}", oid);
                            self.finalize_pending_hedge(&oid, true, fill_price).await;
                        } else if status == "CANCELED" || status == "EXPIRED" || status == "REJECTED" || status == "FAILED" {
                            self.finalize_pending_hedge(&oid, false, 0.0).await;
                        }
                    }
                }
            }
        }
    }

    pub fn print_latency(&self, signal: &crate::types::Signal, lat_opt: &Option<crate::types::LatencyMetrics>) {
        if let Some(lat) = lat_opt {
            let decision_us = lat.signal_decision_finished_at.duration_since(lat.signal_decision_started_at).as_micros();
            
            let mut order_build_us = 0;
            if let (Some(s), Some(f)) = (lat.order_build_started_at, lat.order_build_finished_at) {
                order_build_us = f.duration_since(s).as_micros();
            }

            let mut http_ack_us = 0;
            if let (Some(s), Some(f)) = (lat.http_send_started_at, lat.http_ack_received_at) {
                http_ack_us = f.duration_since(s).as_micros();
            }

            let mut ack_to_fill_us = 0;
            let mut total_us = 0;
            if let (Some(s), Some(f)) = (lat.http_ack_received_at, lat.user_ws_fill_received_at) {
                ack_to_fill_us = f.duration_since(s).as_micros();
            }
            if let Some(f) = lat.position_recorded_at {
                total_us = f.duration_since(lat.binance_tick_received_at).as_micros();
            }

            println!("[LATENCY] ✅ [{}] Decision: {}µs | Build: {}µs | HTTP: {}µs | WS_Fill: {}µs | Total: {}µs",
                signal.symbol, decision_us, order_build_us, http_ack_us, ack_to_fill_us, total_us);
        }
    }
}

pub async fn run_user_ws_fill_processor(trader: Arc<LiveTrader>, mut user_ws_rx: mpsc::Receiver<crate::types::UserWsMessage>) {
    println!("[TRADER] User WS Fill Processor started.");
    loop {
        tokio::select! {
            Some(msg) = user_ws_rx.recv() => {
                let status = msg.status.clone().unwrap_or_default();
                let event = msg.event_type.clone().or(msg.event).unwrap_or_default();
                let oid = msg.order_id.clone().or(msg.order_id_alt).unwrap_or_default();
                
                if oid.is_empty() {
                    continue;
                }

                if status == "MATCHED" || status == "FILLED" || event.to_uppercase().contains("FILL") {
                    let mut fill_price = 0.0;
                    let mut fill_shares = 0.0;
                    if let Some(ep_str) = &msg.average_price {
                        if let Ok(ep) = ep_str.parse::<f64>() { fill_price = ep; }
                    }
                    if let Some(sh_str) = &msg.size_matched {
                        if let Ok(sh) = sh_str.parse::<f64>() { fill_shares = sh; }
                    }

                    let is_hedge = {
                        let state = trader.live_state.read().await;
                        state.pending_hedges.contains_key(&oid)
                    };

                    if is_hedge {
                        trader.finalize_pending_hedge(&oid, true, fill_price).await;
                    } else {
                        trader.finalize_pending_order(&oid, true, fill_price, fill_shares).await;
                    }
                } else if status == "CANCELED" || status == "EXPIRED" || status == "FAILED" || status == "REJECTED" {
                    let is_hedge = {
                        let state = trader.live_state.read().await;
                        state.pending_hedges.contains_key(&oid)
                    };
                    
                    if is_hedge {
                        trader.finalize_pending_hedge(&oid, false, 0.0).await;
                    } else {
                        trader.finalize_pending_order(&oid, false, 0.0, 0.0).await;
                    }
                }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                trader.reconcile_pending_orders().await;
            }
        }
    }
}

pub async fn run_live_trader(
    trader: Arc<LiveTrader>,
    mut signal_rx: mpsc::Receiver<Signal>,
    mut hedge_rx: mpsc::Receiver<String>,
) {
    trader.startup_checks().await;
    println!("[TRADER] Live trader loop started.");
    
    // TCP Warmer: Keep reqwest connection pool alive (Prevents 50ms TLS handshake latency)
    let tcp_client = trader.client.clone();
    let tcp_host = trader.config.polymarket_host.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
            let url = format!("{}/time", tcp_host);
            let _ = tcp_client.get(&url).send().await;
        }
    });

    let execution_semaphore = Arc::new(tokio::sync::Semaphore::new(4));

    let settle_trader = trader.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            settle_trader.check_settlements().await;
        }
    });

    let mut last_day = Utc::now().date_naive();

    loop {
        // Daily Reset
        let current_day = Utc::now().date_naive();
        if current_day > last_day {
            let mut state = trader.live_state.write().await;
            state.daily_profit = 0.0;
            state.daily_trades = 0;
            last_day = current_day;
            println!("[TRADER] 🌅 Daily Reset triggered.");
        }

        // Daily Halt Check
        let daily_pnl_pct = {
            let state = trader.live_state.read().await;
            if state.capital > 0.0 {
                (state.daily_profit / state.capital) * 100.0
            } else {
                0.0
            }
        };

        if daily_pnl_pct <= -trader.config.daily_loss_limit_pct {
            println!("[TRADER] 🚨 DAILY LOSS LIMIT EXCEEDED ({}%). Halting trading.", daily_pnl_pct);
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            continue; // Wait until next day
        }

        tokio::select! {
            Some(signal) = signal_rx.recv() => {
                let t = trader.clone();
                let sem = execution_semaphore.clone();
                tokio::spawn(async move {
                    if let Ok(_permit) = sem.acquire().await {
                        t.process_signal(signal).await;
                    }
                });
            }
            Some(hedge_cid) = hedge_rx.recv() => {
                let t = trader.clone();
                let sem = execution_semaphore.clone();
                tokio::spawn(async move {
                    if let Ok(_permit) = sem.acquire().await {
                        t.process_hedge(hedge_cid).await;
                    }
                });
            }
        }
    }
}
