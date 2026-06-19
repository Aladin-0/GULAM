use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use std::time::SystemTime;

use crate::config::Config;
use crate::types::{Signal, PriceTick, LiveState};
use crate::oracle::OracleCache;
use crate::orderbook_cache::OrderbookCache;
use crate::scanner::Scanner;

pub struct SignalEngine {
    pub config: Config,
    pub oracle: OracleCache,
    pub orderbook: OrderbookCache,
    pub scanner: Scanner,
    pub signaled_markets: Arc<RwLock<HashSet<String>>>,
    pub signal_history: Arc<RwLock<Vec<Signal>>>,
    pub atomic_capital: Arc<std::sync::atomic::AtomicU64>,
    pub whale_funding_rate: Arc<RwLock<f64>>,
}

impl SignalEngine {
    pub fn new(
        config: Config,
        oracle: OracleCache,
        orderbook: OrderbookCache,
        scanner: Scanner,
        atomic_capital: Arc<std::sync::atomic::AtomicU64>,
        whale_funding_rate: Arc<RwLock<f64>>,
    ) -> Self {
        Self {
            config,
            oracle,
            orderbook,
            scanner,
            signaled_markets: Arc::new(RwLock::new(HashSet::new())),
            signal_history: Arc::new(RwLock::new(Vec::new())),
            atomic_capital,
            whale_funding_rate,
        }
    }
}

pub async fn run_signal_engine(
    engine: SignalEngine,
    queue: mpsc::Sender<Signal>,
    live_state: Arc<RwLock<LiveState>>,
    hedge_tx: mpsc::Sender<String>,
    mut tick_rx: mpsc::Receiver<PriceTick>,
) {
    let mut last_clear_ts = SystemTime::now();
    let mut last_diag_ts = SystemTime::now();
    let mut last_escape_ts = SystemTime::now();
    let mut _dropped_signals = 0u64;
    let mut dropped_hedges = 0u64;
    let mut active_subscriptions: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut last_sub_update_ts = std::time::SystemTime::UNIX_EPOCH;

    // ── Layer 3: Volatility Shield ────────────────────────────────────────────
    // Tracks last 120 Binance price ticks (~30 seconds at 250ms/tick) per symbol.
    // Before firing any signal, we check if the price range in the last 30s
    // exceeds a per-symbol threshold. If it does, the market is violent and
    // we skip the signal entirely to avoid pump-and-dump / sudden spike losses.
    let mut price_history: std::collections::HashMap<String, std::collections::VecDeque<f64>> =
        std::collections::HashMap::new();
    // Thresholds derived from forensic database analysis:
    // BTC violent = >$120 range in 30s | ETH violent = >$3.00 | SOL violent = >$0.40
    let volatility_thresholds: std::collections::HashMap<&str, f64> = [
        ("BTC", 120.0),
        ("ETH", 3.0),
        ("SOL", 0.40),
    ].iter().cloned().collect();

    println!("[SIGNAL] Signal engine started. Listening for Price Ticks...");

    while let Some(tick) = tick_rx.recv().await {
        let tick_received_at = std::time::Instant::now();
        let decision_started_at = tick_received_at;

        if !engine.orderbook.is_connected() {
            println!("[SIGNAL] ⚠️ WS DISCONNECTED — All signal evaluation BLOCKED until reconnect.");
            continue;
        }

        // Clear signaled markets every 900 seconds
        if let Ok(elapsed) = last_clear_ts.elapsed() {
            if elapsed.as_secs() >= 900 {
                let mut sm = engine.signaled_markets.write().await;
                let cleared = sm.len();
                sm.clear();
                last_clear_ts = SystemTime::now();
                if cleared > 0 {
                    println!("[SIGNAL] Period reset — cleared {} condition_id(s) from SIGNALED_MARKETS.", cleared);
                }
            }
        }

        engine.scanner.prune_expired_markets().await;
        let markets = engine.scanner.get_active_markets().await;
        
        let mut token_set = std::collections::HashSet::new();
        for market in markets.values() {
            if !market.up_token_id.is_empty() {
                token_set.insert(market.up_token_id.clone());
            }
            if !market.down_token_id.is_empty() {
                token_set.insert(market.down_token_id.clone());
            }
        }
        if token_set != active_subscriptions {
            if last_sub_update_ts.elapsed().unwrap_or(std::time::Duration::from_secs(0)).as_secs() > 10 {
                println!("[SIGNAL] Market topology changed. Updating WS subscriptions ({} tokens).", token_set.len());
                engine.orderbook.update_subscriptions(token_set.clone()).await;
                active_subscriptions = token_set;
                last_sub_update_ts = std::time::SystemTime::now();
            }
        }

        // Diagnostic printout
        if let Ok(elapsed) = last_diag_ts.elapsed() {
            if elapsed.as_secs() >= 10 {
                // In full implementation, print diagnostics.
                // We'll skip the full print output here to keep code size manageable.
                last_diag_ts = SystemTime::now();
            }
        }

        // Phase 3: Escape Hatch
        let mut run_escape = false;
        if let Ok(elapsed) = last_escape_ts.elapsed() {
            if elapsed.as_millis() >= 250 {
                run_escape = true;
                last_escape_ts = SystemTime::now();
            }
        }

        if run_escape {
            let positions = live_state.read().await.open_positions.clone();
            if !positions.is_empty() {
            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();
            let oracle_read = engine.oracle.read().await;

            for (cid, pos) in positions.iter() {
                let elapsed = now - pos.entry_time;
                if elapsed > 90.0 { // HEDGE_WINDOW_SECONDS
                    continue;
                }

                let oracle_key = format!("{}/usd", pos.symbol.to_lowercase());
                let spot = if let Some(entry) = oracle_read.get(&oracle_key) {
                    entry.price
                } else {
                    continue;
                };

                if spot == 0.0 { continue; }

                let mut oracle_breach = false;
                if (pos.side == "UP" && spot < pos.price_to_beat) || (pos.side == "DOWN" && spot > pos.price_to_beat) {
                    oracle_breach = true;
                }

                // ── Token-Price Stop-Loss ───────────────────────────────────
                // If the crowd panics and drops the token price by 9 cents from our entry,
                // we sell instantly regardless of Binance Oracle to cap our losses at ~$0.50.
                let mut token_stop_loss = false;
                let current_bid = {
                    let ob = engine.orderbook.get_orderbook(&pos.token_id).await;
                    ob.and_then(|b| b.best_bid).unwrap_or(1.0)
                };
                if current_bid < pos.entry_price - 0.09 && current_bid > 0.0 {
                    token_stop_loss = true;
                }

                let mut should_escape = false;

                if token_stop_loss {
                    println!("[HEDGE] 🛑 STOP-LOSS TRIGGERED for {}! Token dropped ≥9¢ (Entry: {:.2}¢, Now: {:.2}¢). Liquidating...", 
                        pos.symbol, pos.entry_price * 100.0, current_bid * 100.0);
                    should_escape = true;
                } else if oracle_breach {
                    // ── Crowd Oracle Confirmation ────────────────────────────────
                    let crowd_confirms = {
                        let ob = engine.orderbook.get_orderbook(&pos.token_id).await;
                        match ob {
                            Some(book) => {
                                let bid = book.best_bid.unwrap_or(1.0);
                                if bid >= engine.config.escape_crowd_threshold {
                                    println!("[HEDGE] 🧠 Crowd Oracle: Binance breached for {} but crowd bid={:.2}¢ ≥ {:.0}¢ threshold — holding, likely fake dip.",
                                        pos.symbol, bid * 100.0, engine.config.escape_crowd_threshold * 100.0);
                                    false
                                } else {
                                    true
                                }
                            }
                            None => true,
                        }
                    };

                    if crowd_confirms {
                        println!("[HEDGE] 🚨 ESCAPE HATCH TRIGGERED for {}! Crowd confirmed Binance breach. Liquidating...", pos.symbol);
                        should_escape = true;
                    }
                }

                if should_escape {
                    if let Err(e) = hedge_tx.try_send(cid.clone()) {
                        dropped_hedges += 1;
                        println!("[HEDGE] ⚠️ Trader queue full! Dropped hedge (Total dropped: {}). Err: {}", dropped_hedges, e);
                    }
                }
            }
        }
        }

        let tick_base = tick.symbol.split('/').next().unwrap_or("");

        // ── Layer 3: Update rolling price history for this symbol ─────────────
        {
            let history = price_history.entry(tick_base.to_uppercase()).or_insert_with(std::collections::VecDeque::new);
            history.push_back(tick.price);
            if history.len() > 240 { // keep last 240 ticks = ~60 seconds for Acceleration Test
                history.pop_front();
            }
        }

        // Evaluate markets
        let mut generated_this_pass: HashSet<u64> = HashSet::new();
        for (_, market) in markets.iter() {
            if !market.symbol.eq_ignore_ascii_case(tick_base) { continue; }
            let valid_symbols = ["BTC", "ETH"];
            if !valid_symbols.contains(&tick_base.to_uppercase().as_str()) {
                continue;
            }

            let cid = market.condition_id.clone();
            if engine.signaled_markets.read().await.contains(&cid) {
                continue;
            }
            let current_price = tick.price;
            let price_to_beat = {
                let r = engine.oracle.read().await;
                if let Some(entry) = r.get(&tick.symbol) {
                    entry.open_price
                } else {
                    current_price
                }
            };

            if current_price == 0.0 || price_to_beat == 0.0 { continue; }

            let t_left_s = market.time_remaining_seconds;
            if t_left_s <= 0.0 || t_left_s > engine.config.max_execution_time_seconds { continue; }

            let move_pct = (current_price - price_to_beat) / price_to_beat;
            let (correct_side, token_id, static_token_price) = if current_price >= price_to_beat {
                ("UP", &market.up_token_id, market.up_price)
            } else {
                ("DOWN", &market.down_token_id, market.down_price)
            };

            let current_capital = f64::from_bits(engine.atomic_capital.load(std::sync::atomic::Ordering::Acquire));
            let capital = if current_capital > engine.config.initial_capital { current_capital } else { engine.config.initial_capital };
            let mut required_capital = capital * engine.config.max_position_size_pct;
            if required_capital < engine.config.min_order_size_usd {
                required_capital = engine.config.min_order_size_usd;
            }
            if required_capital > capital {
                required_capital = capital;
            }
            
            let approx_shares = required_capital / static_token_price;

            let token_price = if let Some(sweep) = engine.orderbook.calculate_sweep_price(token_id, "BUY", approx_shares).await {
                if sweep > 0.0 && sweep < 1.0 { sweep } else { static_token_price }
            } else {
                static_token_price
            };

            let risk_multiplier = 1.0 + (token_price * 0.4);
            let base = engine.config.base_gap_bps / 10000.0;
            let mut dynamic_need_pct = base * risk_multiplier;
            if dynamic_need_pct < engine.config.min_dynamic_need_pct {
                dynamic_need_pct = engine.config.min_dynamic_need_pct;
            }
            if dynamic_need_pct > engine.config.max_dynamic_need_pct {
                dynamic_need_pct = engine.config.max_dynamic_need_pct;
            }

            let c1 = move_pct.abs() > dynamic_need_pct;
            let c3 = token_price <= 0.99 && token_price >= 0.97;

            if !(c1 && c3) { continue; }

            // ── Layer 2: Hard Time Window ────────────────────────
            if t_left_s > 45.0 || t_left_s < 20.0 {
                println!("[SIGNAL] ⏳ Time-Gate: Skipping {} {}¢ token — {:.1}s left (Must be 20s-45s).",
                    market.symbol, (token_price * 100.0) as u32, t_left_s);
                continue;
            }
            
            // ── Layer 2.5: Whale Radar (Funding Rate) ────────────────────────
            let current_funding = *engine.whale_funding_rate.read().await;
            if current_funding.abs() > 0.05 { // Absolute value just in case
                println!("[SIGNAL] 🐋 WHALE RADAR: High Binance Funding Rate ({:.4}%). Market is dangerous. Skipping trade.", current_funding);
                continue;
            }

            // ── Layer 3: Predictive Smart Shield ──────────────────────────────
            // Check 1: The Range Test & Check 2: The Acceleration Test
            let sym_upper = market.symbol.to_uppercase();
            let mut shield_blocked = false;
            if let Some(threshold) = volatility_thresholds.get(sym_upper.as_str()) {
                if let Some(history) = price_history.get(&sym_upper) {
                    if history.len() >= 120 { // need at least 30s of data to compare speeds
                        // Split history into recent 15s (last 60 ticks) and prev 15s (ticks 120..60 from end)
                        let recent_slice: Vec<f64> = history.iter().rev().take(60).cloned().collect();
                        let prev_slice: Vec<f64> = history.iter().rev().skip(60).take(60).cloned().collect();
                        
                        let recent_max = recent_slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                        let recent_min = recent_slice.iter().cloned().fold(f64::INFINITY, f64::min);
                        let recent_range = recent_max - recent_min;
                        
                        let prev_max = prev_slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                        let prev_min = prev_slice.iter().cloned().fold(f64::INFINITY, f64::min);
                        let prev_range = prev_max - prev_min;

                        // Check 1: Raw Range (is 30s range > threshold?)
                        let total_max = history.iter().rev().take(120).cloned().fold(f64::NEG_INFINITY, f64::max);
                        let total_min = history.iter().rev().take(120).cloned().fold(f64::INFINITY, f64::min);
                        let total_range = total_max - total_min;

                        if total_range > *threshold {
                            println!("[SHIELD] 🛡️ Range Test Failed for {} — 30s range ${:.2} > ${:.2} threshold. Skipping signal.", market.symbol, total_range, threshold);
                            shield_blocked = true;
                        } 
                        // Check 2: Acceleration Test (Did it speed up 3x? And is the recent move significant?)
                        else if recent_range > (prev_range * 3.0) && recent_range > (*threshold * 0.4) {
                            println!("[SHIELD] 🚀 Acceleration Test Failed for {} — Speed 3x normal (Recent range ${:.2} vs Prev ${:.2}). Pump detected! Skipping signal.", market.symbol, recent_range, prev_range);
                            shield_blocked = true;
                        }
                    }
                }
            }

            // Check 3: The Rubber Band Test
            // If the price is pumped too far from the target (>0.8%), the rubber band will snap.
            if move_pct.abs() > 0.008 {
                println!("[SHIELD] 🏹 Rubber Band Test Failed for {} — Price is overstretched ({:.2}% away from target). Snapback imminent! Skipping signal.", market.symbol, move_pct.abs() * 100.0);
                shield_blocked = true;
            }

            if shield_blocked { continue; }

            // Freshness check: must have received orderbook WS data recently
            if !engine.orderbook.is_fresh(token_id, 30).await {
                println!("[SIGNAL] ⚠️ Guard: Skipping signal for {} (stale orderbook data >30s)", market.symbol);
                continue;
            }

            let (is_liquid, _avail_val) = engine.orderbook.validate_liquidity(token_id, "BUY", token_price, required_capital).await;
            if !is_liquid {
                continue;
            }

            let gap = current_price - price_to_beat;
            let decision_finished_at = std::time::Instant::now();
            let lat = crate::types::LatencyMetrics {
                binance_tick_received_at: tick_received_at,
                signal_decision_started_at: decision_started_at,
                signal_decision_finished_at: decision_finished_at,
                ..Default::default()
            };

            let signal = Signal {
                condition_id: cid.clone(),
                question: market.question.clone(),
                symbol: market.symbol.clone(),
                slug: market.slug.clone(),
                side: correct_side.to_string(),
                token_id: token_id.clone(),
                entry_price: token_price,
                price_to_beat,
                current_price,
                gap,
                gap_pct: move_pct * 100.0,
                time_remaining: t_left_s,
                dynamic_need_pct,
                risk_multiplier,
                timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
                latency: Some(lat),
            };

            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            signal.symbol.hash(&mut hasher);
            signal.side.hash(&mut hasher);
            let key = hasher.finish();

            if generated_this_pass.contains(&key) { continue; }
            generated_this_pass.insert(key);

            engine.signaled_markets.write().await.insert(cid.clone());
            
            {
                let mut hist = engine.signal_history.write().await;
                hist.push(signal.clone());
                if hist.len() > 100 {
                    hist.remove(0);
                }
            }

            // (Commented out to remove hot path blocking I/O)
            // println!("\n[SIGNAL] *** SIGNAL FIRED *** | {} | {} | token: ${:.4}", signal.symbol, signal.side, signal.entry_price);
            if let Err(_e) = queue.try_send(signal) {
                _dropped_signals += 1;
                // println!("[SIGNAL] ⚠️ Trader queue full! Dropping signal to prevent backpressure (Total dropped: {}). Err: {}", _dropped_signals, _e);
            }
        }
    }
}
