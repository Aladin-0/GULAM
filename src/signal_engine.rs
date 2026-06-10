use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use chrono::Utc;
use std::time::SystemTime;

use crate::config::Config;
use crate::types::{Signal, Position, PriceTick, LiveState};
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
}

impl SignalEngine {
    pub fn new(
        config: Config,
        oracle: OracleCache,
        orderbook: OrderbookCache,
        scanner: Scanner,
        atomic_capital: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            config,
            oracle,
            orderbook,
            scanner,
            signaled_markets: Arc::new(RwLock::new(HashSet::new())),
            signal_history: Arc::new(RwLock::new(Vec::new())),
            atomic_capital,
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
    let mut dropped_signals = 0u64;
    let mut dropped_hedges = 0u64;
    let mut active_subscriptions: std::collections::HashSet<String> = std::collections::HashSet::new();

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
            println!("[SIGNAL] Market topology changed. Updating WS subscriptions ({} tokens).", token_set.len());
            engine.orderbook.update_subscriptions(token_set.clone()).await;
            active_subscriptions = token_set;
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

                let mut breach = false;
                if pos.side == "UP" && spot < pos.price_to_beat {
                    breach = true;
                } else if pos.side == "DOWN" && spot > pos.price_to_beat {
                    breach = true;
                }

                if breach {
                    println!("[HEDGE] 🚨 ESCAPE HATCH TRIGGERED for {}! Liquidating...", pos.symbol);
                    if let Err(e) = hedge_tx.try_send(cid.clone()) {
                        dropped_hedges += 1;
                        println!("[HEDGE] ⚠️ Trader queue full! Dropped hedge (Total dropped: {}). Err: {}", dropped_hedges, e);
                    }
                }
            }
        }
        }

        let tick_base = tick.symbol.split('/').next().unwrap_or("");

        // Evaluate markets
        let mut generated_this_pass: HashSet<u64> = HashSet::new();
        for (_, market) in markets.iter() {
            if !market.symbol.eq_ignore_ascii_case(tick_base) { continue; }

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
            let target_gap_pct = engine.config.base_gap_bps / 100.0;
            let (correct_side, token_id, static_token_price) = if current_price >= price_to_beat {
                ("UP", &market.up_token_id, market.up_price)
            } else {
                ("DOWN", &market.down_token_id, market.down_price)
            };

            let current_capital = f64::from_bits(engine.atomic_capital.load(std::sync::atomic::Ordering::Acquire));
            let capital = if current_capital > engine.config.initial_capital { current_capital } else { engine.config.initial_capital };
            let required_capital = capital * engine.config.max_position_size_pct;
            let approx_shares = required_capital / static_token_price;

            let mut token_price = if let Some(sweep) = engine.orderbook.calculate_sweep_price(token_id, "BUY", approx_shares).await {
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
            let c3 = token_price < engine.config.max_token_price;

            if !(c1 && c3) { continue; }

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
            let mut lat = crate::types::LatencyMetrics::default();
            lat.binance_tick_received_at = tick_received_at;
            lat.signal_decision_started_at = decision_started_at;
            lat.signal_decision_finished_at = decision_finished_at;

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
            if let Err(e) = queue.try_send(signal) {
                dropped_signals += 1;
                // println!("[SIGNAL] ⚠️ Trader queue full! Dropping signal to prevent backpressure (Total dropped: {}). Err: {}", dropped_signals, e);
            }
        }
    }
}
