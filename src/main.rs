mod config;
mod types;
mod oracle;
mod orderbook_cache;
mod scanner;
mod signal_engine;
mod live_trader;
mod state_store;
mod execution;
mod user_ws;

use tokio::sync::{mpsc, RwLock};
use std::sync::Arc;
use tokio::signal;
use crate::config::Config;
use crate::types::LiveState;
use crate::state_store::StateStore;
use crate::oracle::{OracleCache, run_oracle};
use crate::orderbook_cache::{OrderbookCache, run_orderbook_cache};
use crate::scanner::{Scanner, run_scanner};
use crate::signal_engine::{SignalEngine, run_signal_engine};
use crate::live_trader::{LiveTrader, run_live_trader, run_user_ws_fill_processor};
use crate::user_ws::run_user_ws;

async fn supervised_task<F, Fut>(name: &'static str, f: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    loop {
        let task = tokio::spawn(f());
        match task.await {
            Ok(_) => {
                println!("[MAIN] Task {} exited cleanly. Restarting in 5s...", name);
            }
            Err(e) => {
                println!("[MAIN] ⚠️ Task {} crashed: {}. Restarting in 5s...", name, e);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

#[tokio::main]
async fn main() {
    let config = Config::load();
    println!("[MAIN] Starting GULAM Core Engine");
    println!("[MAIN] Mode: {}", if config.paper_trading { "PAPER" } else { "LIVE" });

    let state_store = match StateStore::new() {
        Ok(store) => Arc::new(store),
        Err(e) => {
            eprintln!("[MAIN] Failed to open SQLite DB: {}", e);
            std::process::exit(1);
        }
    };

    let mut initial_state = LiveState::default();
    initial_state.available_capital = state_store.load_scalar("live_available_capital", config.initial_capital);
    initial_state.capital = initial_state.available_capital;
    initial_state.open_positions = state_store.load_all_positions();
    let atomic_capital = Arc::new(std::sync::atomic::AtomicU64::new(initial_state.available_capital.to_bits()));
    let live_state = Arc::new(RwLock::new(initial_state));

    // Setup Shared Queues
    let (signal_tx, signal_rx) = mpsc::channel(100);
    let (hedge_tx, hedge_rx) = mpsc::channel(100);
    let (tick_tx, tick_rx) = mpsc::channel(1000);
    let (user_ws_tx, user_ws_rx) = mpsc::channel(500);

    // Setup Modules
    let oracle_cache: OracleCache = Arc::new(RwLock::new(std::collections::HashMap::new()));
    let (orderbook_cache, ob_sub_rx) = OrderbookCache::new();
    let scanner = Scanner::new();
    
    let signal_engine = SignalEngine::new(
        config.clone(),
        oracle_cache.clone(),
        orderbook_cache.clone(),
        scanner.clone(),
        atomic_capital.clone(),
    );

    let live_trader = Arc::new(LiveTrader::new(
        config.clone(),
        state_store.clone(),
        live_state.clone(),
        orderbook_cache.clone(),
        oracle_cache.clone(),
        atomic_capital.clone(),
    ));


    // Spawn Supervised Tasks
    let oc = oracle_cache.clone();
    let oracle_tick_tx = tick_tx.clone();
    tokio::spawn(supervised_task("Oracle", move || {
        let oc = oc.clone();
        let tx = oracle_tick_tx.clone();
        async move { run_oracle(oc, tx).await }
    }));

    let obc = orderbook_cache.clone();
    tokio::spawn(async move {
        run_orderbook_cache(obc, ob_sub_rx).await;
    });

    let sc = scanner.clone();
    tokio::spawn(supervised_task("Scanner", move || {
        let sc = sc.clone();
        async move { run_scanner(sc).await }
    }));

    let uc = config.clone();
    let uc_tx = user_ws_tx.clone();
    tokio::spawn(supervised_task("UserWS", move || {
        let uc = uc.clone();
        let tx = uc_tx.clone();
        async move { run_user_ws(uc, tx).await }
    }));

    // Signal Engine Task
    let se_tx = signal_tx.clone();
    let he_tx = hedge_tx.clone();
    let ls = live_state.clone();
    std::thread::spawn(move || {
        if let Some(core_ids) = core_affinity::get_core_ids() {
            if let Some(core) = core_ids.first() {
                core_affinity::set_for_current(*core);
                println!("[MAIN] Pinned SignalEngine to core {}", core.id);
            }
        }
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            run_signal_engine(signal_engine, se_tx, ls, he_tx, tick_rx).await;
        });
    });

    // Live Trader Task
    let lt = live_trader.clone();
    std::thread::spawn(move || {
        if let Some(core_ids) = core_affinity::get_core_ids() {
            if let Some(core) = core_ids.last() {
                core_affinity::set_for_current(*core);
                println!("[MAIN] Pinned LiveTrader to core {}", core.id);
            }
        }
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            run_live_trader(lt, signal_rx, hedge_rx).await;
        });
    });

    let lt_fills = live_trader.clone();
    tokio::spawn(async move {
        run_user_ws_fill_processor(lt_fills, user_ws_rx).await;
    });

    // Graceful Shutdown
    match signal::ctrl_c().await {
        Ok(()) => {
            println!("\n[MAIN] Graceful shutdown initiated...");
            state_store.checkpoint_and_close();
            println!("[MAIN] State checkpointed. Goodbye.");
        }
        Err(err) => {
            eprintln!("[MAIN] Unable to listen for shutdown signal: {}", err);
        }
    }
}
