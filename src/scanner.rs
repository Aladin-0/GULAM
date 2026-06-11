use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::Deserialize;
use chrono::{DateTime, Utc};
use crate::types::Market;

pub type ActiveMarkets = Arc<RwLock<HashMap<String, Market>>>;

const ASSETS: &[&str] = &["btc", "eth", "sol"];
const INTERVAL_SECONDS: i64 = 900; // 15 minutes
const MAX_TIME_REMAINING_SECONDS: f64 = 5400.0; // 90 minutes (matches Python MAX_TIME_REMAINING_MINUTES=90)

#[derive(Clone)]
pub struct Scanner {
    pub markets: ActiveMarkets,
}

#[derive(Debug, Deserialize)]
pub struct GammaMarket {
    #[serde(rename = "conditionId", default)]
    pub condition_id: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(rename = "clobTokenIds", default)]
    pub clob_token_ids: String,
    #[serde(rename = "outcomePrices", default)]
    pub outcome_prices: String,
    // Full RFC3339 datetime e.g. "2026-06-09T13:15:00Z"
    #[serde(rename = "endDate", default)]
    pub end_date: String,
}

impl Scanner {
    pub fn new() -> Self {
        Self {
            markets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_active_markets(&self) -> HashMap<String, Market> {
        self.markets.read().await.clone()
    }

    pub async fn prune_expired_markets(&self) {
        let mut r = self.markets.write().await;
        let now = Utc::now();
        r.retain(|_, m| {
            if let Some(ref ed_str) = m.end_date {
                if let Ok(ed) = DateTime::parse_from_rfc3339(ed_str) {
                    // Keep markets that expired less than 15 seconds ago (Python: -15s grace)
                    return (ed.with_timezone(&Utc) - now).num_seconds() > -15;
                }
            }
            true
        });
    }

}

/// Compute the Unix timestamp of the current 15-minute slot boundary (floor).
fn current_slot_ts() -> i64 {
    let now = Utc::now();
    let total_secs = now.timestamp();
    (total_secs / INTERVAL_SECONDS) * INTERVAL_SECONDS
}

/// Generate all slug candidates to try for this cycle (matches Python _slugs_to_fetch).
/// Format: "{asset}-updown-15m-{unix_ts}"
fn slugs_to_fetch() -> Vec<(String, String)> {
    let base = current_slot_ts();
    let offsets: [i64; 4] = [0, INTERVAL_SECONDS, -INTERVAL_SECONDS, 2 * INTERVAL_SECONDS];
    let mut pairs = Vec::new();
    for &asset in ASSETS {
        for &offset in &offsets {
            let ts = base + offset;
            pairs.push((asset.to_string(), format!("{}-updown-15m-{}", asset, ts)));
        }
    }
    pairs
}

/// Fetch a single market by its slug from the Gamma API.
async fn fetch_by_slug(client: &reqwest::Client, slug: &str) -> Result<Option<GammaMarket>, ()> {
    let url = format!("https://gamma-api.polymarket.com/markets?slug={}", slug);
    let mut backoff = 1;

    for attempt in 1..=3 {
        match client.get(&url).send().await {
            Ok(res) => {
                let status = res.status();
                if status.is_success() {
                    match res.text().await {
                        Ok(text) => {
                            if text.trim() == "[]" {
                                return Ok(None);
                            }
                            match serde_json::from_str::<Vec<GammaMarket>>(&text) {
                                Ok(mut vec) if !vec.is_empty() => return Ok(Some(vec.remove(0))),
                                Ok(_) => return Ok(None),
                                Err(e) => {
                                    println!("[SCANNER] ❌ JSON Parse error for slug {} on attempt {}: {}", slug, attempt, e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("[SCANNER] ⚠️ Failed to read response body for {}: {}", slug, e);
                        }
                    }
                } else {
                    println!("[SCANNER] ⚠️ HTTP {} for slug {} on attempt {}", status, slug, attempt);
                    if status == reqwest::StatusCode::NOT_FOUND {
                        return Ok(None);
                    }
                }
            }
            Err(e) => {
                println!("[SCANNER] ❌ Network error for slug {} on attempt {}: {}", slug, attempt, e);
            }
        }
        if attempt < 3 {
            tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
    Err(())
}

pub async fn run_scanner(scanner: Scanner) {
    println!("[SCANNER] Scanner started. Fetching 15-min interval markets...");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    loop {
        let slugs = slugs_to_fetch();
        let mut new_map = HashMap::new();
        let mut scan_failed = false;
        let now = Utc::now();

        for (asset, slug) in &slugs {
            let raw = match fetch_by_slug(&client, slug).await {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(_) => {
                    scan_failed = true;
                    continue;
                }
            };

            if !raw.active || raw.closed { continue; }
            if raw.condition_id.is_empty() { continue; }

            // Parse end_date and compute time remaining
            let end_dt = match DateTime::parse_from_rfc3339(&raw.end_date) {
                Ok(dt) => dt.with_timezone(&Utc),
                Err(_) => continue,
            };
            let time_remaining_seconds = (end_dt - now).num_milliseconds() as f64 / 1000.0;

            // Python: reject if > MAX_TIME_REMAINING_MINUTES (90min) or expired > 15s
            if time_remaining_seconds > MAX_TIME_REMAINING_SECONDS { continue; }
            if time_remaining_seconds < -15.0 { continue; }

            // Parse token IDs from JSON string
            let token_ids: Vec<String> = match serde_json::from_str(&raw.clob_token_ids) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if token_ids.len() < 2 { continue; }

            // Parse outcome prices
            let prices: Vec<String> = serde_json::from_str(&raw.outcome_prices).unwrap_or_default();
            let up_p: f64 = prices.first().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let down_p: f64 = prices.get(1).and_then(|s| s.parse().ok()).unwrap_or(0.0);

            let symbol = match asset.as_str() {
                "btc" => "BTC",
                "eth" => "ETH",
                "sol" => "SOL",
                _ => continue,
            };

            // println!("[SCANNER] ✅ {} → {} | {:.1}min remaining | ACCEPTED",
            //     slug, raw.question, time_remaining_seconds / 60.0);

            let m = Market {
                condition_id: raw.condition_id.clone(),
                question: raw.question,
                symbol: symbol.to_string(),
                slug: slug.clone(),
                up_token_id: token_ids[0].clone(),
                down_token_id: token_ids[1].clone(),
                up_price: up_p,
                down_price: down_p,
                end_date: Some(end_dt.to_rfc3339()),
                time_remaining_seconds,
            };
            new_map.insert(m.condition_id.clone(), m);

            // Avoid hammering the API — small delay between slug fetches
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }


        {
            let mut r = scanner.markets.write().await;
            if !scan_failed {
                *r = new_map;
            } else {
                for (k, v) in new_map {
                    r.insert(k, v);
                }
            }
        }
        scanner.prune_expired_markets().await;

        /*
        if count > 0 {
            println!("[SCANNER] {} active 15-min market(s) tracked.", count);
        } else {
            println!("[SCANNER] ⚠️ No active 15-min markets found this cycle.");
        }
        */

        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}
