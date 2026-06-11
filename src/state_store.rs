use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::types::Position;

pub struct StateStore {
    conn: Arc<Mutex<Connection>>,
}

impl StateStore {
    pub fn new() -> rusqlite::Result<Self> {
        let conn = Connection::open("bot_state.db")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS paper_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS positions (
                condition_id TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS trade_history (
                order_id TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )",
            [],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn save_scalar<T: serde::Serialize>(&self, key: &str, value: &T) {
        if let Ok(json_val) = serde_json::to_string(value) {
            let conn = self.conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT INTO paper_state (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, json_val],
            );
        }
    }

    pub fn load_scalar<T: serde::de::DeserializeOwned>(&self, key: &str, default: T) -> T {
        let conn = self.conn.lock().unwrap();
        let result: Option<String> = conn.query_row(
            "SELECT value FROM paper_state WHERE key = ?1",
            params![key],
            |row| row.get(0),
        ).optional().unwrap_or(None);

        if let Some(val_str) = result {
            serde_json::from_str(&val_str).unwrap_or(default)
        } else {
            default
        }
    }

    pub fn save_position(&self, condition_id: &str, position: &Position) {
        if let Ok(json_val) = serde_json::to_string(position) {
            let conn = self.conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT INTO positions (condition_id, data) VALUES (?1, ?2)
                 ON CONFLICT(condition_id) DO UPDATE SET data = excluded.data",
                params![condition_id, json_val],
            );
        }
    }

    pub fn save_trade_record(&self, record: &crate::types::TradeRecord) {
        if let Ok(json_val) = serde_json::to_string(record) {
            let conn = self.conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT INTO trade_history (order_id, data) VALUES (?1, ?2)
                 ON CONFLICT(order_id) DO UPDATE SET data = excluded.data",
                params![record.order_id, json_val],
            );
        }
    }

    pub fn delete_position(&self, condition_id: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "DELETE FROM positions WHERE condition_id = ?1",
            params![condition_id],
        );
    }

    pub fn load_all_positions(&self) -> HashMap<String, Position> {
        let mut map = HashMap::new();
        let conn = self.conn.lock().unwrap();
        if let Ok(mut stmt) = conn.prepare("SELECT condition_id, data FROM positions") {
            let iter = stmt.query_map([], |row| {
                let condition_id: String = row.get(0)?;
                let data: String = row.get(1)?;
                Ok((condition_id, data))
            }).unwrap();

            for item in iter.flatten() {
                if let Ok(pos) = serde_json::from_str::<Position>(&item.1) {
                    map.insert(item.0, pos);
                }
            }
        }
        map
    }

    pub fn checkpoint_and_close(&self) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);", []);
    }
}
