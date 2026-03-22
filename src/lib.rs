//! mochizuki — generic SQLite + FTS5 database operations.
//!
//! Provides reusable patterns for timestamped, searchable, prunable
//! SQLite databases with FTS5 full-text search.
//!
//! Used by: andro-log (log storage), andro-farm (device inventory),
//! andro-build (size tracking), and any future tool needing
//! structured local storage with search.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MochizukiError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, MochizukiError>;

/// A generic row returned from queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: serde_json::Map<String, serde_json::Value>,
}

/// Generic SQLite database with FTS5 support.
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Open or create a database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        Ok(Self { conn })
    }

    /// Open an in-memory database (for testing).
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    /// Execute raw SQL for schema creation.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    /// Execute a single SQL statement with parameters.
    pub fn execute(&self, sql: &str, params: &[&dyn rusqlite::types::ToSql]) -> Result<usize> {
        let count = self.conn.execute(sql, params)?;
        Ok(count)
    }

    /// Query and return results as JSON rows.
    pub fn query(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
        columns: &[&str],
    ) -> Result<Vec<Row>> {
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt
            .query_map(params, |row| {
                let mut values = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let value: rusqlite::types::Value = row.get(i)?;
                    let json_val = match value {
                        rusqlite::types::Value::Null => serde_json::Value::Null,
                        rusqlite::types::Value::Integer(n) => serde_json::Value::Number(n.into()),
                        rusqlite::types::Value::Real(f) => {
                            serde_json::Number::from_f64(f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        }
                        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                        rusqlite::types::Value::Blob(b) => {
                            serde_json::Value::String(format!("<blob:{} bytes>", b.len()))
                        }
                    };
                    values.insert(col.to_string(), json_val);
                }
                Ok(Row { values })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// FTS5 full-text search.
    pub fn search_fts(
        &self,
        fts_table: &str,
        join_table: &str,
        query: &str,
        columns: &[&str],
        limit: usize,
    ) -> Result<Vec<Row>> {
        let col_list = columns.join(", ");
        let sql = format!(
            "SELECT {col_list} FROM {fts_table} f \
             JOIN {join_table} t ON f.rowid = t.id \
             WHERE {fts_table} MATCH ?1 \
             ORDER BY t.id DESC LIMIT ?2"
        );
        self.query(&sql, &[&query as &dyn rusqlite::types::ToSql, &(limit as i64)], columns)
    }

    /// Count rows in a table.
    pub fn count(&self, table: &str) -> Result<u64> {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        let count: i64 = self.conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Delete rows older than N days based on a timestamp column.
    pub fn prune(&self, table: &str, timestamp_col: &str, retention_days: u32) -> Result<u64> {
        let sql = format!(
            "DELETE FROM {table} WHERE {timestamp_col} < datetime('now', ?1)"
        );
        let deleted = self.conn.execute(&sql, params![format!("-{retention_days} days")])?;
        Ok(deleted as u64)
    }

    /// Get direct access to the underlying connection (for advanced queries).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_memory_and_create_table() {
        let db = Database::open_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT DEFAULT (datetime('now')))",
        )
        .unwrap();

        db.execute(
            "INSERT INTO test (name) VALUES (?1)",
            &[&"hello" as &dyn rusqlite::types::ToSql],
        )
        .unwrap();

        let count = db.count("test").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn query_returns_rows() {
        let db = Database::open_memory().unwrap();
        db.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (label) VALUES (?1)", &[&"alpha"]).unwrap();
        db.execute("INSERT INTO items (label) VALUES (?1)", &[&"beta"]).unwrap();

        let rows = db
            .query("SELECT id, label FROM items ORDER BY id", &[], &["id", "label"])
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values["label"], "alpha");
        assert_eq!(rows[1].values["label"], "beta");
    }

    #[test]
    fn fts5_search() {
        let db = Database::open_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT);
             CREATE VIRTUAL TABLE docs_fts USING fts5(title, body, content=docs, content_rowid=id);
             CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN
                 INSERT INTO docs_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
             END;",
        )
        .unwrap();

        db.execute(
            "INSERT INTO docs (title, body) VALUES (?1, ?2)",
            &[&"Rust Guide", &"Learn Rust programming language"],
        )
        .unwrap();
        db.execute(
            "INSERT INTO docs (title, body) VALUES (?1, ?2)",
            &[&"Python Guide", &"Learn Python scripting"],
        )
        .unwrap();

        let results = db
            .search_fts("docs_fts", "docs", "Rust", &["t.title", "t.body"], 10)
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn prune_old_entries() {
        let db = Database::open_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT, ts TEXT)",
        )
        .unwrap();
        // Insert entry with old timestamp
        db.execute(
            "INSERT INTO logs (msg, ts) VALUES (?1, datetime('now', '-10 days'))",
            &[&"old" as &dyn rusqlite::types::ToSql],
        )
        .unwrap();
        db.execute(
            "INSERT INTO logs (msg, ts) VALUES (?1, datetime('now'))",
            &[&"recent" as &dyn rusqlite::types::ToSql],
        )
        .unwrap();

        // Prune entries older than 5 days — should delete only the old one
        let deleted = db.prune("logs", "ts", 5).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(db.count("logs").unwrap(), 1);
    }
}
