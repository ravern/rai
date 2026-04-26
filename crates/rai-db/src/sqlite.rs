use std::collections::HashMap;
use std::path::Path;

use chrono::{NaiveDate, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row, ToSql};
use rust_decimal::Decimal;
use serde_json::{json, Map, Value};

use rai_core::types::*;

use crate::error::DbError;
use crate::provider::{
    AuditEvent, AuditEventId, AuditEventKind, AuditFilter, QueryResult, QueryValue, StorageProvider,
};

pub struct SqliteProvider {
    conn: Connection,
}

impl SqliteProvider {
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        Ok(Self { conn })
    }

    fn initialize_audit_log(&mut self) -> Result<(), DbError> {
        let backfill_completed = self
            .conn
            .query_row(
                "SELECT value FROM audit_metadata WHERE key = 'backfill_completed'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        if backfill_completed.as_deref() == Some("true") {
            ensure_id_counters(&self.conn)?;
            return Ok(());
        }

        let db_tx = self.conn.transaction()?;
        backfill_audit_log(&db_tx)?;
        ensure_id_counters(&db_tx)?;
        db_tx.execute(
            "INSERT OR REPLACE INTO audit_metadata (key, value) VALUES (?1, ?2)",
            params!["backfill_completed", "true"],
        )?;
        db_tx.commit()?;
        Ok(())
    }

    fn record_mutation(
        &self,
        operation: &str,
        entity_type: &str,
        entity_id: i64,
        before_json: Option<String>,
        after_json: Option<String>,
    ) -> Result<(), DbError> {
        if before_json == after_json {
            return Ok(());
        }

        clear_redo_stack(&self.conn)?;
        let summary = format!("{operation} {entity_type} {entity_id}");
        let event_id = insert_audit_event(
            &self.conn,
            AuditEventKind::Mutation,
            operation,
            entity_type,
            Some(entity_id),
            &summary,
            None,
            before_json.as_deref(),
            after_json.as_deref(),
        )?;
        push_stack(&self.conn, "audit_undo_stack", event_id)?;
        Ok(())
    }
}

// ── Schema ───────────────────────────────────────────────────────────

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS commodities (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    precision   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    account_type    TEXT NOT NULL,
    is_open         INTEGER NOT NULL DEFAULT 1,
    opened_at       TEXT NOT NULL,
    closed_at       TEXT,
    booking_method  TEXT NOT NULL DEFAULT 'strict',
    CHECK (account_type IN ('assets', 'liabilities', 'income', 'expenses', 'equity'))
);

CREATE TABLE IF NOT EXISTS account_currencies (
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id),
    PRIMARY KEY (account_id, commodity_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    id          INTEGER PRIMARY KEY,
    date        TEXT NOT NULL,
    time        TEXT,
    status      TEXT NOT NULL DEFAULT 'completed',
    payee       TEXT,
    narration   TEXT,
    CHECK (status IN ('completed', 'pending', 'flagged'))
);

CREATE TABLE IF NOT EXISTS postings (
    id              INTEGER PRIMARY KEY,
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    amount          TEXT NOT NULL,
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id),
    cost_amount     TEXT,
    cost_commodity_id INTEGER REFERENCES commodities(id),
    cost_date       TEXT,
    cost_label      TEXT,
    price_amount    TEXT,
    price_commodity_id INTEGER REFERENCES commodities(id)
);

CREATE TABLE IF NOT EXISTS transaction_tags (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    tag             TEXT NOT NULL,
    PRIMARY KEY (transaction_id, tag)
);

CREATE TABLE IF NOT EXISTS transaction_links (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    link            TEXT NOT NULL,
    PRIMARY KEY (transaction_id, link)
);

CREATE TABLE IF NOT EXISTS transaction_metadata (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,
    value           TEXT NOT NULL,
    PRIMARY KEY (transaction_id, key)
);

CREATE TABLE IF NOT EXISTS posting_metadata (
    posting_id      INTEGER NOT NULL REFERENCES postings(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,
    value           TEXT NOT NULL,
    PRIMARY KEY (posting_id, key)
);

CREATE TABLE IF NOT EXISTS account_metadata (
    account_id      INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,
    value           TEXT NOT NULL,
    PRIMARY KEY (account_id, key)
);

CREATE TABLE IF NOT EXISTS commodity_metadata (
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,
    value           TEXT NOT NULL,
    PRIMARY KEY (commodity_id, key)
);

CREATE TABLE IF NOT EXISTS prices (
    id                  INTEGER PRIMARY KEY,
    date                TEXT NOT NULL,
    commodity_id        INTEGER NOT NULL REFERENCES commodities(id),
    target_commodity_id INTEGER NOT NULL REFERENCES commodities(id),
    value               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS balance_assertions (
    id              INTEGER PRIMARY KEY,
    date            TEXT NOT NULL,
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    amount          TEXT NOT NULL,
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    id              INTEGER PRIMARY KEY,
    created_at      TEXT NOT NULL,
    kind            TEXT NOT NULL,
    operation       TEXT NOT NULL,
    entity_type     TEXT NOT NULL,
    entity_id       INTEGER,
    summary         TEXT NOT NULL,
    target_event_id INTEGER REFERENCES audit_events(id),
    before_json     TEXT,
    after_json      TEXT,
    CHECK (kind IN ('baseline', 'mutation', 'undo', 'redo'))
);

CREATE TABLE IF NOT EXISTS audit_undo_stack (
    position    INTEGER PRIMARY KEY,
    event_id    INTEGER NOT NULL UNIQUE REFERENCES audit_events(id)
);

CREATE TABLE IF NOT EXISTS audit_redo_stack (
    position    INTEGER PRIMARY KEY,
    event_id    INTEGER NOT NULL UNIQUE REFERENCES audit_events(id)
);

CREATE TABLE IF NOT EXISTS audit_metadata (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_id_counters (
    name     TEXT PRIMARY KEY,
    next_id  INTEGER NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_postings_transaction ON postings(transaction_id);
CREATE INDEX IF NOT EXISTS idx_postings_account ON postings(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_prices_commodity_date ON prices(commodity_id, date);
CREATE INDEX IF NOT EXISTS idx_balance_assertions_date ON balance_assertions(date);
CREATE INDEX IF NOT EXISTS idx_audit_events_created ON audit_events(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_entity ON audit_events(entity_type, entity_id);

-- Helper views
CREATE VIEW IF NOT EXISTS v_journal AS
SELECT
    t.id AS transaction_id,
    t.date,
    t.time,
    t.status,
    t.payee,
    t.narration,
    p.id AS posting_id,
    a.name AS account,
    a.account_type,
    p.amount,
    c.name AS commodity,
    p.cost_amount,
    cc.name AS cost_commodity,
    p.cost_date,
    p.price_amount,
    pc.name AS price_commodity
FROM postings p
JOIN transactions t ON p.transaction_id = t.id
JOIN accounts a ON p.account_id = a.id
JOIN commodities c ON p.commodity_id = c.id
LEFT JOIN commodities cc ON p.cost_commodity_id = cc.id
LEFT JOIN commodities pc ON p.price_commodity_id = pc.id
ORDER BY t.date, t.time, t.id, p.id;

CREATE VIEW IF NOT EXISTS v_account_balances AS
SELECT
    a.id AS account_id,
    a.name AS account,
    a.account_type,
    c.name AS commodity,
    SUM(CAST(p.amount AS REAL)) AS balance
FROM postings p
JOIN accounts a ON p.account_id = a.id
JOIN commodities c ON p.commodity_id = c.id
GROUP BY a.id, p.commodity_id;
"#;

// ── Audit helpers ───────────────────────────────────────────────────

const ID_COUNTERS: [(&str, &str); 6] = [
    ("commodity", "commodities"),
    ("account", "accounts"),
    ("transaction", "transactions"),
    ("posting", "postings"),
    ("price", "prices"),
    ("balance_assertion", "balance_assertions"),
];

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn rows_as_json(conn: &Connection, sql: &str, args: &[&dyn ToSql]) -> Result<Vec<Value>, DbError> {
    let mut stmt = conn.prepare(sql)?;
    let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
    let mut rows = stmt.query(args)?;
    let mut out = Vec::new();

    while let Some(row) = rows.next()? {
        let mut obj = Map::new();
        for (idx, column) in columns.iter().enumerate() {
            let value = match row.get_ref(idx)? {
                rusqlite::types::ValueRef::Null => Value::Null,
                rusqlite::types::ValueRef::Integer(n) => json!(n),
                rusqlite::types::ValueRef::Real(f) => json!(f),
                rusqlite::types::ValueRef::Text(bytes) => {
                    json!(String::from_utf8_lossy(bytes).into_owned())
                }
                rusqlite::types::ValueRef::Blob(bytes) => {
                    json!(format!("<blob {} bytes>", bytes.len()))
                }
            };
            obj.insert(column.clone(), value);
        }
        out.push(Value::Object(obj));
    }

    Ok(out)
}

fn json_to_sql_value(value: &Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Text(n.to_string())
            }
        }
        Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        Value::Array(_) | Value::Object(_) => rusqlite::types::Value::Text(value.to_string()),
    }
}

fn table_rows<'a>(snapshot: &'a Value, key: &str) -> Result<&'a [Value], DbError> {
    snapshot
        .get(key)
        .and_then(|v| v.as_array())
        .map(|v| v.as_slice())
        .ok_or_else(|| DbError::InvalidData(format!("audit snapshot missing table '{key}'")))
}

fn insert_json_rows(
    conn: &Connection,
    table: &str,
    columns: &[&str],
    rows: &[Value],
) -> Result<(), DbError> {
    if rows.is_empty() {
        return Ok(());
    }

    let placeholders = (1..=columns.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {table} ({}) VALUES ({placeholders})",
        columns.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;

    for row in rows {
        let obj = row
            .as_object()
            .ok_or_else(|| DbError::InvalidData("audit row is not an object".into()))?;
        let values = columns
            .iter()
            .map(|column| json_to_sql_value(obj.get(*column).unwrap_or(&Value::Null)))
            .collect::<Vec<_>>();
        stmt.execute(rusqlite::params_from_iter(values.iter()))?;
    }

    Ok(())
}

fn upsert_json_row(
    conn: &Connection,
    table: &str,
    columns: &[&str],
    row: &Value,
) -> Result<(), DbError> {
    let placeholders = (1..=columns.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let updates = columns
        .iter()
        .copied()
        .filter(|column| *column != "id")
        .map(|column| format!("{column} = excluded.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {table} ({}) VALUES ({placeholders}) \
         ON CONFLICT(id) DO UPDATE SET {updates}",
        columns.join(", ")
    );
    let obj = row
        .as_object()
        .ok_or_else(|| DbError::InvalidData("audit row is not an object".into()))?;
    let values = columns
        .iter()
        .map(|column| json_to_sql_value(obj.get(*column).unwrap_or(&Value::Null)))
        .collect::<Vec<_>>();
    conn.execute(&sql, rusqlite::params_from_iter(values.iter()))?;
    Ok(())
}

fn max_id_plus_one(conn: &Connection, table: &str) -> Result<i64, DbError> {
    let sql = format!("SELECT COALESCE(MAX(id), 0) + 1 FROM {table}");
    Ok(conn.query_row(&sql, [], |row| row.get(0))?)
}

fn ensure_id_counters(conn: &Connection) -> Result<(), DbError> {
    for (name, table) in ID_COUNTERS {
        let next_id = max_id_plus_one(conn, table)?;
        conn.execute(
            "INSERT INTO audit_id_counters (name, next_id) VALUES (?1, ?2) \
             ON CONFLICT(name) DO UPDATE SET \
             next_id = CASE \
                 WHEN audit_id_counters.next_id < excluded.next_id THEN excluded.next_id \
                 ELSE audit_id_counters.next_id \
             END",
            params![name, next_id],
        )?;
    }
    Ok(())
}

fn allocate_id(conn: &Connection, name: &str) -> Result<i64, DbError> {
    let next_id = conn
        .query_row(
            "SELECT next_id FROM audit_id_counters WHERE name = ?1",
            params![name],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or_else(|| DbError::InvalidData(format!("missing id counter '{name}'")))?;
    conn.execute(
        "UPDATE audit_id_counters SET next_id = ?1 WHERE name = ?2",
        params![next_id + 1, name],
    )?;
    Ok(next_id)
}

fn bump_counter_past(conn: &Connection, name: &str, id: i64) -> Result<(), DbError> {
    conn.execute(
        "UPDATE audit_id_counters SET next_id = CASE \
             WHEN next_id <= ?1 THEN ?1 + 1 ELSE next_id END \
         WHERE name = ?2",
        params![id, name],
    )?;
    Ok(())
}

fn snapshot_commodity(conn: &Connection, id: CommodityId) -> Result<Option<String>, DbError> {
    let commodity = rows_as_json(
        conn,
        "SELECT id, name, precision FROM commodities WHERE id = ?1",
        &[&id.0],
    )?;
    if commodity.is_empty() {
        return Ok(None);
    }

    let metadata = rows_as_json(
        conn,
        "SELECT commodity_id, key, value_type, value FROM commodity_metadata \
         WHERE commodity_id = ?1 ORDER BY key",
        &[&id.0],
    )?;
    Ok(Some(
        json!({ "commodities": commodity, "commodity_metadata": metadata }).to_string(),
    ))
}

fn snapshot_account(conn: &Connection, id: AccountId) -> Result<Option<String>, DbError> {
    let accounts = rows_as_json(
        conn,
        "SELECT id, name, account_type, is_open, opened_at, closed_at, booking_method \
         FROM accounts WHERE id = ?1",
        &[&id.0],
    )?;
    if accounts.is_empty() {
        return Ok(None);
    }

    let currencies = rows_as_json(
        conn,
        "SELECT account_id, commodity_id FROM account_currencies \
         WHERE account_id = ?1 ORDER BY commodity_id",
        &[&id.0],
    )?;
    let metadata = rows_as_json(
        conn,
        "SELECT account_id, key, value_type, value FROM account_metadata \
         WHERE account_id = ?1 ORDER BY key",
        &[&id.0],
    )?;
    Ok(Some(
        json!({
            "accounts": accounts,
            "account_currencies": currencies,
            "account_metadata": metadata
        })
        .to_string(),
    ))
}

fn snapshot_transaction(conn: &Connection, id: TransactionId) -> Result<Option<String>, DbError> {
    let transactions = rows_as_json(
        conn,
        "SELECT id, date, time, status, payee, narration FROM transactions WHERE id = ?1",
        &[&id.0],
    )?;
    if transactions.is_empty() {
        return Ok(None);
    }

    let postings = rows_as_json(
        conn,
        "SELECT id, transaction_id, account_id, amount, commodity_id, cost_amount, \
                cost_commodity_id, cost_date, cost_label, price_amount, price_commodity_id \
         FROM postings WHERE transaction_id = ?1 ORDER BY id",
        &[&id.0],
    )?;
    let tags = rows_as_json(
        conn,
        "SELECT transaction_id, tag FROM transaction_tags \
         WHERE transaction_id = ?1 ORDER BY tag",
        &[&id.0],
    )?;
    let links = rows_as_json(
        conn,
        "SELECT transaction_id, link FROM transaction_links \
         WHERE transaction_id = ?1 ORDER BY link",
        &[&id.0],
    )?;
    let transaction_metadata = rows_as_json(
        conn,
        "SELECT transaction_id, key, value_type, value FROM transaction_metadata \
         WHERE transaction_id = ?1 ORDER BY key",
        &[&id.0],
    )?;
    let posting_metadata = rows_as_json(
        conn,
        "SELECT pm.posting_id, pm.key, pm.value_type, pm.value \
         FROM posting_metadata pm \
         JOIN postings p ON p.id = pm.posting_id \
         WHERE p.transaction_id = ?1 \
         ORDER BY pm.posting_id, pm.key",
        &[&id.0],
    )?;
    Ok(Some(
        json!({
            "transactions": transactions,
            "postings": postings,
            "transaction_tags": tags,
            "transaction_links": links,
            "transaction_metadata": transaction_metadata,
            "posting_metadata": posting_metadata
        })
        .to_string(),
    ))
}

fn snapshot_price(conn: &Connection, id: PriceId) -> Result<Option<String>, DbError> {
    let prices = rows_as_json(
        conn,
        "SELECT id, date, commodity_id, target_commodity_id, value \
         FROM prices WHERE id = ?1",
        &[&id.0],
    )?;
    if prices.is_empty() {
        Ok(None)
    } else {
        Ok(Some(json!({ "prices": prices }).to_string()))
    }
}

fn snapshot_balance_assertion(
    conn: &Connection,
    id: BalanceAssertionId,
) -> Result<Option<String>, DbError> {
    let assertions = rows_as_json(
        conn,
        "SELECT id, date, account_id, amount, commodity_id \
         FROM balance_assertions WHERE id = ?1",
        &[&id.0],
    )?;
    if assertions.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            json!({ "balance_assertions": assertions }).to_string(),
        ))
    }
}

fn snapshot_entity(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
) -> Result<Option<String>, DbError> {
    match entity_type {
        "commodity" => snapshot_commodity(conn, CommodityId(entity_id)),
        "account" => snapshot_account(conn, AccountId(entity_id)),
        "transaction" => snapshot_transaction(conn, TransactionId(entity_id)),
        "price" => snapshot_price(conn, PriceId(entity_id)),
        "balance_assertion" => snapshot_balance_assertion(conn, BalanceAssertionId(entity_id)),
        _ => Err(DbError::InvalidData(format!(
            "unknown audit entity type: {entity_type}"
        ))),
    }
}

fn delete_commodity_raw(conn: &Connection, id: i64) -> Result<(), DbError> {
    conn.execute(
        "DELETE FROM commodity_metadata WHERE commodity_id = ?1",
        params![id],
    )?;
    conn.execute("DELETE FROM commodities WHERE id = ?1", params![id])?;
    Ok(())
}

fn delete_account_raw(conn: &Connection, id: i64) -> Result<(), DbError> {
    conn.execute(
        "DELETE FROM account_metadata WHERE account_id = ?1",
        params![id],
    )?;
    conn.execute(
        "DELETE FROM account_currencies WHERE account_id = ?1",
        params![id],
    )?;
    conn.execute("DELETE FROM accounts WHERE id = ?1", params![id])?;
    Ok(())
}

fn delete_transaction_raw(conn: &Connection, id: i64) -> Result<(), DbError> {
    conn.execute(
        "DELETE FROM posting_metadata \
         WHERE posting_id IN (SELECT id FROM postings WHERE transaction_id = ?1)",
        params![id],
    )?;
    conn.execute(
        "DELETE FROM transaction_metadata WHERE transaction_id = ?1",
        params![id],
    )?;
    conn.execute(
        "DELETE FROM transaction_tags WHERE transaction_id = ?1",
        params![id],
    )?;
    conn.execute(
        "DELETE FROM transaction_links WHERE transaction_id = ?1",
        params![id],
    )?;
    conn.execute(
        "DELETE FROM postings WHERE transaction_id = ?1",
        params![id],
    )?;
    conn.execute("DELETE FROM transactions WHERE id = ?1", params![id])?;
    Ok(())
}

fn restore_commodity(conn: &Connection, snapshot: Option<&str>, id: i64) -> Result<(), DbError> {
    match snapshot {
        None => delete_commodity_raw(conn, id),
        Some(snapshot) => {
            let value: Value = serde_json::from_str(snapshot)
                .map_err(|e| DbError::InvalidData(format!("invalid audit JSON: {e}")))?;
            let rows = table_rows(&value, "commodities")?;
            let row = rows
                .first()
                .ok_or_else(|| DbError::InvalidData("missing commodity row".into()))?;
            upsert_json_row(conn, "commodities", &["id", "name", "precision"], row)?;
            conn.execute(
                "DELETE FROM commodity_metadata WHERE commodity_id = ?1",
                params![id],
            )?;
            insert_json_rows(
                conn,
                "commodity_metadata",
                &["commodity_id", "key", "value_type", "value"],
                table_rows(&value, "commodity_metadata")?,
            )?;
            bump_counter_past(conn, "commodity", id)?;
            Ok(())
        }
    }
}

fn restore_account(conn: &Connection, snapshot: Option<&str>, id: i64) -> Result<(), DbError> {
    match snapshot {
        None => delete_account_raw(conn, id),
        Some(snapshot) => {
            let value: Value = serde_json::from_str(snapshot)
                .map_err(|e| DbError::InvalidData(format!("invalid audit JSON: {e}")))?;
            let rows = table_rows(&value, "accounts")?;
            let row = rows
                .first()
                .ok_or_else(|| DbError::InvalidData("missing account row".into()))?;
            upsert_json_row(
                conn,
                "accounts",
                &[
                    "id",
                    "name",
                    "account_type",
                    "is_open",
                    "opened_at",
                    "closed_at",
                    "booking_method",
                ],
                row,
            )?;
            conn.execute(
                "DELETE FROM account_metadata WHERE account_id = ?1",
                params![id],
            )?;
            conn.execute(
                "DELETE FROM account_currencies WHERE account_id = ?1",
                params![id],
            )?;
            insert_json_rows(
                conn,
                "account_currencies",
                &["account_id", "commodity_id"],
                table_rows(&value, "account_currencies")?,
            )?;
            insert_json_rows(
                conn,
                "account_metadata",
                &["account_id", "key", "value_type", "value"],
                table_rows(&value, "account_metadata")?,
            )?;
            bump_counter_past(conn, "account", id)?;
            Ok(())
        }
    }
}

fn restore_transaction(conn: &Connection, snapshot: Option<&str>, id: i64) -> Result<(), DbError> {
    match snapshot {
        None => delete_transaction_raw(conn, id),
        Some(snapshot) => {
            let value: Value = serde_json::from_str(snapshot)
                .map_err(|e| DbError::InvalidData(format!("invalid audit JSON: {e}")))?;
            let rows = table_rows(&value, "transactions")?;
            let row = rows
                .first()
                .ok_or_else(|| DbError::InvalidData("missing transaction row".into()))?;

            conn.execute(
                "DELETE FROM posting_metadata \
                 WHERE posting_id IN (SELECT id FROM postings WHERE transaction_id = ?1)",
                params![id],
            )?;
            conn.execute(
                "DELETE FROM transaction_metadata WHERE transaction_id = ?1",
                params![id],
            )?;
            conn.execute(
                "DELETE FROM transaction_tags WHERE transaction_id = ?1",
                params![id],
            )?;
            conn.execute(
                "DELETE FROM transaction_links WHERE transaction_id = ?1",
                params![id],
            )?;
            conn.execute(
                "DELETE FROM postings WHERE transaction_id = ?1",
                params![id],
            )?;

            upsert_json_row(
                conn,
                "transactions",
                &["id", "date", "time", "status", "payee", "narration"],
                row,
            )?;
            let postings = table_rows(&value, "postings")?;
            insert_json_rows(
                conn,
                "postings",
                &[
                    "id",
                    "transaction_id",
                    "account_id",
                    "amount",
                    "commodity_id",
                    "cost_amount",
                    "cost_commodity_id",
                    "cost_date",
                    "cost_label",
                    "price_amount",
                    "price_commodity_id",
                ],
                postings,
            )?;
            insert_json_rows(
                conn,
                "transaction_tags",
                &["transaction_id", "tag"],
                table_rows(&value, "transaction_tags")?,
            )?;
            insert_json_rows(
                conn,
                "transaction_links",
                &["transaction_id", "link"],
                table_rows(&value, "transaction_links")?,
            )?;
            insert_json_rows(
                conn,
                "transaction_metadata",
                &["transaction_id", "key", "value_type", "value"],
                table_rows(&value, "transaction_metadata")?,
            )?;
            insert_json_rows(
                conn,
                "posting_metadata",
                &["posting_id", "key", "value_type", "value"],
                table_rows(&value, "posting_metadata")?,
            )?;
            bump_counter_past(conn, "transaction", id)?;
            for posting in postings {
                if let Some(posting_id) = posting.get("id").and_then(|v| v.as_i64()) {
                    bump_counter_past(conn, "posting", posting_id)?;
                }
            }
            Ok(())
        }
    }
}

fn restore_price(conn: &Connection, snapshot: Option<&str>, id: i64) -> Result<(), DbError> {
    match snapshot {
        None => {
            conn.execute("DELETE FROM prices WHERE id = ?1", params![id])?;
            Ok(())
        }
        Some(snapshot) => {
            let value: Value = serde_json::from_str(snapshot)
                .map_err(|e| DbError::InvalidData(format!("invalid audit JSON: {e}")))?;
            let rows = table_rows(&value, "prices")?;
            let row = rows
                .first()
                .ok_or_else(|| DbError::InvalidData("missing price row".into()))?;
            upsert_json_row(
                conn,
                "prices",
                &["id", "date", "commodity_id", "target_commodity_id", "value"],
                row,
            )?;
            bump_counter_past(conn, "price", id)?;
            Ok(())
        }
    }
}

fn restore_balance_assertion(
    conn: &Connection,
    snapshot: Option<&str>,
    id: i64,
) -> Result<(), DbError> {
    match snapshot {
        None => {
            conn.execute("DELETE FROM balance_assertions WHERE id = ?1", params![id])?;
            Ok(())
        }
        Some(snapshot) => {
            let value: Value = serde_json::from_str(snapshot)
                .map_err(|e| DbError::InvalidData(format!("invalid audit JSON: {e}")))?;
            let rows = table_rows(&value, "balance_assertions")?;
            let row = rows
                .first()
                .ok_or_else(|| DbError::InvalidData("missing balance assertion row".into()))?;
            upsert_json_row(
                conn,
                "balance_assertions",
                &["id", "date", "account_id", "amount", "commodity_id"],
                row,
            )?;
            bump_counter_past(conn, "balance_assertion", id)?;
            Ok(())
        }
    }
}

fn restore_entity(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
    snapshot: Option<&str>,
) -> Result<(), DbError> {
    match entity_type {
        "commodity" => restore_commodity(conn, snapshot, entity_id),
        "account" => restore_account(conn, snapshot, entity_id),
        "transaction" => restore_transaction(conn, snapshot, entity_id),
        "price" => restore_price(conn, snapshot, entity_id),
        "balance_assertion" => restore_balance_assertion(conn, snapshot, entity_id),
        _ => Err(DbError::InvalidData(format!(
            "unknown audit entity type: {entity_type}"
        ))),
    }
}

fn read_audit_event_row(row: &Row<'_>) -> Result<AuditEvent, DbError> {
    let id = row.get::<_, i64>(0)?;
    let created_at = row.get::<_, String>(1)?;
    let kind_str = row.get::<_, String>(2)?;
    let operation = row.get::<_, String>(3)?;
    let entity_type = row.get::<_, String>(4)?;
    let entity_id = row.get::<_, Option<i64>>(5)?;
    let summary = row.get::<_, String>(6)?;
    let target_event_id = row.get::<_, Option<i64>>(7)?.map(AuditEventId);
    let before_json = row.get::<_, Option<String>>(8)?;
    let after_json = row.get::<_, Option<String>>(9)?;
    let kind = AuditEventKind::from_str(&kind_str)
        .ok_or_else(|| DbError::InvalidData(format!("invalid audit event kind: {kind_str}")))?;

    Ok(AuditEvent {
        id: AuditEventId(id),
        created_at,
        kind,
        operation,
        entity_type,
        entity_id,
        summary,
        target_event_id,
        before_json,
        after_json,
    })
}

fn get_audit_event_conn(
    conn: &Connection,
    id: AuditEventId,
) -> Result<Option<AuditEvent>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, created_at, kind, operation, entity_type, entity_id, summary, \
                target_event_id, before_json, after_json \
         FROM audit_events WHERE id = ?1",
    )?;
    let mut rows = stmt.query(params![id.0])?;
    match rows.next()? {
        Some(row) => Ok(Some(read_audit_event_row(row)?)),
        None => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_audit_event(
    conn: &Connection,
    kind: AuditEventKind,
    operation: &str,
    entity_type: &str,
    entity_id: Option<i64>,
    summary: &str,
    target_event_id: Option<AuditEventId>,
    before_json: Option<&str>,
    after_json: Option<&str>,
) -> Result<AuditEventId, DbError> {
    conn.execute(
        "INSERT INTO audit_events \
         (created_at, kind, operation, entity_type, entity_id, summary, \
          target_event_id, before_json, after_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            now_rfc3339(),
            kind.as_str(),
            operation,
            entity_type,
            entity_id,
            summary,
            target_event_id.map(|id| id.0),
            before_json,
            after_json,
        ],
    )?;
    Ok(AuditEventId(conn.last_insert_rowid()))
}

fn push_stack(conn: &Connection, table: &str, event_id: AuditEventId) -> Result<(), DbError> {
    let sql = format!(
        "INSERT INTO {table} (position, event_id) \
         VALUES ((SELECT COALESCE(MAX(position), 0) + 1 FROM {table}), ?1)"
    );
    conn.execute(&sql, params![event_id.0])?;
    Ok(())
}

fn last_stack_event(conn: &Connection, table: &str) -> Result<Option<AuditEventId>, DbError> {
    let sql = format!("SELECT event_id FROM {table} ORDER BY position DESC LIMIT 1");
    Ok(conn
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .optional()?
        .map(AuditEventId))
}

fn remove_stack_event(
    conn: &Connection,
    table: &str,
    event_id: AuditEventId,
) -> Result<(), DbError> {
    let sql = format!("DELETE FROM {table} WHERE event_id = ?1");
    conn.execute(&sql, params![event_id.0])?;
    Ok(())
}

fn clear_redo_stack(conn: &Connection) -> Result<(), DbError> {
    conn.execute("DELETE FROM audit_redo_stack", [])?;
    Ok(())
}

fn backfill_audit_log(conn: &Connection) -> Result<(), DbError> {
    let entities = [
        ("commodity", "commodities"),
        ("account", "accounts"),
        ("transaction", "transactions"),
        ("price", "prices"),
        ("balance_assertion", "balance_assertions"),
    ];

    for (entity_type, table) in entities {
        let sql = format!("SELECT id FROM {table} ORDER BY id");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
        let ids = rows.collect::<Result<Vec<_>, _>>()?;

        for id in ids {
            let after_json = snapshot_entity(conn, entity_type, id)?;
            let summary = format!("Existing {entity_type} {id} imported into audit log baseline");
            insert_audit_event(
                conn,
                AuditEventKind::Baseline,
                "backfill",
                entity_type,
                Some(id),
                &summary,
                None,
                None,
                after_json.as_deref(),
            )?;
        }
    }

    Ok(())
}

// ── Metadata helpers ─────────────────────────────────────────────────

fn metadata_value_type(v: &MetadataValue) -> &'static str {
    match v {
        MetadataValue::String(_) => "string",
        MetadataValue::Number(_) => "number",
        MetadataValue::Date(_) => "date",
        MetadataValue::Bool(_) => "bool",
    }
}

fn metadata_value_to_string(v: &MetadataValue) -> String {
    match v {
        MetadataValue::String(s) => s.clone(),
        MetadataValue::Number(d) => d.to_string(),
        MetadataValue::Date(d) => d.format("%Y-%m-%d").to_string(),
        MetadataValue::Bool(b) => b.to_string(),
    }
}

fn parse_metadata_value(value_type: &str, value: &str) -> Result<MetadataValue, DbError> {
    match value_type {
        "string" => Ok(MetadataValue::String(value.to_string())),
        "number" => {
            let d: Decimal = value
                .parse()
                .map_err(|e| DbError::InvalidData(format!("invalid decimal: {e}")))?;
            Ok(MetadataValue::Number(d))
        }
        "date" => {
            let d = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|e| DbError::InvalidData(format!("invalid date: {e}")))?;
            Ok(MetadataValue::Date(d))
        }
        "bool" => match value {
            "true" => Ok(MetadataValue::Bool(true)),
            "false" => Ok(MetadataValue::Bool(false)),
            _ => Err(DbError::InvalidData(format!("invalid bool: {value}"))),
        },
        _ => Err(DbError::InvalidData(format!(
            "unknown metadata type: {value_type}"
        ))),
    }
}

fn parse_decimal(s: &str) -> Result<Decimal, DbError> {
    s.parse()
        .map_err(|e| DbError::InvalidData(format!("invalid decimal '{s}': {e}")))
}

fn parse_date(s: &str) -> Result<NaiveDate, DbError> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| DbError::InvalidData(format!("invalid date '{s}': {e}")))
}

fn parse_account_type(s: &str) -> Result<AccountType, DbError> {
    AccountType::from_name(s)
        .or_else(|| match s {
            "assets" => Some(AccountType::Assets),
            "liabilities" => Some(AccountType::Liabilities),
            "income" => Some(AccountType::Income),
            "expenses" => Some(AccountType::Expenses),
            "equity" => Some(AccountType::Equity),
            _ => None,
        })
        .ok_or_else(|| DbError::InvalidData(format!("invalid account type: {s}")))
}

fn parse_booking_method(s: &str) -> Result<BookingMethod, DbError> {
    BookingMethod::from_str(s)
        .ok_or_else(|| DbError::InvalidData(format!("invalid booking method: {s}")))
}

fn parse_transaction_status(s: &str) -> Result<TransactionStatus, DbError> {
    TransactionStatus::from_str(s)
        .ok_or_else(|| DbError::InvalidData(format!("invalid transaction status: {s}")))
}

// ── Loading helpers ──────────────────────────────────────────────────

fn load_metadata(
    conn: &Connection,
    table: &str,
    fk_column: &str,
    fk_value: i64,
) -> Result<Metadata, DbError> {
    let sql = format!("SELECT key, value_type, value FROM {table} WHERE {fk_column} = ?1");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![fk_value], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut metadata = HashMap::new();
    for r in rows {
        let (key, vtype, value) = r?;
        metadata.insert(key, parse_metadata_value(&vtype, &value)?);
    }
    Ok(metadata)
}

fn save_metadata(
    conn: &Connection,
    table: &str,
    fk_column: &str,
    fk_value: i64,
    metadata: &Metadata,
) -> Result<(), DbError> {
    // Delete existing
    let del_sql = format!("DELETE FROM {table} WHERE {fk_column} = ?1");
    conn.execute(&del_sql, params![fk_value])?;

    if metadata.is_empty() {
        return Ok(());
    }

    let ins_sql = format!(
        "INSERT INTO {table} ({fk_column}, key, value_type, value) VALUES (?1, ?2, ?3, ?4)"
    );
    let mut stmt = conn.prepare(&ins_sql)?;
    for (key, val) in metadata {
        stmt.execute(params![
            fk_value,
            key,
            metadata_value_type(val),
            metadata_value_to_string(val),
        ])?;
    }
    Ok(())
}

fn load_account_currencies(
    conn: &Connection,
    account_id: i64,
) -> Result<Vec<CommodityId>, DbError> {
    let mut stmt =
        conn.prepare("SELECT commodity_id FROM account_currencies WHERE account_id = ?1")?;
    let rows = stmt.query_map(params![account_id], |row| {
        Ok(CommodityId(row.get::<_, i64>(0)?))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
}

fn save_account_currencies(
    conn: &Connection,
    account_id: i64,
    currencies: &[CommodityId],
) -> Result<(), DbError> {
    conn.execute(
        "DELETE FROM account_currencies WHERE account_id = ?1",
        params![account_id],
    )?;
    if currencies.is_empty() {
        return Ok(());
    }
    let mut stmt =
        conn.prepare("INSERT INTO account_currencies (account_id, commodity_id) VALUES (?1, ?2)")?;
    for c in currencies {
        stmt.execute(params![account_id, c.0])?;
    }
    Ok(())
}

fn load_commodity(_conn: &Connection, row: &Row<'_>) -> Result<Commodity, rusqlite::Error> {
    Ok(Commodity {
        id: CommodityId(row.get::<_, i64>(0)?),
        name: row.get(1)?,
        precision: row.get::<_, u8>(2)?,
        metadata: HashMap::new(), // filled in after
    })
}

fn load_commodity_full(conn: &Connection, id: i64) -> Result<Option<Commodity>, DbError> {
    let mut stmt = conn.prepare("SELECT id, name, precision FROM commodities WHERE id = ?1")?;
    let mut rows = stmt.query(params![id])?;
    match rows.next()? {
        Some(row) => {
            let mut commodity = load_commodity(conn, row)?;
            commodity.metadata =
                load_metadata(conn, "commodity_metadata", "commodity_id", commodity.id.0)?;
            Ok(Some(commodity))
        }
        None => Ok(None),
    }
}

fn load_account_full(conn: &Connection, id: i64) -> Result<Option<Account>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, name, account_type, is_open, opened_at, closed_at, booking_method FROM accounts WHERE id = ?1",
    )?;
    let mut rows = stmt.query(params![id])?;
    match rows.next()? {
        Some(row) => {
            let account = read_account_row(conn, row)?;
            Ok(Some(account))
        }
        None => Ok(None),
    }
}

fn read_account_row(conn: &Connection, row: &Row<'_>) -> Result<Account, DbError> {
    let id = row.get::<_, i64>(0)?;
    let name: String = row.get(1)?;
    let account_type_str: String = row.get(2)?;
    let is_open = row.get::<_, i64>(3)? != 0;
    let opened_at_str: String = row.get(4)?;
    let closed_at_str: Option<String> = row.get(5)?;
    let booking_method_str: String = row.get(6)?;

    let account_type = parse_account_type(&account_type_str)?;
    let opened_at = parse_date(&opened_at_str)?;
    let closed_at = closed_at_str.map(|s| parse_date(&s)).transpose()?;
    let booking_method = parse_booking_method(&booking_method_str)?;
    let currencies = load_account_currencies(conn, id)?;
    let metadata = load_metadata(conn, "account_metadata", "account_id", id)?;

    Ok(Account {
        id: AccountId(id),
        name,
        account_type,
        is_open,
        opened_at,
        closed_at,
        currencies,
        booking_method,
        metadata,
    })
}

fn load_postings_for_transaction(
    conn: &Connection,
    transaction_id: i64,
) -> Result<Vec<Posting>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, transaction_id, account_id, amount, commodity_id, \
         cost_amount, cost_commodity_id, cost_date, cost_label, \
         price_amount, price_commodity_id \
         FROM postings WHERE transaction_id = ?1 ORDER BY id",
    )?;
    let rows = stmt.query_map(params![transaction_id], |row| {
        // We can't return DbError from here, so collect raw data
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<i64>>(10)?,
        ))
    })?;

    let mut postings = Vec::new();
    for r in rows {
        let (
            id,
            txn_id,
            acct_id,
            amount_str,
            commodity_id,
            cost_amount_str,
            cost_commodity_id,
            cost_date_str,
            cost_label,
            price_amount_str,
            price_commodity_id,
        ) = r?;

        let units = Amount {
            value: parse_decimal(&amount_str)?,
            commodity_id: CommodityId(commodity_id),
        };

        let cost = match (cost_amount_str, cost_commodity_id, cost_date_str) {
            (Some(amt), Some(cid), Some(dt)) => Some(Cost {
                amount: Amount {
                    value: parse_decimal(&amt)?,
                    commodity_id: CommodityId(cid),
                },
                date: parse_date(&dt)?,
                label: cost_label,
            }),
            _ => None,
        };

        let price = match (price_amount_str, price_commodity_id) {
            (Some(amt), Some(cid)) => Some(Amount {
                value: parse_decimal(&amt)?,
                commodity_id: CommodityId(cid),
            }),
            _ => None,
        };

        let metadata = load_metadata(conn, "posting_metadata", "posting_id", id)?;

        postings.push(Posting {
            id: PostingId(id),
            transaction_id: TransactionId(txn_id),
            account_id: AccountId(acct_id),
            units,
            cost,
            price,
            metadata,
        });
    }
    Ok(postings)
}

fn load_tags(conn: &Connection, transaction_id: i64) -> Result<Vec<String>, DbError> {
    let mut stmt =
        conn.prepare("SELECT tag FROM transaction_tags WHERE transaction_id = ?1 ORDER BY tag")?;
    let rows = stmt.query_map(params![transaction_id], |row| row.get::<_, String>(0))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
}

fn load_links(conn: &Connection, transaction_id: i64) -> Result<Vec<String>, DbError> {
    let mut stmt =
        conn.prepare("SELECT link FROM transaction_links WHERE transaction_id = ?1 ORDER BY link")?;
    let rows = stmt.query_map(params![transaction_id], |row| row.get::<_, String>(0))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
}

fn load_transaction_full(conn: &Connection, id: i64) -> Result<Option<Transaction>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, date, time, status, payee, narration FROM transactions WHERE id = ?1",
    )?;
    let mut rows = stmt.query(params![id])?;
    match rows.next()? {
        Some(row) => {
            let tx = read_transaction_row(conn, row)?;
            Ok(Some(tx))
        }
        None => Ok(None),
    }
}

fn read_transaction_row(conn: &Connection, row: &Row<'_>) -> Result<Transaction, DbError> {
    let id = row.get::<_, i64>(0)?;
    let date_str: String = row.get(1)?;
    let time_str: Option<String> = row.get(2)?;
    let status_str: String = row.get(3)?;
    let payee: Option<String> = row.get(4)?;
    let narration: Option<String> = row.get(5)?;

    let date = parse_date(&date_str)?;
    let time = time_str
        .map(|s| {
            chrono::NaiveTime::parse_from_str(&s, "%H:%M:%S")
                .or_else(|_| chrono::NaiveTime::parse_from_str(&s, "%H:%M"))
                .map_err(|e| DbError::InvalidData(format!("invalid time '{s}': {e}")))
        })
        .transpose()?;
    let status = parse_transaction_status(&status_str)?;
    let postings = load_postings_for_transaction(conn, id)?;
    let tags = load_tags(conn, id)?;
    let links = load_links(conn, id)?;
    let metadata = load_metadata(conn, "transaction_metadata", "transaction_id", id)?;

    Ok(Transaction {
        id: TransactionId(id),
        date,
        time,
        status,
        payee,
        narration,
        tags,
        links,
        postings,
        metadata,
    })
}

// ── StorageProvider impl ─────────────────────────────────────────────

impl StorageProvider for SqliteProvider {
    fn initialize(&mut self) -> Result<(), DbError> {
        self.conn.execute_batch(SCHEMA_SQL)?;
        self.initialize_audit_log()?;
        Ok(())
    }

    // ── Commodities ──────────────────────────────────────────────────

    fn create_commodity(&mut self, commodity: &NewCommodity) -> Result<Commodity, DbError> {
        let id = allocate_id(&self.conn, "commodity")?;
        self.conn.execute(
            "INSERT INTO commodities (id, name, precision) VALUES (?1, ?2, ?3)",
            params![id, commodity.name, commodity.precision],
        )?;
        save_metadata(
            &self.conn,
            "commodity_metadata",
            "commodity_id",
            id,
            &commodity.metadata,
        )?;
        let created = load_commodity_full(&self.conn, id)?
            .ok_or_else(|| DbError::NotFound("commodity just created".into()))?;
        let after = snapshot_commodity(&self.conn, created.id)?;
        self.record_mutation("create", "commodity", id, None, after)?;
        Ok(created)
    }

    fn get_commodity(&self, id: CommodityId) -> Result<Option<Commodity>, DbError> {
        load_commodity_full(&self.conn, id.0)
    }

    fn get_commodity_by_name(&self, name: &str) -> Result<Option<Commodity>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, precision FROM commodities WHERE name = ?1")?;
        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => {
                let id = row.get::<_, i64>(0)?;
                load_commodity_full(&self.conn, id)
            }
            None => Ok(None),
        }
    }

    fn list_commodities(&self) -> Result<Vec<Commodity>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, precision FROM commodities ORDER BY name")?;
        let rows = stmt.query_map([], |row| Ok(row.get::<_, i64>(0)?))?;
        let ids: Vec<i64> = rows.collect::<Result<Vec<_>, _>>()?;
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(c) = load_commodity_full(&self.conn, id)? {
                result.push(c);
            }
        }
        Ok(result)
    }

    fn update_commodity(
        &mut self,
        id: CommodityId,
        update: &CommodityUpdate,
    ) -> Result<Commodity, DbError> {
        let before = snapshot_commodity(&self.conn, id)?;
        // Verify exists
        if before.is_none() {
            return Err(DbError::NotFound(format!("commodity {}", id.0)));
        }
        if let Some(precision) = update.precision {
            self.conn.execute(
                "UPDATE commodities SET precision = ?1 WHERE id = ?2",
                params![precision, id.0],
            )?;
        }
        let updated = load_commodity_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("commodity {}", id.0)))?;
        let after = snapshot_commodity(&self.conn, id)?;
        self.record_mutation("update", "commodity", id.0, before, after)?;
        Ok(updated)
    }

    fn delete_commodity(&mut self, id: CommodityId) -> Result<(), DbError> {
        let before = snapshot_commodity(&self.conn, id)?;
        if before.is_none() {
            return Err(DbError::NotFound(format!("commodity {}", id.0)));
        }
        delete_commodity_raw(&self.conn, id.0)?;
        self.record_mutation("delete", "commodity", id.0, before, None)?;
        Ok(())
    }

    // ── Accounts ─────────────────────────────────────────────────────

    fn create_account(&mut self, account: &NewAccount) -> Result<Account, DbError> {
        let account_type = account.account_type().ok_or_else(|| {
            DbError::InvalidData(format!(
                "cannot derive account type from name '{}'",
                account.name
            ))
        })?;

        let id = allocate_id(&self.conn, "account")?;
        self.conn.execute(
            "INSERT INTO accounts (id, name, account_type, is_open, opened_at, booking_method) \
             VALUES (?1, ?2, ?3, 1, ?4, ?5)",
            params![
                id,
                account.name,
                account_type.as_str(),
                account.opened_at.format("%Y-%m-%d").to_string(),
                account.booking_method.as_str(),
            ],
        )?;
        save_account_currencies(&self.conn, id, &account.currencies)?;
        save_metadata(
            &self.conn,
            "account_metadata",
            "account_id",
            id,
            &account.metadata,
        )?;
        let created = load_account_full(&self.conn, id)?
            .ok_or_else(|| DbError::NotFound("account just created".into()))?;
        let after = snapshot_account(&self.conn, created.id)?;
        self.record_mutation("create", "account", id, None, after)?;
        Ok(created)
    }

    fn get_account(&self, id: AccountId) -> Result<Option<Account>, DbError> {
        load_account_full(&self.conn, id.0)
    }

    fn get_account_by_name(&self, name: &str) -> Result<Option<Account>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, account_type, is_open, opened_at, closed_at, booking_method \
             FROM accounts WHERE name = ?1",
        )?;
        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => {
                let account = read_account_row(&self.conn, row)?;
                Ok(Some(account))
            }
            None => Ok(None),
        }
    }

    fn list_accounts(&self, filter: &AccountFilter) -> Result<Vec<Account>, DbError> {
        let mut sql = String::from(
            "SELECT id, name, account_type, is_open, opened_at, closed_at, booking_method FROM accounts WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref at) = filter.account_type {
            param_values.push(Box::new(at.as_str().to_string()));
            sql.push_str(&format!(" AND account_type = ?{}", param_values.len()));
        }
        if let Some(is_open) = filter.is_open {
            param_values.push(Box::new(if is_open { 1i64 } else { 0i64 }));
            sql.push_str(&format!(" AND is_open = ?{}", param_values.len()));
        }

        sql.push_str(" ORDER BY name");

        let mut stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();
        let mut rows = stmt.query(param_refs.as_slice())?;

        let mut accounts = Vec::new();
        while let Some(row) = rows.next()? {
            accounts.push(read_account_row(&self.conn, row)?);
        }
        Ok(accounts)
    }

    fn update_account(
        &mut self,
        id: AccountId,
        update: &AccountUpdate,
    ) -> Result<Account, DbError> {
        let before = snapshot_account(&self.conn, id)?;
        if before.is_none() {
            return Err(DbError::NotFound(format!("account {}", id.0)));
        }
        if let Some(ref bm) = update.booking_method {
            self.conn.execute(
                "UPDATE accounts SET booking_method = ?1 WHERE id = ?2",
                params![bm.as_str(), id.0],
            )?;
        }
        if let Some(ref currencies) = update.currencies {
            save_account_currencies(&self.conn, id.0, currencies)?;
        }
        let updated = load_account_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("account {}", id.0)))?;
        let after = snapshot_account(&self.conn, id)?;
        self.record_mutation("update", "account", id.0, before, after)?;
        Ok(updated)
    }

    fn open_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account, DbError> {
        let before = snapshot_account(&self.conn, id)?;
        let affected = self.conn.execute(
            "UPDATE accounts SET is_open = 1, opened_at = ?1, closed_at = NULL WHERE id = ?2",
            params![date.format("%Y-%m-%d").to_string(), id.0],
        )?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("account {}", id.0)));
        }
        let opened = load_account_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("account {}", id.0)))?;
        let after = snapshot_account(&self.conn, id)?;
        self.record_mutation("open", "account", id.0, before, after)?;
        Ok(opened)
    }

    fn close_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account, DbError> {
        let before = snapshot_account(&self.conn, id)?;
        let affected = self.conn.execute(
            "UPDATE accounts SET is_open = 0, closed_at = ?1 WHERE id = ?2",
            params![date.format("%Y-%m-%d").to_string(), id.0],
        )?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("account {}", id.0)));
        }
        let closed = load_account_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("account {}", id.0)))?;
        let after = snapshot_account(&self.conn, id)?;
        self.record_mutation("close", "account", id.0, before, after)?;
        Ok(closed)
    }

    fn delete_account(&mut self, id: AccountId) -> Result<(), DbError> {
        let before = snapshot_account(&self.conn, id)?;
        if before.is_none() {
            return Err(DbError::NotFound(format!("account {}", id.0)));
        }
        delete_account_raw(&self.conn, id.0)?;
        self.record_mutation("delete", "account", id.0, before, None)?;
        Ok(())
    }

    // ── Transactions ─────────────────────────────────────────────────

    fn create_transaction(&mut self, tx: &NewTransaction) -> Result<Transaction, DbError> {
        let txn_id = allocate_id(&self.conn, "transaction")?;
        let posting_ids = (0..tx.postings.len())
            .map(|_| allocate_id(&self.conn, "posting"))
            .collect::<Result<Vec<_>, _>>()?;
        let db_tx = self.conn.transaction()?;

        let date_str = tx.date.format("%Y-%m-%d").to_string();
        let time_str = tx.time.map(|t| t.format("%H:%M:%S").to_string());

        db_tx.execute(
            "INSERT INTO transactions (id, date, time, status, payee, narration) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                txn_id,
                date_str,
                time_str,
                tx.status.as_str(),
                tx.payee,
                tx.narration
            ],
        )?;

        // Postings
        {
            let mut stmt = db_tx.prepare(
                "INSERT INTO postings (id, transaction_id, account_id, amount, commodity_id, \
                 cost_amount, cost_commodity_id, cost_date, cost_label, \
                 price_amount, price_commodity_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            )?;
            for (p, posting_id) in tx.postings.iter().zip(posting_ids.iter()) {
                let cost_amount = p.cost.as_ref().map(|c| c.amount.value.to_string());
                let cost_commodity_id = p.cost.as_ref().map(|c| c.amount.commodity_id.0);
                let cost_date = p
                    .cost
                    .as_ref()
                    .map(|c| c.date.format("%Y-%m-%d").to_string());
                let cost_label = p.cost.as_ref().and_then(|c| c.label.clone());
                let price_amount = p.price.as_ref().map(|pr| pr.value.to_string());
                let price_commodity_id = p.price.as_ref().map(|pr| pr.commodity_id.0);

                stmt.execute(params![
                    posting_id,
                    txn_id,
                    p.account_id.0,
                    p.units.value.to_string(),
                    p.units.commodity_id.0,
                    cost_amount,
                    cost_commodity_id,
                    cost_date,
                    cost_label,
                    price_amount,
                    price_commodity_id,
                ])?;

                if !p.metadata.is_empty() {
                    save_metadata(
                        &db_tx,
                        "posting_metadata",
                        "posting_id",
                        *posting_id,
                        &p.metadata,
                    )?;
                }
            }
        }

        // Tags
        if !tx.tags.is_empty() {
            let mut stmt = db_tx
                .prepare("INSERT INTO transaction_tags (transaction_id, tag) VALUES (?1, ?2)")?;
            for tag in &tx.tags {
                stmt.execute(params![txn_id, tag])?;
            }
        }

        // Links
        if !tx.links.is_empty() {
            let mut stmt = db_tx
                .prepare("INSERT INTO transaction_links (transaction_id, link) VALUES (?1, ?2)")?;
            for link in &tx.links {
                stmt.execute(params![txn_id, link])?;
            }
        }

        // Metadata
        if !tx.metadata.is_empty() {
            save_metadata(
                &db_tx,
                "transaction_metadata",
                "transaction_id",
                txn_id,
                &tx.metadata,
            )?;
        }

        db_tx.commit()?;

        let created = load_transaction_full(&self.conn, txn_id)?
            .ok_or_else(|| DbError::NotFound("transaction just created".into()))?;
        let after = snapshot_transaction(&self.conn, created.id)?;
        self.record_mutation("create", "transaction", txn_id, None, after)?;
        Ok(created)
    }

    fn get_transaction(&self, id: TransactionId) -> Result<Option<Transaction>, DbError> {
        load_transaction_full(&self.conn, id.0)
    }

    fn list_transactions(&self, filter: &TransactionFilter) -> Result<Vec<Transaction>, DbError> {
        let mut sql = String::from("SELECT DISTINCT t.id FROM transactions t");
        let mut joins = String::new();
        let mut conditions = Vec::new();
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if filter.account_id.is_some() {
            joins.push_str(" JOIN postings p ON p.transaction_id = t.id");
        }
        if filter.tag.is_some() {
            joins.push_str(" JOIN transaction_tags tt ON tt.transaction_id = t.id");
        }

        sql.push_str(&joins);

        if let Some(ref from) = filter.from {
            param_values.push(Box::new(from.format("%Y-%m-%d").to_string()));
            conditions.push(format!("t.date >= ?{}", param_values.len()));
        }
        if let Some(ref to) = filter.to {
            param_values.push(Box::new(to.format("%Y-%m-%d").to_string()));
            conditions.push(format!("t.date <= ?{}", param_values.len()));
        }
        if let Some(ref account_id) = filter.account_id {
            param_values.push(Box::new(account_id.0));
            conditions.push(format!("p.account_id = ?{}", param_values.len()));
        }
        if let Some(ref payee) = filter.payee {
            param_values.push(Box::new(payee.clone()));
            conditions.push(format!("t.payee = ?{}", param_values.len()));
        }
        if let Some(ref tag) = filter.tag {
            param_values.push(Box::new(tag.clone()));
            conditions.push(format!("tt.tag = ?{}", param_values.len()));
        }
        if let Some(ref status) = filter.status {
            param_values.push(Box::new(status.as_str().to_string()));
            conditions.push(format!("t.status = ?{}", param_values.len()));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY t.date, t.time, t.id");

        let mut stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| row.get::<_, i64>(0))?;
        let ids: Vec<i64> = rows.collect::<Result<Vec<_>, _>>()?;

        let mut transactions = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(tx) = load_transaction_full(&self.conn, id)? {
                transactions.push(tx);
            }
        }
        Ok(transactions)
    }

    fn update_transaction(
        &mut self,
        id: TransactionId,
        update: &TransactionUpdate,
    ) -> Result<Transaction, DbError> {
        let before = snapshot_transaction(&self.conn, id)?;
        if before.is_none() {
            return Err(DbError::NotFound(format!("transaction {}", id.0)));
        }

        if let Some(ref date) = update.date {
            self.conn.execute(
                "UPDATE transactions SET date = ?1 WHERE id = ?2",
                params![date.format("%Y-%m-%d").to_string(), id.0],
            )?;
        }
        if let Some(ref time) = update.time {
            let time_str = time.map(|t| t.format("%H:%M:%S").to_string());
            self.conn.execute(
                "UPDATE transactions SET time = ?1 WHERE id = ?2",
                params![time_str, id.0],
            )?;
        }
        if let Some(ref status) = update.status {
            self.conn.execute(
                "UPDATE transactions SET status = ?1 WHERE id = ?2",
                params![status.as_str(), id.0],
            )?;
        }
        if let Some(ref payee) = update.payee {
            self.conn.execute(
                "UPDATE transactions SET payee = ?1 WHERE id = ?2",
                params![*payee, id.0],
            )?;
        }
        if let Some(ref narration) = update.narration {
            self.conn.execute(
                "UPDATE transactions SET narration = ?1 WHERE id = ?2",
                params![*narration, id.0],
            )?;
        }

        let updated = load_transaction_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("transaction {}", id.0)))?;
        let after = snapshot_transaction(&self.conn, id)?;
        self.record_mutation("update", "transaction", id.0, before, after)?;
        Ok(updated)
    }

    fn delete_transaction(&mut self, id: TransactionId) -> Result<(), DbError> {
        let before = snapshot_transaction(&self.conn, id)?;
        if before.is_none() {
            return Err(DbError::NotFound(format!("transaction {}", id.0)));
        }
        delete_transaction_raw(&self.conn, id.0)?;
        self.record_mutation("delete", "transaction", id.0, before, None)?;
        Ok(())
    }

    // ── Prices ───────────────────────────────────────────────────────

    fn create_price(&mut self, price: &NewPrice) -> Result<Price, DbError> {
        let id = allocate_id(&self.conn, "price")?;
        self.conn.execute(
            "INSERT INTO prices (id, date, commodity_id, target_commodity_id, value) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id,
                price.date.format("%Y-%m-%d").to_string(),
                price.commodity_id.0,
                price.target_commodity_id.0,
                price.value.to_string(),
            ],
        )?;
        let created = Price {
            id: PriceId(id),
            date: price.date,
            commodity_id: price.commodity_id,
            target_commodity_id: price.target_commodity_id,
            value: price.value,
        };
        let after = snapshot_price(&self.conn, created.id)?;
        self.record_mutation("create", "price", id, None, after)?;
        Ok(created)
    }

    fn get_price(
        &self,
        commodity: CommodityId,
        target: CommodityId,
        date: NaiveDate,
    ) -> Result<Option<Price>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, date, commodity_id, target_commodity_id, value FROM prices \
             WHERE commodity_id = ?1 AND target_commodity_id = ?2 AND date = ?3",
        )?;
        let mut rows = stmt.query(params![
            commodity.0,
            target.0,
            date.format("%Y-%m-%d").to_string()
        ])?;
        match rows.next()? {
            Some(row) => {
                let id = row.get::<_, i64>(0)?;
                let date_str: String = row.get(1)?;
                let cid = row.get::<_, i64>(2)?;
                let tid = row.get::<_, i64>(3)?;
                let value_str: String = row.get(4)?;
                Ok(Some(Price {
                    id: PriceId(id),
                    date: parse_date(&date_str)?,
                    commodity_id: CommodityId(cid),
                    target_commodity_id: CommodityId(tid),
                    value: parse_decimal(&value_str)?,
                }))
            }
            None => Ok(None),
        }
    }

    fn list_prices(&self, filter: &PriceFilter) -> Result<Vec<Price>, DbError> {
        let mut sql = String::from(
            "SELECT id, date, commodity_id, target_commodity_id, value FROM prices WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref cid) = filter.commodity_id {
            param_values.push(Box::new(cid.0));
            sql.push_str(&format!(" AND commodity_id = ?{}", param_values.len()));
        }
        if let Some(ref from) = filter.from {
            param_values.push(Box::new(from.format("%Y-%m-%d").to_string()));
            sql.push_str(&format!(" AND date >= ?{}", param_values.len()));
        }
        if let Some(ref to) = filter.to {
            param_values.push(Box::new(to.format("%Y-%m-%d").to_string()));
            sql.push_str(&format!(" AND date <= ?{}", param_values.len()));
        }

        sql.push_str(" ORDER BY date, commodity_id");

        let mut stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();
        let mut rows = stmt.query(param_refs.as_slice())?;

        let mut prices = Vec::new();
        while let Some(row) = rows.next()? {
            let id = row.get::<_, i64>(0)?;
            let date_str: String = row.get(1)?;
            let cid = row.get::<_, i64>(2)?;
            let tid = row.get::<_, i64>(3)?;
            let value_str: String = row.get(4)?;
            prices.push(Price {
                id: PriceId(id),
                date: parse_date(&date_str)?,
                commodity_id: CommodityId(cid),
                target_commodity_id: CommodityId(tid),
                value: parse_decimal(&value_str)?,
            });
        }
        Ok(prices)
    }

    fn delete_price(&mut self, id: PriceId) -> Result<(), DbError> {
        let before = snapshot_price(&self.conn, id)?;
        if before.is_none() {
            return Err(DbError::NotFound(format!("price {}", id.0)));
        }
        let affected = self
            .conn
            .execute("DELETE FROM prices WHERE id = ?1", params![id.0])?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("price {}", id.0)));
        }
        self.record_mutation("delete", "price", id.0, before, None)?;
        Ok(())
    }

    // ── Balance assertions ───────────────────────────────────────────

    fn create_balance_assertion(
        &mut self,
        assertion: &NewBalanceAssertion,
    ) -> Result<BalanceAssertion, DbError> {
        let id = allocate_id(&self.conn, "balance_assertion")?;
        self.conn.execute(
            "INSERT INTO balance_assertions (id, date, account_id, amount, commodity_id) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id,
                assertion.date.format("%Y-%m-%d").to_string(),
                assertion.account_id.0,
                assertion.expected.value.to_string(),
                assertion.expected.commodity_id.0,
            ],
        )?;
        let created = BalanceAssertion {
            id: BalanceAssertionId(id),
            date: assertion.date,
            account_id: assertion.account_id,
            expected: Amount {
                value: assertion.expected.value,
                commodity_id: assertion.expected.commodity_id,
            },
        };
        let after = snapshot_balance_assertion(&self.conn, created.id)?;
        self.record_mutation("create", "balance_assertion", id, None, after)?;
        Ok(created)
    }

    fn list_balance_assertions(
        &self,
        filter: &BalanceAssertionFilter,
    ) -> Result<Vec<BalanceAssertion>, DbError> {
        let mut sql = String::from(
            "SELECT id, date, account_id, amount, commodity_id FROM balance_assertions WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref aid) = filter.account_id {
            param_values.push(Box::new(aid.0));
            sql.push_str(&format!(" AND account_id = ?{}", param_values.len()));
        }

        sql.push_str(" ORDER BY date");

        let mut stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();
        let mut rows = stmt.query(param_refs.as_slice())?;

        let mut assertions = Vec::new();
        while let Some(row) = rows.next()? {
            let id = row.get::<_, i64>(0)?;
            let date_str: String = row.get(1)?;
            let account_id = row.get::<_, i64>(2)?;
            let amount_str: String = row.get(3)?;
            let commodity_id = row.get::<_, i64>(4)?;
            assertions.push(BalanceAssertion {
                id: BalanceAssertionId(id),
                date: parse_date(&date_str)?,
                account_id: AccountId(account_id),
                expected: Amount {
                    value: parse_decimal(&amount_str)?,
                    commodity_id: CommodityId(commodity_id),
                },
            });
        }
        Ok(assertions)
    }

    fn delete_balance_assertion(&mut self, id: BalanceAssertionId) -> Result<(), DbError> {
        let before = snapshot_balance_assertion(&self.conn, id)?;
        if before.is_none() {
            return Err(DbError::NotFound(format!("balance assertion {}", id.0)));
        }
        let affected = self.conn.execute(
            "DELETE FROM balance_assertions WHERE id = ?1",
            params![id.0],
        )?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("balance assertion {}", id.0)));
        }
        self.record_mutation("delete", "balance_assertion", id.0, before, None)?;
        Ok(())
    }

    // ── Raw query ────────────────────────────────────────────────────

    fn query_raw(&self, sql: &str) -> Result<QueryResult, DbError> {
        let mut stmt = self.conn.prepare(sql)?;
        if !stmt.readonly() {
            return Err(DbError::InvalidData(
                "raw SQL queries are read-only; use rai commands so writes are audited".into(),
            ));
        }
        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let mut rows_out = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let val = row.get_ref(i)?;
                let qv = match val {
                    rusqlite::types::ValueRef::Null => QueryValue::Null,
                    rusqlite::types::ValueRef::Integer(n) => QueryValue::Integer(n),
                    rusqlite::types::ValueRef::Real(f) => QueryValue::Real(f),
                    rusqlite::types::ValueRef::Text(bytes) => {
                        QueryValue::Text(String::from_utf8_lossy(bytes).into_owned())
                    }
                    rusqlite::types::ValueRef::Blob(bytes) => {
                        QueryValue::Text(format!("<blob {} bytes>", bytes.len()))
                    }
                };
                values.push(qv);
            }
            rows_out.push(values);
        }

        Ok(QueryResult {
            columns,
            rows: rows_out,
        })
    }

    // ── Audit log ───────────────────────────────────────────────────

    fn list_audit_events(&self, filter: &AuditFilter) -> Result<Vec<AuditEvent>, DbError> {
        let mut sql = String::from(
            "SELECT id, created_at, kind, operation, entity_type, entity_id, summary, \
                    target_event_id, before_json, after_json \
             FROM audit_events WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn ToSql>> = Vec::new();

        if let Some(ref entity_type) = filter.entity_type {
            param_values.push(Box::new(entity_type.clone()));
            sql.push_str(&format!(" AND entity_type = ?{}", param_values.len()));
        }
        if let Some(entity_id) = filter.entity_id {
            param_values.push(Box::new(entity_id));
            sql.push_str(&format!(" AND entity_id = ?{}", param_values.len()));
        }

        let limit = filter.limit.unwrap_or(50).min(1000) as i64;
        param_values.push(Box::new(limit));
        sql.push_str(&format!(" ORDER BY id DESC LIMIT ?{}", param_values.len()));

        let mut stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn ToSql> = param_values.iter().map(|b| b.as_ref()).collect();
        let mut rows = stmt.query(param_refs.as_slice())?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            events.push(read_audit_event_row(row)?);
        }
        Ok(events)
    }

    fn get_audit_event(&self, id: AuditEventId) -> Result<Option<AuditEvent>, DbError> {
        get_audit_event_conn(&self.conn, id)
    }

    fn undo_last_audit_event(&mut self) -> Result<AuditEvent, DbError> {
        let db_tx = self.conn.transaction()?;
        let event_id = last_stack_event(&db_tx, "audit_undo_stack")?
            .ok_or_else(|| DbError::NotFound("undoable audit event".into()))?;
        let event = get_audit_event_conn(&db_tx, event_id)?
            .ok_or_else(|| DbError::NotFound(format!("audit event {}", event_id.0)))?;
        if event.kind != AuditEventKind::Mutation {
            return Err(DbError::InvalidData(format!(
                "audit event {} is not undoable",
                event.id.0
            )));
        }
        let entity_id = event.entity_id.ok_or_else(|| {
            DbError::InvalidData(format!("audit event {} has no entity id", event.id.0))
        })?;
        let current = snapshot_entity(&db_tx, &event.entity_type, entity_id)?;
        if current != event.after_json {
            return Err(DbError::InvalidData(format!(
                "cannot undo audit event {} because current state differs from its after snapshot",
                event.id.0
            )));
        }

        restore_entity(
            &db_tx,
            &event.entity_type,
            entity_id,
            event.before_json.as_deref(),
        )?;
        remove_stack_event(&db_tx, "audit_undo_stack", event.id)?;
        push_stack(&db_tx, "audit_redo_stack", event.id)?;

        let summary = format!("undo audit event {}", event.id.0);
        let undo_id = insert_audit_event(
            &db_tx,
            AuditEventKind::Undo,
            "undo",
            &event.entity_type,
            Some(entity_id),
            &summary,
            Some(event.id),
            current.as_deref(),
            event.before_json.as_deref(),
        )?;
        let undo_event = get_audit_event_conn(&db_tx, undo_id)?
            .ok_or_else(|| DbError::NotFound(format!("audit event {}", undo_id.0)))?;
        db_tx.commit()?;
        Ok(undo_event)
    }

    fn redo_last_audit_event(&mut self) -> Result<AuditEvent, DbError> {
        let db_tx = self.conn.transaction()?;
        let event_id = last_stack_event(&db_tx, "audit_redo_stack")?
            .ok_or_else(|| DbError::NotFound("redoable audit event".into()))?;
        let event = get_audit_event_conn(&db_tx, event_id)?
            .ok_or_else(|| DbError::NotFound(format!("audit event {}", event_id.0)))?;
        if event.kind != AuditEventKind::Mutation {
            return Err(DbError::InvalidData(format!(
                "audit event {} is not redoable",
                event.id.0
            )));
        }
        let entity_id = event.entity_id.ok_or_else(|| {
            DbError::InvalidData(format!("audit event {} has no entity id", event.id.0))
        })?;
        let current = snapshot_entity(&db_tx, &event.entity_type, entity_id)?;
        if current != event.before_json {
            return Err(DbError::InvalidData(format!(
                "cannot redo audit event {} because current state differs from its before snapshot",
                event.id.0
            )));
        }

        restore_entity(
            &db_tx,
            &event.entity_type,
            entity_id,
            event.after_json.as_deref(),
        )?;
        remove_stack_event(&db_tx, "audit_redo_stack", event.id)?;
        push_stack(&db_tx, "audit_undo_stack", event.id)?;

        let summary = format!("redo audit event {}", event.id.0);
        let redo_id = insert_audit_event(
            &db_tx,
            AuditEventKind::Redo,
            "redo",
            &event.entity_type,
            Some(entity_id),
            &summary,
            Some(event.id),
            current.as_deref(),
            event.after_json.as_deref(),
        )?;
        let redo_event = get_audit_event_conn(&db_tx, redo_id)?
            .ok_or_else(|| DbError::NotFound(format!("audit event {}", redo_id.0)))?;
        db_tx.commit()?;
        Ok(redo_event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{AuditEventKind, AuditFilter};
    use rai_core::types::{
        AccountFilter, AccountId, AccountType, Amount, BalanceAssertionFilter, BookingMethod,
        CommodityUpdate, Cost, MetadataValue, NewAccount, NewBalanceAssertion, NewCommodity,
        NewPosting, NewPrice, NewTransaction, TransactionFilter, TransactionStatus,
        TransactionUpdate,
    };
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn setup() -> SqliteProvider {
        let mut db = SqliteProvider::open_in_memory().unwrap();
        db.initialize().unwrap();
        db
    }

    // Verifies that a commodity can be created and retrieved by ID.
    #[test]
    fn commodity_create_and_get() {
        let mut db = setup();
        let commodity = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        assert_eq!(commodity.name, "USD");
        assert_eq!(commodity.precision, 2);

        let fetched = db.get_commodity(commodity.id).unwrap().unwrap();
        assert_eq!(fetched.name, "USD");
    }

    // Verifies that a commodity can be looked up by name.
    #[test]
    fn commodity_get_by_name() {
        let mut db = setup();
        db.create_commodity(&NewCommodity {
            name: "EUR".into(),
            precision: 2,
            metadata: HashMap::new(),
        })
        .unwrap();
        let fetched = db.get_commodity_by_name("EUR").unwrap().unwrap();
        assert_eq!(fetched.name, "EUR");
        assert!(db.get_commodity_by_name("GBP").unwrap().is_none());
    }

    // Verifies that listing commodities returns all created commodities.
    #[test]
    fn commodity_list() {
        let mut db = setup();
        db.create_commodity(&NewCommodity {
            name: "USD".into(),
            precision: 2,
            metadata: HashMap::new(),
        })
        .unwrap();
        db.create_commodity(&NewCommodity {
            name: "EUR".into(),
            precision: 2,
            metadata: HashMap::new(),
        })
        .unwrap();
        let list = db.list_commodities().unwrap();
        assert_eq!(list.len(), 2);
    }

    // Verifies that updating a commodity's precision persists correctly.
    #[test]
    fn commodity_update() {
        let mut db = setup();
        let c = db
            .create_commodity(&NewCommodity {
                name: "BTC".into(),
                precision: 8,
                metadata: HashMap::new(),
            })
            .unwrap();
        let updated = db
            .update_commodity(c.id, &CommodityUpdate { precision: Some(6) })
            .unwrap();
        assert_eq!(updated.precision, 6);
    }

    // Verifies that deleting a commodity removes it from the database.
    #[test]
    fn commodity_delete() {
        let mut db = setup();
        let c = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        db.delete_commodity(c.id).unwrap();
        assert!(db.get_commodity(c.id).unwrap().is_none());
    }

    // Verifies that metadata is preserved through create and retrieve.
    #[test]
    fn commodity_metadata_roundtrip() {
        let mut db = setup();
        let mut meta = HashMap::new();
        meta.insert("symbol".into(), MetadataValue::String("$".into()));
        meta.insert("decimal_places".into(), MetadataValue::Number(dec!(2)));
        let c = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: meta.clone(),
            })
            .unwrap();
        assert_eq!(
            c.metadata.get("symbol"),
            Some(&MetadataValue::String("$".into()))
        );
        assert_eq!(
            c.metadata.get("decimal_places"),
            Some(&MetadataValue::Number(dec!(2)))
        );
    }

    // Verifies that an account can be created with the correct derived
    // account type and retrieved by ID.
    #[test]
    fn account_create_and_get() {
        let mut db = setup();
        let account = db
            .create_account(&NewAccount {
                name: "Assets:Bank:Checking".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Fifo,
                metadata: HashMap::new(),
            })
            .unwrap();
        assert_eq!(account.account_type, AccountType::Assets);
        assert_eq!(account.booking_method, BookingMethod::Fifo);
        assert!(account.is_open);

        let fetched = db.get_account(account.id).unwrap().unwrap();
        assert_eq!(fetched.name, "Assets:Bank:Checking");
    }

    // Verifies that closing and reopening an account updates the state.
    #[test]
    fn account_open_close_cycle() {
        let mut db = setup();
        let a = db
            .create_account(&NewAccount {
                name: "Assets:Bank".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();

        let closed = db.close_account(a.id, date(2024, 6, 1)).unwrap();
        assert!(!closed.is_open);
        assert_eq!(closed.closed_at, Some(date(2024, 6, 1)));

        let reopened = db.open_account(a.id, date(2024, 7, 1)).unwrap();
        assert!(reopened.is_open);
        assert!(reopened.closed_at.is_none());
    }

    // Verifies that account currency constraints are persisted.
    #[test]
    fn account_currencies() {
        let mut db = setup();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        let eur = db
            .create_commodity(&NewCommodity {
                name: "EUR".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();

        let a = db
            .create_account(&NewAccount {
                name: "Assets:Bank".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![usd.id, eur.id],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();
        assert_eq!(a.currencies.len(), 2);
    }

    // Verifies that listing accounts with filters works correctly.
    #[test]
    fn account_list_filter() {
        let mut db = setup();
        db.create_account(&NewAccount {
            name: "Assets:Bank".into(),
            opened_at: date(2024, 1, 1),
            currencies: vec![],
            booking_method: BookingMethod::Strict,
            metadata: HashMap::new(),
        })
        .unwrap();
        db.create_account(&NewAccount {
            name: "Expenses:Food".into(),
            opened_at: date(2024, 1, 1),
            currencies: vec![],
            booking_method: BookingMethod::Strict,
            metadata: HashMap::new(),
        })
        .unwrap();

        let assets = db
            .list_accounts(&AccountFilter {
                account_type: Some(AccountType::Assets),
                is_open: None,
            })
            .unwrap();
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].name, "Assets:Bank");
    }

    // Verifies that a transaction with postings, tags, and links is
    // created and retrieved correctly with all associated data intact.
    #[test]
    fn transaction_create_and_get() {
        let mut db = setup();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        let a1 = db
            .create_account(&NewAccount {
                name: "Assets:Bank".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();
        let a2 = db
            .create_account(&NewAccount {
                name: "Expenses:Food".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();

        let tx = db
            .create_transaction(&NewTransaction {
                date: date(2024, 3, 15),
                time: None,
                status: TransactionStatus::Completed,
                payee: Some("Grocery Store".into()),
                narration: Some("Weekly groceries".into()),
                tags: vec!["food".into(), "weekly".into()],
                links: vec!["receipt-001".into()],
                postings: vec![
                    NewPosting {
                        account_id: a1.id,
                        units: Amount {
                            value: dec!(-50),
                            commodity_id: usd.id,
                        },
                        cost: None,
                        price: None,
                        metadata: HashMap::new(),
                    },
                    NewPosting {
                        account_id: a2.id,
                        units: Amount {
                            value: dec!(50),
                            commodity_id: usd.id,
                        },
                        cost: None,
                        price: None,
                        metadata: HashMap::new(),
                    },
                ],
                metadata: HashMap::new(),
            })
            .unwrap();

        assert_eq!(tx.payee, Some("Grocery Store".into()));
        assert_eq!(tx.postings.len(), 2);
        assert_eq!(tx.tags.len(), 2);
        assert_eq!(tx.links.len(), 1);

        let fetched = db.get_transaction(tx.id).unwrap().unwrap();
        assert_eq!(fetched.postings.len(), 2);
        assert_eq!(fetched.tags, vec!["food", "weekly"]);
    }

    // Verifies that transaction filtering by date range works.
    #[test]
    fn transaction_list_filter_by_date() {
        let mut db = setup();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        let a = db
            .create_account(&NewAccount {
                name: "Assets:Bank".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();

        for month in [1, 3, 6] {
            db.create_transaction(&NewTransaction {
                date: date(2024, month, 1),
                time: None,
                status: TransactionStatus::Completed,
                payee: None,
                narration: None,
                tags: vec![],
                links: vec![],
                postings: vec![NewPosting {
                    account_id: a.id,
                    units: Amount {
                        value: dec!(100),
                        commodity_id: usd.id,
                    },
                    cost: None,
                    price: None,
                    metadata: HashMap::new(),
                }],
                metadata: HashMap::new(),
            })
            .unwrap();
        }

        let filtered = db
            .list_transactions(&TransactionFilter {
                from: Some(date(2024, 2, 1)),
                to: Some(date(2024, 5, 1)),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].date, date(2024, 3, 1));
    }

    // Verifies that updating transaction fields persists correctly.
    #[test]
    fn transaction_update() {
        let mut db = setup();
        db.create_account(&NewAccount {
            name: "Assets:Bank".into(),
            opened_at: date(2024, 1, 1),
            currencies: vec![],
            booking_method: BookingMethod::Strict,
            metadata: HashMap::new(),
        })
        .unwrap();

        let tx = db
            .create_transaction(&NewTransaction {
                date: date(2024, 3, 1),
                time: None,
                status: TransactionStatus::Pending,
                payee: None,
                narration: None,
                tags: vec![],
                links: vec![],
                postings: vec![],
                metadata: HashMap::new(),
            })
            .unwrap();

        let updated = db
            .update_transaction(
                tx.id,
                &TransactionUpdate {
                    status: Some(TransactionStatus::Completed),
                    payee: Some(Some("Updated Payee".into())),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.status, TransactionStatus::Completed);
        assert_eq!(updated.payee, Some("Updated Payee".into()));
    }

    // Verifies that deleting a transaction removes it and its postings.
    #[test]
    fn transaction_delete() {
        let mut db = setup();
        let a = db
            .create_account(&NewAccount {
                name: "Assets:Bank".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();

        let tx = db
            .create_transaction(&NewTransaction {
                date: date(2024, 3, 1),
                time: None,
                status: TransactionStatus::Completed,
                payee: None,
                narration: None,
                tags: vec![],
                links: vec![],
                postings: vec![NewPosting {
                    account_id: a.id,
                    units: Amount {
                        value: dec!(100),
                        commodity_id: usd.id,
                    },
                    cost: None,
                    price: None,
                    metadata: HashMap::new(),
                }],
                metadata: HashMap::new(),
            })
            .unwrap();

        db.delete_transaction(tx.id).unwrap();
        assert!(db.get_transaction(tx.id).unwrap().is_none());
    }

    // Verifies that postings with cost and price fields are persisted
    // and retrieved correctly through the database roundtrip.
    #[test]
    fn posting_cost_and_price_roundtrip() {
        let mut db = setup();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        let btc = db
            .create_commodity(&NewCommodity {
                name: "BTC".into(),
                precision: 8,
                metadata: HashMap::new(),
            })
            .unwrap();
        let a = db
            .create_account(&NewAccount {
                name: "Assets:Crypto".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();

        let tx = db
            .create_transaction(&NewTransaction {
                date: date(2024, 3, 1),
                time: None,
                status: TransactionStatus::Completed,
                payee: None,
                narration: None,
                tags: vec![],
                links: vec![],
                postings: vec![NewPosting {
                    account_id: a.id,
                    units: Amount {
                        value: dec!(1.5),
                        commodity_id: btc.id,
                    },
                    cost: Some(Cost {
                        amount: Amount {
                            value: dec!(50000),
                            commodity_id: usd.id,
                        },
                        date: date(2024, 3, 1),
                        label: Some("lot1".into()),
                    }),
                    price: Some(Amount {
                        value: dec!(51000),
                        commodity_id: usd.id,
                    }),
                    metadata: HashMap::new(),
                }],
                metadata: HashMap::new(),
            })
            .unwrap();

        let p = &tx.postings[0];
        assert_eq!(p.units.value, dec!(1.5));
        let cost = p.cost.as_ref().unwrap();
        assert_eq!(cost.amount.value, dec!(50000));
        assert_eq!(cost.label, Some("lot1".into()));
        let price = p.price.as_ref().unwrap();
        assert_eq!(price.value, dec!(51000));
    }

    // Verifies that price entries can be created and looked up by
    // commodity pair and date.
    #[test]
    fn price_create_and_lookup() {
        let mut db = setup();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        let eur = db
            .create_commodity(&NewCommodity {
                name: "EUR".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();

        let price = db
            .create_price(&NewPrice {
                date: date(2024, 3, 1),
                commodity_id: usd.id,
                target_commodity_id: eur.id,
                value: dec!(0.92),
            })
            .unwrap();
        assert_eq!(price.value, dec!(0.92));

        let fetched = db
            .get_price(usd.id, eur.id, date(2024, 3, 1))
            .unwrap()
            .unwrap();
        assert_eq!(fetched.value, dec!(0.92));

        // Different date should return None
        assert!(db
            .get_price(usd.id, eur.id, date(2024, 4, 1))
            .unwrap()
            .is_none());
    }

    // Verifies that balance assertions can be created and listed
    // with account filtering.
    #[test]
    fn balance_assertion_create_and_list() {
        let mut db = setup();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        let a = db
            .create_account(&NewAccount {
                name: "Assets:Bank".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();

        db.create_balance_assertion(&NewBalanceAssertion {
            date: date(2024, 3, 31),
            account_id: a.id,
            expected: Amount {
                value: dec!(1000),
                commodity_id: usd.id,
            },
        })
        .unwrap();

        let all = db
            .list_balance_assertions(&BalanceAssertionFilter { account_id: None })
            .unwrap();
        assert_eq!(all.len(), 1);

        let filtered = db
            .list_balance_assertions(&BalanceAssertionFilter {
                account_id: Some(AccountId(999)),
            })
            .unwrap();
        assert!(filtered.is_empty());
    }

    // Verifies that normal mutations are logged and can be undone and redone.
    #[test]
    fn audit_create_undo_redo_commodity() {
        let mut db = setup();
        let commodity = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();

        let events = db
            .list_audit_events(&AuditFilter {
                limit: Some(10),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, AuditEventKind::Mutation);
        assert_eq!(events[0].operation, "create");
        assert_eq!(events[0].entity_type, "commodity");
        assert_eq!(events[0].entity_id, Some(commodity.id.0));

        let undo = db.undo_last_audit_event().unwrap();
        assert_eq!(undo.kind, AuditEventKind::Undo);
        assert!(db.get_commodity(commodity.id).unwrap().is_none());

        let redo = db.redo_last_audit_event().unwrap();
        assert_eq!(redo.kind, AuditEventKind::Redo);
        let restored = db.get_commodity(commodity.id).unwrap().unwrap();
        assert_eq!(restored.name, "USD");
    }

    // Verifies that a one-time baseline is created for data that predates
    // the audit tables, and that baseline events are not undoable.
    #[test]
    fn audit_backfills_existing_data_once() {
        let mut db = SqliteProvider::open_in_memory().unwrap();
        db.conn.execute_batch(SCHEMA_SQL).unwrap();
        db.conn
            .execute(
                "INSERT INTO commodities (id, name, precision) VALUES (7, 'USD', 2)",
                [],
            )
            .unwrap();

        db.initialize().unwrap();
        db.initialize().unwrap();

        let events = db
            .list_audit_events(&AuditFilter {
                entity_type: Some("commodity".into()),
                entity_id: Some(7),
                limit: Some(10),
            })
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, AuditEventKind::Baseline);
        assert_eq!(events[0].operation, "backfill");

        assert!(matches!(
            db.undo_last_audit_event(),
            Err(DbError::NotFound(_))
        ));
    }

    // Verifies that a new mutation after undo clears the redo stack.
    #[test]
    fn audit_new_mutation_clears_redo_stack() {
        let mut db = setup();
        db.create_commodity(&NewCommodity {
            name: "USD".into(),
            precision: 2,
            metadata: HashMap::new(),
        })
        .unwrap();
        db.undo_last_audit_event().unwrap();

        db.create_commodity(&NewCommodity {
            name: "EUR".into(),
            precision: 2,
            metadata: HashMap::new(),
        })
        .unwrap();

        assert!(matches!(
            db.redo_last_audit_event(),
            Err(DbError::NotFound(_))
        ));
    }

    // Verifies that transaction audit snapshots preserve child rows such as
    // postings, links, tags, and posting metadata across redo.
    #[test]
    fn audit_transaction_redo_restores_child_rows() {
        let mut db = setup();
        let usd = db
            .create_commodity(&NewCommodity {
                name: "USD".into(),
                precision: 2,
                metadata: HashMap::new(),
            })
            .unwrap();
        let account = db
            .create_account(&NewAccount {
                name: "Assets:Bank".into(),
                opened_at: date(2024, 1, 1),
                currencies: vec![],
                booking_method: BookingMethod::Strict,
                metadata: HashMap::new(),
            })
            .unwrap();

        let mut posting_metadata = HashMap::new();
        posting_metadata.insert("source".into(), MetadataValue::String("card".into()));
        let tx = db
            .create_transaction(&NewTransaction {
                date: date(2024, 5, 1),
                time: None,
                status: TransactionStatus::Completed,
                payee: Some("Shop".into()),
                narration: Some("Purchase".into()),
                tags: vec!["shopping".into()],
                links: vec!["receipt-1".into()],
                postings: vec![NewPosting {
                    account_id: account.id,
                    units: Amount {
                        value: dec!(-25),
                        commodity_id: usd.id,
                    },
                    cost: None,
                    price: None,
                    metadata: posting_metadata,
                }],
                metadata: HashMap::new(),
            })
            .unwrap();

        db.undo_last_audit_event().unwrap();
        assert!(db.get_transaction(tx.id).unwrap().is_none());

        db.redo_last_audit_event().unwrap();
        let restored = db.get_transaction(tx.id).unwrap().unwrap();
        assert_eq!(restored.tags, vec!["shopping"]);
        assert_eq!(restored.links, vec!["receipt-1"]);
        assert_eq!(
            restored.postings[0].metadata.get("source"),
            Some(&MetadataValue::String("card".into()))
        );
    }

    // Verifies that raw SQL queries work and return correct column
    // names and values.
    #[test]
    fn raw_query() {
        let mut db = setup();
        db.create_commodity(&NewCommodity {
            name: "USD".into(),
            precision: 2,
            metadata: HashMap::new(),
        })
        .unwrap();

        let result = db
            .query_raw("SELECT name, precision FROM commodities")
            .unwrap();
        assert_eq!(result.columns, vec!["name", "precision"]);
        assert_eq!(result.rows.len(), 1);
        match &result.rows[0][0] {
            QueryValue::Text(s) => assert_eq!(s, "USD"),
            _ => panic!("expected text"),
        }
    }

    // Verifies that raw SQL cannot mutate the database and bypass audit.
    #[test]
    fn raw_query_rejects_writes() {
        let db = setup();
        assert!(matches!(
            db.query_raw("INSERT INTO commodities (id, name, precision) VALUES (99, 'BAD', 2)"),
            Err(DbError::InvalidData(_))
        ));
    }
}
