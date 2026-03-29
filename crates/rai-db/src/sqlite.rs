use std::collections::HashMap;
use std::path::Path;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rusqlite::{params, Connection, Row};

use rai_core::types::*;

use crate::error::DbError;
use crate::provider::{QueryResult, QueryValue, StorageProvider};

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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_postings_transaction ON postings(transaction_id);
CREATE INDEX IF NOT EXISTS idx_postings_account ON postings(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_prices_commodity_date ON prices(commodity_id, date);
CREATE INDEX IF NOT EXISTS idx_balance_assertions_date ON balance_assertions(date);

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
    let sql = format!(
        "SELECT key, value_type, value FROM {table} WHERE {fk_column} = ?1"
    );
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

fn load_account_currencies(conn: &Connection, account_id: i64) -> Result<Vec<CommodityId>, DbError> {
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
    let mut stmt = conn.prepare(
        "INSERT INTO account_currencies (account_id, commodity_id) VALUES (?1, ?2)",
    )?;
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
        let (id, txn_id, acct_id, amount_str, commodity_id, cost_amount_str, cost_commodity_id, cost_date_str, cost_label, price_amount_str, price_commodity_id) = r?;

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
    let mut stmt = conn
        .prepare("SELECT link FROM transaction_links WHERE transaction_id = ?1 ORDER BY link")?;
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
        Ok(())
    }

    // ── Commodities ──────────────────────────────────────────────────

    fn create_commodity(&mut self, commodity: &NewCommodity) -> Result<Commodity, DbError> {
        self.conn.execute(
            "INSERT INTO commodities (name, precision) VALUES (?1, ?2)",
            params![commodity.name, commodity.precision],
        )?;
        let id = self.conn.last_insert_rowid();
        save_metadata(
            &self.conn,
            "commodity_metadata",
            "commodity_id",
            id,
            &commodity.metadata,
        )?;
        load_commodity_full(&self.conn, id)?
            .ok_or_else(|| DbError::NotFound("commodity just created".into()))
    }

    fn get_commodity(&self, id: CommodityId) -> Result<Option<Commodity>, DbError> {
        load_commodity_full(&self.conn, id.0)
    }

    fn get_commodity_by_name(&self, name: &str) -> Result<Option<Commodity>, DbError> {
        let mut stmt =
            self.conn
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
        // Verify exists
        if load_commodity_full(&self.conn, id.0)?.is_none() {
            return Err(DbError::NotFound(format!("commodity {}", id.0)));
        }
        if let Some(precision) = update.precision {
            self.conn.execute(
                "UPDATE commodities SET precision = ?1 WHERE id = ?2",
                params![precision, id.0],
            )?;
        }
        load_commodity_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("commodity {}", id.0)))
    }

    fn delete_commodity(&mut self, id: CommodityId) -> Result<(), DbError> {
        self.conn
            .execute("DELETE FROM commodity_metadata WHERE commodity_id = ?1", params![id.0])?;
        let affected = self
            .conn
            .execute("DELETE FROM commodities WHERE id = ?1", params![id.0])?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("commodity {}", id.0)));
        }
        Ok(())
    }

    // ── Accounts ─────────────────────────────────────────────────────

    fn create_account(&mut self, account: &NewAccount) -> Result<Account, DbError> {
        let account_type = account
            .account_type()
            .ok_or_else(|| {
                DbError::InvalidData(format!(
                    "cannot derive account type from name '{}'",
                    account.name
                ))
            })?;

        self.conn.execute(
            "INSERT INTO accounts (name, account_type, is_open, opened_at, booking_method) \
             VALUES (?1, ?2, 1, ?3, ?4)",
            params![
                account.name,
                account_type.as_str(),
                account.opened_at.format("%Y-%m-%d").to_string(),
                account.booking_method.as_str(),
            ],
        )?;
        let id = self.conn.last_insert_rowid();
        save_account_currencies(&self.conn, id, &account.currencies)?;
        save_metadata(
            &self.conn,
            "account_metadata",
            "account_id",
            id,
            &account.metadata,
        )?;
        load_account_full(&self.conn, id)?
            .ok_or_else(|| DbError::NotFound("account just created".into()))
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
        if load_account_full(&self.conn, id.0)?.is_none() {
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
        load_account_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("account {}", id.0)))
    }

    fn open_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account, DbError> {
        let affected = self.conn.execute(
            "UPDATE accounts SET is_open = 1, opened_at = ?1, closed_at = NULL WHERE id = ?2",
            params![date.format("%Y-%m-%d").to_string(), id.0],
        )?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("account {}", id.0)));
        }
        load_account_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("account {}", id.0)))
    }

    fn close_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account, DbError> {
        let affected = self.conn.execute(
            "UPDATE accounts SET is_open = 0, closed_at = ?1 WHERE id = ?2",
            params![date.format("%Y-%m-%d").to_string(), id.0],
        )?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("account {}", id.0)));
        }
        load_account_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("account {}", id.0)))
    }

    fn delete_account(&mut self, id: AccountId) -> Result<(), DbError> {
        self.conn
            .execute("DELETE FROM account_metadata WHERE account_id = ?1", params![id.0])?;
        self.conn
            .execute("DELETE FROM account_currencies WHERE account_id = ?1", params![id.0])?;
        let affected = self
            .conn
            .execute("DELETE FROM accounts WHERE id = ?1", params![id.0])?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("account {}", id.0)));
        }
        Ok(())
    }

    // ── Transactions ─────────────────────────────────────────────────

    fn create_transaction(&mut self, tx: &NewTransaction) -> Result<Transaction, DbError> {
        let db_tx = self.conn.transaction()?;

        let date_str = tx.date.format("%Y-%m-%d").to_string();
        let time_str = tx.time.map(|t| t.format("%H:%M:%S").to_string());

        db_tx.execute(
            "INSERT INTO transactions (date, time, status, payee, narration) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![date_str, time_str, tx.status.as_str(), tx.payee, tx.narration],
        )?;
        let txn_id = db_tx.last_insert_rowid();

        // Postings
        {
            let mut stmt = db_tx.prepare(
                "INSERT INTO postings (transaction_id, account_id, amount, commodity_id, \
                 cost_amount, cost_commodity_id, cost_date, cost_label, \
                 price_amount, price_commodity_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?;
            for p in &tx.postings {
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

                let posting_id = db_tx.last_insert_rowid();
                if !p.metadata.is_empty() {
                    save_metadata(
                        &db_tx,
                        "posting_metadata",
                        "posting_id",
                        posting_id,
                        &p.metadata,
                    )?;
                }
            }
        }

        // Tags
        if !tx.tags.is_empty() {
            let mut stmt = db_tx.prepare(
                "INSERT INTO transaction_tags (transaction_id, tag) VALUES (?1, ?2)",
            )?;
            for tag in &tx.tags {
                stmt.execute(params![txn_id, tag])?;
            }
        }

        // Links
        if !tx.links.is_empty() {
            let mut stmt = db_tx.prepare(
                "INSERT INTO transaction_links (transaction_id, link) VALUES (?1, ?2)",
            )?;
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

        load_transaction_full(&self.conn, txn_id)?
            .ok_or_else(|| DbError::NotFound("transaction just created".into()))
    }

    fn get_transaction(&self, id: TransactionId) -> Result<Option<Transaction>, DbError> {
        load_transaction_full(&self.conn, id.0)
    }

    fn list_transactions(
        &self,
        filter: &TransactionFilter,
    ) -> Result<Vec<Transaction>, DbError> {
        let mut sql = String::from(
            "SELECT DISTINCT t.id FROM transactions t",
        );
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
        if load_transaction_full(&self.conn, id.0)?.is_none() {
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

        load_transaction_full(&self.conn, id.0)?
            .ok_or_else(|| DbError::NotFound(format!("transaction {}", id.0)))
    }

    fn delete_transaction(&mut self, id: TransactionId) -> Result<(), DbError> {
        // Cascade handles postings, tags, links, metadata for tx and postings
        // But we need to manually delete posting_metadata since cascades are on postings
        let posting_ids: Vec<i64> = {
            let mut stmt = self
                .conn
                .prepare("SELECT id FROM postings WHERE transaction_id = ?1")?;
            let rows = stmt.query_map(params![id.0], |row| row.get::<_, i64>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for pid in posting_ids {
            self.conn
                .execute("DELETE FROM posting_metadata WHERE posting_id = ?1", params![pid])?;
        }
        self.conn
            .execute("DELETE FROM transaction_metadata WHERE transaction_id = ?1", params![id.0])?;
        self.conn
            .execute("DELETE FROM transaction_tags WHERE transaction_id = ?1", params![id.0])?;
        self.conn
            .execute("DELETE FROM transaction_links WHERE transaction_id = ?1", params![id.0])?;
        self.conn
            .execute("DELETE FROM postings WHERE transaction_id = ?1", params![id.0])?;
        let affected = self
            .conn
            .execute("DELETE FROM transactions WHERE id = ?1", params![id.0])?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("transaction {}", id.0)));
        }
        Ok(())
    }

    // ── Prices ───────────────────────────────────────────────────────

    fn create_price(&mut self, price: &NewPrice) -> Result<Price, DbError> {
        self.conn.execute(
            "INSERT INTO prices (date, commodity_id, target_commodity_id, value) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                price.date.format("%Y-%m-%d").to_string(),
                price.commodity_id.0,
                price.target_commodity_id.0,
                price.value.to_string(),
            ],
        )?;
        let id = self.conn.last_insert_rowid();
        Ok(Price {
            id: PriceId(id),
            date: price.date,
            commodity_id: price.commodity_id,
            target_commodity_id: price.target_commodity_id,
            value: price.value,
        })
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
        let affected = self
            .conn
            .execute("DELETE FROM prices WHERE id = ?1", params![id.0])?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("price {}", id.0)));
        }
        Ok(())
    }

    // ── Balance assertions ───────────────────────────────────────────

    fn create_balance_assertion(
        &mut self,
        assertion: &NewBalanceAssertion,
    ) -> Result<BalanceAssertion, DbError> {
        self.conn.execute(
            "INSERT INTO balance_assertions (date, account_id, amount, commodity_id) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                assertion.date.format("%Y-%m-%d").to_string(),
                assertion.account_id.0,
                assertion.expected.value.to_string(),
                assertion.expected.commodity_id.0,
            ],
        )?;
        let id = self.conn.last_insert_rowid();
        Ok(BalanceAssertion {
            id: BalanceAssertionId(id),
            date: assertion.date,
            account_id: assertion.account_id,
            expected: Amount {
                value: assertion.expected.value,
                commodity_id: assertion.expected.commodity_id,
            },
        })
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
        let affected = self.conn.execute(
            "DELETE FROM balance_assertions WHERE id = ?1",
            params![id.0],
        )?;
        if affected == 0 {
            return Err(DbError::NotFound(format!("balance assertion {}", id.0)));
        }
        Ok(())
    }

    // ── Raw query ────────────────────────────────────────────────────

    fn query_raw(&self, sql: &str) -> Result<QueryResult, DbError> {
        let mut stmt = self.conn.prepare(sql)?;
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
}
