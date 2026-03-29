# rai — Database Schema

The schema is a **public, stable contract**. Users and AI agents query these tables directly via SQL.

## SQLite Schema

```sql
-- Commodities
CREATE TABLE commodities (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,   -- e.g., "USD", "AAPL"
    precision   INTEGER NOT NULL        -- decimal places
);

-- Accounts (chart of accounts)
CREATE TABLE accounts (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,    -- e.g., "Assets:Bank:Checking"
    account_type    TEXT NOT NULL,           -- "assets", "liabilities", "income", "expenses", "equity"
    is_open         INTEGER NOT NULL DEFAULT 1,
    opened_at       TEXT NOT NULL,           -- ISO date
    closed_at       TEXT,                    -- ISO date, NULL if open
    booking_method  TEXT NOT NULL DEFAULT 'strict',  -- "strict", "fifo", "lifo", "hifo", "average", "none", "strict_with_size"
    CHECK (account_type IN ('assets', 'liabilities', 'income', 'expenses', 'equity'))
);

-- Optional currency constraints on accounts
CREATE TABLE account_currencies (
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id),
    PRIMARY KEY (account_id, commodity_id)
);

-- Transactions
CREATE TABLE transactions (
    id          INTEGER PRIMARY KEY,
    date        TEXT NOT NULL,               -- ISO date
    time        TEXT,                        -- ISO time, nullable
    status      TEXT NOT NULL DEFAULT 'completed',  -- "completed", "pending", "flagged"
    payee       TEXT,
    narration   TEXT,
    CHECK (status IN ('completed', 'pending', 'flagged'))
);

-- Postings (legs of a transaction)
CREATE TABLE postings (
    id              INTEGER PRIMARY KEY,
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    amount          TEXT NOT NULL,           -- decimal string, exact
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id),
    -- cost fields (NULL if not held at cost)
    cost_amount     TEXT,                    -- per-unit cost, decimal string
    cost_commodity_id INTEGER REFERENCES commodities(id),
    cost_date       TEXT,                    -- acquisition date
    cost_label      TEXT,                    -- optional lot label
    -- price field (NULL if no conversion)
    price_amount    TEXT,                    -- per-unit price, decimal string
    price_commodity_id INTEGER REFERENCES commodities(id)
);

-- Tags
CREATE TABLE transaction_tags (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    tag             TEXT NOT NULL,
    PRIMARY KEY (transaction_id, tag)
);

-- Links
CREATE TABLE transaction_links (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    link            TEXT NOT NULL,
    PRIMARY KEY (transaction_id, link)
);

-- Metadata on transactions
CREATE TABLE transaction_metadata (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,           -- "string", "number", "date", "bool"
    value           TEXT NOT NULL,
    PRIMARY KEY (transaction_id, key)
);

-- Metadata on postings
CREATE TABLE posting_metadata (
    posting_id      INTEGER NOT NULL REFERENCES postings(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,
    value           TEXT NOT NULL,
    PRIMARY KEY (posting_id, key)
);

-- Metadata on accounts
CREATE TABLE account_metadata (
    account_id      INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,
    value           TEXT NOT NULL,
    PRIMARY KEY (account_id, key)
);

-- Metadata on commodities
CREATE TABLE commodity_metadata (
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value_type      TEXT NOT NULL,
    value           TEXT NOT NULL,
    PRIMARY KEY (commodity_id, key)
);

-- Prices (market/reference prices)
CREATE TABLE prices (
    id                  INTEGER PRIMARY KEY,
    date                TEXT NOT NULL,           -- ISO date
    commodity_id        INTEGER NOT NULL REFERENCES commodities(id),
    target_commodity_id INTEGER NOT NULL REFERENCES commodities(id),
    value               TEXT NOT NULL            -- decimal string
);

-- Balance assertions
CREATE TABLE balance_assertions (
    id              INTEGER PRIMARY KEY,
    date            TEXT NOT NULL,               -- ISO date
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    amount          TEXT NOT NULL,               -- expected balance, decimal string
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id)
);
```

## Indexes

```sql
CREATE INDEX idx_postings_transaction ON postings(transaction_id);
CREATE INDEX idx_postings_account ON postings(account_id);
CREATE INDEX idx_transactions_date ON transactions(date);
CREATE INDEX idx_prices_commodity_date ON prices(commodity_id, date);
CREATE INDEX idx_balance_assertions_date ON balance_assertions(date);
```

## Helper Views

Provided to make common queries easier in the REPL:

```sql
-- Posting with joined transaction and account info
CREATE VIEW v_journal AS
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

-- Account balances (simple, single-commodity per row)
CREATE VIEW v_account_balances AS
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
```

## Notes

- Decimal amounts are stored as TEXT strings to preserve exact precision. Queries that need arithmetic should CAST as needed.
- The schema is designed to be queried directly. Documentation will include common query recipes for humans and AI agents.
- No ORM abstraction — the storage provider trait maps directly to these tables.
