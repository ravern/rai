# rai Database Schema Reference

rai stores all accounting data in a SQLite database. The schema is a public, stable contract -- users and AI agents can query these tables directly via SQL using `rai query`.

All decimal amounts are stored as TEXT strings to preserve exact precision. Queries that need arithmetic should use `CAST(amount AS REAL)` or SQLite's built-in numeric coercion.

## Tables

### commodities

Currencies, securities, and other tradable units.

| Column    | Type    | Description                                      |
|-----------|---------|--------------------------------------------------|
| id        | INTEGER | Primary key (auto-increment)                     |
| name      | TEXT    | Unique identifier, e.g. "USD", "BTC", "AAPL"    |
| precision | INTEGER | Number of decimal places for display and rounding |

**Constraints:** `name` is UNIQUE and NOT NULL.

```sql
-- Example: list all commodities
SELECT * FROM commodities;
```

---

### accounts

The chart of accounts. Every account belongs to one of five types, determined by the first segment of its colon-separated name.

| Column         | Type    | Description                                                                    |
|----------------|---------|--------------------------------------------------------------------------------|
| id             | INTEGER | Primary key (auto-increment)                                                   |
| name           | TEXT    | Unique hierarchical name, e.g. "Assets:Bank:Checking"                          |
| account_type   | TEXT    | One of: `assets`, `liabilities`, `income`, `expenses`, `equity`                |
| is_open        | INTEGER | 1 if the account is open, 0 if closed                                         |
| opened_at      | TEXT    | ISO date (YYYY-MM-DD) when the account was opened                              |
| closed_at      | TEXT    | ISO date when closed, or NULL if still open                                    |
| booking_method | TEXT    | Lot booking method: `strict`, `fifo`, `lifo`, `hifo`, `average`, `none`, `strict_with_size` |

**Constraints:** `name` is UNIQUE. `account_type` is checked against the five valid values.

```sql
-- Example: list all open asset accounts
SELECT * FROM accounts WHERE account_type = 'assets' AND is_open = 1;
```

---

### account_currencies

Optional constraints on which commodities an account may hold. If an account has no rows in this table, it may hold any commodity.

| Column       | Type    | Description                              |
|--------------|---------|------------------------------------------|
| account_id   | INTEGER | Foreign key to `accounts(id)`            |
| commodity_id | INTEGER | Foreign key to `commodities(id)`         |

**Primary key:** `(account_id, commodity_id)`

```sql
-- Example: find which currencies an account is restricted to
SELECT c.name
FROM account_currencies ac
JOIN commodities c ON ac.commodity_id = c.id
JOIN accounts a ON ac.account_id = a.id
WHERE a.name = 'Assets:Bank:Checking';
```

---

### transactions

Each transaction represents a financial event on a specific date. A transaction contains one or more postings that must balance.

| Column    | Type    | Description                                               |
|-----------|---------|-----------------------------------------------------------|
| id        | INTEGER | Primary key (auto-increment)                              |
| date      | TEXT    | ISO date (YYYY-MM-DD)                                     |
| time      | TEXT    | ISO time (HH:MM:SS), nullable                             |
| status    | TEXT    | One of: `completed`, `pending`, `flagged`                  |
| payee     | TEXT    | Who the transaction is with (nullable)                     |
| narration | TEXT    | Description of the transaction (nullable)                  |

**Constraints:** `status` is checked against the three valid values.

```sql
-- Example: find all pending transactions
SELECT * FROM transactions WHERE status = 'pending';
```

---

### postings

The individual legs of a transaction. Each posting moves an amount of a commodity into or out of an account. The sum of all posting weights in a transaction must be zero.

| Column             | Type    | Description                                            |
|--------------------|---------|--------------------------------------------------------|
| id                 | INTEGER | Primary key (auto-increment)                           |
| transaction_id     | INTEGER | Foreign key to `transactions(id)` (CASCADE delete)     |
| account_id         | INTEGER | Foreign key to `accounts(id)`                          |
| amount             | TEXT    | Decimal string, positive for debits, negative for credits |
| commodity_id       | INTEGER | Foreign key to `commodities(id)` -- the posting currency |
| cost_amount        | TEXT    | Per-unit cost (decimal string), NULL if not held at cost |
| cost_commodity_id  | INTEGER | Foreign key to `commodities(id)` for cost currency     |
| cost_date          | TEXT    | ISO date of acquisition for cost lots                   |
| cost_label         | TEXT    | Optional label to identify a specific lot               |
| price_amount       | TEXT    | Per-unit price for currency conversion (decimal string) |
| price_commodity_id | INTEGER | Foreign key to `commodities(id)` for price currency    |

**Key relationships:**
- `transaction_id` -> `transactions(id)` (ON DELETE CASCADE)
- `account_id` -> `accounts(id)`
- `commodity_id` -> `commodities(id)`
- `cost_commodity_id` -> `commodities(id)` (nullable)
- `price_commodity_id` -> `commodities(id)` (nullable)

```sql
-- Example: find all postings for a specific account
SELECT p.*, t.date, t.payee
FROM postings p
JOIN transactions t ON p.transaction_id = t.id
JOIN accounts a ON p.account_id = a.id
WHERE a.name = 'Expenses:Food';
```

---

### transaction_tags

Tags attached to transactions for categorization and filtering.

| Column         | Type    | Description                                        |
|----------------|---------|----------------------------------------------------|
| transaction_id | INTEGER | Foreign key to `transactions(id)` (CASCADE delete) |
| tag            | TEXT    | The tag string                                     |

**Primary key:** `(transaction_id, tag)`

```sql
-- Example: find transactions tagged "vacation"
SELECT t.*
FROM transactions t
JOIN transaction_tags tt ON t.id = tt.transaction_id
WHERE tt.tag = 'vacation';
```

---

### transaction_links

Links that connect related transactions (e.g. invoice and payment).

| Column         | Type    | Description                                        |
|----------------|---------|----------------------------------------------------|
| transaction_id | INTEGER | Foreign key to `transactions(id)` (CASCADE delete) |
| link           | TEXT    | The link identifier string                         |

**Primary key:** `(transaction_id, link)`

```sql
-- Example: find all transactions sharing a link
SELECT t.*
FROM transactions t
JOIN transaction_links tl ON t.id = tl.transaction_id
WHERE tl.link = 'invoice-2024-001';
```

---

### transaction_metadata

Arbitrary key-value metadata on transactions.

| Column         | Type    | Description                                        |
|----------------|---------|----------------------------------------------------|
| transaction_id | INTEGER | Foreign key to `transactions(id)` (CASCADE delete) |
| key            | TEXT    | Metadata key                                       |
| value_type     | TEXT    | Type hint: `string`, `number`, `date`, `bool`      |
| value          | TEXT    | The value as a string                              |

**Primary key:** `(transaction_id, key)`

---

### posting_metadata

Arbitrary key-value metadata on individual postings.

| Column     | Type    | Description                                     |
|------------|---------|-------------------------------------------------|
| posting_id | INTEGER | Foreign key to `postings(id)` (CASCADE delete)  |
| key        | TEXT    | Metadata key                                    |
| value_type | TEXT    | Type hint: `string`, `number`, `date`, `bool`   |
| value      | TEXT    | The value as a string                           |

**Primary key:** `(posting_id, key)`

---

### account_metadata

Arbitrary key-value metadata on accounts.

| Column     | Type    | Description                                     |
|------------|---------|-------------------------------------------------|
| account_id | INTEGER | Foreign key to `accounts(id)` (CASCADE delete)  |
| key        | TEXT    | Metadata key                                    |
| value_type | TEXT    | Type hint: `string`, `number`, `date`, `bool`   |
| value      | TEXT    | The value as a string                           |

**Primary key:** `(account_id, key)`

---

### commodity_metadata

Arbitrary key-value metadata on commodities.

| Column       | Type    | Description                                        |
|--------------|---------|----------------------------------------------------|
| commodity_id | INTEGER | Foreign key to `commodities(id)` (CASCADE delete)  |
| key          | TEXT    | Metadata key                                       |
| value_type   | TEXT    | Type hint: `string`, `number`, `date`, `bool`      |
| value        | TEXT    | The value as a string                              |

**Primary key:** `(commodity_id, key)`

---

### prices

Market or reference prices that establish exchange rates between commodities on a given date. Used for currency conversion in reports.

| Column              | Type    | Description                                     |
|---------------------|---------|-------------------------------------------------|
| id                  | INTEGER | Primary key (auto-increment)                    |
| date                | TEXT    | ISO date (YYYY-MM-DD)                           |
| commodity_id        | INTEGER | Foreign key to `commodities(id)` -- the source  |
| target_commodity_id | INTEGER | Foreign key to `commodities(id)` -- the target  |
| value               | TEXT    | Decimal string: 1 unit of source = value units of target |

```sql
-- Example: get the latest USD price for BTC
SELECT p.date, p.value, c2.name AS target
FROM prices p
JOIN commodities c1 ON p.commodity_id = c1.id
JOIN commodities c2 ON p.target_commodity_id = c2.id
WHERE c1.name = 'BTC' AND c2.name = 'USD'
ORDER BY p.date DESC
LIMIT 1;
```

---

### balance_assertions

Checkpoints that declare what an account's balance should be on a given date. Used by `rai validate` to catch data entry errors.

| Column       | Type    | Description                                    |
|--------------|---------|------------------------------------------------|
| id           | INTEGER | Primary key (auto-increment)                   |
| date         | TEXT    | ISO date (YYYY-MM-DD) of the assertion         |
| account_id   | INTEGER | Foreign key to `accounts(id)`                  |
| amount       | TEXT    | Expected balance (decimal string)              |
| commodity_id | INTEGER | Foreign key to `commodities(id)`               |

```sql
-- Example: list all balance assertions for an account
SELECT ba.date, ba.amount, c.name AS commodity
FROM balance_assertions ba
JOIN commodities c ON ba.commodity_id = c.id
JOIN accounts a ON ba.account_id = a.id
WHERE a.name = 'Assets:Bank:Checking'
ORDER BY ba.date;
```

---

## Indexes

The following indexes are created automatically for query performance:

| Index                           | Columns                    |
|---------------------------------|----------------------------|
| idx_postings_transaction        | postings(transaction_id)   |
| idx_postings_account            | postings(account_id)       |
| idx_transactions_date           | transactions(date)         |
| idx_prices_commodity_date       | prices(commodity_id, date) |
| idx_balance_assertions_date     | balance_assertions(date)   |

---

## Helper Views

These views are created automatically and simplify common queries.

### v_journal

A denormalized view joining postings with their parent transaction and account/commodity names. This is the most useful view for day-to-day querying.

| Column          | Source                      | Description                    |
|-----------------|-----------------------------|--------------------------------|
| transaction_id  | transactions.id             | Transaction ID                 |
| date            | transactions.date           | Transaction date               |
| time            | transactions.time           | Transaction time               |
| status          | transactions.status         | Transaction status             |
| payee           | transactions.payee          | Payee                          |
| narration       | transactions.narration      | Narration                      |
| posting_id      | postings.id                 | Posting ID                     |
| account         | accounts.name               | Account name                   |
| account_type    | accounts.account_type       | Account type                   |
| amount          | postings.amount             | Posting amount (decimal text)  |
| commodity       | commodities.name            | Commodity of the posting       |
| cost_amount     | postings.cost_amount        | Per-unit cost (if applicable)  |
| cost_commodity  | commodities.name (cost)     | Cost commodity name            |
| cost_date       | postings.cost_date          | Cost lot date                  |
| price_amount    | postings.price_amount       | Per-unit price (if applicable) |
| price_commodity | commodities.name (price)    | Price commodity name           |

Ordered by `date`, `time`, `transaction_id`, `posting_id`.

```sql
-- Example: last 10 journal entries
SELECT date, payee, account, amount, commodity
FROM v_journal
ORDER BY date DESC, transaction_id DESC
LIMIT 10;
```

### v_account_balances

Current balance for every account, broken down by commodity. Each row represents one (account, commodity) pair.

| Column       | Source              | Description                           |
|--------------|---------------------|---------------------------------------|
| account_id   | accounts.id         | Account ID                            |
| account      | accounts.name       | Account name                          |
| account_type | accounts.account_type | Account type                        |
| commodity    | commodities.name    | Commodity name                        |
| balance      | SUM(amount)         | Sum of all posting amounts (as REAL)  |

```sql
-- Example: check all balances
SELECT account, commodity, balance FROM v_account_balances ORDER BY account;
```
