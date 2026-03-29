# rai SQL Query Recipes

Practical SQL queries for use with `rai query "<sql>"` or in the interactive REPL (`rai query`).

Amounts in rai are stored as TEXT for exact decimal precision. Use `CAST(amount AS REAL)` when you need arithmetic. The helper views `v_journal` and `v_account_balances` handle common joins for you.

---

## Account Balances

### Current balance for all accounts

```sql
SELECT account, commodity, balance
FROM v_account_balances
ORDER BY account_type, account;
```

Shows every account's current balance grouped by commodity.

### Balance for a specific account

```sql
SELECT commodity, balance
FROM v_account_balances
WHERE account = 'Assets:Bank:Checking';
```

### Balance at a point in time (before a date)

```sql
SELECT
    a.name AS account,
    c.name AS commodity,
    SUM(CAST(p.amount AS REAL)) AS balance
FROM postings p
JOIN transactions t ON p.transaction_id = t.id
JOIN accounts a ON p.account_id = a.id
JOIN commodities c ON p.commodity_id = c.id
WHERE t.date < '2025-01-01'
GROUP BY a.id, p.commodity_id
ORDER BY a.name;
```

Returns balances as of all transactions before the given date.

### Multi-currency account balances

```sql
SELECT account, commodity, balance
FROM v_account_balances
WHERE account = 'Assets:Investments'
ORDER BY commodity;
```

Accounts that hold multiple commodities will have one row per commodity.

---

## Transaction Queries

### Recent transactions

```sql
SELECT DISTINCT transaction_id, date, status, payee, narration
FROM v_journal
ORDER BY date DESC, transaction_id DESC
LIMIT 20;
```

### Transactions by payee

```sql
SELECT DISTINCT transaction_id, date, payee, narration
FROM v_journal
WHERE payee LIKE '%Grocery%'
ORDER BY date DESC;
```

Use `LIKE` for partial matching or `=` for exact matches.

### Transactions in a date range

```sql
SELECT DISTINCT transaction_id, date, payee, narration
FROM v_journal
WHERE date >= '2025-01-01' AND date <= '2025-03-31'
ORDER BY date;
```

### Transactions affecting a specific account

```sql
SELECT transaction_id, date, payee, narration, amount, commodity
FROM v_journal
WHERE account = 'Assets:Bank:Checking'
ORDER BY date DESC;
```

### Monthly spending by category (expense accounts)

```sql
SELECT
    SUBSTR(date, 1, 7) AS month,
    account,
    commodity,
    SUM(CAST(amount AS REAL)) AS total
FROM v_journal
WHERE account_type = 'expenses'
GROUP BY month, account, commodity
ORDER BY month DESC, total DESC;
```

---

## Analysis

### Monthly income vs expenses

```sql
SELECT
    SUBSTR(t.date, 1, 7) AS month,
    SUM(CASE WHEN a.account_type = 'income' THEN -CAST(p.amount AS REAL) ELSE 0 END) AS income,
    SUM(CASE WHEN a.account_type = 'expenses' THEN CAST(p.amount AS REAL) ELSE 0 END) AS expenses,
    SUM(CASE WHEN a.account_type = 'income' THEN -CAST(p.amount AS REAL) ELSE 0 END)
    - SUM(CASE WHEN a.account_type = 'expenses' THEN CAST(p.amount AS REAL) ELSE 0 END) AS savings
FROM postings p
JOIN transactions t ON p.transaction_id = t.id
JOIN accounts a ON p.account_id = a.id
GROUP BY month
ORDER BY month;
```

Income is negated because income postings are negative by accounting convention.

### Running balance over time for an account

```sql
SELECT
    t.date,
    t.payee,
    CAST(p.amount AS REAL) AS change,
    SUM(CAST(p.amount AS REAL)) OVER (ORDER BY t.date, t.id) AS running_balance
FROM postings p
JOIN transactions t ON p.transaction_id = t.id
JOIN accounts a ON p.account_id = a.id
WHERE a.name = 'Assets:Bank:Checking'
ORDER BY t.date, t.id;
```

Uses a window function to compute a cumulative running balance.

### Top expense categories

```sql
SELECT
    account,
    commodity,
    SUM(CAST(amount AS REAL)) AS total
FROM v_journal
WHERE account_type = 'expenses'
GROUP BY account, commodity
ORDER BY total DESC
LIMIT 10;
```

### Average monthly spending per category

```sql
SELECT
    account,
    commodity,
    ROUND(SUM(CAST(amount AS REAL)) / COUNT(DISTINCT SUBSTR(date, 1, 7)), 2) AS avg_monthly
FROM v_journal
WHERE account_type = 'expenses'
GROUP BY account, commodity
ORDER BY avg_monthly DESC;
```

Divides total spending by the number of distinct months with activity.

### Net worth over time (assets minus liabilities)

```sql
SELECT
    SUBSTR(t.date, 1, 7) AS month,
    SUM(CASE
        WHEN a.account_type = 'assets' THEN CAST(p.amount AS REAL)
        WHEN a.account_type = 'liabilities' THEN CAST(p.amount AS REAL)
        ELSE 0
    END) AS net_worth_change
FROM postings p
JOIN transactions t ON p.transaction_id = t.id
JOIN accounts a ON p.account_id = a.id
WHERE a.account_type IN ('assets', 'liabilities')
GROUP BY month
ORDER BY month;
```

Shows the change in net worth each month. For cumulative net worth, wrap with a window function:

```sql
SELECT
    month,
    SUM(net_worth_change) OVER (ORDER BY month) AS cumulative_net_worth
FROM (
    SELECT
        SUBSTR(t.date, 1, 7) AS month,
        SUM(CASE
            WHEN a.account_type = 'assets' THEN CAST(p.amount AS REAL)
            WHEN a.account_type = 'liabilities' THEN CAST(p.amount AS REAL)
            ELSE 0
        END) AS net_worth_change
    FROM postings p
    JOIN transactions t ON p.transaction_id = t.id
    JOIN accounts a ON p.account_id = a.id
    WHERE a.account_type IN ('assets', 'liabilities')
    GROUP BY month
)
ORDER BY month;
```

### Year-over-year comparison

```sql
SELECT
    account,
    commodity,
    SUM(CASE WHEN SUBSTR(date, 1, 4) = '2024' THEN CAST(amount AS REAL) ELSE 0 END) AS year_2024,
    SUM(CASE WHEN SUBSTR(date, 1, 4) = '2025' THEN CAST(amount AS REAL) ELSE 0 END) AS year_2025,
    SUM(CASE WHEN SUBSTR(date, 1, 4) = '2025' THEN CAST(amount AS REAL) ELSE 0 END)
    - SUM(CASE WHEN SUBSTR(date, 1, 4) = '2024' THEN CAST(amount AS REAL) ELSE 0 END) AS change
FROM v_journal
WHERE account_type = 'expenses'
GROUP BY account, commodity
HAVING year_2024 > 0 OR year_2025 > 0
ORDER BY change DESC;
```

Compares expense totals between two years. Adjust the year values as needed.

---

## Price Queries

### Latest price for a commodity

```sql
SELECT p.date, p.value, c2.name AS target_currency
FROM prices p
JOIN commodities c1 ON p.commodity_id = c1.id
JOIN commodities c2 ON p.target_commodity_id = c2.id
WHERE c1.name = 'BTC'
ORDER BY p.date DESC
LIMIT 1;
```

### Price history for a commodity pair

```sql
SELECT p.date, p.value
FROM prices p
JOIN commodities c1 ON p.commodity_id = c1.id
JOIN commodities c2 ON p.target_commodity_id = c2.id
WHERE c1.name = 'AAPL' AND c2.name = 'USD'
ORDER BY p.date;
```

---

## Metadata

### Find transactions with specific metadata

```sql
SELECT t.*, tm.key, tm.value
FROM transactions t
JOIN transaction_metadata tm ON t.id = tm.transaction_id
WHERE tm.key = 'receipt' AND tm.value LIKE '%scan%';
```

### List all tags

```sql
SELECT DISTINCT tag FROM transaction_tags ORDER BY tag;
```

### Find transactions with a specific tag

```sql
SELECT t.date, t.payee, t.narration
FROM transactions t
JOIN transaction_tags tt ON t.id = tt.transaction_id
WHERE tt.tag = 'reimbursable'
ORDER BY t.date;
```

### List all linked transaction groups

```sql
SELECT link, COUNT(*) AS tx_count
FROM transaction_links
GROUP BY link
ORDER BY link;
```

### List all metadata keys in use

```sql
SELECT 'transaction' AS entity, key, COUNT(*) AS usage_count
FROM transaction_metadata GROUP BY key
UNION ALL
SELECT 'posting', key, COUNT(*) FROM posting_metadata GROUP BY key
UNION ALL
SELECT 'account', key, COUNT(*) FROM account_metadata GROUP BY key
UNION ALL
SELECT 'commodity', key, COUNT(*) FROM commodity_metadata GROUP BY key
ORDER BY entity, usage_count DESC;
```
