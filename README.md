# rai

Double-entry accounting for the command line, backed by SQLite.

All data lives in a local database that you can query directly with SQL. Use profiles to maintain separate ledgers (personal, business, etc.).

## Crates

| Crate | Description |
|---|---|
| `rai-cli` | Command-line interface with profile support |
| `rai-core` | Types, balance computation, inventory, and validation |
| `rai-db` | SQLite storage provider |
| `rai-report` | Balance sheet, income statement, trial balance, journal, and trends |

## Quick start

```sh
# Create a profile and set up commodities
rai profile create personal
rai commodity create USD --precision 2

# Open some accounts
rai account create Assets:Bank:Checking --currency USD
rai account create Expenses:Food

# Record a transaction
rai tx create --date 2025-03-15 --payee "Grocery Store" --narration "Weekly groceries" \
    --posting "Expenses:Food 50.00 USD" \
    --posting "Assets:Bank:Checking"

# Check your balances
rai report balance-sheet

# Query the database directly
rai query "SELECT * FROM v_account_balances"
```

## Building

```sh
cargo build --release
```

## Documentation

- [CLI reference](docs/cli.md)
- [Database schema](docs/schema.md)
- [SQL recipes](docs/sql-recipes.md)
