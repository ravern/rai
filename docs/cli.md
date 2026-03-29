# rai CLI Reference

rai is a double-entry accounting system with a SQLite backend. All data is managed through CLI commands and can be queried directly with SQL.

## Global Options

```
rai [--profile <name>] <command> [args...]
```

| Option              | Description                                                         |
|---------------------|---------------------------------------------------------------------|
| `--profile <name>`  | Use a specific profile instead of the default. Applies to all commands except `profile`. |

If `--profile` is omitted, the default profile from `~/.config/rai/config.toml` is used.

---

## profile -- Manage profiles

A profile is a named database. Each profile has its own independent ledger stored at `~/.local/share/rai/<name>.db` by default.

### profile create

Create a new profile and initialize its database.

```
rai profile create <name>
```

| Argument | Description         |
|----------|---------------------|
| `name`   | Name for the new profile |

If no default profile is set, the newly created profile becomes the default.

**Example:**
```
rai profile create personal
rai profile create business
```

### profile list

List all configured profiles. The default profile is marked with `*`.

```
rai profile list
```

**Example output:**
```
business (/home/user/.local/share/rai/business.db)
personal * (/home/user/.local/share/rai/personal.db)
```

### profile delete

Delete a profile and remove its database file.

```
rai profile delete <name>
```

### profile default

Set the default profile.

```
rai profile default <name>
```

---

## commodity -- Manage commodities

Commodities represent currencies, securities, cryptocurrencies, or any trackable unit. Every posting references a commodity.

### commodity create

```
rai commodity create <name> --precision <n>
```

| Argument/Flag      | Description                                      |
|---------------------|--------------------------------------------------|
| `name`              | Unique commodity identifier (e.g. USD, BTC, AAPL) |
| `--precision <n>`   | Number of decimal places (e.g. 2 for USD, 8 for BTC) |

**Example:**
```
rai commodity create USD --precision 2
rai commodity create BTC --precision 8
rai commodity create AAPL --precision 2
```

### commodity list

```
rai commodity list
```

Displays a table of all commodities with their ID, name, and precision.

### commodity show

```
rai commodity show <name>
```

Shows detailed information about a commodity including its metadata.

### commodity update

```
rai commodity update <name> [--precision <n>]
```

### commodity delete

```
rai commodity delete <name>
```

Fails if the commodity is still referenced by postings or accounts.

---

## account -- Manage accounts

Accounts follow a hierarchical colon-separated naming convention. The first segment determines the account type: `Assets`, `Liabilities`, `Income`, `Expenses`, or `Equity`.

### account create

```
rai account create <name> [--booking-method <method>] [--currency <commodity>...] [--date <YYYY-MM-DD>]
```

| Argument/Flag               | Description                                              |
|-----------------------------|----------------------------------------------------------|
| `name`                      | Hierarchical account name (e.g. Assets:Bank:Checking)    |
| `--booking-method <method>` | Lot booking method (default: `strict`). Options: `strict`, `fifo`, `lifo`, `hifo`, `average`, `none`, `strict_with_size` |
| `--currency <commodity>`    | Restrict account to specific commodities (repeatable)    |
| `--date <YYYY-MM-DD>`       | Open date (defaults to today)                            |

**Examples:**
```
rai account create Assets:Bank:Checking --currency USD
rai account create Expenses:Food
rai account create Assets:Investments --booking-method fifo --currency AAPL --currency BTC
```

### account list

```
rai account list [--type <type>] [--open] [--closed]
```

| Flag              | Description                                             |
|-------------------|---------------------------------------------------------|
| `--type <type>`   | Filter by type: assets, liabilities, income, expenses, equity |
| `--open`          | Show only open accounts                                 |
| `--closed`        | Show only closed accounts                               |

**Examples:**
```
rai account list
rai account list --type assets --open
rai account list --closed
```

### account show

```
rai account show <name>
```

Shows full details: type, status, open/close dates, booking method, currencies, metadata.

### account open

Re-open a previously closed account.

```
rai account open <name> [--date <YYYY-MM-DD>]
```

### account close

Close an account. The account must have a zero balance.

```
rai account close <name> [--date <YYYY-MM-DD>]
```

### account delete

```
rai account delete <name>
```

---

## tx -- Manage transactions

Transactions consist of a header (date, payee, narration, status) and two or more postings that must balance.

### tx create

```
rai tx create --date <YYYY-MM-DD> [options] --posting <spec> --posting <spec> [--posting <spec>...]
```

| Flag                     | Description                                              |
|--------------------------|----------------------------------------------------------|
| `--date <YYYY-MM-DD>`    | Transaction date (required)                              |
| `--time <HH:MM:SS>`      | Transaction time (optional)                              |
| `--payee <text>`          | Payee name                                               |
| `--narration <text>`      | Description                                              |
| `--status <status>`       | `completed` (default), `pending`, or `flagged`           |
| `--tag <tag>`             | Add a tag (repeatable)                                   |
| `--link <link>`           | Add a link (repeatable)                                  |
| `--posting <spec>`        | Posting specification (required, at least 2, see below)  |

**Posting format:**

```
"Account:Name amount commodity"
"Account:Name amount commodity cost:per_unit_cost cost_commodity cost_date"
"Account:Name amount commodity price:per_unit_price price_commodity"
"Account:Name"    # amount inferred (only one posting may omit the amount)
```

**Examples:**

Simple expense:
```
rai tx create --date 2025-03-15 --payee "Grocery Store" --narration "Weekly groceries" \
    --posting "Expenses:Food 50.00 USD" \
    --posting "Assets:Bank:Checking"
```

The second posting's amount (-50.00 USD) is automatically inferred.

With cost basis (buying stock):
```
rai tx create --date 2025-03-15 --payee "Broker" \
    --posting "Assets:Investments 10 AAPL cost:150.00 USD 2025-03-15" \
    --posting "Assets:Bank:Checking -1500.00 USD"
```

With price annotation (currency exchange):
```
rai tx create --date 2025-03-15 --narration "Currency exchange" \
    --posting "Assets:EUR 1000.00 EUR price:1.08 USD" \
    --posting "Assets:Bank:Checking -1080.00 USD"
```

With tags and links:
```
rai tx create --date 2025-03-15 --payee "Hotel" --narration "Business trip" \
    --tag travel --tag reimbursable --link trip-2025-03 \
    --posting "Expenses:Travel 200.00 USD" \
    --posting "Assets:Bank:Checking"
```

### tx list

```
rai tx list [--from <date>] [--to <date>] [--account <name>] [--payee <text>] [--tag <tag>] [--status <status>]
```

| Flag                 | Description                           |
|----------------------|---------------------------------------|
| `--from <date>`      | Start date (inclusive)                |
| `--to <date>`        | End date (inclusive)                  |
| `--account <name>`   | Filter to transactions touching this account |
| `--payee <text>`     | Filter by payee                       |
| `--tag <tag>`        | Filter by tag                         |
| `--status <status>`  | Filter by status                      |

**Examples:**
```
rai tx list --from 2025-01-01 --to 2025-03-31
rai tx list --account Assets:Bank:Checking
rai tx list --tag vacation --status pending
```

### tx show

```
rai tx show <id>
```

Displays full transaction details including all postings, tags, links, and metadata.

### tx update

```
rai tx update <id> [--date <date>] [--payee <text>] [--narration <text>] [--status <status>]
```

Updates transaction header fields. Does not modify postings.

### tx delete

```
rai tx delete <id>
```

Deletes a transaction and all its postings (cascade).

---

## price -- Manage prices

Prices record exchange rates between commodities on specific dates. They are used by reports for currency conversion.

### price create

```
rai price create <commodity> <amount> <target-commodity> --date <YYYY-MM-DD>
```

Records that 1 unit of `<commodity>` equals `<amount>` units of `<target-commodity>` on the given date.

| Argument              | Description                    |
|-----------------------|--------------------------------|
| `commodity`           | Source commodity name           |
| `amount`              | Exchange rate value             |
| `target-commodity`    | Target commodity name           |
| `--date <YYYY-MM-DD>` | Price date (required)          |

**Examples:**
```
rai price create BTC 65000.00 USD --date 2025-03-15
rai price create EUR 1.08 USD --date 2025-03-15
rai price create AAPL 172.50 USD --date 2025-03-15
```

### price list

```
rai price list [--commodity <name>] [--from <date>] [--to <date>]
```

### price delete

```
rai price delete <id>
```

---

## balance -- Manage balance assertions

Balance assertions declare what an account's balance should be on a specific date. They are verified by `rai validate`.

### balance assert

```
rai balance assert <account> <amount> <commodity> --date <YYYY-MM-DD>
```

| Argument              | Description                       |
|-----------------------|-----------------------------------|
| `account`             | Account name                      |
| `amount`              | Expected balance                  |
| `commodity`           | Commodity name                    |
| `--date <YYYY-MM-DD>` | Date of the assertion (required) |

**Example:**
```
rai balance assert Assets:Bank:Checking 5432.10 USD --date 2025-03-31
```

### balance list

```
rai balance list [--account <name>]
```

### balance delete

```
rai balance delete <id>
```

---

## validate -- Run validation checks

```
rai validate
```

Runs all validation checks on the ledger data:

- Transaction balancing (all postings sum to zero)
- Balance assertion verification
- Account currency constraints
- Date ordering consistency

Reports all errors found or prints a success message.

**Example:**
```
rai validate
```

---

## report -- Generate financial reports

### report balance-sheet

```
rai report balance-sheet [--as-of <YYYY-MM-DD>] [--currency <commodity>]
```

Shows assets, liabilities, and equity as of a given date (defaults to today).

| Flag                  | Description                                  |
|-----------------------|----------------------------------------------|
| `--as-of <date>`      | Report date (defaults to today)              |
| `--currency <name>`   | Convert all amounts to this commodity        |

### report income-statement

```
rai report income-statement [--from <date>] [--to <date>] [--currency <commodity>]
```

Shows income and expenses for a period, plus net income.

### report trial-balance

```
rai report trial-balance [--as-of <YYYY-MM-DD>]
```

Shows debits, credits, and net balance for every account.

### report journal

```
rai report journal [--from <date>] [--to <date>] [--account <name>]
```

Prints a chronological journal of transactions, optionally filtered by date range and account.

### report trend

```
rai report trend [--account <name>] [--from <date>] [--to <date>]
```

Shows a monthly balance trend with sparkline and bar chart visualization.

| Flag              | Description                        |
|-------------------|------------------------------------|
| `--account <name>` | Show trend for this account       |
| `--from <date>`    | Start date (YYYY-MM-DD)           |
| `--to <date>`      | End date (YYYY-MM-DD)             |

**Example:**
```
rai report trend --account Assets:Bank:Checking --from 2025-01-01
```

---

## query -- Run SQL queries

```
rai query                  # enter interactive SQL REPL
rai query "<sql>"          # run a single SQL query
```

Executes SQL directly against the underlying SQLite database. Results are displayed as formatted tables.

**Interactive REPL commands:**
- Type SQL and press Enter to execute
- `.quit` or Ctrl-D to exit

**Examples:**
```
rai query "SELECT * FROM v_account_balances"
rai query "SELECT date, payee, narration FROM v_journal WHERE account = 'Expenses:Food' ORDER BY date DESC LIMIT 10"
```

See `docs/sql-recipes.md` for a collection of useful queries.

---

## Tips

### Posting amount inference

When creating a transaction, exactly one posting may omit its amount. rai will calculate the balancing amount automatically:

```
rai tx create --date 2025-03-15 --payee "Cafe" \
    --posting "Expenses:Food 4.50 USD" \
    --posting "Assets:Cash"
# Assets:Cash will automatically get -4.50 USD
```

This only works when the remaining postings involve a single commodity. Multi-currency transactions require all amounts to be specified explicitly.

### Multi-currency setup

1. Create both commodities:
   ```
   rai commodity create USD --precision 2
   rai commodity create EUR --precision 2
   ```

2. Record prices so reports can convert between currencies:
   ```
   rai price create EUR 1.08 USD --date 2025-03-15
   ```

3. Use price annotations on postings for currency exchanges:
   ```
   rai tx create --date 2025-03-15 \
       --posting "Assets:EUR 1000.00 EUR price:1.08 USD" \
       --posting "Assets:USD -1080.00 USD"
   ```

4. Generate reports in a single currency:
   ```
   rai report balance-sheet --currency USD
   ```

### Using profiles for separate books

Keep personal and business accounting completely separate:

```
rai profile create personal
rai profile create business
rai profile default personal

# These use the personal profile (the default):
rai commodity create USD --precision 2
rai account create Assets:Bank:Checking

# Use --profile to target a different profile:
rai --profile business commodity create USD --precision 2
rai --profile business account create Assets:Bank:Business
```

### Configuration file

rai stores its configuration at `~/.config/rai/config.toml`:

```toml
default_profile = "personal"

[profiles.personal]
# path is optional; defaults to ~/.local/share/rai/personal.db
# path = "/custom/path/to/personal.db"

[profiles.business]
```
