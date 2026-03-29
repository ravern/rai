# rai — CLI (rai-cli)

## Binary Name

`rai`

## Global Flags

```
rai [--profile <name>] <noun> <verb> [args...]
```

If `--profile` is omitted, uses the default profile from `~/.config/rai/config.toml`.

## Command Structure

Noun-verb pattern. All commands output to terminal with formatted tables/charts. No emojis in output.

### Profile Management

```
rai profile list                           # list all profiles
rai profile create <name>                  # create a new profile (and its database)
rai profile delete <name>                  # delete a profile
rai profile default <name>                 # set the default profile
```

### Commodities

```
rai commodity create <name> --precision <n>
rai commodity list
rai commodity show <name>
rai commodity update <name> [--precision <n>]
rai commodity delete <name>
```

### Accounts

```
rai account create <name> [--booking-method <method>] [--currency <commodity>...]
rai account list [--type <type>] [--open | --closed]
rai account show <name>
rai account open <name> --date <date>       # set open date (default: today)
rai account close <name> --date <date>
rai account delete <name>
```

Account type is derived from the first segment of the name (e.g., `Assets:Bank:Checking` -> Assets).

### Transactions

```
rai tx create --date <date> [--time <time>] [--payee <payee>] [--narration <text>] \
    [--status <status>] [--tag <tag>...] [--link <link>...] \
    --posting <account> <amount> <commodity> [cost:<amount> <commodity> <date>] [price:<amount> <commodity>] \
    --posting <account> <amount> <commodity> \
    [--posting <account>]                   # one posting may omit amount (CLI infers it)

rai tx list [--from <date>] [--to <date>] [--account <name>] [--payee <text>] [--tag <tag>] [--status <status>]
rai tx show <id>
rai tx update <id> [--date <date>] [--payee <payee>] [--narration <text>] [--status <status>]
rai tx delete <id>
```

### Prices

```
rai price create <commodity> <amount> <target-commodity> --date <date>
rai price list [--commodity <name>] [--from <date>] [--to <date>]
rai price delete <id>
```

### Balance Assertions

```
rai balance assert <account> <amount> <commodity> --date <date>
rai balance list [--account <name>]
rai balance delete <id>
```

### Validation

```
rai validate                                # run full validation, report all errors
```

### Reports

```
rai report balance-sheet [--as-of <date>] [--currency <commodity>]
rai report income-statement [--from <date>] [--to <date>] [--currency <commodity>]
rai report trial-balance [--as-of <date>]
rai report journal [--from <date>] [--to <date>] [--account <name>]
```

### Query REPL

```
rai query                                   # enter interactive SQL REPL
rai query "<sql>"                           # run a single SQL query and print results
```

Passes SQL directly to the underlying database via `StorageProvider::query_raw`. Results are rendered as terminal tables.

## Output Formatting

- Tables: bordered Unicode tables (using comfy-table or tabled)
- Amounts: right-aligned, formatted to commodity precision, with commodity symbol
- Dates: ISO format (YYYY-MM-DD)
- Account trees: indented hierarchy for reports
- Sparklines/bar charts: for balance-over-time visualizations in reports (future enhancement)
- Colors: use terminal colors for positive/negative amounts (green/red), account types, errors

## Configuration

`~/.config/rai/config.toml`:

```toml
default_profile = "personal"

[profiles.personal]
# path is optional; defaults to ~/.local/share/rai/personal.db
# path = "/custom/path/to/personal.db"

[profiles.business]
# path = "/custom/path/to/business.db"
```

Profile databases default to `~/.local/share/rai/<name>.db`.
