# rai — Overview

rai is a double-entry accounting system written in Rust, inspired by beancount. It stores data in a database (SQLite initially) instead of plain text files, eliminates the need for a custom query language by exposing a stable public schema, and is designed to be used both as a Rust library and through a CLI.

## Design Principles

- **Library-first.** The CLI is a thin wrapper around the library. All accounting logic lives in library crates.
- **Database-backed.** No text ledger files. SQLite for now, storage-provider-independent by design.
- **Public schema.** The database schema is a stable product surface. Users and AI agents query it directly via SQL.
- **Currency-independent.** All commodities (USD, AAPL, BTC, VACHR) are treated identically. No built-in base currency.
- **Weight-based balancing.** Same semantics as beancount: cost basis is the balancing weight for held-at-cost postings, price is the weight for simple conversions.
- **Compiler-style errors.** Collect all errors and report them together, rather than failing on the first one.

## What rai Is

- A Rust library (`rai-core`, `rai-db`, `rai-report`) for double-entry accounting
- A CLI tool (`rai-cli`, binary name `rai`) for humans and AI agents
- A multi-profile system where each profile is a separate SQLite database

## What rai Is Not (for now)

- Not a web application (future `rai-api` crate planned)
- Not a beancount importer
- Not extensible via plugins (may come later)
- No custom query language — use SQL directly

## Crate Structure

```
rai/
  rai-core/       # Domain types, accounting logic, validation
  rai-db/         # Storage provider trait + SQLite implementation
  rai-report/     # Report generation (balance sheet, income statement, etc.)
  rai-cli/        # CLI binary, thin wrapper around library crates
  rai-api/        # (future) HTTP/gRPC API
```

## Key Dependencies

- `clap` — CLI argument parsing
- `rusqlite` — SQLite access
- `rust_decimal` — fixed-point decimal arithmetic
- `chrono` — date and optional time handling
- `comfy-table` or `tabled` — terminal table rendering
- `thiserror` / `anyhow` — error handling
