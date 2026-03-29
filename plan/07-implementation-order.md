# rai — Implementation Order

Build bottom-up, with each phase producing a usable, testable layer.

## Phase 1: Foundation

**Goal:** Domain types, storage, and basic CRUD.

1. **rai-core: domain types**
   - All structs and enums from the data model
   - Amount arithmetic (add, subtract, multiply by scalar, negate)
   - No logic yet, just types

2. **rai-db: SQLite provider**
   - Schema creation (`initialize`)
   - CRUD for commodities, accounts
   - CRUD for transactions with postings
   - CRUD for prices, balance assertions
   - Metadata, tags, links
   - `query_raw` passthrough
   - Unit tests with in-memory SQLite

3. **rai-cli: skeleton**
   - Profile management (create, list, delete, default)
   - Config file read/write
   - Commodity and account CRUD commands
   - Transaction CRUD commands
   - Price and balance assertion commands
   - Query REPL (interactive + one-shot)
   - Basic table output

## Phase 2: Accounting Logic

**Goal:** The system actually does accounting.

4. **rai-core: weight and balancing**
   - `compute_weight` for postings
   - `check_transaction_balance`
   - Unit tests with hand-crafted transactions

5. **rai-core: inventory and lot booking**
   - Inventory type and operations
   - Booking methods: FIFO, LIFO, HIFO, Strict, StrictWithSize, Average, None
   - `compute_inventory` for an account
   - Unit tests for each booking method

6. **rai-core: validation pipeline**
   - Transaction balance checks
   - Account open/close date checks
   - Currency constraint checks
   - Balance assertion checks
   - Error collection

7. **rai-cli: validate command**
   - Wire up `validate` command
   - Pretty error output

## Phase 3: Reports

**Goal:** Useful financial reports.

8. **rai-report: balance sheet**
9. **rai-report: income statement**
10. **rai-report: trial balance**
11. **rai-report: journal**
12. **rai-report: currency conversion at report time**

13. **rai-cli: report commands**
    - Wire up all report subcommands
    - Formatted terminal output with account trees, aligned amounts

## Phase 4: Polish

14. **CLI inference** — auto-compute one missing posting amount
15. **Helper views** — create SQL views on schema init
16. **Terminal charts** — sparklines, bar charts for balance trends
17. **Documentation** — schema reference, common SQL recipes, CLI help text
