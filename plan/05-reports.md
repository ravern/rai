# rai — Reports (rai-report)

## Design

Reports are functions that take domain types (loaded from storage) and return structured data. The `rai-report` crate depends on `rai-core` but NOT on `rai-db` — it operates on domain types.

The CLI is responsible for rendering report output as terminal tables/charts. Library consumers receive structured Rust types and render however they want.

## Report Interface

```rust
/// Common parameters for date-bounded reports.
struct ReportPeriod {
    start: Option<NaiveDate>,  // inclusive
    end: Option<NaiveDate>,    // inclusive
}

/// Every report returns a typed result struct.
trait Report {
    type Params;
    type Result;

    fn generate(params: &Self::Params, data: &LedgerData) -> Result<Self::Result, Vec<Error>>;
}
```

`LedgerData` is a snapshot of all relevant domain objects, loaded from storage before report generation.

## Reports

### Balance Sheet

Shows assets, liabilities, and equity at a point in time.

```rust
struct BalanceSheetParams {
    as_of: NaiveDate,               // point-in-time snapshot
    currency: Option<CommodityId>,  // optional: convert to this currency using latest prices
}

struct BalanceSheetResult {
    as_of: NaiveDate,
    assets: Vec<AccountBalance>,
    liabilities: Vec<AccountBalance>,
    equity: Vec<AccountBalance>,
    // Totals
    total_assets: Vec<Amount>,
    total_liabilities: Vec<Amount>,
    total_equity: Vec<Amount>,
}

struct AccountBalance {
    account: Account,
    balances: Vec<Amount>,  // one per commodity held
}
```

### Income Statement

Shows income and expenses over a period. Net income = income - expenses.

```rust
struct IncomeStatementParams {
    period: ReportPeriod,
    currency: Option<CommodityId>,
}

struct IncomeStatementResult {
    period: ReportPeriod,
    income: Vec<AccountBalance>,
    expenses: Vec<AccountBalance>,
    total_income: Vec<Amount>,
    total_expenses: Vec<Amount>,
    net_income: Vec<Amount>,
}
```

### Trial Balance

Shows debit and credit totals for every account.

```rust
struct TrialBalanceParams {
    as_of: NaiveDate,
}

struct TrialBalanceResult {
    as_of: NaiveDate,
    rows: Vec<TrialBalanceRow>,
}

struct TrialBalanceRow {
    account: Account,
    debits: Vec<Amount>,
    credits: Vec<Amount>,
    balance: Vec<Amount>,
}
```

### Journal

Chronological list of transactions with postings.

```rust
struct JournalParams {
    period: ReportPeriod,
    account: Option<AccountId>,  // filter to one account
}

struct JournalResult {
    entries: Vec<JournalEntry>,
}

struct JournalEntry {
    transaction: Transaction,
    running_balances: Option<Vec<Amount>>,  // if filtered to one account
}
```

## Currency Conversion in Reports

When a report specifies a target currency, the report engine:

1. Looks up the latest price for each commodity relative to the target currency
2. Converts all amounts using those prices
3. This is purely for display — the underlying data is never modified

This avoids beancount's synthetic conversion entries. Conversion is a report-time operation only.
