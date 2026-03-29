# rai — Data Model

## Entities

### Commodity

A named unit of measurement. All commodities are equal — no distinction between fiat, equity, crypto, etc.

```rust
struct Commodity {
    id: CommodityId,
    name: String,          // e.g., "USD", "AAPL", "BTC"
    precision: u8,         // decimal places (USD=2, BTC=8)
    metadata: Metadata,
}
```

Precision defines the fixed-point representation. An amount of 100.50 USD (precision 2) is stored as the integer 10050 with precision 2, or as a `rust_decimal::Decimal` constrained to that precision. Balance checks are exact within the commodity's precision.

### Account

Accounts form a hierarchical chart of accounts with five fixed root types, matching beancount.

```rust
enum AccountType {
    Assets,
    Liabilities,
    Income,
    Expenses,
    Equity,
}

struct Account {
    id: AccountId,
    name: String,              // full path, e.g., "Assets:Bank:Checking"
    account_type: AccountType, // derived from root segment
    is_open: bool,
    opened_at: NaiveDate,
    closed_at: Option<NaiveDate>,
    currencies: Vec<CommodityId>,  // optional constraint, empty = any
    booking_method: BookingMethod,
    metadata: Metadata,
}
```

Account type semantics follow beancount: the system tracks the sign convention (debit-normal for Assets/Expenses, credit-normal for Liabilities/Income/Equity) but does not enforce balance sign — it uses this for report presentation (e.g., income statement shows Income as positive).

Opening and closing are fields/actions on an Account, not separate directives.

### BookingMethod

```rust
enum BookingMethod {
    Strict,         // exact lot match required
    StrictWithSize, // match on size
    Fifo,
    Lifo,
    Hifo,
    Average,        // actually implemented, unlike beancount
    None,           // no lot matching
}
```

Default: `Strict` (or configurable per-profile).

### Amount

Fixed-point amount tied to a commodity's precision.

```rust
struct Amount {
    value: Decimal,       // rust_decimal, constrained to commodity precision
    commodity_id: CommodityId,
}
```

### Transaction

```rust
enum TransactionStatus {
    Completed,
    Pending,
    Flagged,
}

struct Transaction {
    id: TransactionId,
    date: NaiveDate,
    time: Option<NaiveTime>,  // optional
    status: TransactionStatus,
    payee: Option<String>,
    narration: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,       // cross-references to other transactions
    postings: Vec<Posting>,
    metadata: Metadata,
}
```

### Posting

```rust
struct Posting {
    id: PostingId,
    transaction_id: TransactionId,
    account_id: AccountId,
    units: Amount,                    // what you're moving (e.g., 10 AAPL)
    cost: Option<Cost>,               // held-at-cost basis
    price: Option<Amount>,            // conversion price
    metadata: Metadata,
}
```

All postings must have explicit amounts in the library. The CLI may infer one missing posting amount per transaction for convenience.

### Cost (Lot Tracking)

```rust
struct Cost {
    amount: Amount,              // per-unit cost (e.g., 150 USD per AAPL)
    date: NaiveDate,             // acquisition date
    label: Option<String>,       // optional lot label
}
```

### Position and Inventory

Used in computation, not necessarily stored directly:

```rust
struct Position {
    units: Amount,
    cost: Option<Cost>,
}

// Inventory is a collection of positions, keyed by (commodity, cost)
struct Inventory {
    positions: HashMap<(CommodityId, Option<Cost>), Position>,
}
```

### Price

Market/reference price for a commodity pair on a given date.

```rust
struct Price {
    id: PriceId,
    date: NaiveDate,
    commodity_id: CommodityId,      // what is being priced (e.g., AAPL)
    target_commodity_id: CommodityId, // denominated in (e.g., USD)
    value: Decimal,
}
```

### Balance Assertion

```rust
struct BalanceAssertion {
    id: BalanceAssertionId,
    date: NaiveDate,
    account_id: AccountId,
    expected: Amount,
}
```

Hard error on mismatch. Checked during validation.

### Metadata

Arbitrary key-value data on transactions and postings.

```rust
type Metadata = HashMap<String, MetadataValue>;

enum MetadataValue {
    String(String),
    Number(Decimal),
    Date(NaiveDate),
    Bool(bool),
}
```

## Weight-Based Balancing

Every posting has a "weight" used for the double-entry balance check:

1. **Simple posting** (no cost, no price): weight = units
2. **Price conversion** (no cost, has price): weight = units.number * price
3. **Held at cost** (has cost): weight = units.number * cost.amount

A transaction balances when the sum of all posting weights, grouped by commodity, is zero within each commodity's precision.

## Same-Day Ordering

Within a single date, operations are ordered:
1. Account opens
2. Balance assertions (checked before transactions)
3. Transactions (ordered by time if present, then by insertion order)
4. Account closes
