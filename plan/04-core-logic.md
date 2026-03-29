# rai — Core Logic (rai-core)

## Responsibilities

`rai-core` contains:

1. Domain types (all the structs/enums from the data model)
2. Accounting logic (weight computation, balance checking, lot booking)
3. Validation (balance assertions, account constraints, transaction balancing)
4. Error collection

It does NOT depend on `rai-db`. It operates on domain types passed to it.

## Weight Computation

```rust
fn compute_weight(posting: &Posting) -> Amount {
    if let Some(cost) = &posting.cost {
        // Held at cost: weight = units.number * cost.amount
        Amount {
            value: posting.units.value * cost.amount.value,
            commodity_id: cost.amount.commodity_id,
        }
    } else if let Some(price) = &posting.price {
        // Price conversion: weight = units.number * price
        Amount {
            value: posting.units.value * price.value,
            commodity_id: price.commodity_id,
        }
    } else {
        // Simple: weight = units
        posting.units.clone()
    }
}
```

## Transaction Balancing

```rust
fn check_transaction_balance(tx: &Transaction, commodities: &[Commodity]) -> Vec<Error> {
    // 1. Compute weight for each posting
    // 2. Group weights by commodity
    // 3. Sum each group
    // 4. Check each sum is zero within the commodity's precision
    // 5. Return errors for any non-zero residuals
}
```

## Lot Booking

When a posting reduces an inventory position (e.g., selling shares), the booking engine selects which lots to reduce based on the account's booking method.

```rust
fn book_reduction(
    inventory: &Inventory,
    posting: &Posting,
    method: BookingMethod,
) -> Result<Vec<BookedLot>, Vec<Error>> {
    match method {
        BookingMethod::Fifo => { /* oldest lots first */ }
        BookingMethod::Lifo => { /* newest lots first */ }
        BookingMethod::Hifo => { /* highest cost first */ }
        BookingMethod::Average => { /* weighted average cost */ }
        BookingMethod::Strict => { /* must match exactly one lot */ }
        BookingMethod::StrictWithSize => { /* match on size */ }
        BookingMethod::None => { /* no matching, just reduce total */ }
    }
}
```

## Validation Pipeline

Validation runs over the full dataset and collects all errors:

```rust
struct ValidationResult {
    errors: Vec<Error>,
    warnings: Vec<Warning>,
}

fn validate(
    transactions: &[Transaction],
    accounts: &[Account],
    commodities: &[Commodity],
    balance_assertions: &[BalanceAssertion],
) -> ValidationResult {
    let mut errors = vec![];

    // 1. Check all transactions balance
    for tx in transactions {
        errors.extend(check_transaction_balance(tx, commodities));
    }

    // 2. Check account currency constraints
    for tx in transactions {
        for posting in &tx.postings {
            errors.extend(check_currency_constraint(posting, accounts));
        }
    }

    // 3. Check postings reference open accounts on the transaction date
    for tx in transactions {
        for posting in &tx.postings {
            errors.extend(check_account_open(posting, &tx.date, accounts));
        }
    }

    // 4. Check balance assertions
    // Build account balances up to each assertion date, compare
    errors.extend(check_balance_assertions(transactions, balance_assertions, commodities));

    ValidationResult { errors, warnings: vec![] }
}
```

## Error Types

```rust
enum Error {
    TransactionDoesNotBalance {
        transaction_id: TransactionId,
        residuals: Vec<Amount>,  // non-zero residuals per commodity
    },
    BalanceAssertionFailed {
        assertion_id: BalanceAssertionId,
        expected: Amount,
        actual: Amount,
    },
    AccountNotOpen {
        account_id: AccountId,
        date: NaiveDate,
    },
    CurrencyNotAllowed {
        account_id: AccountId,
        commodity_id: CommodityId,
    },
    AmbiguousLotMatch {
        posting_id: PostingId,
        matches: Vec<Position>,
    },
    NoMatchingLot {
        posting_id: PostingId,
    },
}
```

## Inventory Computation

Building an inventory for an account up to a given date:

```rust
fn compute_inventory(
    account_id: AccountId,
    postings: &[Posting],      // filtered and sorted by date
    booking_method: BookingMethod,
) -> Result<Inventory, Vec<Error>> {
    let mut inventory = Inventory::new();
    for posting in postings {
        if posting.cost.is_some() && posting.units.value < Decimal::ZERO {
            // Reduction — apply booking method
            book_reduction(&mut inventory, posting, booking_method)?;
        } else {
            // Addition or simple amount
            inventory.add(posting.into());
        }
    }
    Ok(inventory)
}
```
