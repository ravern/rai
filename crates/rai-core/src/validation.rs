use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::balance::check_transaction_balance;
use crate::error::ValidationError;
use crate::types::{
    Account, AccountId, Amount, BalanceAssertion, Commodity, CommodityId, Transaction,
};

#[derive(Debug, Clone, Default)]
pub struct ValidationResult {
    pub errors: Vec<ValidationError>,
}

/// Run the full validation pipeline on a ledger's data.
///
/// Checks:
/// 1. All transactions balance
/// 2. Currency constraints (postings only use allowed commodities)
/// 3. Account open/close dates
/// 4. Balance assertions
pub fn validate(
    transactions: &[Transaction],
    accounts: &[Account],
    commodities: &[Commodity],
    balance_assertions: &[BalanceAssertion],
) -> ValidationResult {
    let mut errors = Vec::new();

    let account_map: HashMap<AccountId, &Account> =
        accounts.iter().map(|a| (a.id, a)).collect();

    // 1. Check that every transaction balances
    for tx in transactions {
        errors.extend(check_transaction_balance(tx, commodities));
    }

    // 2 & 3. Per-posting checks
    for tx in transactions {
        for posting in &tx.postings {
            if let Some(account) = account_map.get(&posting.account_id) {
                // Check account is open on the transaction date
                if tx.date < account.opened_at {
                    errors.push(ValidationError::AccountNotOpen {
                        account_id: posting.account_id,
                        date: tx.date,
                    });
                } else if let Some(closed) = account.closed_at {
                    if tx.date >= closed {
                        errors.push(ValidationError::AccountNotOpen {
                            account_id: posting.account_id,
                            date: tx.date,
                        });
                    }
                }

                // Check currency constraints (only if currencies list is non-empty)
                if !account.currencies.is_empty()
                    && !account.currencies.contains(&posting.units.commodity_id)
                {
                    errors.push(ValidationError::CurrencyNotAllowed {
                        account_id: posting.account_id,
                        commodity_id: posting.units.commodity_id,
                    });
                }
            }
        }
    }

    // 4. Balance assertions
    // Build running balances per (account, commodity) by processing
    // transactions in date order.
    if !balance_assertions.is_empty() {
        // Collect all postings with their transaction date, sorted by date
        let mut dated_postings: Vec<(chrono::NaiveDate, AccountId, CommodityId, Decimal)> = Vec::new();
        for tx in transactions {
            for posting in &tx.postings {
                dated_postings.push((
                    tx.date,
                    posting.account_id,
                    posting.units.commodity_id,
                    posting.units.value,
                ));
            }
        }
        dated_postings.sort_by_key(|(date, _, _, _)| *date);

        // Sort assertions by date
        let mut sorted_assertions: Vec<&BalanceAssertion> = balance_assertions.iter().collect();
        sorted_assertions.sort_by_key(|a| a.date);

        // Process assertions: for each assertion, accumulate all postings up
        // to and including the assertion date, then check.
        let mut balances: HashMap<(AccountId, CommodityId), Decimal> = HashMap::new();
        let mut posting_idx = 0;

        for assertion in &sorted_assertions {
            // Apply all postings up to and including the assertion date
            while posting_idx < dated_postings.len()
                && dated_postings[posting_idx].0 <= assertion.date
            {
                let (_, acct, comm, value) = &dated_postings[posting_idx];
                *balances.entry((*acct, *comm)).or_insert(Decimal::ZERO) += value;
                posting_idx += 1;
            }

            let actual_value = balances
                .get(&(assertion.account_id, assertion.expected.commodity_id))
                .copied()
                .unwrap_or(Decimal::ZERO);

            if actual_value != assertion.expected.value {
                errors.push(ValidationError::BalanceAssertionFailed {
                    assertion_id: assertion.id,
                    expected: assertion.expected.clone(),
                    actual: Amount {
                        value: actual_value,
                        commodity_id: assertion.expected.commodity_id,
                    },
                });
            }
        }
    }

    ValidationResult { errors }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ValidationError;
    use crate::types::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn usd() -> Commodity {
        Commodity {
            id: CommodityId(1),
            name: "USD".into(),
            precision: 2,
            metadata: HashMap::new(),
        }
    }

    fn account(id: i64, name: &str, opened: NaiveDate, closed: Option<NaiveDate>, currencies: Vec<CommodityId>) -> Account {
        Account {
            id: AccountId(id),
            name: name.into(),
            account_type: AccountType::from_name(name).unwrap(),
            is_open: closed.is_none(),
            opened_at: opened,
            closed_at: closed,
            currencies,
            booking_method: BookingMethod::Strict,
            metadata: HashMap::new(),
        }
    }

    fn posting(id: i64, account_id: i64, value: Decimal, commodity_id: i64) -> Posting {
        Posting {
            id: PostingId(id),
            transaction_id: TransactionId(1),
            account_id: AccountId(account_id),
            units: Amount { value, commodity_id: CommodityId(commodity_id) },
            cost: None,
            price: None,
            metadata: HashMap::new(),
        }
    }

    fn tx(id: i64, d: NaiveDate, postings: Vec<Posting>) -> Transaction {
        Transaction {
            id: TransactionId(id),
            date: d,
            time: None,
            status: TransactionStatus::Completed,
            payee: None,
            narration: None,
            tags: vec![],
            links: vec![],
            postings,
            metadata: HashMap::new(),
        }
    }

    // A fully valid ledger with balanced transactions should produce
    // no validation errors.
    #[test]
    fn valid_ledger_no_errors() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 1, 1), None, vec![]),
            account(2, "Expenses:Food", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            tx(1, date(2024, 3, 1), vec![
                posting(1, 1, dec!(-50), 1),
                posting(2, 2, dec!(50), 1),
            ]),
        ];
        let result = validate(&transactions, &accounts, &[usd()], &[]);
        assert!(result.errors.is_empty());
    }

    // Posting to an account before its opening date should produce
    // an AccountNotOpen error.
    #[test]
    fn posting_before_account_opened() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 6, 1), None, vec![]),
            account(2, "Expenses:Food", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            tx(1, date(2024, 3, 1), vec![
                posting(1, 1, dec!(-50), 1),
                posting(2, 2, dec!(50), 1),
            ]),
        ];
        let result = validate(&transactions, &accounts, &[usd()], &[]);
        assert!(result.errors.iter().any(|e| matches!(e, ValidationError::AccountNotOpen { account_id, .. } if *account_id == AccountId(1))));
    }

    // Posting to a closed account (on or after close date) should produce
    // an AccountNotOpen error.
    #[test]
    fn posting_to_closed_account() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 1, 1), Some(date(2024, 6, 1)), vec![]),
            account(2, "Expenses:Food", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            tx(1, date(2024, 6, 1), vec![
                posting(1, 1, dec!(-50), 1),
                posting(2, 2, dec!(50), 1),
            ]),
        ];
        let result = validate(&transactions, &accounts, &[usd()], &[]);
        assert!(result.errors.iter().any(|e| matches!(e, ValidationError::AccountNotOpen { .. })));
    }

    // Using a commodity not in the account's allowed currencies list
    // should produce a CurrencyNotAllowed error.
    #[test]
    fn currency_not_allowed() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 1, 1), None, vec![CommodityId(1)]),
            account(2, "Expenses:Food", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            tx(1, date(2024, 3, 1), vec![
                posting(1, 1, dec!(-50), 2), // commodity 2 not allowed on account 1
                posting(2, 2, dec!(50), 2),
            ]),
        ];
        let result = validate(&transactions, &accounts, &[usd()], &[]);
        assert!(result.errors.iter().any(|e| matches!(e, ValidationError::CurrencyNotAllowed { .. })));
    }

    // When currencies list is empty, any commodity should be allowed
    // (empty = unrestricted).
    #[test]
    fn empty_currencies_allows_any() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 1, 1), None, vec![]),
            account(2, "Expenses:Food", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            tx(1, date(2024, 3, 1), vec![
                posting(1, 1, dec!(-50), 99),
                posting(2, 2, dec!(50), 99),
            ]),
        ];
        let result = validate(&transactions, &accounts, &[], &[]);
        let currency_errors: Vec<_> = result.errors.iter()
            .filter(|e| matches!(e, ValidationError::CurrencyNotAllowed { .. }))
            .collect();
        assert!(currency_errors.is_empty());
    }

    // A correct balance assertion (expected matches actual running
    // balance) should pass without errors.
    #[test]
    fn balance_assertion_passes() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 1, 1), None, vec![]),
            account(2, "Income:Salary", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            tx(1, date(2024, 1, 15), vec![
                posting(1, 1, dec!(1000), 1),
                posting(2, 2, dec!(-1000), 1),
            ]),
        ];
        let assertions = vec![
            BalanceAssertion {
                id: BalanceAssertionId(1),
                date: date(2024, 1, 31),
                account_id: AccountId(1),
                expected: Amount { value: dec!(1000), commodity_id: CommodityId(1) },
            },
        ];
        let result = validate(&transactions, &accounts, &[usd()], &assertions);
        assert!(result.errors.is_empty());
    }

    // A balance assertion with the wrong expected amount should produce
    // a BalanceAssertionFailed error.
    #[test]
    fn balance_assertion_fails() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 1, 1), None, vec![]),
            account(2, "Income:Salary", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            tx(1, date(2024, 1, 15), vec![
                posting(1, 1, dec!(1000), 1),
                posting(2, 2, dec!(-1000), 1),
            ]),
        ];
        let assertions = vec![
            BalanceAssertion {
                id: BalanceAssertionId(1),
                date: date(2024, 1, 31),
                account_id: AccountId(1),
                expected: Amount { value: dec!(999), commodity_id: CommodityId(1) },
            },
        ];
        let result = validate(&transactions, &accounts, &[usd()], &assertions);
        assert!(result.errors.iter().any(|e| matches!(e, ValidationError::BalanceAssertionFailed { .. })));
    }

    // Multiple errors from different checks should all be accumulated
    // in a single ValidationResult.
    #[test]
    fn multiple_errors_accumulated() {
        let accounts = vec![
            account(1, "Assets:Bank", date(2024, 6, 1), None, vec![CommodityId(1)]),
            account(2, "Expenses:Food", date(2024, 1, 1), None, vec![]),
        ];
        let transactions = vec![
            // Unbalanced + before open date + wrong currency
            tx(1, date(2024, 3, 1), vec![
                posting(1, 1, dec!(100), 2),
            ]),
        ];
        let result = validate(&transactions, &accounts, &[usd()], &[]);
        // Should have: unbalanced tx, account not open, currency not allowed
        assert!(result.errors.len() >= 2);
    }
}
