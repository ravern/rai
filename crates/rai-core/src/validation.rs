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
