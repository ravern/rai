use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::error::ValidationError;
use crate::types::{Commodity, CommodityId, Transaction};
use crate::weight::compute_weight;
use crate::types::Amount;

/// Check whether a transaction balances.
///
/// Computes the weight of every posting, groups by commodity, sums them, and
/// checks that each residual is zero within the commodity's precision.
/// Returns a list of validation errors (empty if balanced).
pub fn check_transaction_balance(
    tx: &Transaction,
    commodities: &[Commodity],
) -> Vec<ValidationError> {
    let precision_map: HashMap<CommodityId, u8> = commodities
        .iter()
        .map(|c| (c.id, c.precision))
        .collect();

    // Accumulate weights per commodity
    let mut residuals: HashMap<CommodityId, Decimal> = HashMap::new();
    for posting in &tx.postings {
        let weight = compute_weight(posting);
        *residuals.entry(weight.commodity_id).or_insert_with(Decimal::default) += weight.value;
    }

    // Check each residual against the commodity precision tolerance
    let non_zero: Vec<Amount> = residuals
        .into_iter()
        .filter(|(commodity_id, value)| {
            let precision = precision_map.get(commodity_id).copied().unwrap_or(2);
            let tolerance = Decimal::new(5, (precision + 1) as u32); // 0.5 * 10^-precision
            value.abs() > tolerance
        })
        .map(|(commodity_id, value)| Amount {
            value,
            commodity_id,
        })
        .collect();

    if non_zero.is_empty() {
        vec![]
    } else {
        vec![ValidationError::TransactionDoesNotBalance {
            transaction_id: tx.id,
            residuals: non_zero,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;

    fn usd() -> Commodity {
        Commodity {
            id: CommodityId(1),
            name: "USD".into(),
            precision: 2,
            metadata: HashMap::new(),
        }
    }

    fn eur() -> Commodity {
        Commodity {
            id: CommodityId(2),
            name: "EUR".into(),
            precision: 2,
            metadata: HashMap::new(),
        }
    }

    fn posting(account_id: i64, value: Decimal, commodity_id: i64) -> Posting {
        Posting {
            id: PostingId(0),
            transaction_id: TransactionId(1),
            account_id: AccountId(account_id),
            units: Amount {
                value,
                commodity_id: CommodityId(commodity_id),
            },
            cost: None,
            price: None,
            metadata: HashMap::new(),
        }
    }

    fn tx_with_postings(postings: Vec<Posting>) -> Transaction {
        Transaction {
            id: TransactionId(1),
            date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
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

    // A simple balanced transaction (debit + credit = 0) should produce
    // no validation errors.
    #[test]
    fn balanced_transaction_no_errors() {
        let tx = tx_with_postings(vec![
            posting(1, dec!(100), 1),
            posting(2, dec!(-100), 1),
        ]);
        let errors = check_transaction_balance(&tx, &[usd()]);
        assert!(errors.is_empty());
    }

    // An unbalanced transaction (postings don't sum to zero) should
    // produce a TransactionDoesNotBalance error.
    #[test]
    fn unbalanced_transaction_produces_error() {
        let tx = tx_with_postings(vec![
            posting(1, dec!(100), 1),
            posting(2, dec!(-50), 1),
        ]);
        let errors = check_transaction_balance(&tx, &[usd()]);
        assert_eq!(errors.len(), 1);
        assert!(matches!(&errors[0], ValidationError::TransactionDoesNotBalance { .. }));
    }

    // Small rounding differences within the commodity's precision tolerance
    // (0.5 * 10^-precision) should be accepted as balanced.
    #[test]
    fn rounding_within_tolerance_accepted() {
        let tx = tx_with_postings(vec![
            posting(1, dec!(100.004), 1),
            posting(2, dec!(-100), 1),
        ]);
        let errors = check_transaction_balance(&tx, &[usd()]);
        assert!(errors.is_empty(), "residual of 0.004 should be within tolerance for precision 2");
    }

    // Residuals exceeding the tolerance threshold should be rejected.
    #[test]
    fn rounding_beyond_tolerance_rejected() {
        let tx = tx_with_postings(vec![
            posting(1, dec!(100.01), 1),
            posting(2, dec!(-100), 1),
        ]);
        let errors = check_transaction_balance(&tx, &[usd()]);
        assert_eq!(errors.len(), 1);
    }

    // Multi-commodity transactions where each commodity independently
    // balances should pass validation.
    #[test]
    fn multi_commodity_balanced() {
        let tx = tx_with_postings(vec![
            posting(1, dec!(100), 1),
            posting(2, dec!(-100), 1),
            posting(3, dec!(200), 2),
            posting(4, dec!(-200), 2),
        ]);
        let errors = check_transaction_balance(&tx, &[usd(), eur()]);
        assert!(errors.is_empty());
    }

    // When a commodity is not found in the precision map, the default
    // precision of 2 should be used.
    #[test]
    fn unknown_commodity_uses_default_precision() {
        let tx = tx_with_postings(vec![
            posting(1, dec!(100.004), 99),
            posting(2, dec!(-100), 99),
        ]);
        // No commodity definition provided — should use default precision 2
        let errors = check_transaction_balance(&tx, &[]);
        assert!(errors.is_empty());
    }

    // A transaction with a single posting can never balance (unless zero),
    // confirming double-entry is enforced.
    #[test]
    fn single_posting_unbalanced() {
        let tx = tx_with_postings(vec![posting(1, dec!(100), 1)]);
        let errors = check_transaction_balance(&tx, &[usd()]);
        assert_eq!(errors.len(), 1);
    }

    // A transaction with no postings trivially balances (residual is zero).
    #[test]
    fn empty_transaction_balanced() {
        let tx = tx_with_postings(vec![]);
        let errors = check_transaction_balance(&tx, &[usd()]);
        assert!(errors.is_empty());
    }
}
