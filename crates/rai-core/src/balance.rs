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
