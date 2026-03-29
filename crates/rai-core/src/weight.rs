use crate::types::{Amount, Posting};

/// Compute the "weight" of a posting for balance checking purposes.
///
/// - If the posting has a cost, the weight is `units * cost_per_unit` in the
///   cost commodity.
/// - Otherwise if the posting has a price, the weight is `units * price` in the
///   price commodity.
/// - Otherwise the weight is just the units themselves.
pub fn compute_weight(posting: &Posting) -> Amount {
    if let Some(cost) = &posting.cost {
        Amount {
            value: posting.units.value * cost.amount.value,
            commodity_id: cost.amount.commodity_id,
        }
    } else if let Some(price) = &posting.price {
        Amount {
            value: posting.units.value * price.value,
            commodity_id: price.commodity_id,
        }
    } else {
        posting.units.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn make_posting(
        units: Amount,
        cost: Option<Cost>,
        price: Option<Amount>,
    ) -> Posting {
        Posting {
            id: PostingId(1),
            transaction_id: TransactionId(1),
            account_id: AccountId(1),
            units,
            cost,
            price,
            metadata: HashMap::new(),
        }
    }

    // When a posting has no cost or price, its weight is simply the units
    // themselves — the most common case for single-currency transactions.
    #[test]
    fn weight_no_cost_no_price() {
        let posting = make_posting(
            Amount { value: dec!(100), commodity_id: CommodityId(1) },
            None,
            None,
        );
        let w = compute_weight(&posting);
        assert_eq!(w.value, dec!(100));
        assert_eq!(w.commodity_id, CommodityId(1));
    }

    // When a posting has a cost (e.g. buying stock at a per-unit cost), the
    // weight is units * cost_per_unit in the cost's commodity. This ensures
    // the transaction balances in the cost currency.
    #[test]
    fn weight_with_cost() {
        let posting = make_posting(
            Amount { value: dec!(10), commodity_id: CommodityId(1) },
            Some(Cost {
                amount: Amount { value: dec!(50), commodity_id: CommodityId(2) },
                date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                label: None,
            }),
            None,
        );
        let w = compute_weight(&posting);
        assert_eq!(w.value, dec!(500));
        assert_eq!(w.commodity_id, CommodityId(2));
    }

    // When a posting has a price but no cost, the weight uses units * price
    // in the price's commodity. This handles currency conversions at a
    // specific exchange rate.
    #[test]
    fn weight_with_price() {
        let posting = make_posting(
            Amount { value: dec!(100), commodity_id: CommodityId(1) },
            None,
            Some(Amount { value: dec!(1.35), commodity_id: CommodityId(2) }),
        );
        let w = compute_weight(&posting);
        assert_eq!(w.value, dec!(135.00));
        assert_eq!(w.commodity_id, CommodityId(2));
    }

    // When both cost and price are present, cost takes precedence. This
    // mirrors beancount semantics where cost is the authoritative basis.
    #[test]
    fn weight_cost_takes_precedence_over_price() {
        let posting = make_posting(
            Amount { value: dec!(10), commodity_id: CommodityId(1) },
            Some(Cost {
                amount: Amount { value: dec!(50), commodity_id: CommodityId(2) },
                date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                label: None,
            }),
            Some(Amount { value: dec!(99), commodity_id: CommodityId(3) }),
        );
        let w = compute_weight(&posting);
        assert_eq!(w.value, dec!(500));
        assert_eq!(w.commodity_id, CommodityId(2));
    }

    // Verifies that negative units produce a negative weight, which is
    // essential for selling/disposing assets.
    #[test]
    fn weight_negative_units_with_cost() {
        let posting = make_posting(
            Amount { value: dec!(-5), commodity_id: CommodityId(1) },
            Some(Cost {
                amount: Amount { value: dec!(100), commodity_id: CommodityId(2) },
                date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                label: None,
            }),
            None,
        );
        let w = compute_weight(&posting);
        assert_eq!(w.value, dec!(-500));
    }
}
