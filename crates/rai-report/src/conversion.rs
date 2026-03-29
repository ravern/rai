use rai_core::types::{Amount, CommodityId, Price};

/// Convert an amount to a target commodity using the latest available price.
///
/// Looks up the most recent price entry where `commodity_id` matches the
/// amount's commodity and `target_commodity_id` matches the target. If no
/// direct price is found, tries the reverse direction (target -> source) and
/// inverts the rate.
///
/// Returns `None` if the amount is already in the target commodity (in which
/// case no conversion is needed) or if no price can be found.
pub fn convert_amount(amount: &Amount, target: CommodityId, prices: &[Price]) -> Option<Amount> {
    if amount.commodity_id == target {
        return Some(amount.clone());
    }

    // Find the latest direct price: amount.commodity_id -> target
    let direct = prices
        .iter()
        .filter(|p| p.commodity_id == amount.commodity_id && p.target_commodity_id == target)
        .max_by_key(|p| p.date);

    if let Some(price) = direct {
        return Some(Amount {
            value: amount.value * price.value,
            commodity_id: target,
        });
    }

    // Try reverse: target -> amount.commodity_id, then invert
    let reverse = prices
        .iter()
        .filter(|p| p.commodity_id == target && p.target_commodity_id == amount.commodity_id)
        .max_by_key(|p| p.date);

    if let Some(price) = reverse {
        if price.value.is_zero() {
            return None;
        }
        return Some(Amount {
            value: amount.value / price.value,
            commodity_id: target,
        });
    }

    None
}

/// Convert a list of amounts to a target commodity, merging converted amounts
/// into a single amount in the target commodity. Amounts that cannot be
/// converted are left as-is.
pub fn convert_amounts(
    amounts: &[Amount],
    target: CommodityId,
    prices: &[Price],
) -> Vec<Amount> {
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    let mut by_commodity: HashMap<CommodityId, Decimal> = HashMap::new();

    for amount in amounts {
        if let Some(converted) = convert_amount(amount, target, prices) {
            *by_commodity.entry(converted.commodity_id).or_insert(Decimal::ZERO) += converted.value;
        } else {
            // Cannot convert; keep original commodity
            *by_commodity.entry(amount.commodity_id).or_insert(Decimal::ZERO) += amount.value;
        }
    }

    by_commodity
        .into_iter()
        .map(|(commodity_id, value)| Amount {
            value,
            commodity_id,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn price(commodity: i64, target: i64, value: rust_decimal::Decimal, d: NaiveDate) -> Price {
        Price {
            id: rai_core::types::PriceId(1),
            date: d,
            commodity_id: CommodityId(commodity),
            target_commodity_id: CommodityId(target),
            value,
        }
    }

    // Converting an amount already in the target commodity should return
    // it unchanged (no-op identity conversion).
    #[test]
    fn convert_same_commodity_returns_clone() {
        let amount = Amount { value: dec!(100), commodity_id: CommodityId(1) };
        let result = convert_amount(&amount, CommodityId(1), &[]);
        assert_eq!(result.unwrap().value, dec!(100));
    }

    // Direct price conversion multiplies the amount by the exchange rate.
    #[test]
    fn convert_with_direct_price() {
        let amount = Amount { value: dec!(100), commodity_id: CommodityId(1) };
        let prices = vec![price(1, 2, dec!(1.35), date(2024, 1, 1))];
        let result = convert_amount(&amount, CommodityId(2), &prices).unwrap();
        assert_eq!(result.value, dec!(135.00));
        assert_eq!(result.commodity_id, CommodityId(2));
    }

    // When no direct price exists but a reverse price does (target->source),
    // the function should invert the rate and convert.
    #[test]
    fn convert_with_reverse_price() {
        let amount = Amount { value: dec!(135), commodity_id: CommodityId(1) };
        // Price defined as 2->1 at rate 1.35
        let prices = vec![price(2, 1, dec!(1.35), date(2024, 1, 1))];
        let result = convert_amount(&amount, CommodityId(2), &prices).unwrap();
        assert_eq!(result.value, dec!(100));
        assert_eq!(result.commodity_id, CommodityId(2));
    }

    // When no price path exists between commodities, conversion should
    // return None.
    #[test]
    fn convert_no_price_returns_none() {
        let amount = Amount { value: dec!(100), commodity_id: CommodityId(1) };
        let result = convert_amount(&amount, CommodityId(3), &[]);
        assert!(result.is_none());
    }

    // When multiple prices exist for the same pair, the most recent one
    // (by date) should be used.
    #[test]
    fn convert_uses_latest_price() {
        let amount = Amount { value: dec!(100), commodity_id: CommodityId(1) };
        let prices = vec![
            price(1, 2, dec!(1.30), date(2024, 1, 1)),
            price(1, 2, dec!(1.40), date(2024, 6, 1)),
        ];
        let result = convert_amount(&amount, CommodityId(2), &prices).unwrap();
        assert_eq!(result.value, dec!(140.00));
    }

    // Verifies that reverse conversion with a zero-rate price returns
    // None rather than dividing by zero.
    #[test]
    fn convert_reverse_zero_price_returns_none() {
        let amount = Amount { value: dec!(100), commodity_id: CommodityId(1) };
        let prices = vec![price(2, 1, dec!(0), date(2024, 1, 1))];
        let result = convert_amount(&amount, CommodityId(2), &prices);
        assert!(result.is_none());
    }

    // convert_amounts should merge converted amounts into one per
    // commodity, and leave unconvertible amounts as-is.
    #[test]
    fn convert_amounts_merges_and_preserves() {
        let amounts = vec![
            Amount { value: dec!(100), commodity_id: CommodityId(1) },
            Amount { value: dec!(50), commodity_id: CommodityId(1) },
            Amount { value: dec!(200), commodity_id: CommodityId(3) }, // no price for this
        ];
        let prices = vec![price(1, 2, dec!(2), date(2024, 1, 1))];
        let result = convert_amounts(&amounts, CommodityId(2), &prices);
        // 100+50=150 USD * 2 = 300 in target, plus 200 of commodity 3 kept as-is
        let target_amount = result.iter().find(|a| a.commodity_id == CommodityId(2));
        let unconverted = result.iter().find(|a| a.commodity_id == CommodityId(3));
        assert_eq!(target_amount.unwrap().value, dec!(300));
        assert_eq!(unconverted.unwrap().value, dec!(200));
    }
}
