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
