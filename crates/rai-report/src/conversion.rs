use std::collections::{HashMap, HashSet, VecDeque};

use chrono::NaiveDate;
use rai_core::types::{Amount, CommodityId, Price};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy)]
struct ConversionEdge {
    to: CommodityId,
    rate: Decimal,
    is_inverse: bool,
}

/// Convert an amount to a target commodity using the latest available prices.
///
/// Builds a price graph from the latest price entry for each commodity pair
/// and searches for the shortest conversion path. Recorded prices are treated
/// as directed edges, and safe inverse edges are added for compatibility with
/// existing reverse conversion behavior.
///
/// Returns `None` if the amount is already in the target commodity (in which
/// case no conversion is needed) or if no price can be found.
pub fn convert_amount(amount: &Amount, target: CommodityId, prices: &[Price]) -> Option<Amount> {
    convert_amount_as_of(amount, target, prices, None)
}

/// Convert an amount using only prices dated on or before `as_of`.
pub fn convert_amount_as_of(
    amount: &Amount,
    target: CommodityId,
    prices: &[Price],
    as_of: Option<NaiveDate>,
) -> Option<Amount> {
    if amount.commodity_id == target {
        return Some(amount.clone());
    }

    let value = find_converted_value(amount.value, amount.commodity_id, target, prices, as_of)?;
    Some(Amount {
        value,
        commodity_id: target,
    })
}

fn find_converted_value(
    value: Decimal,
    source: CommodityId,
    target: CommodityId,
    prices: &[Price],
    as_of: Option<NaiveDate>,
) -> Option<Decimal> {
    if source == target {
        return Some(value);
    }

    let latest_prices = latest_prices_by_pair(prices, as_of);

    if let Some(price) = latest_prices.get(&(source, target)) {
        return Some(value * price.value);
    }

    if let Some(price) = latest_prices.get(&(target, source)) {
        if price.value.is_zero() {
            return None;
        }
        return Some(value / price.value);
    }

    let graph = build_price_graph(&latest_prices);
    let mut seen = HashSet::from([source]);
    let mut queue = VecDeque::from([(source, value)]);

    while let Some((commodity, value_so_far)) = queue.pop_front() {
        let Some(edges) = graph.get(&commodity) else {
            continue;
        };

        for edge in edges {
            if !seen.insert(edge.to) {
                continue;
            }

            let converted_value = if edge.is_inverse {
                value_so_far / edge.rate
            } else {
                value_so_far * edge.rate
            };
            if edge.to == target {
                return Some(converted_value);
            }
            queue.push_back((edge.to, converted_value));
        }
    }

    None
}

fn latest_prices_by_pair(
    prices: &[Price],
    as_of: Option<NaiveDate>,
) -> HashMap<(CommodityId, CommodityId), &Price> {
    let mut latest: HashMap<(CommodityId, CommodityId), &Price> = HashMap::new();

    for price in prices {
        if let Some(as_of) = as_of {
            if price.date > as_of {
                continue;
            }
        }

        let key = (price.commodity_id, price.target_commodity_id);
        match latest.get(&key) {
            Some(existing) if (existing.date, existing.id.0) >= (price.date, price.id.0) => {}
            _ => {
                latest.insert(key, price);
            }
        }
    }

    latest
}

fn build_price_graph(
    latest_prices: &HashMap<(CommodityId, CommodityId), &Price>,
) -> HashMap<CommodityId, Vec<ConversionEdge>> {
    let mut graph: HashMap<CommodityId, Vec<ConversionEdge>> = HashMap::new();

    let mut prices: Vec<_> = latest_prices.values().copied().collect();
    prices.sort_by_key(|p| (p.commodity_id.0, p.target_commodity_id.0));

    for price in &prices {
        graph
            .entry(price.commodity_id)
            .or_default()
            .push(ConversionEdge {
                to: price.target_commodity_id,
                rate: price.value,
                is_inverse: false,
            });
    }

    for price in &prices {
        if price.value.is_zero()
            || latest_prices.contains_key(&(price.target_commodity_id, price.commodity_id))
        {
            continue;
        }

        graph
            .entry(price.target_commodity_id)
            .or_default()
            .push(ConversionEdge {
                to: price.commodity_id,
                rate: price.value,
                is_inverse: true,
            });
    }

    for edges in graph.values_mut() {
        edges.sort_by_key(|edge| (edge.is_inverse, edge.to.0));
    }

    graph
}

/// Convert a list of amounts to a target commodity, merging converted amounts
/// into a single amount in the target commodity. Amounts that cannot be
/// converted are left as-is.
pub fn convert_amounts(amounts: &[Amount], target: CommodityId, prices: &[Price]) -> Vec<Amount> {
    convert_amounts_as_of(amounts, target, prices, None)
}

/// Convert a list of amounts using only prices dated on or before `as_of`.
pub fn convert_amounts_as_of(
    amounts: &[Amount],
    target: CommodityId,
    prices: &[Price],
    as_of: Option<NaiveDate>,
) -> Vec<Amount> {
    let mut by_commodity: HashMap<CommodityId, Decimal> = HashMap::new();

    for amount in amounts {
        if let Some(converted) = convert_amount_as_of(amount, target, prices, as_of) {
            *by_commodity
                .entry(converted.commodity_id)
                .or_insert(Decimal::ZERO) += converted.value;
        } else {
            // Cannot convert; keep original commodity
            *by_commodity
                .entry(amount.commodity_id)
                .or_insert(Decimal::ZERO) += amount.value;
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

    // Multi-hop paths should allow report currencies to be reached through an
    // intermediate commodity, such as BTC -> USD -> SGD.
    #[test]
    fn convert_with_multi_hop_path() {
        let amount = Amount {
            value: dec!(10),
            commodity_id: CommodityId(1),
        };
        let prices = vec![
            price(1, 2, dec!(2), date(2024, 1, 1)),
            price(2, 3, dec!(3), date(2024, 1, 1)),
        ];
        let result = convert_amount(&amount, CommodityId(3), &prices).unwrap();
        assert_eq!(result.value, dec!(60));
        assert_eq!(result.commodity_id, CommodityId(3));
    }

    // The graph search is not limited to two-hop paths.
    #[test]
    fn convert_with_arbitrary_hop_path() {
        let amount = Amount {
            value: dec!(10),
            commodity_id: CommodityId(1),
        };
        let prices = vec![
            price(1, 2, dec!(2), date(2024, 1, 1)),
            price(2, 3, dec!(3), date(2024, 1, 1)),
            price(3, 4, dec!(4), date(2024, 1, 1)),
        ];
        let result = convert_amount(&amount, CommodityId(4), &prices).unwrap();
        assert_eq!(result.value, dec!(240));
        assert_eq!(result.commodity_id, CommodityId(4));
    }

    // As-of conversion should ignore prices after the report date and use the
    // latest eligible rate for each hop.
    #[test]
    fn convert_as_of_ignores_future_prices_per_hop() {
        let amount = Amount {
            value: dec!(10),
            commodity_id: CommodityId(1),
        };
        let prices = vec![
            price(1, 2, dec!(2), date(2024, 1, 1)),
            price(1, 2, dec!(5), date(2024, 7, 1)),
            price(2, 3, dec!(3), date(2024, 1, 1)),
            price(2, 3, dec!(7), date(2024, 7, 1)),
        ];
        let result =
            convert_amount_as_of(&amount, CommodityId(3), &prices, Some(date(2024, 6, 30)))
                .unwrap();
        assert_eq!(result.value, dec!(60));
        assert_eq!(result.commodity_id, CommodityId(3));
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
