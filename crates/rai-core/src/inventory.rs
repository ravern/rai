use rust_decimal::Decimal;

use crate::error::ValidationError;
use crate::types::{
    AccountId, Amount, BookingMethod, CommodityId, Cost, Position, Posting,
};

// ---------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Inventory {
    positions: Vec<Position>,
}

impl Inventory {
    pub fn new() -> Self {
        Self {
            positions: Vec::new(),
        }
    }

    pub fn add(&mut self, position: Position) {
        // Try to merge into an existing position with the same commodity and cost
        for existing in &mut self.positions {
            if existing.units.commodity_id == position.units.commodity_id
                && existing.cost == position.cost
            {
                existing.units.value += position.units.value;
                return;
            }
        }
        self.positions.push(position);
    }

    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    pub fn balance_for_commodity(&self, commodity_id: CommodityId) -> Decimal {
        self.positions
            .iter()
            .filter(|p| p.units.commodity_id == commodity_id)
            .map(|p| p.units.value)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// BookedLot
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BookedLot {
    pub units: Amount,
    pub cost: Cost,
}

// ---------------------------------------------------------------------------
// book_reduction - reduce inventory positions according to booking method
// ---------------------------------------------------------------------------

/// Book a cost-reducing posting against the inventory, returning the lots that
/// were consumed. The posting must have negative units (a reduction).
pub fn book_reduction(
    inventory: &mut Inventory,
    posting: &Posting,
    method: BookingMethod,
) -> Result<Vec<BookedLot>, Vec<ValidationError>> {
    let commodity_id = posting.units.commodity_id;
    let reduction = posting.units.value.abs(); // positive quantity to reduce

    match method {
        BookingMethod::Strict => book_strict(inventory, posting, commodity_id, reduction),
        BookingMethod::StrictWithSize => {
            book_strict_with_size(inventory, posting, commodity_id, reduction)
        }
        BookingMethod::Fifo => book_ordered(inventory, posting, commodity_id, reduction, Ordering::Fifo),
        BookingMethod::Lifo => book_ordered(inventory, posting, commodity_id, reduction, Ordering::Lifo),
        BookingMethod::Hifo => book_ordered(inventory, posting, commodity_id, reduction, Ordering::Hifo),
        BookingMethod::Average => book_average(inventory, posting, commodity_id, reduction),
        BookingMethod::None => book_none(inventory, posting, commodity_id, reduction),
    }
}

// ---------------------------------------------------------------------------
// Strict: match exactly one lot by commodity and cost
// ---------------------------------------------------------------------------

fn book_strict(
    inventory: &mut Inventory,
    posting: &Posting,
    commodity_id: CommodityId,
    reduction: Decimal,
) -> Result<Vec<BookedLot>, Vec<ValidationError>> {
    // Find lots matching commodity AND cost (if posting specifies a cost)
    let matching_indices: Vec<usize> = inventory
        .positions
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.units.commodity_id == commodity_id
                && p.cost.is_some()
                && match &posting.cost {
                    Some(pc) => p.cost.as_ref().unwrap().amount == pc.amount,
                    None => true,
                }
        })
        .map(|(i, _)| i)
        .collect();

    if matching_indices.is_empty() {
        return Err(vec![ValidationError::NoMatchingLot {
            posting_id: posting.id,
        }]);
    }

    if matching_indices.len() > 1 && posting.cost.is_none() {
        let matches: Vec<Position> = matching_indices
            .iter()
            .map(|&i| inventory.positions[i].clone())
            .collect();
        return Err(vec![ValidationError::AmbiguousLotMatch {
            posting_id: posting.id,
            matches,
        }]);
    }

    let idx = matching_indices[0];
    let lot_cost = inventory.positions[idx].cost.clone().unwrap();

    let booked = BookedLot {
        units: Amount {
            value: reduction,
            commodity_id,
        },
        cost: lot_cost,
    };

    inventory.positions[idx].units.value -= reduction;
    if inventory.positions[idx].units.value.is_zero() {
        inventory.positions.remove(idx);
    }

    Ok(vec![booked])
}

// ---------------------------------------------------------------------------
// StrictWithSize: match on commodity and unit amount
// ---------------------------------------------------------------------------

fn book_strict_with_size(
    inventory: &mut Inventory,
    posting: &Posting,
    commodity_id: CommodityId,
    reduction: Decimal,
) -> Result<Vec<BookedLot>, Vec<ValidationError>> {
    let matching_indices: Vec<usize> = inventory
        .positions
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.units.commodity_id == commodity_id
                && p.cost.is_some()
                && p.units.value == reduction
        })
        .map(|(i, _)| i)
        .collect();

    if matching_indices.is_empty() {
        return Err(vec![ValidationError::NoMatchingLot {
            posting_id: posting.id,
        }]);
    }

    if matching_indices.len() > 1 {
        let matches: Vec<Position> = matching_indices
            .iter()
            .map(|&i| inventory.positions[i].clone())
            .collect();
        return Err(vec![ValidationError::AmbiguousLotMatch {
            posting_id: posting.id,
            matches,
        }]);
    }

    let idx = matching_indices[0];
    let lot_cost = inventory.positions[idx].cost.clone().unwrap();

    let booked = BookedLot {
        units: Amount {
            value: reduction,
            commodity_id,
        },
        cost: lot_cost,
    };

    inventory.positions.remove(idx);

    Ok(vec![booked])
}

// ---------------------------------------------------------------------------
// Ordered booking: FIFO, LIFO, HIFO
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Ordering {
    Fifo,
    Lifo,
    Hifo,
}

fn book_ordered(
    inventory: &mut Inventory,
    posting: &Posting,
    commodity_id: CommodityId,
    reduction: Decimal,
    ordering: Ordering,
) -> Result<Vec<BookedLot>, Vec<ValidationError>> {
    // Collect indices of matching lots (same commodity, with cost)
    let mut matching: Vec<usize> = inventory
        .positions
        .iter()
        .enumerate()
        .filter(|(_, p)| p.units.commodity_id == commodity_id && p.cost.is_some())
        .map(|(i, _)| i)
        .collect();

    if matching.is_empty() {
        return Err(vec![ValidationError::NoMatchingLot {
            posting_id: posting.id,
        }]);
    }

    // Sort according to ordering
    match ordering {
        Ordering::Fifo => {
            matching.sort_by(|&a, &b| {
                let da = inventory.positions[a].cost.as_ref().unwrap().date;
                let db = inventory.positions[b].cost.as_ref().unwrap().date;
                da.cmp(&db)
            });
        }
        Ordering::Lifo => {
            matching.sort_by(|&a, &b| {
                let da = inventory.positions[a].cost.as_ref().unwrap().date;
                let db = inventory.positions[b].cost.as_ref().unwrap().date;
                db.cmp(&da)
            });
        }
        Ordering::Hifo => {
            matching.sort_by(|&a, &b| {
                let ca = &inventory.positions[a].cost.as_ref().unwrap().amount.value;
                let cb = &inventory.positions[b].cost.as_ref().unwrap().amount.value;
                cb.cmp(ca) // highest first
            });
        }
    }

    let mut remaining = reduction;
    let mut booked = Vec::new();
    let mut to_remove = Vec::new();

    for &idx in &matching {
        if remaining.is_zero() {
            break;
        }

        let pos = &mut inventory.positions[idx];
        let available = pos.units.value;
        let lot_cost = pos.cost.clone().unwrap();

        if available <= remaining {
            // consume entire lot
            booked.push(BookedLot {
                units: Amount {
                    value: available,
                    commodity_id,
                },
                cost: lot_cost,
            });
            remaining -= available;
            to_remove.push(idx);
        } else {
            // partial consumption
            booked.push(BookedLot {
                units: Amount {
                    value: remaining,
                    commodity_id,
                },
                cost: lot_cost,
            });
            pos.units.value -= remaining;
            remaining = Decimal::ZERO;
        }
    }

    // Remove fully consumed lots (reverse order to keep indices valid)
    to_remove.sort_unstable();
    for idx in to_remove.into_iter().rev() {
        inventory.positions.remove(idx);
    }

    Ok(booked)
}

// ---------------------------------------------------------------------------
// Average: weighted average cost, reduce proportionally
// ---------------------------------------------------------------------------

fn book_average(
    inventory: &mut Inventory,
    posting: &Posting,
    commodity_id: CommodityId,
    reduction: Decimal,
) -> Result<Vec<BookedLot>, Vec<ValidationError>> {
    let matching: Vec<usize> = inventory
        .positions
        .iter()
        .enumerate()
        .filter(|(_, p)| p.units.commodity_id == commodity_id && p.cost.is_some())
        .map(|(i, _)| i)
        .collect();

    if matching.is_empty() {
        return Err(vec![ValidationError::NoMatchingLot {
            posting_id: posting.id,
        }]);
    }

    // Compute weighted average cost
    let mut total_units = Decimal::ZERO;
    let mut total_cost_value = Decimal::ZERO;
    let mut cost_commodity_id = None;
    let mut cost_date = None;

    for &idx in &matching {
        let pos = &inventory.positions[idx];
        let cost = pos.cost.as_ref().unwrap();
        total_units += pos.units.value;
        total_cost_value += pos.units.value * cost.amount.value;
        if cost_commodity_id.is_none() {
            cost_commodity_id = Some(cost.amount.commodity_id);
            cost_date = Some(cost.date);
        }
    }

    let avg_cost_value = if total_units.is_zero() {
        Decimal::ZERO
    } else {
        total_cost_value / total_units
    };

    let avg_cost = Cost {
        amount: Amount {
            value: avg_cost_value,
            commodity_id: cost_commodity_id.unwrap(),
        },
        date: cost_date.unwrap(),
        label: None,
    };

    // Reduce each lot proportionally
    let mut to_remove = Vec::new();
    for &idx in &matching {
        let pos = &mut inventory.positions[idx];
        let fraction = if total_units.is_zero() {
            Decimal::ZERO
        } else {
            pos.units.value / total_units
        };
        let lot_reduction = reduction * fraction;
        pos.units.value -= lot_reduction;
        // Update cost to average
        pos.cost = Some(avg_cost.clone());
        if pos.units.value.is_zero() {
            to_remove.push(idx);
        }
    }

    to_remove.sort_unstable();
    for idx in to_remove.into_iter().rev() {
        inventory.positions.remove(idx);
    }

    Ok(vec![BookedLot {
        units: Amount {
            value: reduction,
            commodity_id,
        },
        cost: avg_cost,
    }])
}

// ---------------------------------------------------------------------------
// None: just reduce total units, no lot matching
// ---------------------------------------------------------------------------

fn book_none(
    inventory: &mut Inventory,
    posting: &Posting,
    commodity_id: CommodityId,
    reduction: Decimal,
) -> Result<Vec<BookedLot>, Vec<ValidationError>> {
    // Find any position with the same commodity (with or without cost)
    let matching: Vec<usize> = inventory
        .positions
        .iter()
        .enumerate()
        .filter(|(_, p)| p.units.commodity_id == commodity_id)
        .map(|(i, _)| i)
        .collect();

    if matching.is_empty() {
        return Err(vec![ValidationError::NoMatchingLot {
            posting_id: posting.id,
        }]);
    }

    // Reduce from the first matching position
    let mut remaining = reduction;
    let mut booked = Vec::new();
    let mut to_remove = Vec::new();

    for &idx in &matching {
        if remaining.is_zero() {
            break;
        }

        let pos = &mut inventory.positions[idx];
        let available = pos.units.value;

        let take = if available <= remaining {
            to_remove.push(idx);
            remaining -= available;
            available
        } else {
            pos.units.value -= remaining;
            let take = remaining;
            remaining = Decimal::ZERO;
            take
        };

        if let Some(cost) = pos.cost.clone() {
            booked.push(BookedLot {
                units: Amount {
                    value: take,
                    commodity_id,
                },
                cost,
            });
        }
    }

    to_remove.sort_unstable();
    for idx in to_remove.into_iter().rev() {
        inventory.positions.remove(idx);
    }

    Ok(booked)
}

// ---------------------------------------------------------------------------
// compute_inventory
// ---------------------------------------------------------------------------

/// Build an inventory for a single account by replaying all its postings in
/// order. Reductions (negative units where a cost exists in the inventory) are
/// booked according to the given booking method.
pub fn compute_inventory(
    account_id: AccountId,
    postings: &[Posting],
    booking_method: BookingMethod,
) -> Result<Inventory, Vec<ValidationError>> {
    let mut inventory = Inventory::new();
    let mut errors = Vec::new();

    let relevant: Vec<&Posting> = postings
        .iter()
        .filter(|p| p.account_id == account_id)
        .collect();

    for posting in relevant {
        let is_reduction = posting.units.value < Decimal::ZERO
            && inventory
                .positions
                .iter()
                .any(|p| p.units.commodity_id == posting.units.commodity_id && p.cost.is_some());

        if is_reduction {
            match book_reduction(&mut inventory, posting, booking_method) {
                Ok(_) => {}
                Err(errs) => errors.extend(errs),
            }
        } else {
            inventory.add(Position {
                units: posting.units.clone(),
                cost: posting.cost.clone(),
            });
        }
    }

    if errors.is_empty() {
        Ok(inventory)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ValidationError;
    use crate::types::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn cost(value: Decimal, commodity_id: i64, y: i32, m: u32, d: u32) -> Cost {
        Cost {
            amount: Amount { value, commodity_id: CommodityId(commodity_id) },
            date: date(y, m, d),
            label: None,
        }
    }

    fn position(units: Decimal, commodity_id: i64, cost: Option<Cost>) -> Position {
        Position {
            units: Amount { value: units, commodity_id: CommodityId(commodity_id) },
            cost,
        }
    }

    fn sell_posting(units: Decimal, commodity_id: i64, cost: Option<Cost>) -> Posting {
        Posting {
            id: PostingId(1),
            transaction_id: TransactionId(1),
            account_id: AccountId(1),
            units: Amount { value: units, commodity_id: CommodityId(commodity_id) },
            cost,
            price: None,
            metadata: HashMap::new(),
        }
    }

    // Verifies that adding a position to an empty inventory creates it.
    #[test]
    fn inventory_add_new_position() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, None));
        assert_eq!(inv.positions().len(), 1);
        assert_eq!(inv.positions()[0].units.value, dec!(10));
    }

    // Verifies that adding a position with the same commodity and cost
    // merges into the existing position rather than creating a duplicate.
    #[test]
    fn inventory_merge_same_commodity_and_cost() {
        let mut inv = Inventory::new();
        let c = cost(dec!(50), 2, 2024, 1, 1);
        inv.add(position(dec!(10), 1, Some(c.clone())));
        inv.add(position(dec!(5), 1, Some(c)));
        assert_eq!(inv.positions().len(), 1);
        assert_eq!(inv.positions()[0].units.value, dec!(15));
    }

    // Verifies that positions with different costs are kept separate,
    // which is essential for lot tracking.
    #[test]
    fn inventory_separate_lots_different_costs() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(60), 2, 2024, 2, 1))));
        assert_eq!(inv.positions().len(), 2);
    }

    // Verifies balance_for_commodity sums across all lots of that commodity.
    #[test]
    fn inventory_balance_for_commodity() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(5), 1, Some(cost(dec!(60), 2, 2024, 2, 1))));
        inv.add(position(dec!(20), 2, None));
        assert_eq!(inv.balance_for_commodity(CommodityId(1)), dec!(15));
        assert_eq!(inv.balance_for_commodity(CommodityId(2)), dec!(20));
    }

    // Strict booking: when only one lot matches, it should be consumed.
    #[test]
    fn book_strict_single_lot() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        let p = sell_posting(dec!(-5), 1, Some(cost(dec!(50), 2, 2024, 1, 1)));
        let result = book_reduction(&mut inv, &p, BookingMethod::Strict).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].units.value, dec!(5));
        assert_eq!(inv.positions()[0].units.value, dec!(5));
    }

    // Strict booking: when multiple lots exist and no cost is specified,
    // it should return an AmbiguousLotMatch error.
    #[test]
    fn book_strict_ambiguous() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(60), 2, 2024, 2, 1))));
        let p = sell_posting(dec!(-5), 1, None);
        let err = book_reduction(&mut inv, &p, BookingMethod::Strict).unwrap_err();
        assert!(matches!(&err[0], ValidationError::AmbiguousLotMatch { .. }));
    }

    // Strict booking: when no lot matches, it should return NoMatchingLot.
    #[test]
    fn book_strict_no_match() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        let p = sell_posting(dec!(-5), 1, Some(cost(dec!(99), 2, 2024, 1, 1)));
        let err = book_reduction(&mut inv, &p, BookingMethod::Strict).unwrap_err();
        assert!(matches!(&err[0], ValidationError::NoMatchingLot { .. }));
    }

    // StrictWithSize matches lots by exact unit amount, useful when lots
    // are distinguishable by size rather than cost basis.
    #[test]
    fn book_strict_with_size_matches_exact_amount() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(5), 1, Some(cost(dec!(60), 2, 2024, 2, 1))));
        let p = sell_posting(dec!(-5), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::StrictWithSize).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].cost.amount.value, dec!(60));
        assert_eq!(inv.positions().len(), 1);
    }

    // FIFO booking should consume the earliest lot first (by cost date).
    #[test]
    fn book_fifo_consumes_oldest_first() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(60), 2, 2024, 6, 1))));
        let p = sell_posting(dec!(-10), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::Fifo).unwrap();
        assert_eq!(result[0].cost.amount.value, dec!(50));
        assert_eq!(inv.positions().len(), 1);
        assert_eq!(inv.positions()[0].cost.as_ref().unwrap().amount.value, dec!(60));
    }

    // LIFO booking should consume the most recent lot first.
    #[test]
    fn book_lifo_consumes_newest_first() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(60), 2, 2024, 6, 1))));
        let p = sell_posting(dec!(-10), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::Lifo).unwrap();
        assert_eq!(result[0].cost.amount.value, dec!(60));
        assert_eq!(inv.positions().len(), 1);
        assert_eq!(inv.positions()[0].cost.as_ref().unwrap().amount.value, dec!(50));
    }

    // HIFO booking should consume the highest-cost lot first, which
    // maximizes realized losses for tax purposes.
    #[test]
    fn book_hifo_consumes_highest_cost_first() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(80), 2, 2024, 3, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(60), 2, 2024, 6, 1))));
        let p = sell_posting(dec!(-10), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::Hifo).unwrap();
        assert_eq!(result[0].cost.amount.value, dec!(80));
    }

    // FIFO partial consumption: when reducing fewer units than the oldest
    // lot holds, only part of that lot should be consumed.
    #[test]
    fn book_fifo_partial_consumption() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        let p = sell_posting(dec!(-3), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::Fifo).unwrap();
        assert_eq!(result[0].units.value, dec!(3));
        assert_eq!(inv.positions()[0].units.value, dec!(7));
    }

    // FIFO spanning multiple lots: a large reduction should consume
    // multiple lots in order until fulfilled.
    #[test]
    fn book_fifo_spans_multiple_lots() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(5), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(60), 2, 2024, 6, 1))));
        let p = sell_posting(dec!(-8), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::Fifo).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].units.value, dec!(5));
        assert_eq!(result[1].units.value, dec!(3));
        assert_eq!(inv.positions().len(), 1);
        assert_eq!(inv.positions()[0].units.value, dec!(7));
    }

    // Average booking should compute the weighted average cost across all
    // lots and reduce proportionally.
    #[test]
    fn book_average_computes_weighted_avg() {
        let mut inv = Inventory::new();
        // 10 units at $50 + 10 units at $70 = avg cost $60
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        inv.add(position(dec!(10), 1, Some(cost(dec!(70), 2, 2024, 6, 1))));
        let p = sell_posting(dec!(-10), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::Average).unwrap();
        assert_eq!(result[0].cost.amount.value, dec!(60));
    }

    // None booking: reduces inventory without requiring cost lot matching.
    // Useful for commodities where lot tracking isn't needed.
    #[test]
    fn book_none_reduces_without_lot_match() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        let p = sell_posting(dec!(-3), 1, None);
        let result = book_reduction(&mut inv, &p, BookingMethod::None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(inv.positions()[0].units.value, dec!(7));
    }

    // Verifies that reducing to exactly zero removes the position from
    // the inventory entirely (no zero-unit ghosts).
    #[test]
    fn book_fifo_full_consumption_removes_position() {
        let mut inv = Inventory::new();
        inv.add(position(dec!(10), 1, Some(cost(dec!(50), 2, 2024, 1, 1))));
        let p = sell_posting(dec!(-10), 1, None);
        book_reduction(&mut inv, &p, BookingMethod::Fifo).unwrap();
        assert!(inv.positions().is_empty());
    }

    // compute_inventory should replay postings and build up inventory
    // correctly for a simple buy-then-sell scenario.
    #[test]
    fn compute_inventory_buy_and_sell() {
        let buy = Posting {
            id: PostingId(1),
            transaction_id: TransactionId(1),
            account_id: AccountId(1),
            units: Amount { value: dec!(10), commodity_id: CommodityId(1) },
            cost: Some(cost(dec!(50), 2, 2024, 1, 1)),
            price: None,
            metadata: HashMap::new(),
        };
        let sell = Posting {
            id: PostingId(2),
            transaction_id: TransactionId(2),
            account_id: AccountId(1),
            units: Amount { value: dec!(-3), commodity_id: CommodityId(1) },
            cost: None,
            price: None,
            metadata: HashMap::new(),
        };
        let inv = compute_inventory(AccountId(1), &[buy, sell], BookingMethod::Fifo).unwrap();
        assert_eq!(inv.balance_for_commodity(CommodityId(1)), dec!(7));
    }

    // compute_inventory should filter postings to only the specified
    // account, ignoring postings for other accounts.
    #[test]
    fn compute_inventory_filters_by_account() {
        let p1 = Posting {
            id: PostingId(1),
            transaction_id: TransactionId(1),
            account_id: AccountId(1),
            units: Amount { value: dec!(10), commodity_id: CommodityId(1) },
            cost: None,
            price: None,
            metadata: HashMap::new(),
        };
        let p2 = Posting {
            id: PostingId(2),
            transaction_id: TransactionId(1),
            account_id: AccountId(2),
            units: Amount { value: dec!(20), commodity_id: CommodityId(1) },
            cost: None,
            price: None,
            metadata: HashMap::new(),
        };
        let inv = compute_inventory(AccountId(1), &[p1, p2], BookingMethod::Strict).unwrap();
        assert_eq!(inv.balance_for_commodity(CommodityId(1)), dec!(10));
    }
}
