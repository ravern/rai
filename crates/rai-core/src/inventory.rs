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
