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
