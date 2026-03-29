use chrono::NaiveDate;

use crate::types::{AccountId, Amount, BalanceAssertionId, CommodityId, Position, PostingId, TransactionId};

#[derive(Debug, Clone, thiserror::Error)]
pub enum ValidationError {
    #[error("Transaction {transaction_id} does not balance: residuals {residuals:?}")]
    TransactionDoesNotBalance {
        transaction_id: TransactionId,
        residuals: Vec<Amount>,
    },

    #[error("Balance assertion {assertion_id} failed: expected {expected:?}, got {actual:?}")]
    BalanceAssertionFailed {
        assertion_id: BalanceAssertionId,
        expected: Amount,
        actual: Amount,
    },

    #[error("Account {account_id} not open on {date}")]
    AccountNotOpen {
        account_id: AccountId,
        date: NaiveDate,
    },

    #[error("Currency {commodity_id} not allowed on account {account_id}")]
    CurrencyNotAllowed {
        account_id: AccountId,
        commodity_id: CommodityId,
    },

    #[error("Ambiguous lot match for posting {posting_id}")]
    AmbiguousLotMatch {
        posting_id: PostingId,
        matches: Vec<Position>,
    },

    #[error("No matching lot for posting {posting_id}")]
    NoMatchingLot {
        posting_id: PostingId,
    },
}
