use std::collections::HashMap;
use std::fmt;
use std::ops::{Add, Neg, Sub};

use chrono::{NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Id newtypes
// ---------------------------------------------------------------------------

macro_rules! id_newtype {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub i64);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_newtype!(CommodityId);
id_newtype!(AccountId);
id_newtype!(TransactionId);
id_newtype!(PostingId);
id_newtype!(PriceId);
id_newtype!(BalanceAssertionId);

// ---------------------------------------------------------------------------
// Commodity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Commodity {
    pub id: CommodityId,
    pub name: String,
    pub precision: u8,
    pub metadata: Metadata,
}

// ---------------------------------------------------------------------------
// AccountType
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccountType {
    Assets,
    Liabilities,
    Income,
    Expenses,
    Equity,
}

impl AccountType {
    /// Parse account type from the first segment of an account name,
    /// e.g. "Assets:Bank:Checking" -> Assets.
    pub fn from_name(name: &str) -> Option<Self> {
        let first = name.split(':').next()?;
        match first.to_lowercase().as_str() {
            "assets" => Some(Self::Assets),
            "liabilities" => Some(Self::Liabilities),
            "income" => Some(Self::Income),
            "expenses" => Some(Self::Expenses),
            "equity" => Some(Self::Equity),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Assets => "assets",
            Self::Liabilities => "liabilities",
            Self::Income => "income",
            Self::Expenses => "expenses",
            Self::Equity => "equity",
        }
    }
}

// ---------------------------------------------------------------------------
// BookingMethod
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum BookingMethod {
    #[default]
    Strict,
    StrictWithSize,
    Fifo,
    Lifo,
    Hifo,
    Average,
    None,
}

impl BookingMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "strict" => Some(Self::Strict),
            "strict_with_size" | "strictwithsize" => Some(Self::StrictWithSize),
            "fifo" => Some(Self::Fifo),
            "lifo" => Some(Self::Lifo),
            "hifo" => Some(Self::Hifo),
            "average" | "avg" => Some(Self::Average),
            "none" => Some(Self::None),
            _ => Option::None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::StrictWithSize => "strict_with_size",
            Self::Fifo => "fifo",
            Self::Lifo => "lifo",
            Self::Hifo => "hifo",
            Self::Average => "average",
            Self::None => "none",
        }
    }
}

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Account {
    pub id: AccountId,
    pub name: String,
    pub account_type: AccountType,
    pub is_open: bool,
    pub opened_at: NaiveDate,
    pub closed_at: Option<NaiveDate>,
    pub currencies: Vec<CommodityId>,
    pub booking_method: BookingMethod,
    pub metadata: Metadata,
}

// ---------------------------------------------------------------------------
// Amount
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Amount {
    pub value: Decimal,
    pub commodity_id: CommodityId,
}

impl Amount {
    pub fn is_zero(&self) -> bool {
        self.value.is_zero()
    }

    pub fn negate(&self) -> Amount {
        Amount {
            value: -self.value,
            commodity_id: self.commodity_id,
        }
    }

    pub fn mul_scalar(&self, scalar: Decimal) -> Amount {
        Amount {
            value: self.value * scalar,
            commodity_id: self.commodity_id,
        }
    }
}

impl Add for Amount {
    type Output = Amount;
    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.commodity_id, rhs.commodity_id,
            "Cannot add amounts of different commodities"
        );
        Amount {
            value: self.value + rhs.value,
            commodity_id: self.commodity_id,
        }
    }
}

impl Sub for Amount {
    type Output = Amount;
    fn sub(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.commodity_id, rhs.commodity_id,
            "Cannot subtract amounts of different commodities"
        );
        Amount {
            value: self.value - rhs.value,
            commodity_id: self.commodity_id,
        }
    }
}

impl Neg for Amount {
    type Output = Amount;
    fn neg(self) -> Self::Output {
        Amount {
            value: -self.value,
            commodity_id: self.commodity_id,
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionStatus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionStatus {
    Completed,
    Pending,
    Flagged,
}

impl TransactionStatus {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "completed" | "*" => Some(Self::Completed),
            "pending" | "!" => Some(Self::Pending),
            "flagged" | "#" => Some(Self::Flagged),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Pending => "pending",
            Self::Flagged => "flagged",
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: TransactionId,
    pub date: NaiveDate,
    pub time: Option<NaiveTime>,
    pub status: TransactionStatus,
    pub payee: Option<String>,
    pub narration: Option<String>,
    pub tags: Vec<String>,
    pub links: Vec<String>,
    pub postings: Vec<Posting>,
    pub metadata: Metadata,
}

// ---------------------------------------------------------------------------
// Posting
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Posting {
    pub id: PostingId,
    pub transaction_id: TransactionId,
    pub account_id: AccountId,
    pub units: Amount,
    pub cost: Option<Cost>,
    pub price: Option<Amount>,
    pub metadata: Metadata,
}

// ---------------------------------------------------------------------------
// Cost
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cost {
    pub amount: Amount,
    pub date: NaiveDate,
    pub label: Option<String>,
}

// ---------------------------------------------------------------------------
// Position
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub units: Amount,
    pub cost: Option<Cost>,
}

// ---------------------------------------------------------------------------
// Price
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Price {
    pub id: PriceId,
    pub date: NaiveDate,
    pub commodity_id: CommodityId,
    pub target_commodity_id: CommodityId,
    pub value: Decimal,
}

// ---------------------------------------------------------------------------
// BalanceAssertion
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BalanceAssertion {
    pub id: BalanceAssertionId,
    pub date: NaiveDate,
    pub account_id: AccountId,
    pub expected: Amount,
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

pub type Metadata = HashMap<String, MetadataValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataValue {
    String(String),
    Number(Decimal),
    Date(NaiveDate),
    Bool(bool),
}

// ---------------------------------------------------------------------------
// "New" types (for creation, without id fields)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct NewCommodity {
    pub name: String,
    pub precision: u8,
    pub metadata: Metadata,
}

#[derive(Debug, Clone)]
pub struct NewAccount {
    pub name: String,
    pub opened_at: NaiveDate,
    pub currencies: Vec<CommodityId>,
    pub booking_method: BookingMethod,
    pub metadata: Metadata,
}

impl NewAccount {
    /// Derive the account type from the account name.
    pub fn account_type(&self) -> Option<AccountType> {
        AccountType::from_name(&self.name)
    }
}

#[derive(Debug, Clone)]
pub struct NewPosting {
    pub account_id: AccountId,
    pub units: Amount,
    pub cost: Option<Cost>,
    pub price: Option<Amount>,
    pub metadata: Metadata,
}

#[derive(Debug, Clone)]
pub struct NewTransaction {
    pub date: NaiveDate,
    pub time: Option<NaiveTime>,
    pub status: TransactionStatus,
    pub payee: Option<String>,
    pub narration: Option<String>,
    pub tags: Vec<String>,
    pub links: Vec<String>,
    pub postings: Vec<NewPosting>,
    pub metadata: Metadata,
}

#[derive(Debug, Clone)]
pub struct NewPrice {
    pub date: NaiveDate,
    pub commodity_id: CommodityId,
    pub target_commodity_id: CommodityId,
    pub value: Decimal,
}

#[derive(Debug, Clone)]
pub struct NewBalanceAssertion {
    pub date: NaiveDate,
    pub account_id: AccountId,
    pub expected: Amount,
}

// ---------------------------------------------------------------------------
// "Update" types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct CommodityUpdate {
    pub precision: Option<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct AccountUpdate {
    pub booking_method: Option<BookingMethod>,
    pub currencies: Option<Vec<CommodityId>>,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionUpdate {
    pub date: Option<NaiveDate>,
    pub time: Option<Option<NaiveTime>>,
    pub status: Option<TransactionStatus>,
    pub payee: Option<Option<String>>,
    pub narration: Option<Option<String>>,
}

// ---------------------------------------------------------------------------
// Filter types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct AccountFilter {
    pub account_type: Option<AccountType>,
    pub is_open: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionFilter {
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    pub account_id: Option<AccountId>,
    pub payee: Option<String>,
    pub tag: Option<String>,
    pub status: Option<TransactionStatus>,
}

#[derive(Debug, Clone, Default)]
pub struct PriceFilter {
    pub commodity_id: Option<CommodityId>,
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
}

#[derive(Debug, Clone, Default)]
pub struct BalanceAssertionFilter {
    pub account_id: Option<AccountId>,
}
