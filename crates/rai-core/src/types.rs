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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Verifies that account types are correctly parsed from hierarchical
    // account names (e.g. "Assets:Bank:Checking" -> Assets).
    #[test]
    fn account_type_from_name_valid() {
        assert_eq!(AccountType::from_name("Assets:Bank:Checking"), Some(AccountType::Assets));
        assert_eq!(AccountType::from_name("Liabilities:CreditCard"), Some(AccountType::Liabilities));
        assert_eq!(AccountType::from_name("Income:Salary"), Some(AccountType::Income));
        assert_eq!(AccountType::from_name("Expenses:Food"), Some(AccountType::Expenses));
        assert_eq!(AccountType::from_name("Equity:OpeningBalances"), Some(AccountType::Equity));
    }

    // Verifies that parsing is case-insensitive (e.g. "ASSETS" works).
    #[test]
    fn account_type_from_name_case_insensitive() {
        assert_eq!(AccountType::from_name("ASSETS:Bank"), Some(AccountType::Assets));
        assert_eq!(AccountType::from_name("assets:bank"), Some(AccountType::Assets));
        assert_eq!(AccountType::from_name("Assets"), Some(AccountType::Assets));
    }

    // Verifies that invalid account name prefixes return None.
    #[test]
    fn account_type_from_name_invalid() {
        assert_eq!(AccountType::from_name("Unknown:Account"), None);
        assert_eq!(AccountType::from_name(""), None);
    }

    // Verifies the as_str round-trip for AccountType.
    #[test]
    fn account_type_as_str() {
        assert_eq!(AccountType::Assets.as_str(), "assets");
        assert_eq!(AccountType::Equity.as_str(), "equity");
    }

    // Verifies all valid BookingMethod string representations parse correctly,
    // including alternative forms like "avg" for Average.
    #[test]
    fn booking_method_from_str_valid() {
        assert_eq!(BookingMethod::from_str("strict"), Some(BookingMethod::Strict));
        assert_eq!(BookingMethod::from_str("strict_with_size"), Some(BookingMethod::StrictWithSize));
        assert_eq!(BookingMethod::from_str("strictwithsize"), Some(BookingMethod::StrictWithSize));
        assert_eq!(BookingMethod::from_str("fifo"), Some(BookingMethod::Fifo));
        assert_eq!(BookingMethod::from_str("lifo"), Some(BookingMethod::Lifo));
        assert_eq!(BookingMethod::from_str("hifo"), Some(BookingMethod::Hifo));
        assert_eq!(BookingMethod::from_str("average"), Some(BookingMethod::Average));
        assert_eq!(BookingMethod::from_str("avg"), Some(BookingMethod::Average));
        assert_eq!(BookingMethod::from_str("none"), Some(BookingMethod::None));
    }

    // Verifies that invalid BookingMethod strings return None.
    #[test]
    fn booking_method_from_str_invalid() {
        assert_eq!(BookingMethod::from_str("unknown"), None);
    }

    // Verifies the default booking method is Strict.
    #[test]
    fn booking_method_default() {
        assert_eq!(BookingMethod::default(), BookingMethod::Strict);
    }

    // Verifies all TransactionStatus string representations parse correctly,
    // including the symbol shortcuts (*, !, #).
    #[test]
    fn transaction_status_from_str() {
        assert_eq!(TransactionStatus::from_str("completed"), Some(TransactionStatus::Completed));
        assert_eq!(TransactionStatus::from_str("*"), Some(TransactionStatus::Completed));
        assert_eq!(TransactionStatus::from_str("pending"), Some(TransactionStatus::Pending));
        assert_eq!(TransactionStatus::from_str("!"), Some(TransactionStatus::Pending));
        assert_eq!(TransactionStatus::from_str("flagged"), Some(TransactionStatus::Flagged));
        assert_eq!(TransactionStatus::from_str("#"), Some(TransactionStatus::Flagged));
        assert_eq!(TransactionStatus::from_str("unknown"), None);
    }

    // Verifies that Amount addition works for same-commodity amounts.
    #[test]
    fn amount_add() {
        let a = Amount { value: dec!(10), commodity_id: CommodityId(1) };
        let b = Amount { value: dec!(5), commodity_id: CommodityId(1) };
        let result = a + b;
        assert_eq!(result.value, dec!(15));
        assert_eq!(result.commodity_id, CommodityId(1));
    }

    // Verifies that adding amounts of different commodities panics, since
    // mixing currencies is a programming error.
    #[test]
    #[should_panic(expected = "Cannot add amounts of different commodities")]
    fn amount_add_different_commodities_panics() {
        let a = Amount { value: dec!(10), commodity_id: CommodityId(1) };
        let b = Amount { value: dec!(5), commodity_id: CommodityId(2) };
        let _ = a + b;
    }

    // Verifies that Amount subtraction works correctly.
    #[test]
    fn amount_sub() {
        let a = Amount { value: dec!(10), commodity_id: CommodityId(1) };
        let b = Amount { value: dec!(3), commodity_id: CommodityId(1) };
        let result = a - b;
        assert_eq!(result.value, dec!(7));
    }

    // Verifies that negating an Amount flips the sign.
    #[test]
    fn amount_negate() {
        let a = Amount { value: dec!(10), commodity_id: CommodityId(1) };
        assert_eq!(a.negate().value, dec!(-10));
        assert_eq!((-a).value, dec!(-10));
    }

    // Verifies that is_zero correctly detects zero amounts.
    #[test]
    fn amount_is_zero() {
        assert!(Amount { value: dec!(0), commodity_id: CommodityId(1) }.is_zero());
        assert!(!Amount { value: dec!(1), commodity_id: CommodityId(1) }.is_zero());
    }

    // Verifies that scalar multiplication works (used for cost calculations).
    #[test]
    fn amount_mul_scalar() {
        let a = Amount { value: dec!(10), commodity_id: CommodityId(1) };
        let result = a.mul_scalar(dec!(3));
        assert_eq!(result.value, dec!(30));
        assert_eq!(result.commodity_id, CommodityId(1));
    }

    // Verifies that NewAccount correctly derives AccountType from its name.
    #[test]
    fn new_account_derives_type() {
        let acct = NewAccount {
            name: "Assets:Bank".to_string(),
            opened_at: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            currencies: vec![],
            booking_method: BookingMethod::Strict,
            metadata: HashMap::new(),
        };
        assert_eq!(acct.account_type(), Some(AccountType::Assets));
    }
}
