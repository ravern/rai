use chrono::NaiveDate;
use rai_core::types::*;

use crate::error::DbError;

/// A row value returned from a raw SQL query.
#[derive(Debug, Clone)]
pub enum QueryValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

/// The result of a raw SQL query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryValue>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuditEventId(pub i64);

impl std::fmt::Display for AuditEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEventKind {
    Baseline,
    Mutation,
    Undo,
    Redo,
}

impl AuditEventKind {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "baseline" => Some(Self::Baseline),
            "mutation" => Some(Self::Mutation),
            "undo" => Some(Self::Undo),
            "redo" => Some(Self::Redo),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Mutation => "mutation",
            Self::Undo => "undo",
            Self::Redo => "redo",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub id: AuditEventId,
    pub created_at: String,
    pub kind: AuditEventKind,
    pub operation: String,
    pub entity_type: String,
    pub entity_id: Option<i64>,
    pub summary: String,
    pub target_event_id: Option<AuditEventId>,
    pub before_json: Option<String>,
    pub after_json: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub entity_type: Option<String>,
    pub entity_id: Option<i64>,
    pub limit: Option<usize>,
}

/// Core storage operations. Each method maps to CRUD on the public schema.
pub trait StorageProvider {
    /// Create all tables, indexes, and views if they do not already exist.
    fn initialize(&mut self) -> Result<(), DbError>;

    // ── Commodities ──────────────────────────────────────────────────

    fn create_commodity(&mut self, commodity: &NewCommodity) -> Result<Commodity, DbError>;
    fn get_commodity(&self, id: CommodityId) -> Result<Option<Commodity>, DbError>;
    fn get_commodity_by_name(&self, name: &str) -> Result<Option<Commodity>, DbError>;
    fn list_commodities(&self) -> Result<Vec<Commodity>, DbError>;
    fn update_commodity(
        &mut self,
        id: CommodityId,
        update: &CommodityUpdate,
    ) -> Result<Commodity, DbError>;
    fn delete_commodity(&mut self, id: CommodityId) -> Result<(), DbError>;

    // ── Accounts ─────────────────────────────────────────────────────

    fn create_account(&mut self, account: &NewAccount) -> Result<Account, DbError>;
    fn get_account(&self, id: AccountId) -> Result<Option<Account>, DbError>;
    fn get_account_by_name(&self, name: &str) -> Result<Option<Account>, DbError>;
    fn list_accounts(&self, filter: &AccountFilter) -> Result<Vec<Account>, DbError>;
    fn update_account(
        &mut self,
        id: AccountId,
        update: &AccountUpdate,
    ) -> Result<Account, DbError>;
    fn open_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account, DbError>;
    fn close_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account, DbError>;
    fn delete_account(&mut self, id: AccountId) -> Result<(), DbError>;

    // ── Transactions ─────────────────────────────────────────────────

    fn create_transaction(&mut self, tx: &NewTransaction) -> Result<Transaction, DbError>;
    fn get_transaction(&self, id: TransactionId) -> Result<Option<Transaction>, DbError>;
    fn list_transactions(
        &self,
        filter: &TransactionFilter,
    ) -> Result<Vec<Transaction>, DbError>;
    fn update_transaction(
        &mut self,
        id: TransactionId,
        update: &TransactionUpdate,
    ) -> Result<Transaction, DbError>;
    fn delete_transaction(&mut self, id: TransactionId) -> Result<(), DbError>;

    // ── Prices ───────────────────────────────────────────────────────

    fn create_price(&mut self, price: &NewPrice) -> Result<Price, DbError>;
    fn get_price(
        &self,
        commodity: CommodityId,
        target: CommodityId,
        date: NaiveDate,
    ) -> Result<Option<Price>, DbError>;
    fn list_prices(&self, filter: &PriceFilter) -> Result<Vec<Price>, DbError>;
    fn delete_price(&mut self, id: PriceId) -> Result<(), DbError>;

    // ── Balance assertions ───────────────────────────────────────────

    fn create_balance_assertion(
        &mut self,
        assertion: &NewBalanceAssertion,
    ) -> Result<BalanceAssertion, DbError>;
    fn list_balance_assertions(
        &self,
        filter: &BalanceAssertionFilter,
    ) -> Result<Vec<BalanceAssertion>, DbError>;
    fn delete_balance_assertion(&mut self, id: BalanceAssertionId) -> Result<(), DbError>;

    // ── Raw query ────────────────────────────────────────────────────

    fn query_raw(&self, sql: &str) -> Result<QueryResult, DbError>;

    // ── Audit log ───────────────────────────────────────────────────

    fn list_audit_events(&self, filter: &AuditFilter) -> Result<Vec<AuditEvent>, DbError>;
    fn get_audit_event(&self, id: AuditEventId) -> Result<Option<AuditEvent>, DbError>;
    fn undo_last_audit_event(&mut self) -> Result<AuditEvent, DbError>;
    fn redo_last_audit_event(&mut self) -> Result<AuditEvent, DbError>;
}
