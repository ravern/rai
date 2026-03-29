use chrono::NaiveDate;
use rai_core::types::{Account, BalanceAssertion, Commodity, Price, Transaction};

#[derive(Debug, Clone)]
pub struct ReportPeriod {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
}

pub struct LedgerData {
    pub transactions: Vec<Transaction>,
    pub accounts: Vec<Account>,
    pub commodities: Vec<Commodity>,
    pub prices: Vec<Price>,
    pub balance_assertions: Vec<BalanceAssertion>,
}
