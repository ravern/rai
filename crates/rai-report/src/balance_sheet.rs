use std::collections::HashMap;

use chrono::NaiveDate;
use rust_decimal::Decimal;

use rai_core::types::{Account, AccountType, Amount, CommodityId};

use crate::conversion::convert_amounts;
use crate::data::LedgerData;

pub struct BalanceSheetParams {
    pub as_of: NaiveDate,
    pub currency: Option<CommodityId>,
}

#[derive(Debug, Clone)]
pub struct AccountBalance {
    pub account: Account,
    pub balances: Vec<Amount>,
}

pub struct BalanceSheetResult {
    pub as_of: NaiveDate,
    pub assets: Vec<AccountBalance>,
    pub liabilities: Vec<AccountBalance>,
    pub equity: Vec<AccountBalance>,
    pub total_assets: Vec<Amount>,
    pub total_liabilities: Vec<Amount>,
    pub total_equity: Vec<Amount>,
}

pub fn generate_balance_sheet(params: &BalanceSheetParams, data: &LedgerData) -> BalanceSheetResult {
    // Build a map of account_id -> HashMap<CommodityId, Decimal>
    let mut balances_by_account: HashMap<i64, HashMap<CommodityId, Decimal>> = HashMap::new();

    for txn in &data.transactions {
        if txn.date > params.as_of {
            continue;
        }
        for posting in &txn.postings {
            let entry = balances_by_account
                .entry(posting.account_id.0)
                .or_default();
            *entry
                .entry(posting.units.commodity_id)
                .or_insert(Decimal::ZERO) += posting.units.value;
        }
    }

    // Build account index
    let account_map: HashMap<i64, &Account> = data
        .accounts
        .iter()
        .map(|a| (a.id.0, a))
        .collect();

    let mut assets = Vec::new();
    let mut liabilities = Vec::new();
    let mut equity = Vec::new();

    for (account_id, commodity_balances) in &balances_by_account {
        let account = match account_map.get(account_id) {
            Some(a) => a,
            None => continue,
        };

        let balances: Vec<Amount> = commodity_balances
            .iter()
            .filter(|(_, v)| !v.is_zero())
            .map(|(&commodity_id, &value)| Amount {
                value,
                commodity_id,
            })
            .collect();

        if balances.is_empty() {
            continue;
        }

        let balances = if let Some(target) = params.currency {
            convert_amounts(&balances, target, &data.prices)
        } else {
            balances
        };

        match account.account_type {
            AccountType::Assets => {
                assets.push(AccountBalance {
                    account: (*account).clone(),
                    balances,
                });
            }
            AccountType::Liabilities | AccountType::Equity => {
                // Liabilities and equity have credit-normal (negative) balances.
                // Negate for display so they show as positive.
                let balances = balances
                    .into_iter()
                    .map(|a| Amount {
                        value: -a.value,
                        commodity_id: a.commodity_id,
                    })
                    .collect();
                let ab = AccountBalance {
                    account: (*account).clone(),
                    balances,
                };
                match account.account_type {
                    AccountType::Liabilities => liabilities.push(ab),
                    AccountType::Equity => equity.push(ab),
                    _ => unreachable!(),
                }
            }
            _ => {}
        }
    }

    // Sort by account name for deterministic output
    assets.sort_by(|a, b| a.account.name.cmp(&b.account.name));
    liabilities.sort_by(|a, b| a.account.name.cmp(&b.account.name));
    equity.sort_by(|a, b| a.account.name.cmp(&b.account.name));

    let total_assets = sum_balances(&assets, params.currency, &data.prices);
    let total_liabilities = sum_balances(&liabilities, params.currency, &data.prices);
    let total_equity = sum_balances(&equity, params.currency, &data.prices);

    BalanceSheetResult {
        as_of: params.as_of,
        assets,
        liabilities,
        equity,
        total_assets,
        total_liabilities,
        total_equity,
    }
}

fn sum_balances(
    account_balances: &[AccountBalance],
    currency: Option<CommodityId>,
    prices: &[rai_core::types::Price],
) -> Vec<Amount> {
    let mut totals: HashMap<CommodityId, Decimal> = HashMap::new();

    for ab in account_balances {
        for amount in &ab.balances {
            *totals
                .entry(amount.commodity_id)
                .or_insert(Decimal::ZERO) += amount.value;
        }
    }

    let amounts: Vec<Amount> = totals
        .into_iter()
        .map(|(commodity_id, value)| Amount {
            value,
            commodity_id,
        })
        .collect();

    if let Some(target) = currency {
        convert_amounts(&amounts, target, prices)
    } else {
        amounts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::LedgerData;
    use rai_core::types::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn make_account(id: i64, name: &str) -> Account {
        Account {
            id: AccountId(id),
            name: name.into(),
            account_type: AccountType::from_name(name).unwrap(),
            is_open: true,
            opened_at: date(2024, 1, 1),
            closed_at: None,
            currencies: vec![],
            booking_method: BookingMethod::Strict,
            metadata: HashMap::new(),
        }
    }

    fn make_posting(id: i64, tx_id: i64, account_id: i64, value: Decimal, commodity_id: i64) -> Posting {
        Posting {
            id: PostingId(id),
            transaction_id: TransactionId(tx_id),
            account_id: AccountId(account_id),
            units: Amount { value, commodity_id: CommodityId(commodity_id) },
            cost: None,
            price: None,
            metadata: HashMap::new(),
        }
    }

    fn make_tx(id: i64, d: NaiveDate, postings: Vec<Posting>) -> Transaction {
        Transaction {
            id: TransactionId(id),
            date: d,
            time: None,
            status: TransactionStatus::Completed,
            payee: None,
            narration: None,
            tags: vec![],
            links: vec![],
            postings,
            metadata: HashMap::new(),
        }
    }

    // Verifies that asset accounts show positive balances and liability
    // accounts show negated (credit-normal to positive) balances.
    #[test]
    fn balance_sheet_groups_accounts_correctly() {
        let data = LedgerData {
            accounts: vec![
                make_account(1, "Assets:Bank"),
                make_account(2, "Liabilities:CreditCard"),
                make_account(3, "Equity:Opening"),
            ],
            transactions: vec![
                make_tx(1, date(2024, 3, 1), vec![
                    make_posting(1, 1, 1, dec!(1000), 1),
                    make_posting(2, 1, 3, dec!(-1000), 1),
                ]),
                make_tx(2, date(2024, 3, 15), vec![
                    make_posting(3, 2, 2, dec!(-500), 1),
                    make_posting(4, 2, 1, dec!(500), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = BalanceSheetParams { as_of: date(2024, 12, 31), currency: None };
        let result = generate_balance_sheet(&params, &data);

        assert_eq!(result.assets.len(), 1);
        assert_eq!(result.assets[0].balances[0].value, dec!(1500));
        assert_eq!(result.liabilities.len(), 1);
        // Liabilities are negated for display: -(-500) = 500
        assert_eq!(result.liabilities[0].balances[0].value, dec!(500));
        assert_eq!(result.equity.len(), 1);
        // Equity negated: -(-1000) = 1000
        assert_eq!(result.equity[0].balances[0].value, dec!(1000));
    }

    // Transactions after the as_of date should be excluded from the
    // balance sheet.
    #[test]
    fn balance_sheet_respects_as_of_date() {
        let data = LedgerData {
            accounts: vec![
                make_account(1, "Assets:Bank"),
                make_account(2, "Income:Salary"),
            ],
            transactions: vec![
                make_tx(1, date(2024, 1, 1), vec![
                    make_posting(1, 1, 1, dec!(1000), 1),
                    make_posting(2, 1, 2, dec!(-1000), 1),
                ]),
                make_tx(2, date(2024, 7, 1), vec![
                    make_posting(3, 2, 1, dec!(500), 1),
                    make_posting(4, 2, 2, dec!(-500), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = BalanceSheetParams { as_of: date(2024, 3, 1), currency: None };
        let result = generate_balance_sheet(&params, &data);
        // Only the first transaction should be included
        assert_eq!(result.assets[0].balances[0].value, dec!(1000));
    }

    // Income/Expense accounts should not appear on the balance sheet
    // (they belong on the income statement).
    #[test]
    fn balance_sheet_excludes_income_expense() {
        let data = LedgerData {
            accounts: vec![
                make_account(1, "Assets:Bank"),
                make_account(2, "Income:Salary"),
                make_account(3, "Expenses:Food"),
            ],
            transactions: vec![
                make_tx(1, date(2024, 1, 1), vec![
                    make_posting(1, 1, 1, dec!(1000), 1),
                    make_posting(2, 1, 2, dec!(-1000), 1),
                ]),
                make_tx(2, date(2024, 1, 15), vec![
                    make_posting(3, 2, 3, dec!(50), 1),
                    make_posting(4, 2, 1, dec!(-50), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = BalanceSheetParams { as_of: date(2024, 12, 31), currency: None };
        let result = generate_balance_sheet(&params, &data);
        assert!(result.assets.len() == 1);
        assert!(result.liabilities.is_empty());
        assert!(result.equity.is_empty());
    }
}
