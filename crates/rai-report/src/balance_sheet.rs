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
