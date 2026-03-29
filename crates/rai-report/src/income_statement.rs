use std::collections::HashMap;

use rust_decimal::Decimal;

use rai_core::types::{Account, AccountType, Amount, CommodityId};

use crate::balance_sheet::AccountBalance;
use crate::conversion::convert_amounts;
use crate::data::{LedgerData, ReportPeriod};

pub struct IncomeStatementParams {
    pub period: ReportPeriod,
    pub currency: Option<CommodityId>,
}

pub struct IncomeStatementResult {
    pub period: ReportPeriod,
    pub income: Vec<AccountBalance>,
    pub expenses: Vec<AccountBalance>,
    pub total_income: Vec<Amount>,
    pub total_expenses: Vec<Amount>,
    pub net_income: Vec<Amount>,
}

pub fn generate_income_statement(
    params: &IncomeStatementParams,
    data: &LedgerData,
) -> IncomeStatementResult {
    let mut balances_by_account: HashMap<i64, HashMap<CommodityId, Decimal>> = HashMap::new();

    for txn in &data.transactions {
        if let Some(start) = params.period.start {
            if txn.date < start {
                continue;
            }
        }
        if let Some(end) = params.period.end {
            if txn.date > end {
                continue;
            }
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

    let account_map: HashMap<i64, &Account> = data
        .accounts
        .iter()
        .map(|a| (a.id.0, a))
        .collect();

    let mut income = Vec::new();
    let mut expenses = Vec::new();

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
            AccountType::Income => {
                // Income accounts have credit-normal (negative) balances.
                // Negate for display so income shows as positive.
                let balances = balances
                    .into_iter()
                    .map(|a| Amount {
                        value: -a.value,
                        commodity_id: a.commodity_id,
                    })
                    .collect();
                income.push(AccountBalance {
                    account: (*account).clone(),
                    balances,
                });
            }
            AccountType::Expenses => {
                expenses.push(AccountBalance {
                    account: (*account).clone(),
                    balances,
                });
            }
            _ => {}
        }
    }

    income.sort_by(|a, b| a.account.name.cmp(&b.account.name));
    expenses.sort_by(|a, b| a.account.name.cmp(&b.account.name));

    let total_income = sum_account_balances(&income, params.currency, &data.prices);
    let total_expenses = sum_account_balances(&expenses, params.currency, &data.prices);
    let net_income = compute_net_income(&total_income, &total_expenses);

    IncomeStatementResult {
        period: ReportPeriod {
            start: params.period.start,
            end: params.period.end,
        },
        income,
        expenses,
        total_income,
        total_expenses,
        net_income,
    }
}

fn sum_account_balances(
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

/// Net income = total_income - total_expenses per commodity.
/// Both income and expenses are already presented as positive amounts.
fn compute_net_income(total_income: &[Amount], total_expenses: &[Amount]) -> Vec<Amount> {
    let mut net: HashMap<CommodityId, Decimal> = HashMap::new();

    for amount in total_income {
        *net.entry(amount.commodity_id).or_insert(Decimal::ZERO) += amount.value;
    }
    for amount in total_expenses {
        *net.entry(amount.commodity_id).or_insert(Decimal::ZERO) -= amount.value;
    }

    net.into_iter()
        .filter(|(_, v)| !v.is_zero())
        .map(|(commodity_id, value)| Amount {
            value,
            commodity_id,
        })
        .collect()
}
