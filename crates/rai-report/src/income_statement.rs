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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::LedgerData;
    use rai_core::types::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
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

    fn make_tx(id: i64, d: chrono::NaiveDate, postings: Vec<Posting>) -> Transaction {
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

    // Verifies that income shows as positive (negated from credit-normal)
    // and expenses stay positive, and net income = income - expenses.
    #[test]
    fn income_statement_basic() {
        let data = LedgerData {
            accounts: vec![
                make_account(1, "Assets:Bank"),
                make_account(2, "Income:Salary"),
                make_account(3, "Expenses:Food"),
            ],
            transactions: vec![
                make_tx(1, date(2024, 3, 1), vec![
                    make_posting(1, 1, 1, dec!(1000), 1),
                    make_posting(2, 1, 2, dec!(-1000), 1),
                ]),
                make_tx(2, date(2024, 3, 15), vec![
                    make_posting(3, 2, 3, dec!(200), 1),
                    make_posting(4, 2, 1, dec!(-200), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = IncomeStatementParams {
            period: ReportPeriod { start: None, end: None },
            currency: None,
        };
        let result = generate_income_statement(&params, &data);
        // Income: -(-1000) = 1000 (negated for display)
        assert_eq!(result.income[0].balances[0].value, dec!(1000));
        // Expenses: 200 (stays positive)
        assert_eq!(result.expenses[0].balances[0].value, dec!(200));
        // Net income: 1000 - 200 = 800
        assert_eq!(result.net_income[0].value, dec!(800));
    }

    // Verifies that transactions outside the report period are excluded.
    #[test]
    fn income_statement_period_filter() {
        let data = LedgerData {
            accounts: vec![
                make_account(1, "Assets:Bank"),
                make_account(2, "Income:Salary"),
            ],
            transactions: vec![
                make_tx(1, date(2024, 1, 1), vec![
                    make_posting(1, 1, 1, dec!(500), 1),
                    make_posting(2, 1, 2, dec!(-500), 1),
                ]),
                make_tx(2, date(2024, 6, 1), vec![
                    make_posting(3, 2, 1, dec!(1000), 1),
                    make_posting(4, 2, 2, dec!(-1000), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = IncomeStatementParams {
            period: ReportPeriod {
                start: Some(date(2024, 5, 1)),
                end: Some(date(2024, 12, 31)),
            },
            currency: None,
        };
        let result = generate_income_statement(&params, &data);
        // Only the June transaction should be included
        assert_eq!(result.income[0].balances[0].value, dec!(1000));
    }

    // Assets accounts should not appear in the income statement.
    #[test]
    fn income_statement_excludes_balance_sheet_accounts() {
        let data = LedgerData {
            accounts: vec![
                make_account(1, "Assets:Bank"),
                make_account(2, "Income:Salary"),
            ],
            transactions: vec![
                make_tx(1, date(2024, 3, 1), vec![
                    make_posting(1, 1, 1, dec!(1000), 1),
                    make_posting(2, 1, 2, dec!(-1000), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = IncomeStatementParams {
            period: ReportPeriod { start: None, end: None },
            currency: None,
        };
        let result = generate_income_statement(&params, &data);
        assert_eq!(result.income.len(), 1);
        assert!(result.expenses.is_empty());
    }
}
