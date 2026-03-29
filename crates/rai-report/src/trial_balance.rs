use std::collections::HashMap;

use chrono::NaiveDate;
use rust_decimal::Decimal;

use rai_core::types::{Account, Amount, CommodityId};

use crate::data::LedgerData;

pub struct TrialBalanceParams {
    pub as_of: NaiveDate,
}

#[derive(Debug, Clone)]
pub struct TrialBalanceRow {
    pub account: Account,
    pub debits: Vec<Amount>,
    pub credits: Vec<Amount>,
    pub balance: Vec<Amount>,
}

pub struct TrialBalanceResult {
    pub as_of: NaiveDate,
    pub rows: Vec<TrialBalanceRow>,
}

pub fn generate_trial_balance(
    params: &TrialBalanceParams,
    data: &LedgerData,
) -> TrialBalanceResult {
    // For each account, track debits and credits per commodity separately
    // Debits = sum of positive posting amounts, Credits = sum of negative posting amounts (absolute)
    let mut debits_by_account: HashMap<i64, HashMap<CommodityId, Decimal>> = HashMap::new();
    let mut credits_by_account: HashMap<i64, HashMap<CommodityId, Decimal>> = HashMap::new();

    for txn in &data.transactions {
        if txn.date > params.as_of {
            continue;
        }
        for posting in &txn.postings {
            let account_id = posting.account_id.0;
            let commodity_id = posting.units.commodity_id;
            let value = posting.units.value;

            if value > Decimal::ZERO {
                *debits_by_account
                    .entry(account_id)
                    .or_default()
                    .entry(commodity_id)
                    .or_insert(Decimal::ZERO) += value;
            } else if value < Decimal::ZERO {
                *credits_by_account
                    .entry(account_id)
                    .or_default()
                    .entry(commodity_id)
                    .or_insert(Decimal::ZERO) += value.abs();
            }
        }
    }

    let account_map: HashMap<i64, &Account> = data
        .accounts
        .iter()
        .map(|a| (a.id.0, a))
        .collect();

    // Collect all account IDs that have any activity
    let mut all_account_ids: Vec<i64> = debits_by_account
        .keys()
        .chain(credits_by_account.keys())
        .copied()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    all_account_ids.sort();

    let mut rows = Vec::new();

    for account_id in all_account_ids {
        let account = match account_map.get(&account_id) {
            Some(a) => a,
            None => continue,
        };

        let debit_map = debits_by_account.get(&account_id);
        let credit_map = credits_by_account.get(&account_id);

        // Collect all commodities for this account
        let mut all_commodities: Vec<CommodityId> = Vec::new();
        if let Some(dm) = debit_map {
            all_commodities.extend(dm.keys());
        }
        if let Some(cm) = credit_map {
            for k in cm.keys() {
                if !all_commodities.contains(k) {
                    all_commodities.push(*k);
                }
            }
        }

        let debits: Vec<Amount> = all_commodities
            .iter()
            .filter_map(|&cid| {
                let value = debit_map
                    .and_then(|m| m.get(&cid))
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                if value.is_zero() {
                    None
                } else {
                    Some(Amount {
                        value,
                        commodity_id: cid,
                    })
                }
            })
            .collect();

        let credits: Vec<Amount> = all_commodities
            .iter()
            .filter_map(|&cid| {
                let value = credit_map
                    .and_then(|m| m.get(&cid))
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                if value.is_zero() {
                    None
                } else {
                    Some(Amount {
                        value,
                        commodity_id: cid,
                    })
                }
            })
            .collect();

        let balance: Vec<Amount> = all_commodities
            .iter()
            .filter_map(|&cid| {
                let d = debit_map
                    .and_then(|m| m.get(&cid))
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                let c = credit_map
                    .and_then(|m| m.get(&cid))
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                let net = d - c;
                if net.is_zero() {
                    None
                } else {
                    Some(Amount {
                        value: net,
                        commodity_id: cid,
                    })
                }
            })
            .collect();

        if debits.is_empty() && credits.is_empty() {
            continue;
        }

        rows.push(TrialBalanceRow {
            account: (*account).clone(),
            debits,
            credits,
            balance,
        });
    }

    // Sort rows by account name
    rows.sort_by(|a, b| a.account.name.cmp(&b.account.name));

    TrialBalanceResult {
        as_of: params.as_of,
        rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::LedgerData;
    use rai_core::types::*;
    use rust_decimal_macros::dec;

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
            metadata: std::collections::HashMap::new(),
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
            metadata: std::collections::HashMap::new(),
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
            metadata: std::collections::HashMap::new(),
        }
    }

    // Verifies that debits (positive postings) and credits (negative
    // postings) are separated correctly per account, and balance = debit - credit.
    #[test]
    fn trial_balance_separates_debits_credits() {
        let data = LedgerData {
            accounts: vec![
                make_account(1, "Assets:Bank"),
                make_account(2, "Income:Salary"),
            ],
            transactions: vec![
                make_tx(1, date(2024, 3, 1), vec![
                    make_posting(1, 1, 1, dec!(1000), 1),  // debit to Assets
                    make_posting(2, 1, 2, dec!(-1000), 1), // credit to Income
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = TrialBalanceParams { as_of: date(2024, 12, 31) };
        let result = generate_trial_balance(&params, &data);
        assert_eq!(result.rows.len(), 2);

        let bank = result.rows.iter().find(|r| r.account.name == "Assets:Bank").unwrap();
        assert_eq!(bank.debits[0].value, dec!(1000));
        assert!(bank.credits.is_empty());
        assert_eq!(bank.balance[0].value, dec!(1000));

        let salary = result.rows.iter().find(|r| r.account.name == "Income:Salary").unwrap();
        assert!(salary.debits.is_empty());
        assert_eq!(salary.credits[0].value, dec!(1000));
        assert_eq!(salary.balance[0].value, dec!(-1000));
    }

    // Transactions after the as_of date should be excluded.
    #[test]
    fn trial_balance_respects_as_of() {
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
                make_tx(2, date(2024, 7, 1), vec![
                    make_posting(3, 2, 1, dec!(300), 1),
                    make_posting(4, 2, 2, dec!(-300), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        };
        let params = TrialBalanceParams { as_of: date(2024, 3, 1) };
        let result = generate_trial_balance(&params, &data);
        let bank = result.rows.iter().find(|r| r.account.name == "Assets:Bank").unwrap();
        assert_eq!(bank.debits[0].value, dec!(500));
    }
}
