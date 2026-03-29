use std::collections::HashMap;

use rust_decimal::Decimal;

use rai_core::types::{AccountId, Amount, CommodityId, Transaction};

use crate::data::{LedgerData, ReportPeriod};

pub struct JournalParams {
    pub period: ReportPeriod,
    pub account: Option<AccountId>,
}

#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub transaction: Transaction,
    pub running_balances: Option<Vec<Amount>>,
}

pub struct JournalResult {
    pub entries: Vec<JournalEntry>,
}

pub fn generate_journal(params: &JournalParams, data: &LedgerData) -> JournalResult {
    // Filter and sort transactions
    let mut filtered: Vec<&Transaction> = data
        .transactions
        .iter()
        .filter(|txn| {
            if let Some(start) = params.period.start {
                if txn.date < start {
                    return false;
                }
            }
            if let Some(end) = params.period.end {
                if txn.date > end {
                    return false;
                }
            }
            // If account filter, only include transactions with a posting to that account
            if let Some(account_id) = params.account {
                if !txn.postings.iter().any(|p| p.account_id == account_id) {
                    return false;
                }
            }
            true
        })
        .collect();

    // Sort chronologically by date, time, id
    filtered.sort_by(|a, b| {
        a.date
            .cmp(&b.date)
            .then_with(|| a.time.cmp(&b.time))
            .then_with(|| a.id.0.cmp(&b.id.0))
    });

    // Build entries with optional running balance
    let mut entries = Vec::new();

    if let Some(account_id) = params.account {
        // Track running balance per commodity for the filtered account
        let mut running: HashMap<CommodityId, Decimal> = HashMap::new();

        for txn in filtered {
            // Add the postings for this account to the running balance
            for posting in &txn.postings {
                if posting.account_id == account_id {
                    *running
                        .entry(posting.units.commodity_id)
                        .or_insert(Decimal::ZERO) += posting.units.value;
                }
            }

            let running_balances: Vec<Amount> = running
                .iter()
                .filter(|(_, v)| !v.is_zero())
                .map(|(&commodity_id, &value)| Amount {
                    value,
                    commodity_id,
                })
                .collect();

            entries.push(JournalEntry {
                transaction: txn.clone(),
                running_balances: Some(running_balances),
            });
        }
    } else {
        for txn in filtered {
            entries.push(JournalEntry {
                transaction: txn.clone(),
                running_balances: None,
            });
        }
    }

    JournalResult { entries }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::LedgerData;
    use rai_core::types::*;
    use rust_decimal_macros::dec;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
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
            metadata: std::collections::HashMap::new(),
        }
    }

    fn sample_data() -> LedgerData {
        LedgerData {
            accounts: vec![],
            transactions: vec![
                make_tx(1, date(2024, 1, 15), vec![
                    make_posting(1, 1, 1, dec!(500), 1),
                    make_posting(2, 1, 2, dec!(-500), 1),
                ]),
                make_tx(2, date(2024, 3, 1), vec![
                    make_posting(3, 2, 1, dec!(300), 1),
                    make_posting(4, 2, 2, dec!(-300), 1),
                ]),
                make_tx(3, date(2024, 6, 1), vec![
                    make_posting(5, 3, 1, dec!(-100), 1),
                    make_posting(6, 3, 3, dec!(100), 1),
                ]),
            ],
            commodities: vec![],
            prices: vec![],
            balance_assertions: vec![],
        }
    }

    // Journal without account filter should list all transactions in
    // chronological order with no running balances.
    #[test]
    fn journal_all_transactions_chronological() {
        let data = sample_data();
        let params = JournalParams {
            period: ReportPeriod { start: None, end: None },
            account: None,
        };
        let result = generate_journal(&params, &data);
        assert_eq!(result.entries.len(), 3);
        assert!(result.entries[0].transaction.date <= result.entries[1].transaction.date);
        assert!(result.entries[0].running_balances.is_none());
    }

    // Filtering by period should exclude transactions outside the range.
    #[test]
    fn journal_period_filter() {
        let data = sample_data();
        let params = JournalParams {
            period: ReportPeriod {
                start: Some(date(2024, 2, 1)),
                end: Some(date(2024, 4, 1)),
            },
            account: None,
        };
        let result = generate_journal(&params, &data);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].transaction.id, TransactionId(2));
    }

    // Filtering by account should only include transactions that have a
    // posting to that account, and compute running balances for it.
    #[test]
    fn journal_account_filter_with_running_balance() {
        let data = sample_data();
        let params = JournalParams {
            period: ReportPeriod { start: None, end: None },
            account: Some(AccountId(1)),
        };
        let result = generate_journal(&params, &data);
        assert_eq!(result.entries.len(), 3);
        // After tx1: 500, after tx2: 800, after tx3: 700
        let bal1 = result.entries[0].running_balances.as_ref().unwrap();
        assert_eq!(bal1[0].value, dec!(500));
        let bal2 = result.entries[1].running_balances.as_ref().unwrap();
        assert_eq!(bal2[0].value, dec!(800));
        let bal3 = result.entries[2].running_balances.as_ref().unwrap();
        assert_eq!(bal3[0].value, dec!(700));
    }

    // When filtering by an account that has no postings, the journal
    // should return no entries.
    #[test]
    fn journal_account_filter_no_matches() {
        let data = sample_data();
        let params = JournalParams {
            period: ReportPeriod { start: None, end: None },
            account: Some(AccountId(99)),
        };
        let result = generate_journal(&params, &data);
        assert!(result.entries.is_empty());
    }
}
