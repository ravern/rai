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
