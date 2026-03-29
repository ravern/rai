use std::collections::HashMap;

use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;

use rai_core::types::{Account, AccountId, AccountType, Amount, CommodityId};

use crate::data::{LedgerData, ReportPeriod};

pub struct TrendParams {
    pub period: ReportPeriod,
    pub account_id: Option<AccountId>,
    pub account_type: Option<AccountType>,
    pub interval: TrendInterval,
}

#[derive(Debug, Clone, Copy)]
pub enum TrendInterval {
    Monthly,
}

#[derive(Debug, Clone)]
pub struct TrendPoint {
    pub date: NaiveDate,
    pub balances: Vec<Amount>,
}

#[derive(Debug, Clone)]
pub struct AccountTrend {
    pub account: Account,
    pub points: Vec<TrendPoint>,
}

pub struct TrendResult {
    pub trends: Vec<AccountTrend>,
    pub interval: TrendInterval,
}

/// Generate monthly bucket end-dates between `start` and `end` (inclusive of end month).
fn generate_monthly_buckets(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut buckets = Vec::new();
    // Start at end of start's month
    let mut year = start.year();
    let mut month = start.month();

    loop {
        // End of this month
        let (next_year, next_month) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
        let end_of_month = NaiveDate::from_ymd_opt(next_year, next_month, 1)
            .unwrap()
            .pred_opt()
            .unwrap();

        if end_of_month > end {
            // Include the end date's month if we haven't yet
            if buckets.is_empty() || *buckets.last().unwrap() < end {
                buckets.push(end);
            }
            break;
        }

        buckets.push(end_of_month);

        year = next_year;
        month = next_month;
    }

    buckets
}

/// Compute cumulative balance for a single account at each bucket date.
fn compute_account_balances_at_dates(
    account_id: AccountId,
    dates: &[NaiveDate],
    data: &LedgerData,
    negate: bool,
) -> Vec<TrendPoint> {
    if dates.is_empty() {
        return Vec::new();
    }

    // Gather all postings for this account, sorted by transaction date
    let mut postings_with_date: Vec<(NaiveDate, CommodityId, Decimal)> = Vec::new();
    for txn in &data.transactions {
        for posting in &txn.postings {
            if posting.account_id == account_id {
                postings_with_date.push((txn.date, posting.units.commodity_id, posting.units.value));
            }
        }
    }
    postings_with_date.sort_by_key(|(d, _, _)| *d);

    let mut points = Vec::new();
    let mut cumulative: HashMap<CommodityId, Decimal> = HashMap::new();
    let mut posting_idx = 0;

    for &bucket_date in dates {
        // Advance through postings up to and including bucket_date
        while posting_idx < postings_with_date.len()
            && postings_with_date[posting_idx].0 <= bucket_date
        {
            let (_, commodity_id, value) = postings_with_date[posting_idx];
            *cumulative.entry(commodity_id).or_insert(Decimal::ZERO) += value;
            posting_idx += 1;
        }

        let balances: Vec<Amount> = cumulative
            .iter()
            .filter(|(_, v)| !v.is_zero())
            .map(|(&commodity_id, &value)| Amount {
                value: if negate { -value } else { value },
                commodity_id,
            })
            .collect();

        points.push(TrendPoint {
            date: bucket_date,
            balances,
        });
    }

    points
}

pub fn generate_trend(params: &TrendParams, data: &LedgerData) -> TrendResult {
    // Determine date range
    let first_date = data
        .transactions
        .iter()
        .map(|t| t.date)
        .min();
    let last_date = data
        .transactions
        .iter()
        .map(|t| t.date)
        .max();

    let (first_date, last_date) = match (first_date, last_date) {
        (Some(f), Some(l)) => (f, l),
        _ => {
            return TrendResult {
                trends: Vec::new(),
                interval: params.interval,
            };
        }
    };

    let start = params.period.start.unwrap_or(first_date);
    let end = params.period.end.unwrap_or(last_date);

    if start > end {
        return TrendResult {
            trends: Vec::new(),
            interval: params.interval,
        };
    }

    let buckets = match params.interval {
        TrendInterval::Monthly => generate_monthly_buckets(start, end),
    };

    // Determine which accounts to include
    let account_map: HashMap<i64, &Account> = data
        .accounts
        .iter()
        .map(|a| (a.id.0, a))
        .collect();

    let target_accounts: Vec<&Account> = if let Some(account_id) = params.account_id {
        match account_map.get(&account_id.0) {
            Some(a) => vec![a],
            None => Vec::new(),
        }
    } else if let Some(account_type) = params.account_type {
        data.accounts
            .iter()
            .filter(|a| a.account_type == account_type)
            .collect()
    } else {
        // Default: all balance sheet accounts (assets, liabilities, equity)
        data.accounts
            .iter()
            .filter(|a| matches!(
                a.account_type,
                AccountType::Assets | AccountType::Liabilities | AccountType::Equity
            ))
            .collect()
    };

    let mut trends: Vec<AccountTrend> = Vec::new();

    for account in target_accounts {
        // Liabilities and equity are credit-normal; negate for display
        let negate = matches!(
            account.account_type,
            AccountType::Liabilities | AccountType::Equity
        );

        let points = compute_account_balances_at_dates(account.id, &buckets, data, negate);

        // Only include accounts that have at least one non-empty point
        let has_data = points.iter().any(|p| !p.balances.is_empty());
        if has_data {
            trends.push(AccountTrend {
                account: account.clone(),
                points,
            });
        }
    }

    trends.sort_by(|a, b| a.account.name.cmp(&b.account.name));

    TrendResult {
        trends,
        interval: params.interval,
    }
}

/// Generate a trend for the last N months ending at `as_of`, useful for sparklines on balance sheets.
pub fn generate_trailing_trend(
    months: u32,
    as_of: NaiveDate,
    data: &LedgerData,
) -> TrendResult {
    // Compute start date: N months before as_of
    let start_month = if as_of.month() as i32 - months as i32 <= 0 {
        let years_back = ((months as i32 - as_of.month() as i32) / 12) + 1;
        let new_year = as_of.year() - years_back;
        let new_month = ((as_of.month() as i32 - months as i32) + years_back * 12) as u32;
        NaiveDate::from_ymd_opt(new_year, new_month, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(as_of.year(), as_of.month() - months, 1).unwrap()
    };

    let params = TrendParams {
        period: ReportPeriod {
            start: Some(start_month),
            end: Some(as_of),
        },
        account_id: None,
        account_type: None,
        interval: TrendInterval::Monthly,
    };

    generate_trend(&params, data)
}
