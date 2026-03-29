use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate};
use clap::Subcommand;

use rai_core::types::*;
use rai_db::StorageProvider;
use rai_report::balance_sheet::{generate_balance_sheet, BalanceSheetParams};
use rai_report::data::{LedgerData, ReportPeriod};
use rai_report::income_statement::{generate_income_statement, IncomeStatementParams};
use rai_report::journal::{generate_journal, JournalParams};
use rai_report::trend::{generate_trailing_trend, generate_trend, TrendInterval, TrendParams};
use rai_report::trial_balance::{generate_trial_balance, TrialBalanceParams};

use crate::output::{print_raw_table, render_bar_chart, sparkline};

#[derive(Subcommand)]
pub enum ReportAction {
    /// Assets, liabilities, and equity as of a date
    BalanceSheet {
        /// Report date in YYYY-MM-DD format (defaults to today)
        #[arg(long)]
        as_of: Option<String>,
        /// Convert all amounts to this commodity (e.g. USD)
        #[arg(long)]
        currency: Option<String>,
    },
    /// Income and expenses for a period, with net income
    IncomeStatement {
        /// Period start date in YYYY-MM-DD format
        #[arg(long)]
        from: Option<String>,
        /// Period end date in YYYY-MM-DD format
        #[arg(long)]
        to: Option<String>,
        /// Convert all amounts to this commodity (e.g. USD)
        #[arg(long)]
        currency: Option<String>,
    },
    /// Debits, credits, and net balance for every account
    TrialBalance {
        /// Report date in YYYY-MM-DD format (defaults to today)
        #[arg(long)]
        as_of: Option<String>,
    },
    /// Chronological list of transactions with running balances
    Journal {
        /// Period start date in YYYY-MM-DD format
        #[arg(long)]
        from: Option<String>,
        /// Period end date in YYYY-MM-DD format
        #[arg(long)]
        to: Option<String>,
        /// Show only entries affecting this account
        #[arg(long)]
        account: Option<String>,
    },
    /// Monthly balance trend with sparkline and bar chart
    Trend {
        /// Show trend for this account
        #[arg(long)]
        account: Option<String>,
        /// Start date in YYYY-MM-DD format
        #[arg(long)]
        from: Option<String>,
        /// End date in YYYY-MM-DD format
        #[arg(long)]
        to: Option<String>,
    },
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("Invalid date format '{}'. Expected YYYY-MM-DD", s))
}

fn today() -> NaiveDate {
    Local::now().date_naive()
}

fn load_ledger_data(provider: &dyn StorageProvider) -> Result<LedgerData> {
    let transactions = provider
        .list_transactions(&TransactionFilter::default())
        .context("Failed to load transactions")?;
    let accounts = provider
        .list_accounts(&AccountFilter::default())
        .context("Failed to load accounts")?;
    let commodities = provider
        .list_commodities()
        .context("Failed to load commodities")?;
    let prices = provider
        .list_prices(&PriceFilter::default())
        .context("Failed to load prices")?;
    let balance_assertions = provider
        .list_balance_assertions(&BalanceAssertionFilter::default())
        .context("Failed to load balance assertions")?;

    Ok(LedgerData {
        transactions,
        accounts,
        commodities,
        prices,
        balance_assertions,
    })
}

fn build_commodity_name_map(data: &LedgerData) -> HashMap<CommodityId, String> {
    data.commodities
        .iter()
        .map(|c| (c.id, c.name.clone()))
        .collect()
}

fn format_amounts(amounts: &[Amount], commodity_names: &HashMap<CommodityId, String>) -> String {
    if amounts.is_empty() {
        return "0".to_string();
    }
    amounts
        .iter()
        .map(|a| {
            let name = commodity_names
                .get(&a.commodity_id)
                .cloned()
                .unwrap_or_else(|| format!("#{}", a.commodity_id.0));
            format!("{} {}", a.value, name)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn resolve_currency(
    provider: &dyn StorageProvider,
    name: Option<&str>,
) -> Result<Option<CommodityId>> {
    match name {
        Some(n) => {
            let c = provider
                .get_commodity_by_name(n)
                .with_context(|| format!("Failed to look up commodity '{}'", n))?;
            let c = match c {
                Some(c) => c,
                None => bail!("Commodity '{}' not found", n),
            };
            Ok(Some(c.id))
        }
        None => Ok(None),
    }
}

pub fn handle(action: ReportAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        ReportAction::BalanceSheet { as_of, currency } => {
            balance_sheet(provider, as_of.as_deref(), currency.as_deref())
        }
        ReportAction::IncomeStatement { from, to, currency } => {
            income_statement(provider, from.as_deref(), to.as_deref(), currency.as_deref())
        }
        ReportAction::TrialBalance { as_of } => trial_balance(provider, as_of.as_deref()),
        ReportAction::Journal {
            from,
            to,
            account,
        } => journal(provider, from.as_deref(), to.as_deref(), account.as_deref()),
        ReportAction::Trend { account, from, to } => {
            trend(provider, account.as_deref(), from.as_deref(), to.as_deref())
        }
    }
}

/// Extract f64 values from trend points for a given account (sum across commodities).
fn extract_trend_values(
    account_id: rai_core::types::AccountId,
    trend_result: &rai_report::trend::TrendResult,
) -> Vec<f64> {
    use rust_decimal::prelude::ToPrimitive;

    for account_trend in &trend_result.trends {
        if account_trend.account.id == account_id {
            return account_trend
                .points
                .iter()
                .map(|p| {
                    p.balances
                        .iter()
                        .map(|a| a.value.to_f64().unwrap_or(0.0))
                        .sum()
                })
                .collect();
        }
    }
    Vec::new()
}

fn balance_sheet(
    provider: &mut dyn StorageProvider,
    as_of: Option<&str>,
    currency: Option<&str>,
) -> Result<()> {
    let as_of_date = match as_of {
        Some(d) => parse_date(d)?,
        None => today(),
    };
    let currency_id = resolve_currency(provider, currency)?;
    let data = load_ledger_data(provider)?;
    let commodity_names = build_commodity_name_map(&data);

    let params = BalanceSheetParams {
        as_of: as_of_date,
        currency: currency_id,
    };
    let result = generate_balance_sheet(&params, &data);

    // Generate trailing 12-month trend for sparklines
    let trailing = generate_trailing_trend(12, as_of_date, &data);

    println!("Balance Sheet as of {}", result.as_of);
    println!();

    let headers = vec![
        "Account".to_string(),
        "Balance".to_string(),
        "Trend (12m)".to_string(),
    ];

    // Assets
    println!("ASSETS");
    let mut rows = Vec::new();
    for ab in &result.assets {
        let trend_vals = extract_trend_values(ab.account.id, &trailing);
        rows.push(vec![
            ab.account.name.clone(),
            format_amounts(&ab.balances, &commodity_names),
            sparkline(&trend_vals),
        ]);
    }
    rows.push(vec![
        "Total Assets".to_string(),
        format_amounts(&result.total_assets, &commodity_names),
        String::new(),
    ]);
    print_raw_table(&headers, &rows);
    println!();

    // Liabilities
    println!("LIABILITIES");
    let mut rows = Vec::new();
    for ab in &result.liabilities {
        let trend_vals = extract_trend_values(ab.account.id, &trailing);
        rows.push(vec![
            ab.account.name.clone(),
            format_amounts(&ab.balances, &commodity_names),
            sparkline(&trend_vals),
        ]);
    }
    rows.push(vec![
        "Total Liabilities".to_string(),
        format_amounts(&result.total_liabilities, &commodity_names),
        String::new(),
    ]);
    print_raw_table(&headers, &rows);
    println!();

    // Equity
    println!("EQUITY");
    let mut rows = Vec::new();
    for ab in &result.equity {
        let trend_vals = extract_trend_values(ab.account.id, &trailing);
        rows.push(vec![
            ab.account.name.clone(),
            format_amounts(&ab.balances, &commodity_names),
            sparkline(&trend_vals),
        ]);
    }
    rows.push(vec![
        "Total Equity".to_string(),
        format_amounts(&result.total_equity, &commodity_names),
        String::new(),
    ]);
    print_raw_table(&headers, &rows);

    Ok(())
}

fn income_statement(
    provider: &mut dyn StorageProvider,
    from: Option<&str>,
    to: Option<&str>,
    currency: Option<&str>,
) -> Result<()> {
    let from_date = from.map(parse_date).transpose()?;
    let to_date = to.map(parse_date).transpose()?;
    let currency_id = resolve_currency(provider, currency)?;
    let data = load_ledger_data(provider)?;
    let commodity_names = build_commodity_name_map(&data);

    let params = IncomeStatementParams {
        period: ReportPeriod {
            start: from_date,
            end: to_date,
        },
        currency: currency_id,
    };
    let result = generate_income_statement(&params, &data);

    let period_str = match (&result.period.start, &result.period.end) {
        (Some(s), Some(e)) => format!("{} to {}", s, e),
        (Some(s), None) => format!("{} to present", s),
        (None, Some(e)) => format!("up to {}", e),
        (None, None) => "all time".to_string(),
    };
    println!("Income Statement ({})", period_str);
    println!();

    // Income
    println!("INCOME");
    let mut rows = Vec::new();
    for ab in &result.income {
        rows.push(vec![
            ab.account.name.clone(),
            format_amounts(&ab.balances, &commodity_names),
        ]);
    }
    rows.push(vec![
        "Total Income".to_string(),
        format_amounts(&result.total_income, &commodity_names),
    ]);
    print_raw_table(
        &["Account".to_string(), "Balance".to_string()],
        &rows,
    );
    println!();

    // Expenses
    println!("EXPENSES");
    let mut rows = Vec::new();
    for ab in &result.expenses {
        rows.push(vec![
            ab.account.name.clone(),
            format_amounts(&ab.balances, &commodity_names),
        ]);
    }
    rows.push(vec![
        "Total Expenses".to_string(),
        format_amounts(&result.total_expenses, &commodity_names),
    ]);
    print_raw_table(
        &["Account".to_string(), "Balance".to_string()],
        &rows,
    );
    println!();

    println!(
        "Net Income: {}",
        format_amounts(&result.net_income, &commodity_names)
    );

    Ok(())
}

fn trial_balance(provider: &mut dyn StorageProvider, as_of: Option<&str>) -> Result<()> {
    let as_of_date = match as_of {
        Some(d) => parse_date(d)?,
        None => today(),
    };
    let data = load_ledger_data(provider)?;
    let commodity_names = build_commodity_name_map(&data);

    let params = TrialBalanceParams {
        as_of: as_of_date,
    };
    let result = generate_trial_balance(&params, &data);

    println!("Trial Balance as of {}", result.as_of);
    println!();

    let headers = vec![
        "Account".to_string(),
        "Debits".to_string(),
        "Credits".to_string(),
        "Balance".to_string(),
    ];

    let rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|r| {
            vec![
                r.account.name.clone(),
                format_amounts(&r.debits, &commodity_names),
                format_amounts(&r.credits, &commodity_names),
                format_amounts(&r.balance, &commodity_names),
            ]
        })
        .collect();

    print_raw_table(&headers, &rows);
    Ok(())
}

fn journal(
    provider: &mut dyn StorageProvider,
    from: Option<&str>,
    to: Option<&str>,
    account: Option<&str>,
) -> Result<()> {
    let from_date = from.map(parse_date).transpose()?;
    let to_date = to.map(parse_date).transpose()?;

    let account_id = match account {
        Some(name) => {
            let acc = provider
                .get_account_by_name(name)
                .with_context(|| format!("Failed to look up account '{}'", name))?;
            let acc = match acc {
                Some(a) => a,
                None => bail!("Account '{}' not found", name),
            };
            Some(acc.id)
        }
        None => None,
    };

    let data = load_ledger_data(provider)?;
    let commodity_names = build_commodity_name_map(&data);

    // Build account name map
    let account_names: HashMap<AccountId, String> = data
        .accounts
        .iter()
        .map(|a| (a.id, a.name.clone()))
        .collect();

    let params = JournalParams {
        period: ReportPeriod {
            start: from_date,
            end: to_date,
        },
        account: account_id,
    };
    let result = generate_journal(&params, &data);

    if result.entries.is_empty() {
        println!("(no journal entries)");
        return Ok(());
    }

    for entry in &result.entries {
        let tx = &entry.transaction;
        let status_char = match tx.status {
            TransactionStatus::Completed => "*",
            TransactionStatus::Pending => "!",
            TransactionStatus::Flagged => "#",
        };

        let payee_narration = match (&tx.payee, &tx.narration) {
            (Some(p), Some(n)) => format!("\"{}\" \"{}\"", p, n),
            (Some(p), None) => format!("\"{}\"", p),
            (None, Some(n)) => format!("\"{}\"", n),
            (None, None) => String::new(),
        };

        println!("{} {} {}", tx.date, status_char, payee_narration);

        for posting in &tx.postings {
            let acct = account_names
                .get(&posting.account_id)
                .cloned()
                .unwrap_or_else(|| format!("#{}", posting.account_id.0));
            let comm = commodity_names
                .get(&posting.units.commodity_id)
                .cloned()
                .unwrap_or_else(|| format!("#{}", posting.units.commodity_id.0));
            println!("  {:40} {:>12} {}", acct, posting.units.value, comm);
        }

        if let Some(ref running) = entry.running_balances {
            let bal_str = format_amounts(running, &commodity_names);
            println!("  Balance: {}", bal_str);
        }
        println!();
    }

    Ok(())
}

fn trend(
    provider: &mut dyn StorageProvider,
    account: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<()> {
    use rust_decimal::prelude::ToPrimitive;

    let from_date = from.map(parse_date).transpose()?;
    let to_date = to.map(parse_date).transpose()?;

    let account_id = match account {
        Some(name) => {
            let acc = provider
                .get_account_by_name(name)
                .with_context(|| format!("Failed to look up account '{}'", name))?;
            let acc = match acc {
                Some(a) => a,
                None => bail!("Account '{}' not found", name),
            };
            Some(acc.id)
        }
        None => None,
    };

    let data = load_ledger_data(provider)?;
    let commodity_names = build_commodity_name_map(&data);

    let params = TrendParams {
        period: ReportPeriod {
            start: from_date,
            end: to_date,
        },
        account_id,
        account_type: None,
        interval: TrendInterval::Monthly,
    };
    let result = generate_trend(&params, &data);

    if result.trends.is_empty() {
        println!("(no trend data)");
        return Ok(());
    }

    for account_trend in &result.trends {
        println!("Account: {}", account_trend.account.name);
        println!();

        // Build bar chart data: each point is a month
        let items: Vec<(String, f64)> = account_trend
            .points
            .iter()
            .map(|p| {
                let label = p.date.format("%Y-%m").to_string();
                let total: f64 = p
                    .balances
                    .iter()
                    .map(|a| a.value.to_f64().unwrap_or(0.0))
                    .sum();
                (label, total)
            })
            .collect();

        println!("{}", render_bar_chart(&items, 40));

        // Also show sparkline summary
        let vals: Vec<f64> = items.iter().map(|(_, v)| *v).collect();
        println!();
        println!("Sparkline: {}", sparkline(&vals));

        // Show final balance
        if let Some(last) = account_trend.points.last() {
            println!(
                "Latest ({}):",
                last.date.format("%Y-%m-%d")
            );
            if last.balances.is_empty() {
                println!("  0");
            } else {
                for amount in &last.balances {
                    let comm = commodity_names
                        .get(&amount.commodity_id)
                        .cloned()
                        .unwrap_or_else(|| format!("#{}", amount.commodity_id.0));
                    println!("  {} {}", amount.value, comm);
                }
            }
        }
        println!();
    }

    Ok(())
}
