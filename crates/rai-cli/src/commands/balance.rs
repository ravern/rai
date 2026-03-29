use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Subcommand;
use rust_decimal::Decimal;
use tabled::Tabled;

use rai_core::types::*;
use rai_db::StorageProvider;

use crate::output::print_table;

#[derive(Subcommand)]
pub enum BalanceAction {
    /// Declare the expected balance of an account on a date (verified by validate)
    Assert {
        /// Account name (e.g. Assets:Bank:Checking)
        account: String,
        /// Expected balance amount
        amount: Decimal,
        /// Commodity of the balance (e.g. USD)
        commodity: String,
        /// Assertion date in YYYY-MM-DD format
        #[arg(long)]
        date: String,
    },
    /// List all balance assertions, optionally filtered by account
    List {
        /// Show assertions for this account only
        #[arg(long)]
        account: Option<String>,
    },
    /// Remove a balance assertion by ID
    Delete {
        /// Balance assertion ID
        id: i64,
    },
}

#[derive(Tabled)]
struct BalanceRow {
    #[tabled(rename = "ID")]
    id: i64,
    #[tabled(rename = "Date")]
    date: String,
    #[tabled(rename = "Account")]
    account: String,
    #[tabled(rename = "Expected")]
    expected: String,
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("Invalid date format '{}'. Expected YYYY-MM-DD", s))
}

pub fn handle(action: BalanceAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        BalanceAction::Assert {
            account,
            amount,
            commodity,
            date,
        } => assert_balance(provider, &account, amount, &commodity, &date),
        BalanceAction::List { account } => list(provider, account.as_deref()),
        BalanceAction::Delete { id } => delete(provider, id),
    }
}

fn assert_balance(
    provider: &mut dyn StorageProvider,
    account_name: &str,
    amount: Decimal,
    commodity_name: &str,
    date_str: &str,
) -> Result<()> {
    let date = parse_date(date_str)?;

    let account = provider
        .get_account_by_name(account_name)
        .context("Failed to look up account")?;
    let account = match account {
        Some(a) => a,
        None => bail!("Account '{}' not found", account_name),
    };

    let commodity = provider
        .get_commodity_by_name(commodity_name)
        .context("Failed to look up commodity")?;
    let commodity = match commodity {
        Some(c) => c,
        None => bail!("Commodity '{}' not found", commodity_name),
    };

    let new_assertion = NewBalanceAssertion {
        date,
        account_id: account.id,
        expected: Amount {
            value: amount,
            commodity_id: commodity.id,
        },
    };

    let assertion = provider
        .create_balance_assertion(&new_assertion)
        .context("Failed to create balance assertion")?;
    println!(
        "Created balance assertion {} on {}: {} {} = {} {}",
        assertion.id, date, account_name, commodity_name, amount, commodity_name
    );
    Ok(())
}

fn list(provider: &mut dyn StorageProvider, account_name: Option<&str>) -> Result<()> {
    let account_id = match account_name {
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

    let filter = BalanceAssertionFilter { account_id };

    let assertions = provider
        .list_balance_assertions(&filter)
        .context("Failed to list balance assertions")?;

    // Build lookup maps
    let commodities = provider
        .list_commodities()
        .context("Failed to list commodities")?;
    let commodity_map: std::collections::HashMap<CommodityId, String> = commodities
        .into_iter()
        .map(|c| (c.id, c.name))
        .collect();

    let accounts = provider
        .list_accounts(&AccountFilter::default())
        .context("Failed to list accounts")?;
    let account_map: std::collections::HashMap<AccountId, String> = accounts
        .into_iter()
        .map(|a| (a.id, a.name))
        .collect();

    let rows: Vec<BalanceRow> = assertions
        .into_iter()
        .map(|a| {
            let acct_name = account_map
                .get(&a.account_id)
                .cloned()
                .unwrap_or_else(|| format!("#{}", a.account_id.0));
            let comm_name = commodity_map
                .get(&a.expected.commodity_id)
                .cloned()
                .unwrap_or_else(|| format!("#{}", a.expected.commodity_id.0));
            BalanceRow {
                id: a.id.0,
                date: a.date.to_string(),
                account: acct_name,
                expected: format!("{} {}", a.expected.value, comm_name),
            }
        })
        .collect();

    print_table(&rows);
    Ok(())
}

fn delete(provider: &mut dyn StorageProvider, id: i64) -> Result<()> {
    provider
        .delete_balance_assertion(BalanceAssertionId(id))
        .context("Failed to delete balance assertion")?;
    println!("Deleted balance assertion {}", id);
    Ok(())
}
