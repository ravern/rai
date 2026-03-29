use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate};
use clap::Subcommand;
use tabled::Tabled;

use rai_core::types::*;
use rai_db::StorageProvider;

use crate::output::print_table;

#[derive(Subcommand)]
pub enum AccountAction {
    /// Create a new account (type inferred from first name segment)
    Create {
        /// Hierarchical name, e.g. Assets:Bank:Checking or Expenses:Food
        name: String,
        /// Lot booking method: strict, fifo, lifo, hifo, average, none, strict_with_size
        #[arg(long, default_value = "strict")]
        booking_method: String,
        /// Restrict account to specific commodities (repeatable)
        #[arg(long)]
        currency: Vec<String>,
        /// Open date in YYYY-MM-DD format (defaults to today)
        #[arg(long)]
        date: Option<String>,
    },
    /// List accounts with optional filters
    List {
        /// Filter by type: assets, liabilities, income, expenses, equity
        #[arg(long, name = "type")]
        account_type: Option<String>,
        /// Show only open accounts
        #[arg(long)]
        open: bool,
        /// Show only closed accounts
        #[arg(long)]
        closed: bool,
    },
    /// Show full details for an account
    Show {
        /// Account name to look up
        name: String,
    },
    /// Re-open a previously closed account
    Open {
        /// Account name to re-open
        name: String,
        /// New open date in YYYY-MM-DD format (defaults to today)
        #[arg(long)]
        date: Option<String>,
    },
    /// Close an account (balance should be zero)
    Close {
        /// Account name to close
        name: String,
        /// Close date in YYYY-MM-DD format (defaults to today)
        #[arg(long)]
        date: Option<String>,
    },
    /// Remove an account from the chart of accounts
    Delete {
        /// Account name to delete
        name: String,
    },
}

#[derive(Tabled)]
struct AccountRow {
    #[tabled(rename = "ID")]
    id: i64,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Type")]
    account_type: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "Opened")]
    opened: String,
    #[tabled(rename = "Closed")]
    closed: String,
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("Invalid date format '{}'. Expected YYYY-MM-DD", s))
}

fn today() -> NaiveDate {
    Local::now().date_naive()
}

fn resolve_commodity_ids(
    provider: &dyn StorageProvider,
    names: &[String],
) -> Result<Vec<CommodityId>> {
    let mut ids = Vec::new();
    for name in names {
        let commodity = provider
            .get_commodity_by_name(name)
            .with_context(|| format!("Failed to look up commodity '{}'", name))?;
        let commodity = match commodity {
            Some(c) => c,
            None => bail!("Commodity '{}' not found", name),
        };
        ids.push(commodity.id);
    }
    Ok(ids)
}

pub fn handle(action: AccountAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        AccountAction::Create {
            name,
            booking_method,
            currency,
            date,
        } => create(provider, &name, &booking_method, &currency, date.as_deref()),
        AccountAction::List {
            account_type,
            open,
            closed,
        } => list(provider, account_type.as_deref(), open, closed),
        AccountAction::Show { name } => show(provider, &name),
        AccountAction::Open { name, date } => open(provider, &name, date.as_deref()),
        AccountAction::Close { name, date } => close(provider, &name, date.as_deref()),
        AccountAction::Delete { name } => delete(provider, &name),
    }
}

fn create(
    provider: &mut dyn StorageProvider,
    name: &str,
    booking_method: &str,
    currencies: &[String],
    date: Option<&str>,
) -> Result<()> {
    let bm = BookingMethod::from_str(booking_method)
        .with_context(|| format!("Unknown booking method: '{}'", booking_method))?;

    let opened_at = match date {
        Some(d) => parse_date(d)?,
        None => today(),
    };

    let currency_ids = resolve_commodity_ids(provider, currencies)?;

    // Validate account type from name
    if AccountType::from_name(name).is_none() {
        bail!(
            "Cannot determine account type from name '{}'. \
             First segment must be one of: Assets, Liabilities, Income, Expenses, Equity",
            name
        );
    }

    let new = NewAccount {
        name: name.to_string(),
        opened_at,
        currencies: currency_ids,
        booking_method: bm,
        metadata: Metadata::new(),
    };

    let account = provider
        .create_account(&new)
        .context("Failed to create account")?;
    println!(
        "Created account '{}' (id={}, type={:?})",
        account.name, account.id, account.account_type
    );
    Ok(())
}

fn list(
    provider: &mut dyn StorageProvider,
    account_type: Option<&str>,
    open: bool,
    closed: bool,
) -> Result<()> {
    let at = match account_type {
        Some(s) => {
            let parsed = AccountType::from_name(s)
                .with_context(|| format!("Unknown account type: '{}'", s))?;
            Some(parsed)
        }
        None => None,
    };

    let is_open = if open {
        Some(true)
    } else if closed {
        Some(false)
    } else {
        None
    };

    let filter = AccountFilter {
        account_type: at,
        is_open,
    };

    let accounts = provider
        .list_accounts(&filter)
        .context("Failed to list accounts")?;

    let rows: Vec<AccountRow> = accounts
        .into_iter()
        .map(|a| AccountRow {
            id: a.id.0,
            name: a.name,
            account_type: a.account_type.as_str().to_string(),
            status: if a.is_open {
                "open".to_string()
            } else {
                "closed".to_string()
            },
            opened: a.opened_at.to_string(),
            closed: a
                .closed_at
                .map(|d| d.to_string())
                .unwrap_or_default(),
        })
        .collect();

    print_table(&rows);
    Ok(())
}

fn show(provider: &mut dyn StorageProvider, name: &str) -> Result<()> {
    let account = provider
        .get_account_by_name(name)
        .context("Failed to look up account")?;
    let account = match account {
        Some(a) => a,
        None => bail!("Account '{}' not found", name),
    };

    println!("ID:             {}", account.id);
    println!("Name:           {}", account.name);
    println!("Type:           {:?}", account.account_type);
    println!(
        "Status:         {}",
        if account.is_open { "open" } else { "closed" }
    );
    println!("Opened:         {}", account.opened_at);
    println!(
        "Closed:         {}",
        account
            .closed_at
            .map(|d| d.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Booking method: {}", account.booking_method.as_str());

    if !account.currencies.is_empty() {
        let currency_strs: Vec<String> =
            account.currencies.iter().map(|c| c.0.to_string()).collect();
        println!("Currencies:     {}", currency_strs.join(", "));
    }

    if !account.metadata.is_empty() {
        println!("Metadata:");
        for (key, value) in &account.metadata {
            println!("  {}: {:?}", key, value);
        }
    }
    Ok(())
}

fn open(provider: &mut dyn StorageProvider, name: &str, date: Option<&str>) -> Result<()> {
    let account = provider
        .get_account_by_name(name)
        .context("Failed to look up account")?;
    let account = match account {
        Some(a) => a,
        None => bail!("Account '{}' not found", name),
    };

    let date = match date {
        Some(d) => parse_date(d)?,
        None => today(),
    };

    provider
        .open_account(account.id, date)
        .context("Failed to open account")?;
    println!("Opened account '{}' as of {}", name, date);
    Ok(())
}

fn close(provider: &mut dyn StorageProvider, name: &str, date: Option<&str>) -> Result<()> {
    let account = provider
        .get_account_by_name(name)
        .context("Failed to look up account")?;
    let account = match account {
        Some(a) => a,
        None => bail!("Account '{}' not found", name),
    };

    let date = match date {
        Some(d) => parse_date(d)?,
        None => today(),
    };

    provider
        .close_account(account.id, date)
        .context("Failed to close account")?;
    println!("Closed account '{}' as of {}", name, date);
    Ok(())
}

fn delete(provider: &mut dyn StorageProvider, name: &str) -> Result<()> {
    let account = provider
        .get_account_by_name(name)
        .context("Failed to look up account")?;
    let account = match account {
        Some(a) => a,
        None => bail!("Account '{}' not found", name),
    };

    provider
        .delete_account(account.id)
        .context("Failed to delete account")?;
    println!("Deleted account '{}'", name);
    Ok(())
}
