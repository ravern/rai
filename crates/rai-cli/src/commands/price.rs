use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Subcommand;
use rust_decimal::Decimal;
use tabled::Tabled;

use rai_core::types::*;
use rai_db::StorageProvider;

use crate::output::print_table;

#[derive(Subcommand)]
pub enum PriceAction {
    /// Record a price: 1 <commodity> = <amount> <target> on <date>
    Create {
        /// Source commodity (e.g. BTC, AAPL)
        commodity: String,
        /// How many units of the target commodity one unit is worth
        amount: Decimal,
        /// Target commodity (e.g. USD)
        target_commodity: String,
        /// Price date in YYYY-MM-DD format
        #[arg(long)]
        date: String,
    },
    /// List recorded prices with optional commodity and date filters
    List {
        /// Show only prices for this source commodity
        #[arg(long)]
        commodity: Option<String>,
        /// Start date (inclusive, YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,
        /// End date (inclusive, YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
    },
    /// Remove a price entry by ID
    Delete {
        /// Price entry ID
        id: i64,
    },
}

#[derive(Tabled)]
struct PriceRow {
    #[tabled(rename = "ID")]
    id: i64,
    #[tabled(rename = "Date")]
    date: String,
    #[tabled(rename = "Commodity")]
    commodity: String,
    #[tabled(rename = "Price")]
    price: String,
    #[tabled(rename = "Target")]
    target: String,
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("Invalid date format '{}'. Expected YYYY-MM-DD", s))
}

pub fn handle(action: PriceAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        PriceAction::Create {
            commodity,
            amount,
            target_commodity,
            date,
        } => create(provider, &commodity, amount, &target_commodity, &date),
        PriceAction::List {
            commodity,
            from,
            to,
        } => list(
            provider,
            commodity.as_deref(),
            from.as_deref(),
            to.as_deref(),
        ),
        PriceAction::Delete { id } => delete(provider, id),
    }
}

fn create(
    provider: &mut dyn StorageProvider,
    commodity_name: &str,
    amount: Decimal,
    target_name: &str,
    date_str: &str,
) -> Result<()> {
    let date = parse_date(date_str)?;

    let commodity = provider
        .get_commodity_by_name(commodity_name)
        .context("Failed to look up commodity")?;
    let commodity = match commodity {
        Some(c) => c,
        None => bail!("Commodity '{}' not found", commodity_name),
    };

    let target = provider
        .get_commodity_by_name(target_name)
        .context("Failed to look up target commodity")?;
    let target = match target {
        Some(c) => c,
        None => bail!("Commodity '{}' not found", target_name),
    };

    let new_price = NewPrice {
        date,
        commodity_id: commodity.id,
        target_commodity_id: target.id,
        value: amount,
    };

    let price = provider
        .create_price(&new_price)
        .context("Failed to create price")?;
    println!(
        "Created price: 1 {} = {} {} on {} (id={})",
        commodity_name, amount, target_name, date, price.id
    );
    Ok(())
}

fn list(
    provider: &mut dyn StorageProvider,
    commodity_name: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<()> {
    let commodity_id = match commodity_name {
        Some(name) => {
            let c = provider
                .get_commodity_by_name(name)
                .with_context(|| format!("Failed to look up commodity '{}'", name))?;
            let c = match c {
                Some(c) => c,
                None => bail!("Commodity '{}' not found", name),
            };
            Some(c.id)
        }
        None => None,
    };

    let from_date = from.map(parse_date).transpose()?;
    let to_date = to.map(parse_date).transpose()?;

    let filter = PriceFilter {
        commodity_id,
        from: from_date,
        to: to_date,
    };

    let prices = provider
        .list_prices(&filter)
        .context("Failed to list prices")?;

    // Build commodity name map
    let commodities = provider
        .list_commodities()
        .context("Failed to list commodities")?;
    let commodity_map: std::collections::HashMap<CommodityId, String> = commodities
        .into_iter()
        .map(|c| (c.id, c.name))
        .collect();

    let commodity_name_for = |id: CommodityId| -> String {
        commodity_map
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("#{}", id.0))
    };

    let rows: Vec<PriceRow> = prices
        .into_iter()
        .map(|p| PriceRow {
            id: p.id.0,
            date: p.date.to_string(),
            commodity: commodity_name_for(p.commodity_id),
            price: p.value.to_string(),
            target: commodity_name_for(p.target_commodity_id),
        })
        .collect();

    print_table(&rows);
    Ok(())
}

fn delete(provider: &mut dyn StorageProvider, id: i64) -> Result<()> {
    provider
        .delete_price(PriceId(id))
        .context("Failed to delete price")?;
    println!("Deleted price {}", id);
    Ok(())
}
