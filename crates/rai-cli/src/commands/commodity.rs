use anyhow::{bail, Context, Result};
use clap::Subcommand;
use tabled::Tabled;

use rai_core::types::*;
use rai_db::StorageProvider;

use crate::output::print_table;

#[derive(Subcommand)]
pub enum CommodityAction {
    /// Define a new commodity (currency, security, etc.)
    Create {
        /// Unique identifier (e.g. USD, BTC, AAPL)
        name: String,
        /// Number of decimal places (e.g. 2 for USD, 8 for BTC)
        #[arg(long)]
        precision: u8,
    },
    /// List all defined commodities
    List,
    /// Show details and metadata for a commodity
    Show {
        /// Commodity name to look up
        name: String,
    },
    /// Update commodity properties
    Update {
        /// Commodity name to update
        name: String,
        /// New decimal precision
        #[arg(long)]
        precision: Option<u8>,
    },
    /// Remove a commodity (fails if still referenced by postings)
    Delete {
        /// Commodity name to delete
        name: String,
    },
}

#[derive(Tabled)]
struct CommodityRow {
    #[tabled(rename = "ID")]
    id: i64,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Precision")]
    precision: u8,
}

pub fn handle(action: CommodityAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        CommodityAction::Create { name, precision } => create(provider, &name, precision),
        CommodityAction::List => list(provider),
        CommodityAction::Show { name } => show(provider, &name),
        CommodityAction::Update { name, precision } => update(provider, &name, precision),
        CommodityAction::Delete { name } => delete(provider, &name),
    }
}

fn create(provider: &mut dyn StorageProvider, name: &str, precision: u8) -> Result<()> {
    let new = NewCommodity {
        name: name.to_string(),
        precision,
        metadata: Metadata::new(),
    };
    let commodity = provider
        .create_commodity(&new)
        .context("Failed to create commodity")?;
    println!(
        "Created commodity '{}' (id={}, precision={})",
        commodity.name, commodity.id, commodity.precision
    );
    Ok(())
}

fn list(provider: &mut dyn StorageProvider) -> Result<()> {
    let commodities = provider
        .list_commodities()
        .context("Failed to list commodities")?;
    let rows: Vec<CommodityRow> = commodities
        .into_iter()
        .map(|c| CommodityRow {
            id: c.id.0,
            name: c.name,
            precision: c.precision,
        })
        .collect();
    print_table(&rows);
    Ok(())
}

fn show(provider: &mut dyn StorageProvider, name: &str) -> Result<()> {
    let commodity = provider
        .get_commodity_by_name(name)
        .context("Failed to look up commodity")?;
    let commodity = match commodity {
        Some(c) => c,
        None => bail!("Commodity '{}' not found", name),
    };

    println!("ID:        {}", commodity.id);
    println!("Name:      {}", commodity.name);
    println!("Precision: {}", commodity.precision);
    if !commodity.metadata.is_empty() {
        println!("Metadata:");
        for (key, value) in &commodity.metadata {
            println!("  {}: {:?}", key, value);
        }
    }
    Ok(())
}

fn update(provider: &mut dyn StorageProvider, name: &str, precision: Option<u8>) -> Result<()> {
    let commodity = provider
        .get_commodity_by_name(name)
        .context("Failed to look up commodity")?;
    let commodity = match commodity {
        Some(c) => c,
        None => bail!("Commodity '{}' not found", name),
    };

    let update = CommodityUpdate { precision };
    let updated = provider
        .update_commodity(commodity.id, &update)
        .context("Failed to update commodity")?;
    println!(
        "Updated commodity '{}' (precision={})",
        updated.name, updated.precision
    );
    Ok(())
}

fn delete(provider: &mut dyn StorageProvider, name: &str) -> Result<()> {
    let commodity = provider
        .get_commodity_by_name(name)
        .context("Failed to look up commodity")?;
    let commodity = match commodity {
        Some(c) => c,
        None => bail!("Commodity '{}' not found", name),
    };

    provider
        .delete_commodity(commodity.id)
        .context("Failed to delete commodity")?;
    println!("Deleted commodity '{}'", name);
    Ok(())
}
