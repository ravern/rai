use anyhow::{Context, Result};
use colored::Colorize;

use rai_core::types::*;
use rai_core::validation::validate;
use rai_db::StorageProvider;

pub fn handle(provider: &mut dyn StorageProvider) -> Result<()> {
    let transactions = provider
        .list_transactions(&TransactionFilter::default())
        .context("Failed to load transactions")?;
    let accounts = provider
        .list_accounts(&AccountFilter::default())
        .context("Failed to load accounts")?;
    let commodities = provider
        .list_commodities()
        .context("Failed to load commodities")?;
    let assertions = provider
        .list_balance_assertions(&BalanceAssertionFilter::default())
        .context("Failed to load balance assertions")?;

    let result = validate(&transactions, &accounts, &commodities, &assertions);

    if result.errors.is_empty() {
        println!("{}", "Validation passed: no errors found.".green());
        return Ok(());
    }

    println!(
        "{}",
        format!("Validation found {} error(s):", result.errors.len()).red()
    );
    println!();

    for error in &result.errors {
        println!("  {} {}", "ERROR:".red().bold(), error);
    }

    Ok(())
}
