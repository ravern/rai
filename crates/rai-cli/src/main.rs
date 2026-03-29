mod commands;
mod config;
mod output;

use std::fs;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use rai_db::{SqliteProvider, StorageProvider};

use commands::account::AccountAction;
use commands::balance::BalanceAction;
use commands::commodity::CommodityAction;
use commands::file::FileAction;
use commands::price::PriceAction;
use commands::profile::ProfileAction;
use commands::report::ReportAction;
use commands::transaction::TxAction;
use config::Config;

#[derive(Parser)]
#[command(
    name = "rai",
    about = "Double-entry accounting for the command line",
    long_about = "rai is a double-entry accounting system backed by SQLite.\n\n\
        All data lives in a local database that can be queried directly with SQL.\n\
        Use profiles to maintain separate ledgers (personal, business, etc.)."
)]
struct Cli {
    /// Use a specific profile instead of the default
    #[arg(long, global = true)]
    profile: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create, list, and switch between profiles (separate ledgers)
    Profile {
        #[command(subcommand)]
        action: ProfileAction,
    },
    /// Define currencies, securities, and other trackable units
    Commodity {
        #[command(subcommand)]
        action: CommodityAction,
    },
    /// Manage the chart of accounts (Assets, Liabilities, Income, Expenses, Equity)
    Account {
        #[command(subcommand)]
        action: AccountAction,
    },
    /// Record and query transactions with balanced postings
    Tx {
        #[command(subcommand)]
        action: TxAction,
    },
    /// Record market prices and exchange rates between commodities
    Price {
        #[command(subcommand)]
        action: PriceAction,
    },
    /// Assert expected account balances for validation
    Balance {
        #[command(subcommand)]
        action: BalanceAction,
    },
    /// Export or import ledger data as a human-readable .rai file
    File {
        #[command(subcommand)]
        action: FileAction,
    },
    /// Check the ledger for errors (unbalanced transactions, failed assertions)
    Validate,
    /// Generate balance sheets, income statements, and other reports
    Report {
        #[command(subcommand)]
        action: ReportAction,
    },
    /// Run SQL queries against the database (REPL if no query given)
    Query {
        /// SQL to execute; omit to enter the interactive REPL
        sql: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    // Profile commands don't need a database connection
    if let Commands::Profile { action } = cli.command {
        return commands::profile::handle(action);
    }

    // All other commands need a provider
    let mut provider = open_provider(cli.profile.as_deref())?;

    match cli.command {
        Commands::Profile { .. } => unreachable!(),
        Commands::File { action } => commands::file::handle(action, &mut provider),
        Commands::Commodity { action } => commands::commodity::handle(action, &mut provider),
        Commands::Account { action } => commands::account::handle(action, &mut provider),
        Commands::Tx { action } => commands::transaction::handle(action, &mut provider),
        Commands::Price { action } => commands::price::handle(action, &mut provider),
        Commands::Balance { action } => commands::balance::handle(action, &mut provider),
        Commands::Validate => commands::validate::handle(&mut provider),
        Commands::Report { action } => commands::report::handle(action, &mut provider),
        Commands::Query { sql } => commands::query::handle(sql, &mut provider),
    }
}

fn open_provider(profile_name: Option<&str>) -> Result<SqliteProvider> {
    let config = Config::load()?;

    let name = match profile_name {
        Some(n) => n.to_string(),
        None => match config.default_profile {
            Some(ref d) => d.clone(),
            None => bail!(
                "No profile specified and no default profile set. \
                 Create one with: rai profile create <name>"
            ),
        },
    };

    if !config.profiles.contains_key(&name) {
        bail!(
            "Profile '{}' does not exist. Create it with: rai profile create {}",
            name,
            name
        );
    }

    let db_path = config.resolve_db_path(&name);

    // Ensure parent directory exists
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let mut provider = SqliteProvider::open(&db_path)
        .with_context(|| format!("Failed to open database at {}", db_path.display()))?;
    provider
        .initialize()
        .context("Failed to initialize database schema")?;

    Ok(provider)
}
