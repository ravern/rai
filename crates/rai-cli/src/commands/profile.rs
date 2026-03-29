use std::fs;

use anyhow::{bail, Context, Result};
use clap::Subcommand;

use rai_db::SqliteProvider;
use rai_db::StorageProvider;

use crate::config::{Config, ProfileConfig};

#[derive(Subcommand)]
pub enum ProfileAction {
    /// Create a new profile and initialize its database
    Create {
        /// Name for the new profile (e.g. personal, business)
        name: String,
    },
    /// List all configured profiles (default marked with *)
    List,
    /// Delete a profile and remove its database file
    Delete {
        /// Name of the profile to delete
        name: String,
    },
    /// Set which profile is used when --profile is omitted
    Default {
        /// Name of the profile to set as default
        name: String,
    },
}

pub fn handle(action: ProfileAction) -> Result<()> {
    match action {
        ProfileAction::Create { name } => create(&name),
        ProfileAction::List => list(),
        ProfileAction::Delete { name } => delete(&name),
        ProfileAction::Default { name } => set_default(&name),
    }
}

fn create(name: &str) -> Result<()> {
    let mut config = Config::load()?;

    if config.profiles.contains_key(name) {
        bail!("Profile '{}' already exists", name);
    }

    // Create database directory and initialize
    let db_path = Config::db_path(name);
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let mut provider = SqliteProvider::open(&db_path)
        .with_context(|| format!("Failed to create database at {}", db_path.display()))?;
    provider
        .initialize()
        .context("Failed to initialize database schema")?;

    // Add profile to config
    config
        .profiles
        .insert(name.to_string(), ProfileConfig::default());

    // If no default profile, set this one
    if config.default_profile.is_none() {
        config.default_profile = Some(name.to_string());
    }

    config.save()?;
    println!("Created profile '{}'", name);
    println!("Database: {}", db_path.display());
    Ok(())
}

fn list() -> Result<()> {
    let config = Config::load()?;

    if config.profiles.is_empty() {
        println!("No profiles configured. Create one with: rai profile create <name>");
        return Ok(());
    }

    let default = config.default_profile.as_deref().unwrap_or("");

    let mut names: Vec<&String> = config.profiles.keys().collect();
    names.sort();

    for name in names {
        let marker = if name.as_str() == default { " *" } else { "" };
        let db_path = config.resolve_db_path(name);
        println!("{}{} ({})", name, marker, db_path.display());
    }

    Ok(())
}

fn delete(name: &str) -> Result<()> {
    let mut config = Config::load()?;

    if !config.profiles.contains_key(name) {
        bail!("Profile '{}' does not exist", name);
    }

    // Remove database file
    let db_path = config.resolve_db_path(name);
    if db_path.exists() {
        fs::remove_file(&db_path)
            .with_context(|| format!("Failed to delete database: {}", db_path.display()))?;
        // Also try to remove WAL and SHM files
        let wal = db_path.with_extension("db-wal");
        let shm = db_path.with_extension("db-shm");
        let _ = fs::remove_file(wal);
        let _ = fs::remove_file(shm);
    }

    config.profiles.remove(name);

    // If this was the default, clear or pick another
    if config.default_profile.as_deref() == Some(name) {
        config.default_profile = config.profiles.keys().next().cloned();
    }

    config.save()?;
    println!("Deleted profile '{}'", name);
    Ok(())
}

fn set_default(name: &str) -> Result<()> {
    let mut config = Config::load()?;

    if !config.profiles.contains_key(name) {
        bail!("Profile '{}' does not exist", name);
    }

    config.default_profile = Some(name.to_string());
    config.save()?;
    println!("Default profile set to '{}'", name);
    Ok(())
}
