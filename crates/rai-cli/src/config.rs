use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Config {
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: HashMap<String, ProfileConfig>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct ProfileConfig {
    pub path: Option<String>,
}

impl Config {
    /// Load config from disk. Returns default if file does not exist.
    pub fn load() -> Result<Self> {
        let path = Self::config_path();
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: Config =
            toml::from_str(&content).with_context(|| "Failed to parse config file")?;
        Ok(config)
    }

    /// Save config to disk.
    pub fn save(&self) -> Result<()> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config directory: {}", parent.display()))?;
        }
        let content = toml::to_string_pretty(self).context("Failed to serialize config")?;
        fs::write(&path, content)
            .with_context(|| format!("Failed to write config file: {}", path.display()))?;
        Ok(())
    }

    /// Path to the config file: ~/.config/rai/config.toml
    pub fn config_path() -> PathBuf {
        let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from(".config"));
        config_dir.join("rai").join("config.toml")
    }

    /// Default database path for a profile: ~/.local/share/rai/<name>.db
    pub fn db_path(name: &str) -> PathBuf {
        let data_dir = dirs::data_dir().unwrap_or_else(|| PathBuf::from(".local/share"));
        data_dir.join("rai").join(format!("{}.db", name))
    }

    /// Resolve the database path for a profile. If the profile has a custom
    /// path configured, use that; otherwise fall back to the default.
    pub fn resolve_db_path(&self, name: &str) -> PathBuf {
        if let Some(profile) = self.profiles.get(name) {
            if let Some(ref custom_path) = profile.path {
                return PathBuf::from(custom_path);
            }
        }
        Self::db_path(name)
    }
}
