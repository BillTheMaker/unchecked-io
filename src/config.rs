// src/config.rs
use serde::Deserialize;
use anyhow::{Context, Result, anyhow};
use std::fmt::{self, Display};

#[derive(Debug)]
pub struct ConfigError(String);

impl Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Configuration Error: {}", self.0)
    }
}
impl std::error::Error for ConfigError {}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnConfig {
    pub column_name: String,
    pub arrow_type: String,
}

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    pub connection_string: String,
    pub query: String,
    pub schema: Vec<ColumnConfig>,
    // NEW: Optional Batch Size configuration
    pub batch_size: Option<usize>,
}

pub fn load_and_validate_config(path: &str) -> Result<ConnectorConfig> {
    let file_content = std::fs::read_to_string(path)
        .context(format!("Failed to read config file at path: {}", path))?;

    let config: ConnectorConfig = serde_yaml::from_str(&file_content)
        .context("Failed to deserialize YAML configuration")?;

    if config.connection_string.is_empty() {
        return Err(ConfigError("Connection string cannot be empty.".to_string())).map_err(anyhow::Error::from)?;
    }
    if config.query.is_empty() {
        return Err(ConfigError("Query cannot be empty.".to_string())).map_err(anyhow::Error::from)?;
    }
    let uppercase_query = config.query.trim().to_uppercase();
    if !uppercase_query.starts_with("COPY") || !uppercase_query.contains("TO STDOUT") || !uppercase_query.contains("FORMAT BINARY") {
        return Err(ConfigError("Query must be a 'COPY ... TO STDOUT (FORMAT binary)' command.".to_string())).map_err(anyhow::Error::from)?;
    }
    if config.schema.is_empty() {
        return Err(ConfigError("Schema cannot be empty.".to_string())).map_err(anyhow::Error::from)?;
    }

    Ok(config)
}