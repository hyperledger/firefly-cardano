use std::path::PathBuf;

use anyhow::Result;
use firefly_server::{config, instrumentation::LogConfig, server::ApiConfig};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireflySignerConfig {
    pub api: ApiConfig,
    pub file_wallet: Option<FileWalletConfig>,
    pub log: LogConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileWalletConfig {
    pub path: PathBuf,
}

pub fn load_config(config_files: Vec<PathBuf>) -> Result<FireflySignerConfig> {
    config::load_config(
        "cardanosigner",
        include_str!("../config.base.yaml"),
        config_files,
    )
}
