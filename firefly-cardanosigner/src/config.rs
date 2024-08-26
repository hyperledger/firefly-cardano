use std::path::{Path, PathBuf};

use anyhow::Result;
use firefly_server::config::{self, ApiConfig};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct FireflySignerConfig {
    pub api: ApiConfig,
    #[serde(rename = "filewallet")]
    pub file_wallet: Option<FileWalletConfig>,
}

#[derive(Deserialize)]
pub struct FileWalletConfig {
    pub path: PathBuf,
}

pub fn load_config(config_file: Option<&Path>) -> Result<FireflySignerConfig> {
    config::load_config(
        "cardanosigner",
        include_str!("../config.base.yaml"),
        config_file,
    )
}
