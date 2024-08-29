use std::path::Path;

use anyhow::Result;
use firefly_server::{config, http::HttpClientConfig, server::ApiConfig};
use serde::Deserialize;

use crate::blockchain::BlockchainConfig;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CardanoConnectConfig {
    pub api: ApiConfig,
    pub connector: ConnectorConfig,
    #[serde(default)]
    pub http: HttpClientConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorConfig {
    pub signer_url: String,
    pub blockchain: BlockchainConfig,
}

pub fn load_config(config_file: Option<&Path>) -> Result<CardanoConnectConfig> {
    config::load_config(
        "cardanoconnect",
        include_str!("../config.base.yaml"),
        config_file,
    )
}
