use std::{fmt::Debug, path::Path};

use anyhow::Result;
use firefly_server::{
    config, http::HttpClientConfig, instrumentation::LogConfig, server::ApiConfig,
};
use serde::Deserialize;

use crate::blockchain::BlockchainConfig;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CardanoConnectConfig {
    pub api: ApiConfig,
    pub connector: ConnectorConfig,
    #[serde(default)]
    pub http: HttpClientConfig,
    pub log: LogConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorConfig {
    pub signer_url: String,
    pub blockchain: BlockchainConfig,
}

#[derive(Deserialize, Clone)]
pub struct Secret<T>(pub T);

impl<T> Debug for Secret<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

pub fn load_config(config_file: Option<&Path>) -> Result<CardanoConnectConfig> {
    config::load_config(
        "cardanoconnect",
        include_str!("../config.base.yaml"),
        config_file,
    )
}
