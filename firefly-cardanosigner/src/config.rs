use std::path::Path;

use anyhow::Result;
use firefly_server::config::{self, ServerConfig};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct FireflySignerConfig {
    pub server: ServerConfig,
}

pub fn load_config(config_file: Option<&Path>) -> Result<FireflySignerConfig> {
    config::load_config(
        "cardanosigner",
        include_str!("../config.base.yaml"),
        config_file,
    )
}
