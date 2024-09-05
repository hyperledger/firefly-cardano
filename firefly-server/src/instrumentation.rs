use std::str::FromStr;

use anyhow::Result;
use serde::Deserialize;
use tracing::Level;
use tracing_subscriber::{
    filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer, Registry,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogConfig {
    level: String,
}

pub fn init(config: &LogConfig) -> Result<()> {
    let level = Level::from_str(&config.level)?;

    let filter = Targets::new()
        .with_default(level)
        .with_target("hyper_util", Level::INFO);

    Registry::default()
        .with(fmt::layer().compact().with_filter(filter))
        .init();

    Ok(())
}
