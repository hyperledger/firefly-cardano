use std::path::PathBuf;

use anyhow::Result;
use convert_case::{Case, Casing as _};
use figment::{
    providers::{Data, Env, Format, Yaml},
    Figment,
};
use itertools::Itertools;
use serde::Deserialize;

pub fn load_config<T>(suffix: &str, defaults: &str, config_files: Vec<PathBuf>) -> Result<T>
where
    T: for<'a> Deserialize<'a>,
{
    let mut config = Figment::new()
        .merge(Yaml::string(include_str!("../config.base.yaml")))
        .merge(Yaml::string(defaults))
        .merge(default_config_source("/etc/firefly", suffix));

    if let Some(dir) = home::home_dir() {
        if let Some(firefly_dir) = dir.join(".firefly").to_str() {
            config = config.merge(default_config_source(firefly_dir, suffix));
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        if let Some(cwd_str) = cwd.to_str() {
            config = config.merge(default_config_source(cwd_str, suffix));
        }
    }

    for file in config_files {
        config = config.merge(Yaml::file_exact(file));
    }

    for depth in 0..7 {
        // We want to map env vars like "FIREFLY_CONNECTOR_BLOCKCHAIN_BLOCKFROST_KEY" to paths like "connector.blockchain.blockfrostKey".
        // We can't tell which underscores should be dots and which should stay underscores, so just guess.
        // If we get it wrong, the caller can always pass "FIREFLY_CONNECTOR.BLOCKCHAIN.BLOCKFROST_KEY" explicitly.
        let provider = Env::prefixed("FIREFLY_")
            .map(move |env| {
                let name = env.as_str().replacen("_", ".", depth);
                let camel: String = Itertools::intersperse(
                    name.split(".").map(|chunk| chunk.to_case(Case::Camel)),
                    ".".into(),
                )
                .collect();
                camel.into()
            })
            .lowercase(false);
        config = config.merge(provider);
    }
    Ok(config.extract()?)
}

fn default_config_source(dir: &str, suffix: &str) -> Data<Yaml> {
    let name = format!("{dir}/firefly.{suffix}.yaml");
    Yaml::file(name)
}
