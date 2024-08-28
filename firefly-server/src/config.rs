use std::path::Path;

use anyhow::Result;
use figment::{
    providers::{Data, Env, Format, Yaml},
    Figment,
};
use serde::Deserialize;

pub fn load_config<T>(suffix: &str, defaults: &str, config_file: Option<&Path>) -> Result<T>
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

    if let Some(file) = config_file {
        config = config.merge(Yaml::file_exact(file));
    }

    config = config.merge(Env::prefixed("FIREFLY_"));
    Ok(config.extract()?)
}

fn default_config_source(dir: &str, suffix: &str) -> Data<Yaml> {
    let name = format!("{dir}/firefly.{suffix}.yaml");
    Yaml::file(name)
}
