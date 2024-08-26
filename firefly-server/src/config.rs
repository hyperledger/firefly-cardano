use std::path::Path;

use aide::openapi::Info;
use anyhow::Result;
use config::{Config, Environment, File, FileFormat, FileSourceFile};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct ApiConfig {
    pub address: String,
    pub port: u16,
    pub info: Option<Info>,
}

pub fn load_config<T>(suffix: &str, defaults: &str, config_file: Option<&Path>) -> Result<T>
where
    T: for<'a> Deserialize<'a>,
{
    let mut builder = Config::builder()
        .add_source(File::from_str(
            include_str!("../config.base.yaml"),
            FileFormat::Yaml,
        ))
        .add_source(File::from_str(defaults, FileFormat::Yaml))
        .add_source(default_file_source("/etc/firefly", suffix));

    if let Some(dir) = home::home_dir() {
        if let Some(firefly_dir) = dir.join(".firefly").to_str() {
            builder = builder.add_source(default_file_source(firefly_dir, suffix));
        }
    }

    builder = builder.add_source(default_file_source(".", suffix));

    if let Some(file) = config_file {
        let file: File<FileSourceFile, FileFormat> = file.into();
        builder = builder.add_source(file);
    }
    let config = builder
        .add_source(Environment::with_prefix("firefly"))
        .build()?;
    Ok(config.try_deserialize()?)
}

fn default_file_source(dir: &str, suffix: &str) -> File<FileSourceFile, FileFormat> {
    let name = format!("{dir}/firefly.{suffix}.yaml");
    File::with_name(&name).required(false)
}
