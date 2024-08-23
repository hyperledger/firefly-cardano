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

const DEFAULT_CONFIG_PATHS: &[&str] = &["/etc/firefly", "$HOME/.firefly", "."];

pub fn load_config<T>(suffix: &str, defaults: &str, config_file: Option<&Path>) -> Result<T>
where
    T: for<'a> Deserialize<'a>,
{
    let mut builder = Config::builder()
        .add_source(File::from_str(
            include_str!("../config.base.yaml"),
            FileFormat::Yaml,
        ))
        .add_source(File::from_str(defaults, FileFormat::Yaml));
    for path in DEFAULT_CONFIG_PATHS {
        let filename = format!("{path}/firefly.{suffix}.yaml");
        builder = builder.add_source(File::with_name(&filename).required(false));
    }
    if let Some(file) = config_file {
        let file: File<FileSourceFile, FileFormat> = file.into();
        builder = builder.add_source(file);
    }
    let config = builder
        .add_source(Environment::with_prefix("firefly"))
        .build()?;
    Ok(config.try_deserialize()?)
}
