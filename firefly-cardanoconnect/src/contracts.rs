use std::path::PathBuf;

use anyhow::{bail, Result};
use balius_runtime::{Runtime, Store};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{fs, sync::RwLock};

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContractsConfig {
    pub components_path: PathBuf,
    pub store_path: PathBuf,
    pub cache_size: Option<usize>,
}

pub struct ContractManager {
    runtime: Option<RwLock<Runtime>>,
    config: Option<ContractsConfig>,
}

impl ContractManager {
    pub async fn new(config: &ContractsConfig) -> Result<Self> {
        fs::create_dir_all(&config.components_path).await?;
        let runtime = Self::new_runtime(config).await?;
        Ok(Self {
            runtime: Some(RwLock::new(runtime)),
            config: Some(config.clone()),
        })
    }

    pub fn none() -> Self {
        Self {
            runtime: None,
            config: None,
        }
    }

    pub async fn invoke(&self, contract: &str, method: &str, params: Value) -> Result<Value> {
        let Some(rt_lock) = &self.runtime else {
            bail!("Contract manager not configured");
        };

        let runtime = rt_lock.read().await;
        let result = runtime.handle_request(contract, method, params).await?;

        Ok(result)
    }

    pub async fn connect(&self) -> Result<RuntimeWrapper> {
        let Some(config) = &self.config else {
            bail!("Contract manager not configured");
        };
        let runtime = Self::new_runtime(config).await?;
        Ok(RuntimeWrapper { runtime })
    }

    async fn new_runtime(config: &ContractsConfig) -> Result<Runtime> {
        let store = Store::open(&config.store_path, config.cache_size)?;
        let mut runtime = Runtime::builder(store).build()?;
        let mut entries = fs::read_dir(&config.components_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let extless = entry.path().with_extension("");
            let Some(id) = extless.file_name().and_then(|s| s.to_str()) else {
                bail!("invalid file name {:?}", entry.file_name().into_string());
            };
            let wasm_path = entry.path();

            runtime.register_worker(id, wasm_path, json!({})).await?;
        }
        Ok(runtime)
    }
}

pub struct RuntimeWrapper {
    runtime: Runtime,
}
impl RuntimeWrapper {
    #[expect(unused)]
    pub async fn collect_events(&mut self, contract: &str) -> Result<Vec<String>> {
        let _events = self
            .runtime
            .handle_request(contract, "_collect_events", json!({}))
            .await?;
        Ok(vec![])
    }
}
