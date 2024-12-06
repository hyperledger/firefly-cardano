use std::{path::PathBuf, sync::Arc};

use anyhow::{bail, Result};
use balius_runtime::{ledgers::Ledger, Response, Runtime, Store};
use ledger::BlockfrostLedger;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    fs,
    sync::{Mutex, RwLock},
};

use crate::blockfrost::BlockfrostClient;

mod ledger;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContractsConfig {
    pub components_path: PathBuf,
    pub store_path: PathBuf,
    pub cache_size: Option<usize>,
}

pub struct ContractManager {
    components_path: Option<PathBuf>,
    runtime: Option<RwLock<Runtime>>,
}

impl ContractManager {
    pub async fn new(
        config: &ContractsConfig,
        blockfrost: Option<BlockfrostClient>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.components_path).await?;
        let runtime = Self::new_runtime(config, blockfrost).await?;
        Ok(Self {
            components_path: Some(config.components_path.clone()),
            runtime: Some(RwLock::new(runtime)),
        })
    }

    pub fn none() -> Self {
        Self {
            components_path: None,
            runtime: None,
        }
    }

    pub async fn deploy(&self, id: &str, contract: &[u8]) -> Result<()> {
        let Some(components_path) = self.components_path.as_deref() else {
            bail!("No contract directory configured");
        };
        let path = components_path.join(format!("{id}.wasm"));
        fs::write(&path, contract).await?;
        if let Some(rt_lock) = &self.runtime {
            let mut runtime = rt_lock.write().await;
            runtime.register_worker(id, path, json!(null)).await?;
        }
        Ok(())
    }

    pub async fn invoke(
        &self,
        contract: &str,
        method: &str,
        params: Value,
    ) -> Result<Option<Vec<u8>>> {
        let params = serde_json::to_vec(&params)?;
        let Some(rt_lock) = &self.runtime else {
            bail!("Contract manager not configured");
        };

        let runtime = rt_lock.read().await;
        let response = runtime.handle_request(contract, method, params).await?;
        match response {
            Response::PartialTx(bytes) => Ok(Some(bytes)),
            _ => Ok(None),
        }
    }

    async fn new_runtime(
        config: &ContractsConfig,
        blockfrost: Option<BlockfrostClient>,
    ) -> Result<Runtime> {
        let store = Store::open(&config.store_path, config.cache_size)?;
        let mut runtime_builder = Runtime::builder(store);
        if let Some(client) = blockfrost {
            let ledger = BlockfrostLedger::new(client);
            runtime_builder =
                runtime_builder.with_ledger(Ledger::Custom(Arc::new(Mutex::new(ledger))))
        }
        let mut runtime = runtime_builder.build()?;
        let mut entries = fs::read_dir(&config.components_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let extless = entry.path().with_extension("");
            let Some(id) = extless.file_name().and_then(|s| s.to_str()) else {
                bail!("invalid file name {:?}", entry.file_name().into_string());
            };
            let wasm_path = entry.path();

            runtime.register_worker(id, wasm_path, json!(null)).await?;
        }
        Ok(runtime)
    }
}
