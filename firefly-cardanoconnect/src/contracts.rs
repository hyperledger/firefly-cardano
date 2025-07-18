use std::{
    collections::{BTreeSet, HashMap, hash_map},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Result, bail};
use balius_runtime::ledgers::Ledger;
use dashmap::{DashMap, Entry};
pub use runtime::{ContractEvent, NewTx};
use runtime::{ContractRuntime, InvokeResponse};
use serde::Deserialize;
use serde_json::Value;
use tokio::fs;
use tracing::{error, warn};

use crate::{
    blockchain::BlockchainClient,
    contracts::u5c::UtxorpcAdapter,
    streams::{BlockInfo, BlockReference, Listener, ListenerFilter},
};

mod kv;
mod runtime;
mod u5c;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContractsConfig {
    pub components_path: PathBuf,
    pub stores_path: PathBuf,
    pub cache_size: Option<usize>,
}

pub struct ContractManager {
    config: Option<ContractsConfig>,
    ledger: Option<Ledger>,
    u5c: Arc<UtxorpcAdapter>,
    runtimes: DashMap<String, ContractRuntime>,
}

impl ContractManager {
    pub async fn new(config: &ContractsConfig, blockchain: &BlockchainClient) -> Result<Self> {
        fs::create_dir_all(&config.components_path).await?;
        fs::create_dir_all(&config.stores_path).await?;

        let ledger = Some(blockchain.ledger().await);
        let manager = Self {
            config: Some(config.clone()),
            u5c: Arc::new(UtxorpcAdapter::new()),
            ledger,
            runtimes: DashMap::new(),
        };

        let mut entries = fs::read_dir(&config.components_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let extless = entry.path().with_extension("");
            let Some(contract) = extless.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if let Err(error) = manager.init_contract_runtime(contract).await {
                warn!("Could not initialize contract {contract}: {error}");
            }
        }
        Ok(manager)
    }

    pub fn none() -> Self {
        Self {
            config: None,
            ledger: None,
            u5c: Arc::new(UtxorpcAdapter::new()),
            runtimes: DashMap::new(),
        }
    }

    pub async fn deploy(&self, id: &str, contract: &[u8]) -> Result<()> {
        let Some(config) = self.config.as_ref() else {
            bail!("No contract directory configured");
        };
        let path = config.components_path.join(format!("{id}.wasm"));
        fs::write(&path, contract).await?;
        self.init_contract_runtime(id).await
    }

    pub async fn invoke(
        &self,
        contract: &str,
        method: &str,
        params: Value,
    ) -> Result<Option<NewTx>> {
        let Some(runtime) = self.runtimes.get(contract) else {
            bail!("unrecognized contract {contract}");
        };
        let response = runtime.invoke(method, params).await?;
        match response {
            InvokeResponse::NewTx(tx) => Ok(Some(tx)),
            _ => Ok(None),
        }
    }

    pub async fn query(&self, contract: &str, method: &str, params: Value) -> Result<Value> {
        let Some(runtime) = self.runtimes.get(contract) else {
            bail!("unrecognized contract {contract}");
        };
        let response = runtime.invoke(method, params).await?;
        match response {
            InvokeResponse::Json(value) => Ok(value),
            InvokeResponse::NewTx(_) => bail!("Cannot build transactions from query"),
        }
    }

    pub async fn handle_submit(&self, contract: &str, tx_id: &str, tx: NewTx) -> Result<()> {
        let runtime = self.get_contract_runtime(contract).await;
        runtime.handle_submit(tx_id, tx).await
    }

    pub async fn listen(&self, listener: &Listener) -> ContractListener {
        let contract_filters = find_contract_filters(&listener.filters);
        let mut contracts = vec![];
        for (contract, filters) in contract_filters {
            let runtime = self.get_contract_runtime(&contract).await;
            contracts.push(ContractListenerContract { filters, runtime });
        }
        ContractListener {
            contracts,
            cache: HashMap::new(),
        }
    }

    async fn get_contract_runtime(&self, contract: &str) -> ContractRuntime {
        if !self.runtimes.contains_key(contract) {
            if let Err(error) = self.init_contract_runtime(contract).await {
                warn!("Could not init contract {contract}: {error}");
            }
        }
        self.runtimes.get(contract).unwrap().clone()
    }

    async fn init_contract_runtime(&self, contract: &str) -> Result<()> {
        let runtime = match self.runtimes.entry(contract.to_string()) {
            Entry::Vacant(entry) => entry.insert(
                ContractRuntime::new(
                    contract,
                    self.config.as_ref(),
                    self.ledger.clone(),
                    self.u5c.clone(),
                )
                .await,
            ),
            Entry::Occupied(entry) => entry.into_ref(),
        };
        runtime.init().await
    }
}

struct ContractListenerContract {
    filters: BTreeSet<String>,
    runtime: ContractRuntime,
}

pub struct ContractListener {
    contracts: Vec<ContractListenerContract>,
    cache: HashMap<BlockReference, Vec<ContractEvent>>,
}

impl ContractListener {
    pub async fn gather_events(&self, rollbacks: &[BlockInfo], block: &BlockInfo) {
        for contract in &self.contracts {
            if let Err(error) = contract.runtime.apply(rollbacks, block).await {
                error!("could not gather events for new blocks: {error:#}");
            }
        }
    }

    pub async fn events_for(&mut self, block_ref: &BlockReference) -> &[ContractEvent] {
        match self.cache.entry(block_ref.clone()) {
            hash_map::Entry::Occupied(entry) => entry.into_mut(),
            hash_map::Entry::Vacant(entry) => {
                let mut events = vec![];
                for contract in &self.contracts {
                    let contract_events = match contract.runtime.events(block_ref).await {
                        Ok(events) => events,
                        Err(error) => {
                            error!("could not retrieve events for block: {error}");
                            continue;
                        }
                    };
                    for event in contract_events {
                        if contract.filters.contains(&event.signature) {
                            events.push(event);
                        }
                    }
                }
                entry.insert(events)
            }
        }
    }
}

fn find_contract_filters(filters: &[ListenerFilter]) -> HashMap<String, BTreeSet<String>> {
    let mut result: HashMap<String, BTreeSet<String>> = HashMap::new();
    for filter in filters {
        if let ListenerFilter::Event {
            contract,
            event_path,
        } = filter
        {
            result
                .entry(contract.clone())
                .or_default()
                .insert(event_path.clone());
        }
    }
    result
}
