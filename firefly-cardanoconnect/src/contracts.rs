use std::{
    collections::{hash_map, BTreeSet, HashMap},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{bail, Result};
use balius_runtime::{ledgers::Ledger, Response};
use dashmap::{DashMap, Entry};
use ledger::BlockfrostLedger;
pub use runtime::ContractEvent;
use runtime::ContractRuntime;
use serde::Deserialize;
use serde_json::Value;
use tokio::{fs, sync::Mutex};
use tracing::{error, warn};

use crate::{
    blockfrost::BlockfrostClient,
    streams::{BlockInfo, BlockReference, Listener, ListenerFilter},
};

mod kv;
mod ledger;
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
    runtimes: DashMap<String, ContractRuntime>,
}

impl ContractManager {
    pub async fn new(
        config: &ContractsConfig,
        blockfrost: Option<BlockfrostClient>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.components_path).await?;
        fs::create_dir_all(&config.stores_path).await?;
        let ledger = blockfrost.map(|client| {
            let ledger = BlockfrostLedger::new(client);
            Ledger::Custom(Arc::new(Mutex::new(ledger)))
        });
        let manager = Self {
            config: Some(config.clone()),
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
    ) -> Result<Option<Vec<u8>>> {
        let Some(runtime) = self.runtimes.get(contract) else {
            bail!("unrecognized contract {contract}");
        };
        let response = runtime.invoke(method, params).await?;
        match response {
            Response::PartialTx(bytes) => Ok(Some(bytes)),
            _ => Ok(None),
        }
    }

    pub async fn handle_submit(&self, contract: &str, method: &str, tx_id: &str) {
        let params = serde_json::json!({
            "method": method,
            "hash": tx_id,
        });
        let runtime = self.get_contract_runtime(contract).await;
        let _: Result<_, _> = runtime.invoke("__tx_submitted", params).await;
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
                ContractRuntime::new(contract, self.config.as_ref(), self.ledger.clone()).await,
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
                        if contract
                            .filters
                            .iter()
                            .any(|f| event.signature.starts_with(f))
                        {
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
