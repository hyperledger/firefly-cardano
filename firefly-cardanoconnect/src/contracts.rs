use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use balius_runtime::{ledgers::Ledger, ChainPoint, Response, Runtime, Store};
use dashmap::{DashMap, Entry};
use ledger::BlockfrostLedger;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{fs, sync::Mutex};
use tracing::{error, warn};
use u5c::convert_block;

use crate::{
    blockfrost::BlockfrostClient,
    streams::{BlockInfo, BlockReference, Event, Listener, ListenerFilter},
};

mod ledger;
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
    runtimes: DashMap<String, Arc<Mutex<ContractRuntime>>>,
}

impl ContractManager {
    pub async fn new(
        config: &ContractsConfig,
        blockfrost: Option<BlockfrostClient>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.components_path).await?;
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
        let Some(mutex) = self.runtimes.get(contract) else {
            bail!("unrecognized contract {contract}");
        };
        let mut runtime = mutex.lock().await;
        let response = runtime.invoke(method, params).await?;
        match response {
            Response::PartialTx(bytes) => Ok(Some(bytes)),
            _ => Ok(None),
        }
    }

    pub async fn listen(&self, listener: &Listener) -> ContractListener {
        let contracts = find_contract_names(&listener.filters);
        let mut runtimes = vec![];
        for contract in contracts {
            let runtime = self.get_contract_runtime(&contract).await;
            runtimes.push(runtime);
        }
        ContractListener {
            runtimes,
            cache: HashMap::new(),
        }
    }

    async fn get_contract_runtime(&self, contract: &str) -> Arc<Mutex<ContractRuntime>> {
        if !self.runtimes.contains_key(contract) {
            if let Err(error) = self.init_contract_runtime(contract).await {
                warn!("Could not init contract {contract}: {error}");
            }
        }
        self.runtimes.get(contract).unwrap().clone()
    }

    async fn init_contract_runtime(&self, contract: &str) -> Result<()> {
        match self.runtimes.entry(contract.to_string()) {
            Entry::Vacant(entry) => match self.new_runtime_for(contract).await {
                Ok(rt) => {
                    let runtime = ContractRuntime::new(contract, rt).await;
                    entry.insert(Arc::new(Mutex::new(runtime)));
                }
                Err(err) => {
                    entry.insert(Arc::new(Mutex::new(ContractRuntime::empty(contract))));
                    return Err(err);
                }
            },
            Entry::Occupied(entry) => {
                let mutex = entry.into_ref();
                let mut lock = mutex.lock().await;
                lock.runtime = None; // drop the old runtime before creating the new one
                lock.runtime = Some(self.new_runtime_for(contract).await?);
            }
        };
        Ok(())
    }

    async fn new_runtime_for(&self, contract: &str) -> Result<Runtime> {
        let Some(config) = self.config.as_ref() else {
            bail!("No contract directory configured");
        };
        let store_path = config.stores_path.join(contract).with_extension("redb");
        let store = Store::open(&store_path, config.cache_size)?;
        let mut runtime_builder = Runtime::builder(store);
        if let Some(ledger) = self.ledger.clone() {
            runtime_builder = runtime_builder.with_ledger(ledger);
        }

        let mut runtime = runtime_builder.build()?;

        let wasm_path = config.components_path.join(contract).with_extension("wasm");
        runtime
            .register_worker(contract, wasm_path, json!(null))
            .await?;

        Ok(runtime)
    }
}

pub struct ContractListener {
    runtimes: Vec<Arc<Mutex<ContractRuntime>>>,
    cache: HashMap<BlockReference, Vec<Event>>,
}

impl ContractListener {
    pub async fn gather_events(&self, rollbacks: &[BlockInfo], block: &BlockInfo) {
        for runtime in &self.runtimes {
            let mut lock = runtime.lock().await;
            if let Err(error) = lock.apply(rollbacks, block).await {
                error!("could not gather events for new blocks: {error}");
            }
        }

        // TODO: actually gather events
    }

    pub async fn events_for(&self, block_ref: &BlockReference) -> Vec<Event> {
        self.cache.get(block_ref).cloned().unwrap_or_default()
    }
}

struct ContractRuntime {
    contract: String,
    runtime: Option<Runtime>,
    head: BlockReference,
}

impl ContractRuntime {
    async fn new(contract: &str, runtime: Runtime) -> Self {
        let head = match runtime.chain_cursor().await {
            Ok(Some(ChainPoint::Cardano(r))) => {
                BlockReference::Point(Some(r.index), hex::encode(r.hash))
            }
            _ => BlockReference::Origin,
        };
        Self {
            contract: contract.to_string(),
            runtime: Some(runtime),
            head,
        }
    }
    fn empty(contract: &str) -> Self {
        Self {
            contract: contract.to_string(),
            runtime: None,
            head: BlockReference::Origin,
        }
    }

    async fn invoke(&mut self, method: &str, params: Value) -> Result<Response> {
        let params = serde_json::to_vec(&params)?;
        let Some(runtime) = self.runtime.as_mut() else {
            bail!("Contract {} failed to initialize", self.contract);
        };
        Ok(runtime
            .handle_request(&self.contract, method, params)
            .await?)
    }

    async fn apply(&mut self, rollbacks: &[BlockInfo], block: &BlockInfo) -> Result<()> {
        let Some(runtime) = self.runtime.as_mut() else {
            bail!("Contract {} failed to initialize", self.contract);
        };

        if rollbacks
            .first()
            .is_some_and(|rb| rb.as_reference() != self.head)
        {
            // this is a rollback from a point we're not already at, ignore it
            return Ok(());
        } else if block.as_reference() <= self.head {
            // we've already advanced past this point
            return Ok(());
        }

        let undo_blocks = rollbacks.iter().map(convert_block).collect();
        let next_block = convert_block(block);
        runtime
            .handle_chain(&undo_blocks, &next_block)
            .await
            .context("could not apply blocks")?;

        self.head = block.as_reference();

        Ok(())
    }
}

fn find_contract_names(filters: &[ListenerFilter]) -> Vec<String> {
    let mut result = BTreeSet::new();
    for filter in filters {
        if let ListenerFilter::Event { contract, .. } = filter {
            result.insert(contract.clone());
        }
    }
    result.into_iter().collect()
}
