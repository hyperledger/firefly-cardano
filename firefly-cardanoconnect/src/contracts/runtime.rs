use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{bail, Context as _, Result};
use balius_runtime::{
    kv::{CustomKv, Kv, KvError},
    ledgers::Ledger,
    ChainPoint, Response, Runtime, Store,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::warn;

use crate::streams::{BlockInfo, BlockReference};

use super::{kv::SqliteKv, u5c::convert_block, ContractsConfig};

#[derive(Clone)]
pub struct ContractRuntime {
    contract: String,
    kv: Option<Arc<Mutex<SqliteKv>>>,
    tx: mpsc::UnboundedSender<ContractRuntimeCommand>,
}

impl ContractRuntime {
    pub async fn new(
        contract: &str,
        config: Option<&ContractsConfig>,
        ledger: Option<Ledger>,
    ) -> Self {
        let runtime_config = config.map(|c| ContractRuntimeWorkerConfig {
            store_path: c.stores_path.join(contract).with_extension("redb"),
            wasm_path: c.components_path.join(contract).with_extension("wasm"),
            cache_size: c.cache_size,
        });
        let kv = if let Some(config) = config {
            let sqlite_path = config.stores_path.join("kv.sqlite3");
            match SqliteKv::new(&sqlite_path, contract).await {
                Ok(kv) => Some(Arc::new(Mutex::new(kv))),
                Err(error) => {
                    warn!("could not initialize sqlite db: {error:#}");
                    None
                }
            }
        } else {
            None
        };
        let mut worker = {
            let kv = kv.clone().map(|kv| Kv::Custom(kv));
            ContractRuntimeWorker::new(contract, runtime_config, ledger, kv)
        };
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            worker.run(rx).await;
        });
        Self {
            contract: contract.to_string(),
            kv,
            tx,
        }
    }

    pub async fn init(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(ContractRuntimeCommand::Init { done: tx })
            .is_err()
        {
            bail!("runtime for contract {} has stopped", self.contract);
        }
        rx.await?
    }

    pub async fn invoke(&self, method: &str, params: Value) -> Result<Response> {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(ContractRuntimeCommand::Invoke {
                method: method.to_string(),
                params,
                done: tx,
            })
            .is_err()
        {
            bail!("runtime for contract {} has stopped", self.contract);
        }
        rx.await?
    }

    pub async fn apply(&self, rollbacks: &[BlockInfo], block: &BlockInfo) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(ContractRuntimeCommand::Apply {
                rollbacks: rollbacks.to_vec(),
                block: block.clone(),
                done: tx,
            })
            .is_err()
        {
            bail!("runtime for contract {} has stopped", self.contract);
        }
        rx.await?
    }

    pub async fn events(&self, block_ref: &BlockReference) -> Result<Vec<ContractEvent>> {
        let key = match block_ref {
            BlockReference::Origin => {
                return Ok(vec![]);
            }
            BlockReference::Point(_, hash) => format!("__events_{hash}"),
        };
        let Some(kv) = &self.kv else {
            bail!("No contract directory configured");
        };
        let mut lock = kv.lock().await;
        let raw_events: Vec<RawEvent> = lock.get(key).await?.unwrap_or_default();
        Ok(raw_events
            .into_iter()
            .map(|e| ContractEvent {
                address: self.contract.clone(),
                tx_hash: hex::encode(e.tx_hash),
                signature: e.signature,
                data: e.data,
            })
            .collect())
    }
}

enum ContractRuntimeCommand {
    Init {
        done: oneshot::Sender<Result<()>>,
    },
    Invoke {
        method: String,
        params: Value,
        done: oneshot::Sender<Result<Response>>,
    },
    Apply {
        rollbacks: Vec<BlockInfo>,
        block: BlockInfo,
        done: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone)]
struct ContractRuntimeWorkerConfig {
    store_path: PathBuf,
    wasm_path: PathBuf,
    cache_size: Option<usize>,
}

struct ContractRuntimeWorker {
    contract: String,
    config: Option<ContractRuntimeWorkerConfig>,
    ledger: Option<Ledger>,
    kv: Option<Kv>,
    runtime: Option<Runtime>,
    head: BlockReference,
}

impl ContractRuntimeWorker {
    fn new(
        contract: &str,
        config: Option<ContractRuntimeWorkerConfig>,
        ledger: Option<Ledger>,
        kv: Option<Kv>,
    ) -> Self {
        Self {
            contract: contract.to_string(),
            config,
            ledger,
            kv,
            runtime: None,
            head: BlockReference::Origin,
        }
    }

    async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<ContractRuntimeCommand>) {
        while let Some(command) = rx.recv().await {
            match command {
                ContractRuntimeCommand::Init { done } => {
                    let _ = done.send(self.init_runtime().await);
                }
                ContractRuntimeCommand::Invoke {
                    method,
                    params,
                    done,
                } => {
                    let _ = done.send(self.invoke(&method, params).await);
                }
                ContractRuntimeCommand::Apply {
                    rollbacks,
                    block,
                    done,
                } => {
                    let _ = done.send(self.apply(&rollbacks, &block).await);
                }
            }
        }
    }

    async fn init_runtime(&mut self) -> Result<()> {
        // Drop the old runtime first, so that we don't open two redb stores to it at once
        self.runtime.take();

        let Some(config) = self.config.clone() else {
            bail!("Missing contract configuration")
        };

        let store =
            tokio::task::spawn_blocking(move || Store::open(&config.store_path, config.cache_size))
                .await??;
        let mut runtime_builder = Runtime::builder(store);
        if let Some(ledger) = self.ledger.clone() {
            runtime_builder = runtime_builder.with_ledger(ledger);
        }
        if let Some(kv) = self.kv.clone() {
            runtime_builder = runtime_builder.with_kv(kv);
        }

        let mut runtime = runtime_builder.build()?;
        runtime
            .register_worker(&self.contract, config.wasm_path, json!(null))
            .await?;

        let head = match runtime.chain_cursor().await {
            Ok(Some(ChainPoint::Cardano(r))) => {
                BlockReference::Point(Some(r.index), hex::encode(r.hash))
            }
            _ => BlockReference::Origin,
        };

        self.runtime = Some(runtime);
        self.head = head;
        Ok(())
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

        self.check_for_tx_lifecycle_events(rollbacks, block).await?;

        Ok(())
    }

    async fn check_for_tx_lifecycle_events(
        &mut self,
        rollbacks: &[BlockInfo],
        block: &BlockInfo,
    ) -> Result<()> {
        let Some(block_height) = block.block_height else {
            return Ok(());
        };

        let mut monitored_txs: HashMap<String, FinalizationCondition> =
            self.get_value("__monitored_txs").await?.unwrap_or_default();
        let mut tx_finalization_heights: BTreeMap<u64, HashSet<String>> = self
            .get_value("__tx_finalization_heights")
            .await?
            .unwrap_or_default();
        let mut new_events = vec![];

        let mut updated_monitored_txs = false;
        let mut updated_tx_finalization_heights = false;

        for rolled_back_block in rollbacks {
            for hash in &rolled_back_block.transaction_hashes {
                // If a TX which we're watching has been rolled back, don't emit a finalized event for it
                // (unless it gets reapplied in another block)
                if monitored_txs.contains_key(hash) {
                    tx_finalization_heights.retain(|_, hashes| {
                        if hashes.remove(hash) {
                            updated_tx_finalization_heights = true;
                        }
                        !hashes.is_empty()
                    });
                }
            }
        }

        for hash in &block.transaction_hashes {
            if let Some(condition) = monitored_txs.get(hash) {
                // A transaction we were monitoring has been submitted.
                // Now we know when it can be finalized.

                let FinalizationCondition::AfterBlocks(blocks_to_wait) = condition;
                let finalized_at_height = block_height + blocks_to_wait;
                tx_finalization_heights
                    .entry(finalized_at_height)
                    .or_default()
                    .insert(hash.clone());
                updated_tx_finalization_heights = true;
            }
        }

        if let Some(finalized_hash) = block.transaction_hashes.first() {
            while tx_finalization_heights
                .first_key_value()
                .is_some_and(|(height, _)| height <= &block_height)
            {
                // A transaction we were monitoring has been finalized.
                updated_tx_finalization_heights = true;

                let (_, txs) = tx_finalization_heights.pop_first().unwrap();
                for hash in txs {
                    if monitored_txs.remove(&hash).is_some() {
                        updated_monitored_txs = true;
                    }

                    new_events.push(RawEvent {
                        tx_hash: hex::decode(finalized_hash).unwrap(),
                        signature: "TransactionFinalized(string)".into(),
                        data: json!({
                            "transactionId": hash,
                        }),
                    });
                }
            }
        }

        if !new_events.is_empty() {
            let events_key = format!("__events_{}", block.block_hash);
            let mut events: Vec<RawEvent> = self.get_value(&events_key).await?.unwrap_or_default();
            events.append(&mut new_events);
            self.set_value(events_key, events).await?;
        }

        if updated_tx_finalization_heights {
            self.set_value("__tx_finalization_heights", tx_finalization_heights)
                .await?;
        }

        if updated_monitored_txs {
            self.set_value("__monitored_txs", monitored_txs).await?;
        }

        Ok(())
    }

    async fn get_value<T: DeserializeOwned>(&mut self, key: impl AsRef<str>) -> Result<Option<T>> {
        let Some(kv) = &mut self.kv else {
            bail!("No KV store configured");
        };
        match kv.get_value(key.as_ref().into()).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(KvError::NotFound(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn set_value<T: Serialize>(&mut self, key: impl AsRef<str>, value: T) -> Result<()> {
        let Some(kv) = &mut self.kv else {
            bail!("No KV store configured");
        };
        kv.set_value(key.as_ref().into(), serde_json::to_vec(&value)?)
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ContractEvent {
    pub address: String,
    pub tx_hash: String,
    pub signature: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawEvent {
    pub tx_hash: Vec<u8>,
    pub signature: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
enum FinalizationCondition {
    AfterBlocks(u64),
}
