use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context as _, Result, bail};
use balius_runtime::{
    ChainPoint, Response, Runtime, Store,
    kv::{Kv, KvError, KvProvider},
    ledgers::Ledger,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::streams::{BlockInfo, BlockReference};

use super::{ContractsConfig, kv::SqliteKv, u5c::UtxorpcAdapter};

type SharedKvProvider = Arc<Mutex<dyn KvProvider + Send + Sync + 'static>>;

#[derive(Clone)]
pub struct ContractRuntime {
    contract: String,
    kv: Option<SharedKvProvider>,
    tx: mpsc::UnboundedSender<ContractRuntimeCommand>,
}

impl ContractRuntime {
    pub async fn new(
        contract: &str,
        config: Option<&ContractsConfig>,
        ledger: Option<Ledger>,
        u5c: Arc<UtxorpcAdapter>,
    ) -> Self {
        let runtime_config = config.map(|c| ContractRuntimeWorkerConfig {
            store_path: c.stores_path.join(format!("{contract}.redb")),
            wasm_path: c.components_path.join(format!("{contract}.wasm")),
            cache_size: c.cache_size,
        });
        let kv: Option<SharedKvProvider> = if let Some(config) = config {
            let sqlite_path = config.stores_path.join("kv.sqlite3");
            match SqliteKv::new(&sqlite_path).await {
                Ok(kv) => match kv.init(contract).await {
                    Ok(()) => Some(Arc::new(Mutex::new(kv))),
                    Err(error) => {
                        warn!("could not initialize sqlite db: {error:#}");
                        None
                    }
                },
                Err(error) => {
                    warn!("could not initialize sqlite db: {error:#}");
                    None
                }
            }
        } else {
            None
        };
        let mut worker =
            ContractRuntimeWorker::new(contract, runtime_config, ledger, kv.clone(), u5c);
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

    pub async fn invoke(&self, method: &str, params: Value) -> Result<InvokeResponse> {
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

    pub async fn handle_submit(&self, tx_id: &str, new_tx: NewTx) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(ContractRuntimeCommand::HandleSubmit {
                tx_id: tx_id.to_string(),
                new_tx,
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
        let raw_events: Vec<RawEvent> = match lock.get_value(&self.contract, key).await {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(KvError::NotFound(_)) => vec![],
            Err(e) => return Err(e.into()),
        };
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
        done: oneshot::Sender<Result<InvokeResponse>>,
    },
    HandleSubmit {
        tx_id: String,
        new_tx: NewTx,
        done: oneshot::Sender<Result<()>>,
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
    kv: Option<SharedKvProvider>,
    u5c: Arc<UtxorpcAdapter>,
    runtime: Option<Runtime>,
    head: BlockReference,
}

impl ContractRuntimeWorker {
    fn new(
        contract: &str,
        config: Option<ContractRuntimeWorkerConfig>,
        ledger: Option<Ledger>,
        kv: Option<SharedKvProvider>,
        u5c: Arc<UtxorpcAdapter>,
    ) -> Self {
        Self {
            contract: contract.to_string(),
            config,
            ledger,
            kv,
            u5c,
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
                ContractRuntimeCommand::HandleSubmit {
                    tx_id,
                    new_tx,
                    done,
                } => {
                    let _ = done.send(self.handle_submit(&tx_id, new_tx).await);
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
            runtime_builder = runtime_builder.with_kv(Kv::Custom(kv));
        }

        let runtime = runtime_builder.build()?;
        runtime
            .register_worker_from_file(&self.contract, config.wasm_path, json!(null))
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

    async fn invoke(&mut self, method: &str, params: Value) -> Result<InvokeResponse> {
        let params = serde_json::to_vec(&params)?;
        let Some(runtime) = self.runtime.as_mut() else {
            bail!("Contract {} failed to initialize", self.contract);
        };
        let response = runtime
            .handle_request(&self.contract, method, params)
            .await?;
        match response {
            Response::Acknowledge => Ok(InvokeResponse::Json(json!({}))),
            Response::Cbor(bytes) => Ok(InvokeResponse::Json(json!({ "cbor": bytes }))),
            Response::PartialTx(bytes) => Ok(InvokeResponse::NewTx(NewTx {
                bytes,
                method: method.to_string(),
                condition: None,
            })),
            Response::Json(bytes) => {
                let value: Value = serde_json::from_slice(&bytes)?;
                if let Ok(RawNewTx::FireFlyCardanoNewTx { bytes, condition }) =
                    serde_json::from_value(value.clone())
                {
                    Ok(InvokeResponse::NewTx(NewTx {
                        bytes: hex::decode(bytes)?,
                        method: method.to_string(),
                        condition: Some(condition),
                    }))
                } else {
                    Ok(InvokeResponse::Json(value))
                }
            }
        }
    }

    async fn handle_submit(&mut self, tx_id: &str, new_tx: NewTx) -> Result<()> {
        if let Some(condition) = new_tx.condition {
            let mut monitored_txs: HashMap<String, FinalizationCondition> =
                self.get_value("__monitored_txs").await?.unwrap_or_default();
            monitored_txs.insert(tx_id.to_string(), condition);
            self.set_value("__monitored_txs", monitored_txs).await?;
        }

        let params = json!({
            "method": new_tx.method,
            "hash": tx_id,
        });
        let _: Result<_, _> = self.invoke("__tx_submitted", params).await;
        Ok(())
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
        let (undo_blocks, next_block) = {
            let u5c = &self.u5c;
            let undo_blocks_fut =
                futures::future::try_join_all(rollbacks.iter().map(|b| u5c.convert_block(b)));
            let next_block_fut = u5c.convert_block(block);
            tokio::try_join!(undo_blocks_fut, next_block_fut)?
        };
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
                if monitored_txs.contains_key(hash) {
                    // A TX which we're watching has been rolled back
                    new_events.push(RawEvent {
                        tx_hash: hex::decode(hash).unwrap(),
                        signature: "TransactionRolledBack(string)".into(),
                        data: json!({
                            "transactionId": hash,
                        }),
                    });

                    // Don't emit a finalized event for it (unless it gets reapplied in another block)
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
                // A transaction we were monitoring has been accepted.
                new_events.push(RawEvent {
                    tx_hash: hex::decode(hash).unwrap(),
                    signature: "TransactionAccepted(string)".into(),
                    data: json!({
                        "transactionId": hash,
                    }),
                });

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
        let mut lock = kv.lock().await;
        match lock.get_value(&self.contract, key.as_ref().into()).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(KvError::NotFound(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn set_value<T: Serialize>(&mut self, key: impl AsRef<str>, value: T) -> Result<()> {
        let Some(kv) = &mut self.kv else {
            bail!("No KV store configured");
        };
        let mut lock = kv.lock().await;
        lock.set_value(
            &self.contract,
            key.as_ref().into(),
            serde_json::to_vec(&value)?,
        )
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

pub enum InvokeResponse {
    Json(Value),
    NewTx(NewTx),
}

pub struct NewTx {
    pub bytes: Vec<u8>,
    method: String,
    condition: Option<FinalizationCondition>,
}

#[derive(Debug, Serialize, Deserialize)]
enum FinalizationCondition {
    AfterBlocks(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawEvent {
    pub tx_hash: Vec<u8>,
    pub signature: String,
    pub data: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum RawNewTx {
    FireFlyCardanoNewTx {
        bytes: String,
        condition: FinalizationCondition,
    },
}
