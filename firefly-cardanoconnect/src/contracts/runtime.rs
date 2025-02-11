use std::{path::PathBuf, sync::Arc};

use anyhow::{bail, Context as _, Result};
use balius_runtime::{kv::Kv, ledgers::Ledger, ChainPoint, Response, Runtime, Store};
use serde::Deserialize;
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

#[derive(Clone, Deserialize)]
struct RawEvent {
    pub tx_hash: Vec<u8>,
    pub signature: String,
    pub data: serde_json::Value,
}
