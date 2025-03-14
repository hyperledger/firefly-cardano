use std::collections::HashMap;

use balius_sdk::WorkerResult;
use serde::{Deserialize, Serialize};

use crate::kv;

/// Interface which tells the framework to monitor specific transactions.
pub struct FinalityMonitor;

impl FinalityMonitor {
    /// Tells the framework to monitor the given transaction.
    /// The current contract will emit events when the transaction has been accepted or rolled back,
    /// and also when it has been "finalized" (based on the condition passed here).
    pub fn monitor_tx(&self, hash: &str, condition: FinalizationCondition) -> WorkerResult<()> {
        let mut monitored_txs: HashMap<String, FinalizationCondition> =
            kv::get("__monitored_txs")?.unwrap_or_default();
        monitored_txs.insert(hash.to_string(), condition);
        kv::set("__monitored_txs", &monitored_txs)?;
        Ok(())
    }
}

/// How to decide when a transaction has been finalized
#[derive(Serialize, Deserialize)]
pub enum FinalizationCondition {
    // Treat the transaction as finalized after the given number of blocks have reached the chain.
    AfterBlocks(u64),
}
