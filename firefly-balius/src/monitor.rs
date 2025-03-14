use std::collections::HashMap;

use balius_sdk::WorkerResult;
use serde::{Deserialize, Serialize};

use crate::kv;

pub struct FinalityMonitor;

impl FinalityMonitor {
    pub fn monitor_tx(&self, hash: &str, condition: FinalizationCondition) -> WorkerResult<()> {
        let mut monitored_txs: HashMap<String, FinalizationCondition> =
            kv::get("__monitored_txs")?.unwrap_or_default();
        monitored_txs.insert(hash.to_string(), condition);
        kv::set("__monitored_txs", &monitored_txs)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub enum FinalizationCondition {
    AfterBlocks(usize),
}
