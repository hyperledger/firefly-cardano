use std::collections::HashMap;

use balius_sdk::{Utxo, WorkerResult};
use serde::{Deserialize, Serialize};

use crate::kv;

pub trait EventData: Serialize {
    fn signature(&self) -> String;
}

#[derive(Serialize, Deserialize)]
pub struct Event {
    pub block_hash: Vec<u8>,
    pub tx_hash: Vec<u8>,
    pub signature: String,
    pub data: serde_json::Value,
}

impl Event {
    pub fn new<D, E: EventData>(utxo: &Utxo<D>, data: &E) -> WorkerResult<Self> {
        Ok(Self {
            block_hash: utxo.block_hash.clone(),
            tx_hash: utxo.tx_hash.clone(),
            signature: data.signature(),
            data: serde_json::to_value(data)?,
        })
    }
}

pub fn emit_events(events: Vec<Event>) -> WorkerResult<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut block_events: HashMap<String, Vec<Event>> = HashMap::new();
    for event in events {
        let block_hash = hex::encode(&event.block_hash);
        block_events.entry(block_hash).or_default().push(event);
    }

    for (block, mut events) in block_events {
        let events_key = format!("__events_{block}");
        let mut all_events: Vec<Event> = kv::get(&events_key)?.unwrap_or_default();
        all_events.append(&mut events);
        kv::set(&events_key, &all_events)?;
    }

    Ok(())
}
