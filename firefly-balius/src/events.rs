use std::collections::HashMap;

use balius_sdk::{Utxo, WorkerResult};
use serde::{Deserialize, Serialize};

use crate::kv;

/// A helper trait, which makes it easy to define custom events.
pub trait EventData: Serialize {
    /// The signature of this event.
    /// FireFly expects this to follow a certain format based on the schema of the event. For an event with the following schema:
    /// ```json
    /// {
    ///   "name": "Created",
    ///   "params": [
    ///     {
    ///       "name": "from",
    ///       "schema": {
    ///         "type": "string"
    ///       }
    ///     },
    ///     {
    ///       "name": "value",
    ///       "schema": {
    ///         "type": "number"
    ///       }
    ///     }
    ///   ]
    /// }
    /// ```
    /// The signature should be `Created(string, number)`.
    fn signature(&self) -> String;
}

/**
 * A custom event. Pass this to [emit_events] to send it to consumers.
 */
#[derive(Serialize, Deserialize)]
pub struct Event {
    /// The hash of the block which triggered this event.
    pub block_hash: Vec<u8>,
    /// The hash of the transaction which triggered this event.
    pub tx_hash: Vec<u8>,
    /// The signature of this event. See [EventData::signature] for details.
    pub signature: String,
    /// The data attached to this event.
    pub data: serde_json::Value,
}

impl Event {
    /// Builds a new event for the given uTXO.
    pub fn new<D, E: EventData>(utxo: &Utxo<D>, data: &E) -> WorkerResult<Self> {
        Ok(Self {
            block_hash: utxo.block_hash.clone(),
            tx_hash: utxo.tx_hash.clone(),
            signature: data.signature(),
            data: serde_json::to_value(data)?,
        })
    }
}

/// Emits a group of custom events to the FireFly Cardano connector.
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
