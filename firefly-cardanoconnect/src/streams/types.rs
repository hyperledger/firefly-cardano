use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::strong_id;

strong_id!(StreamId, String);

/// Represents a stream of some sort of event.
/// Consumers can create a Listener to listen to events from the stream.
#[derive(Clone, Debug)]
pub struct Stream {
    pub id: StreamId,
    pub name: String,
    pub batch_size: usize,
    pub batch_timeout: Duration,
}

strong_id!(ListenerId, String);

/// Represents some consumer listening on a stream
#[derive(Clone, Debug)]
pub struct Listener {
    pub id: ListenerId,
    pub name: String,
    pub listener_type: ListenerType,
    pub stream_id: StreamId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ListenerType {
    Events,
    Blocks,
}

#[derive(Clone, Debug)]
pub struct BlockInfo {
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub transaction_hashes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct BlockEvent {
    pub listener_id: ListenerId,
    pub block_info: BlockInfo,
}
