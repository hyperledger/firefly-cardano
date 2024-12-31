use std::{
    cmp::Ordering,
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

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
    pub filters: Vec<ListenerFilter>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ListenerType {
    Events,
    Blocks,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ListenerFilter {
    TransactionId(String),
    Event {
        contract: String,
        event_path: String,
    },
}

#[derive(Clone, Debug)]
pub struct StreamCheckpoint {
    pub stream_id: StreamId,
    pub listeners: BTreeMap<ListenerId, EventReference>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockReference {
    Origin,
    Point(Option<u64>, String),
}
impl Default for BlockReference {
    fn default() -> Self {
        Self::Origin
    }
}
impl PartialOrd for BlockReference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            return Some(Ordering::Equal);
        }
        match (self, other) {
            (Self::Origin, Self::Origin) => Some(Ordering::Equal),
            (Self::Origin, _) => Some(Ordering::Less),
            (_, Self::Origin) => Some(Ordering::Greater),
            (
                Self::Point(block_number_l, block_hash_l),
                Self::Point(block_number_r, block_hash_r),
            ) => {
                if block_number_l != block_number_r {
                    block_number_l.partial_cmp(block_number_r)
                } else if block_hash_l != block_hash_r {
                    None
                } else {
                    Some(Ordering::Equal)
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventReference {
    pub block: BlockReference,
    pub rollback: bool,
    pub tx_index: Option<u64>,
    pub log_index: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BlockInfo {
    pub block_height: Option<u64>,
    pub block_slot: Option<u64>,
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub transaction_hashes: Vec<String>,
    pub transactions: Vec<Vec<u8>>,
}
impl BlockInfo {
    pub fn as_reference(&self) -> BlockReference {
        BlockReference::Point(self.block_slot, self.block_hash.clone())
    }
}

#[derive(Clone, Debug)]
pub struct BlockRecord {
    pub block: BlockInfo,
    pub rolled_back: bool,
}

#[derive(Clone, Debug)]
pub struct EventId {
    pub listener_id: ListenerId,
    pub signature: String,
    pub block_hash: String,
    pub block_number: Option<u64>,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub timestamp: Option<SystemTime>,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub id: EventId,
    pub data: serde_json::Value,
}
