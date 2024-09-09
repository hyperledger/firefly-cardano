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
pub enum ListenerFilter {
    TransactionId(String),
}

#[derive(Clone, Debug)]
pub struct StreamCheckpoint {
    pub stream_id: StreamId,
    pub listeners: BTreeMap<ListenerId, EventReference>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum BlockReference {
    Origin,
    Point(u64, String),
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
impl BlockReference {
    pub fn equivalent(&self, other: &Self) -> bool {
        match (self, other) {
            (BlockReference::Origin, BlockReference::Origin) => true,
            (BlockReference::Point(number, _), BlockReference::Origin) => *number == 0,
            (BlockReference::Origin, BlockReference::Point(number, _)) => *number == 0,
            (BlockReference::Point(l_number, l_hash), BlockReference::Point(r_number, r_hash)) => {
                l_number == r_number && l_hash == r_hash
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EventReference {
    pub block: BlockReference,
    pub rollback: bool,
    pub tx_index: Option<u64>,
    pub log_index: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BlockInfo {
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub transaction_hashes: Vec<String>,
}
impl BlockInfo {
    pub fn as_reference(&self) -> BlockReference {
        BlockReference::Point(self.block_number, self.block_hash.clone())
    }
}

#[derive(Clone, Debug)]
pub struct EventId {
    pub listener_id: ListenerId,
    pub block_hash: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub timestamp: Option<SystemTime>,
}

#[derive(Clone, Debug)]
pub enum EventData {
    TransactionAccepted,
    TransactionRolledBack,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub id: EventId,
    pub data: EventData,
}
impl Event {
    pub fn signature(&self) -> String {
        match self.data {
            EventData::TransactionAccepted => "TransactionAccepted(string,string,string)".into(),
            EventData::TransactionRolledBack => {
                "TransactionRolledBack(string,string,string)".into()
            }
        }
    }
    pub fn into_rollback(self) -> Self {
        let data = match self.data {
            EventData::TransactionAccepted => EventData::TransactionRolledBack,
            EventData::TransactionRolledBack => EventData::TransactionAccepted,
        };
        Self { id: self.id, data }
    }
}
