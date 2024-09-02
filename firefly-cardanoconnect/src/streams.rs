use std::time::Duration;

use crate::strong_id;

mod manager;
pub use manager::StreamManager;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
