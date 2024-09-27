use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use firefly_server::apitypes::{ApiError, ApiResult};
use tokio::sync::Mutex;

use crate::streams::{BlockRecord, Listener, ListenerId, Stream, StreamCheckpoint, StreamId};

use super::Persistence;

#[derive(Default)]
pub struct MockPersistence {
    all_streams: Mutex<Vec<Stream>>,
    all_listeners: DashMap<StreamId, Vec<Listener>>,
    all_checkpoints: DashMap<StreamId, StreamCheckpoint>,
    all_blocks: DashMap<ListenerId, HashMap<String, BlockRecord>>,
}

#[async_trait]
impl Persistence for MockPersistence {
    async fn write_stream(&self, stream: &Stream) -> ApiResult<()> {
        let mut streams = self.all_streams.lock().await;
        if streams
            .iter()
            .any(|it| it.name == stream.name && it.id != stream.id)
        {
            return Err(ApiError::conflict("Stream with that name already exists"));
        }

        self.all_listeners.entry(stream.id.clone()).or_default();

        match streams.iter_mut().find(|it| it.id == stream.id) {
            Some(old) => {
                *old = stream.clone();
            }
            None => {
                streams.push(stream.clone());
            }
        }

        Ok(())
    }

    async fn read_stream(&self, id: &StreamId) -> ApiResult<Option<Stream>> {
        let streams = self.all_streams.lock().await;
        Ok(streams.iter().find(|it| it.id == *id).cloned())
    }

    async fn delete_stream(&self, id: &StreamId) -> Result<()> {
        let mut streams = self.all_streams.lock().await;
        streams.retain(|it| it.id != *id);

        self.all_listeners.remove(id);
        self.all_checkpoints.remove(id);

        Ok(())
    }

    async fn list_streams(
        &self,
        after: Option<StreamId>,
        limit: Option<usize>,
    ) -> Result<Vec<Stream>> {
        let all_streams = self.all_streams.lock().await;

        let streams = all_streams
            .iter()
            .filter(|s| !after.as_ref().is_some_and(|v| *v >= s.id))
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();
        Ok(streams)
    }

    async fn write_listener(&self, listener: &Listener) -> ApiResult<()> {
        let Some(mut listeners) = self.all_listeners.get_mut(&listener.stream_id) else {
            return Err(ApiError::not_found("No stream found with that ID"));
        };
        match listeners.iter_mut().find(|it| it.id == listener.id) {
            Some(old) => {
                *old = listener.clone();
            }
            None => {
                listeners.push(listener.clone());
            }
        }

        Ok(())
    }

    async fn read_listener(
        &self,
        stream_id: &StreamId,
        listener_id: &ListenerId,
    ) -> ApiResult<Option<Listener>> {
        let Some(listeners) = self.all_listeners.get(stream_id) else {
            return Err(ApiError::not_found("No stream found with that ID"));
        };
        Ok(listeners.iter().find(|it| it.id == *listener_id).cloned())
    }

    async fn delete_listener(&self, stream_id: &StreamId, listener_id: &ListenerId) -> Result<()> {
        let Some(mut listeners) = self.all_listeners.get_mut(stream_id) else {
            return Ok(());
        };
        listeners.retain(|it| it.id != *listener_id);

        Ok(())
    }

    async fn list_listeners(
        &self,
        stream_id: &StreamId,
        after: Option<ListenerId>,
        limit: Option<usize>,
    ) -> ApiResult<Vec<Listener>> {
        let Some(stream_listeners) = self.all_listeners.get(stream_id) else {
            return Err(ApiError::not_found("No stream found with that ID"));
        };
        let listeners = stream_listeners
            .iter()
            .filter(|l| !after.as_ref().is_some_and(|v| *v >= l.id))
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();
        Ok(listeners)
    }

    async fn write_checkpoint(&self, checkpoint: &StreamCheckpoint) -> ApiResult<()> {
        // check stream existence by checking if we have a (possibly empty) list of listeners for it,
        // because that's cheaper than locking the streams list
        if !self.all_listeners.contains_key(&checkpoint.stream_id) {
            return Err(ApiError::not_found("No stream found with that ID"));
        }
        self.all_checkpoints
            .insert(checkpoint.stream_id.clone(), checkpoint.clone());
        Ok(())
    }

    async fn read_checkpoint(&self, stream_id: &StreamId) -> ApiResult<Option<StreamCheckpoint>> {
        match self.all_checkpoints.get(stream_id) {
            Some(checkpoint) => Ok(Some(checkpoint.clone())),
            None => Ok(None),
        }
    }

    async fn load_history(&self, listener: &ListenerId) -> Result<Vec<BlockRecord>> {
        match self.all_blocks.get(listener) {
            Some(records) => Ok(records.values().cloned().collect()),
            None => Ok(vec![]),
        }
    }

    async fn save_block_record(&self, listener: &ListenerId, record: BlockRecord) -> Result<()> {
        let mut records = self.all_blocks.entry(listener.clone()).or_default();
        records.insert(record.block.block_hash.clone(), record);
        Ok(())
    }
}
