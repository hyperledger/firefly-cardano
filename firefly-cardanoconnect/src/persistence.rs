use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use firefly_server::apitypes::ApiResult;
use mocks::MockPersistence;
use serde::Deserialize;
use sqlite::{SqliteConfig, SqlitePersistence};

use crate::{
    operations::{Operation, OperationId, OperationUpdate, OperationUpdateId},
    streams::{BlockRecord, Listener, ListenerId, Stream, StreamCheckpoint, StreamId},
};

mod mocks;
mod sqlite;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum PersistenceConfig {
    Mock,
    Sqlite(SqliteConfig),
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self::Mock
    }
}

#[async_trait]
pub trait Persistence: Sync + Send {
    async fn write_stream(&self, stream: &Stream) -> ApiResult<()>;
    async fn read_stream(&self, id: &StreamId) -> ApiResult<Option<Stream>>;
    async fn delete_stream(&self, id: &StreamId) -> Result<()>;
    async fn list_streams(
        &self,
        after: Option<StreamId>,
        limit: Option<usize>,
    ) -> Result<Vec<Stream>>;

    async fn write_listener(&self, listener: &Listener) -> ApiResult<()>;
    async fn read_listener(
        &self,
        stream_id: &StreamId,
        listener_id: &ListenerId,
    ) -> ApiResult<Option<Listener>>;
    async fn delete_listener(&self, stream_id: &StreamId, listener_id: &ListenerId) -> Result<()>;
    async fn list_listeners(
        &self,
        stream_id: &StreamId,
        after: Option<ListenerId>,
        limit: Option<usize>,
    ) -> ApiResult<Vec<Listener>>;
    async fn write_checkpoint(&self, checkpoint: &StreamCheckpoint) -> ApiResult<()>;
    async fn read_checkpoint(&self, stream_id: &StreamId) -> ApiResult<Option<StreamCheckpoint>>;

    async fn load_history(&self, listener: &ListenerId) -> Result<Vec<BlockRecord>>;
    async fn save_block_records(
        &self,
        listener: &ListenerId,
        new_records: Vec<BlockRecord>,
    ) -> Result<()>;

    async fn write_operation(&self, op: &Operation) -> ApiResult<OperationUpdateId>;
    async fn read_operation(&self, id: &OperationId) -> ApiResult<Option<Operation>>;
    async fn list_operation_updates(
        &self,
        after: Option<&OperationUpdateId>,
        limit: usize,
    ) -> Result<Vec<OperationUpdate>>;
    async fn latest_operation_update(&self) -> Result<Option<OperationUpdateId>>;
}

pub async fn init(config: &PersistenceConfig) -> Result<Arc<dyn Persistence>> {
    match config {
        PersistenceConfig::Mock => Ok(Arc::new(MockPersistence::default())),
        PersistenceConfig::Sqlite(c) => Ok(Arc::new(SqlitePersistence::new(c).await?)),
    }
}
