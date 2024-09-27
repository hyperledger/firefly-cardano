use anyhow::Result;
use async_trait::async_trait;
use firefly_server::apitypes::ApiResult;
use mocks::MockPersistence;

use crate::streams::{BlockRecord, Listener, ListenerId, Stream, StreamCheckpoint, StreamId};

mod mocks;

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
    async fn save_block_record(&self, listener: &ListenerId, record: BlockRecord) -> Result<()>;
}

pub fn mock() -> MockPersistence {
    MockPersistence::default()
}
