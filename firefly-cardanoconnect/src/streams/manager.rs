use std::{sync::Arc, time::Duration};

use firefly_server::apitypes::{ApiError, ApiResult};
use ulid::Ulid;

use crate::persistence::Persistence;

use super::{Listener, ListenerId, ListenerType, Stream, StreamId};

pub struct StreamManager {
    persistence: Arc<Persistence>,
}

impl StreamManager {
    pub fn new(persistence: Arc<Persistence>) -> Self {
        Self { persistence }
    }

    pub async fn create_stream(
        &self,
        name: &str,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> ApiResult<Stream> {
        let id = Ulid::new().to_string().into();
        let stream = Stream {
            id,
            name: name.to_string(),
            batch_size,
            batch_timeout,
        };
        self.persistence.write_stream(&stream).await?;
        Ok(stream)
    }

    pub async fn get_stream(&self, id: &StreamId) -> ApiResult<Stream> {
        let Some(stream) = self.persistence.read_stream(id).await? else {
            return Err(ApiError::not_found("No stream found with that id"));
        };
        Ok(stream)
    }

    pub async fn list_streams(
        &self,
        after: Option<StreamId>,
        limit: Option<usize>,
    ) -> ApiResult<Vec<Stream>> {
        Ok(self.persistence.list_streams(after, limit).await?)
    }

    pub async fn update_stream(
        &self,
        id: &StreamId,
        batch_size: Option<usize>,
        batch_timeout: Option<Duration>,
    ) -> ApiResult<Stream> {
        let Some(mut stream) = self.persistence.read_stream(id).await? else {
            return Err(ApiError::not_found("No stream found with that id"));
        };
        if let Some(size) = batch_size {
            stream.batch_size = size;
        }
        if let Some(timeout) = batch_timeout {
            stream.batch_timeout = timeout;
        }
        self.persistence.write_stream(&stream).await?;
        Ok(stream)
    }

    pub async fn delete_stream(&self, id: &StreamId) -> ApiResult<()> {
        self.persistence.delete_stream(id).await?;
        Ok(())
    }

    pub async fn create_listener(
        &self,
        stream_id: &StreamId,
        name: &str,
        listener_type: ListenerType,
    ) -> ApiResult<Listener> {
        if listener_type != ListenerType::Blocks {
            return Err(ApiError::not_implemented(
                "Only block event listeners are supported",
            ));
        }
        let id = Ulid::new().to_string().into();
        let listener = Listener {
            id,
            name: name.to_string(),
            listener_type,
            stream_id: stream_id.clone(),
        };
        self.persistence.write_listener(&listener).await?;
        Ok(listener)
    }

    pub async fn get_listener(
        &self,
        stream_id: &StreamId,
        listener_id: &ListenerId,
    ) -> ApiResult<Listener> {
        let Some(listener) = self
            .persistence
            .read_listener(stream_id, listener_id)
            .await?
        else {
            return Err(ApiError::not_found("No listener found with that id"));
        };
        Ok(listener)
    }

    pub async fn list_listeners(
        &self,
        stream_id: &StreamId,
        after: Option<ListenerId>,
        limit: Option<usize>,
    ) -> ApiResult<Vec<Listener>> {
        self.persistence
            .list_listeners(stream_id, after, limit)
            .await
    }

    pub async fn delete_listener(
        &self,
        stream_id: &StreamId,
        listener_id: &ListenerId,
    ) -> ApiResult<()> {
        self.persistence
            .delete_listener(stream_id, listener_id)
            .await?;
        Ok(())
    }
}
