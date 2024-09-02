use std::{sync::Arc, time::Duration};

use firefly_server::apitypes::{ApiError, ApiResult};
use ulid::Ulid;

use crate::persistence::Persistence;

use super::{Stream, StreamId};

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
}
