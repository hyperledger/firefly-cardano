use anyhow::Result;
use firefly_server::apitypes::{ApiError, ApiResult};
use tokio::sync::Mutex;

use crate::streams::{Stream, StreamId};

#[derive(Default)]
pub struct Persistence {
    all_streams: Mutex<Vec<Stream>>,
}

impl Persistence {
    pub async fn write_stream(&self, stream: &Stream) -> ApiResult<()> {
        let mut streams = self.all_streams.lock().await;
        if streams
            .iter()
            .any(|it| it.name == stream.name && it.id != stream.id)
        {
            return Err(ApiError::conflict("Stream with that name already exists"));
        }

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

    pub async fn read_stream(&self, id: &StreamId) -> ApiResult<Option<Stream>> {
        let streams = self.all_streams.lock().await;
        Ok(streams.iter().find(|it| it.id == *id).cloned())
    }

    pub async fn delete_stream(&self, id: &StreamId) -> Result<()> {
        let mut streams = self.all_streams.lock().await;
        streams.retain(|it| it.id != *id);
        Ok(())
    }

    pub async fn list_streams(
        &self,
        after: Option<StreamId>,
        limit: Option<usize>,
    ) -> Result<Vec<Stream>> {
        let all_streams = self.all_streams.lock().await;

        let streams = all_streams
            .iter()
            .filter(|s| !after.as_ref().is_some_and(|v| *v < s.id))
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();
        Ok(streams)
    }
}
