use std::time::Duration;

use axum::extract::{Path, Query};
use axum::{extract::State, Json};
use firefly_server::apitypes::{ApiError, ApiResult, NoContent};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::streams::{BlockReference, ListenerFilter, ListenerType, Stream};
use crate::AppState;

fn example_batch_size() -> usize {
    50
}

fn example_opt_batch_size() -> Option<usize> {
    Some(example_batch_size())
}

fn example_batch_timeout_ms() -> u64 {
    500
}

fn example_opt_batch_timeout_ms() -> Option<u64> {
    Some(example_batch_timeout_ms())
}

fn example_from_block() -> Option<String> {
    Some("earliest".into())
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateStreamRequest {
    pub name: String,
    #[schemars(example = "example_batch_size")]
    pub batch_size: usize,
    #[serde(rename = "batchTimeoutMS")]
    #[schemars(example = "example_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StreamPathParameters {
    pub stream_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStreamRequest {
    #[schemars(example = "example_opt_batch_size")]
    pub batch_size: Option<usize>,
    #[serde(rename = "batchTimeoutMS")]
    #[schemars(example = "example_opt_batch_timeout_ms")]
    pub batch_timeout_ms: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventStream {
    pub id: String,
    pub name: String,
    #[schemars(example = "example_batch_size")]
    pub batch_size: usize,
    #[serde(rename = "batchTimeoutMS")]
    #[schemars(example = "example_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}
impl From<Stream> for EventStream {
    fn from(value: Stream) -> Self {
        Self {
            id: value.id.into(),
            name: value.name,
            batch_size: value.batch_size,
            batch_timeout_ms: value.batch_timeout.as_millis() as u64,
        }
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    pub limit: Option<usize>,
    pub after: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateListenerRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ListenerType,
    #[schemars(example = "example_from_block")]
    pub from_block: Option<String>,
    #[serde(default)]
    pub filters: Vec<ListenerFilter>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListenerPathParameters {
    pub stream_id: String,
    pub listener_id: String,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Listener {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ListenerType,
}
impl From<crate::streams::Listener> for Listener {
    fn from(value: crate::streams::Listener) -> Self {
        Self {
            id: value.id.into(),
            name: value.name,
            type_: value.listener_type,
        }
    }
}

pub async fn create_stream(
    State(AppState { stream_manager, .. }): State<AppState>,
    Json(req): Json<CreateStreamRequest>,
) -> ApiResult<Json<EventStream>> {
    let stream = stream_manager
        .create_stream(
            &req.name,
            req.batch_size,
            Duration::from_millis(req.batch_timeout_ms),
        )
        .await?;
    Ok(Json(stream.into()))
}

pub async fn get_stream(
    State(AppState { stream_manager, .. }): State<AppState>,
    Path(StreamPathParameters { stream_id }): Path<StreamPathParameters>,
) -> ApiResult<Json<EventStream>> {
    let id = stream_id.into();
    let stream = stream_manager.get_stream(&id).await?;
    Ok(Json(stream.into()))
}

pub async fn delete_stream(
    State(AppState { stream_manager, .. }): State<AppState>,
    Path(StreamPathParameters { stream_id }): Path<StreamPathParameters>,
) -> ApiResult<NoContent> {
    let id = stream_id.into();
    stream_manager.delete_stream(&id).await?;
    Ok(NoContent)
}

pub async fn list_streams(
    State(AppState { stream_manager, .. }): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> ApiResult<Json<Vec<EventStream>>> {
    let after = pagination.after.map(|id| id.into());
    let limit = pagination.limit;
    let streams = stream_manager.list_streams(after, limit).await?;
    Ok(Json(streams.into_iter().map(|s| s.into()).collect()))
}

pub async fn update_stream(
    State(AppState { stream_manager, .. }): State<AppState>,
    Path(StreamPathParameters { stream_id }): Path<StreamPathParameters>,
    Json(req): Json<UpdateStreamRequest>,
) -> ApiResult<Json<EventStream>> {
    let id = stream_id.into();
    let batch_size = req.batch_size;
    let batch_timeout = req.batch_timeout_ms.map(Duration::from_millis);
    let stream = stream_manager
        .update_stream(&id, batch_size, batch_timeout)
        .await?;
    Ok(Json(stream.into()))
}

pub async fn create_listener(
    State(AppState { stream_manager, .. }): State<AppState>,
    Path(StreamPathParameters { stream_id }): Path<StreamPathParameters>,
    Json(req): Json<CreateListenerRequest>,
) -> ApiResult<Json<Listener>> {
    let stream_id = stream_id.into();
    let from_block = match req.from_block {
        Some(block) => parse_block_reference(&block)?,
        None => None,
    };
    let listener = stream_manager
        .create_listener(&stream_id, &req.name, req.type_, from_block, &req.filters)
        .await?;
    Ok(Json(listener.into()))
}

pub async fn get_listener(
    State(AppState { stream_manager, .. }): State<AppState>,
    Path(ListenerPathParameters {
        stream_id,
        listener_id,
    }): Path<ListenerPathParameters>,
) -> ApiResult<Json<Listener>> {
    let stream_id = stream_id.into();
    let listener_id = listener_id.into();
    let listener = stream_manager
        .get_listener(&stream_id, &listener_id)
        .await?;
    Ok(Json(listener.into()))
}

pub async fn delete_listener(
    State(AppState { stream_manager, .. }): State<AppState>,
    Path(ListenerPathParameters {
        stream_id,
        listener_id,
    }): Path<ListenerPathParameters>,
) -> ApiResult<NoContent> {
    let stream_id = stream_id.into();
    let listener_id = listener_id.into();
    stream_manager
        .delete_listener(&stream_id, &listener_id)
        .await?;
    Ok(NoContent)
}

pub async fn list_listeners(
    State(AppState { stream_manager, .. }): State<AppState>,
    Path(StreamPathParameters { stream_id }): Path<StreamPathParameters>,
    Query(pagination): Query<Pagination>,
) -> ApiResult<Json<Vec<Listener>>> {
    let stream_id = stream_id.into();
    let after = pagination.after.map(|id| id.into());
    let limit = pagination.limit;
    let listeners = stream_manager
        .list_listeners(&stream_id, after, limit)
        .await?;
    Ok(Json(listeners.into_iter().map(|l| l.into()).collect()))
}

fn parse_block_reference(value: &str) -> ApiResult<Option<BlockReference>> {
    match value {
        "earliest" => Ok(Some(BlockReference::Origin)),
        "latest" => Ok(None),
        other => {
            let Some((slot_number, slot_hash)) = other.split_once(".") else {
                return Err(ApiError::bad_request("invalid block reference"));
            };
            let Ok(slot_number) = slot_number.parse() else {
                return Err(ApiError::bad_request("invalid block reference"));
            };
            Ok(Some(BlockReference::Point(
                Some(slot_number),
                slot_hash.to_string(),
            )))
        }
    }
}
