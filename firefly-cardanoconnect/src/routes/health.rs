use axum::{Json, extract::State};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::AppState;

pub async fn health(
    State(AppState { blockchain, .. }): State<AppState>,
) -> (StatusCode, Json<Value>) {
    match blockchain.health().await {
        Ok(()) => (StatusCode::OK, Json(json!({}))),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "blockchain": error.to_string(),
            })),
        ),
    }
}
