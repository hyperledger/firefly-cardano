use axum::{extract::State, Json};
use firefly_server::apitypes::{ApiError, ApiResult};
use schemars::JsonSchema;
use serde::Serialize;

use crate::{blockchain::ChainSyncClient, streams::BlockReference, AppState};

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChainTip {
    pub block_slot: Option<u64>,
    pub block_hash: String,
}

pub async fn get_chain_tip(
    State(AppState { blockchain, .. }): State<AppState>,
) -> ApiResult<Json<ChainTip>> {
    let mut sync = blockchain.sync().await?;
    let (_, BlockReference::Point(slot, hash)) = sync.find_intersect(&[]).await? else {
        return Err(ApiError::not_found("tip of chain not found"));
    };
    Ok(Json(ChainTip {
        block_slot: slot,
        block_hash: hash,
    }))
}
