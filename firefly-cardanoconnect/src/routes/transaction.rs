use anyhow::Context;
use axum::{extract::State, Json};
use firefly_server::apitypes::{ApiResult, Context as _};
use pallas_primitives::conway::Tx;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Deserialize, JsonSchema)]
pub struct SubmitTransactionRequest {
    /// The address of the key to sign the transaction with.
    address: String,
    /// The raw CBOR-encoded transaction to submit.
    transaction: String,
}

#[derive(Serialize, JsonSchema)]
pub struct SubmitTransactionResponse {
    /// The ID of the submitted transaction
    txid: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct TryBaliusRequest {
    /// The name of the contract to invoke
    contract: String,
    /// The method of the contract to invoke
    method: String,
}

#[derive(Serialize, JsonSchema)]
pub struct TryBaliusResponse {}

pub async fn submit_transaction(
    State(AppState {
        blockchain, signer, ..
    }): State<AppState>,
    Json(req): Json<SubmitTransactionRequest>,
) -> ApiResult<Json<SubmitTransactionResponse>> {
    let mut transaction: Tx = minicbor::decode(&hex::decode(&req.transaction)?)?;
    signer
        .sign(req.address, &mut transaction)
        .await
        .context("could not sign transaction")?;
    let txid = blockchain
        .submit(transaction)
        .await
        .context("could not submit transaction")?;
    Ok(Json(SubmitTransactionResponse { txid }))
}

pub async fn try_balius(
    State(AppState { contracts, .. }): State<AppState>,
    Json(req): Json<TryBaliusRequest>,
) -> ApiResult<Json<TryBaliusResponse>> {
    contracts.invoke(&req.contract, &req.method).await?;
    Ok(Json(TryBaliusResponse {}))
}
