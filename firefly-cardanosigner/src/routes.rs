use axum::{extract::State, Json};
use firefly_server::error::{ApiError, ApiResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Deserialize, JsonSchema)]
pub struct SignTransactionRequest {
    /// The address of the key to sign the transaction with.
    address: String,
    /// The raw CBOR-encoded transaction to sign.
    transaction: String,
}

#[derive(Serialize, JsonSchema)]
pub struct SignTransactionResponse {
    /// The CBOR-encoded signed transaction.
    transaction: String,
}

pub async fn sign_transaction(
    State(AppState { key_store }): State<AppState>,
    Json(req): Json<SignTransactionRequest>,
) -> ApiResult<Json<SignTransactionResponse>> {
    let Some(private_key) = key_store.find_signing_key(&req.address)? else {
        return Err(ApiError::not_found("No key found for the given address"));
    };

    let signed = private_key.sign(req.transaction);

    Ok(Json(SignTransactionResponse {
        transaction: signed.to_string(),
    }))
}
