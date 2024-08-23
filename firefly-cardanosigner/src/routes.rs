use axum::Json;
use firefly_server::error::{ApiError, ApiResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    Json(req): Json<SignTransactionRequest>,
) -> ApiResult<Json<SignTransactionResponse>> {
    if req.address.is_empty() {
        return Err(ApiError::not_found("No address provided"));
    }
    Ok(Json(SignTransactionResponse {
        transaction: req.transaction,
    }))
}
