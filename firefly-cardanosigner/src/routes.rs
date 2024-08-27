use anyhow::Context;
use axum::{extract::State, Json};
use firefly_server::error::{ApiError, ApiResult};
use pallas_crypto::{hash::Hasher, key::ed25519::PublicKey};
use pallas_primitives::conway::{Tx, VKeyWitness};
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

    let tx_bytes = hex::decode(&req.transaction).context("invalid transaction cbor")?;
    let mut tx: Tx = minicbor::decode(&tx_bytes).context("Invalid transaction")?;

    let b2b = Hasher::<256>::hash_cbor(&tx.transaction_body);
    let signature = private_key.sign(b2b);

    let vkey_bytes: [u8; PublicKey::SIZE] = private_key.public_key().into();
    let signature_bytes: Vec<u8> = signature.as_ref().to_vec();

    let vkey_witness = VKeyWitness {
        vkey: vkey_bytes.to_vec().into(),
        signature: signature_bytes.into(),
    };

    let mut vkey_witnesses = tx
        .transaction_witness_set
        .vkeywitness
        .map(|set| set.to_vec())
        .unwrap_or_default();
    vkey_witnesses.push(vkey_witness);
    let vkey_witness_set = vkey_witnesses.try_into().unwrap();
    tx.transaction_witness_set.vkeywitness = Some(vkey_witness_set);

    let serialized_tx = {
        let mut bytes = vec![];
        minicbor::encode(tx, &mut bytes)?;
        bytes
    };

    Ok(Json(SignTransactionResponse {
        transaction: hex::encode(serialized_tx),
    }))
}
