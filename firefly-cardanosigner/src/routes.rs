use anyhow::Context;
use axum::{Json, extract::State};
use firefly_server::apitypes::{ApiError, ApiResult};
use pallas_crypto::{hash::Hasher, key::ed25519::PublicKey};
use pallas_primitives::conway::{Tx, VKeyWitness, WitnessSet};
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
    /// The CBOR-encoded transaction witness set.
    transaction_witness_set: String,
}

pub async fn sign_transaction(
    State(AppState { key_store }): State<AppState>,
    Json(req): Json<SignTransactionRequest>,
) -> ApiResult<Json<SignTransactionResponse>> {
    let Some(private_key) = key_store.find_signing_key(&req.address)? else {
        return Err(ApiError::not_found(format!(
            "No key found for the given address {}",
            req.address
        )));
    };

    let tx_bytes = hex::decode(&req.transaction).context("invalid transaction cbor")?;
    let tx: Tx = minicbor::decode(&tx_bytes).context("Invalid transaction")?;

    let b2b = Hasher::<256>::hash_cbor(&tx.transaction_body);
    let signature = private_key.sign(b2b);

    let vkey_bytes: [u8; PublicKey::SIZE] = private_key.public_key().into();
    let signature_bytes: Vec<u8> = signature.as_ref().to_vec();

    let vkey_witness = VKeyWitness {
        vkey: vkey_bytes.to_vec().into(),
        signature: signature_bytes.into(),
    };

    let vkey_witnesses = vec![vkey_witness];
    let vkey_witness_set = vkey_witnesses.try_into().unwrap();
    let transaction_witness_set = WitnessSet {
        vkeywitness: Some(vkey_witness_set),
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: None,
        redeemer: None,
        plutus_v2_script: None,
        plutus_v3_script: None,
    };

    let serialized_witness_set = {
        let mut bytes = vec![];
        minicbor::encode(transaction_witness_set, &mut bytes)?;
        bytes
    };

    Ok(Json(SignTransactionResponse {
        transaction_witness_set: hex::encode(serialized_witness_set),
    }))
}
