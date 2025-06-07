use crate::config::CardanoConnectConfig;
use anyhow::{Result, anyhow};
use firefly_server::{
    apitypes::{ApiResult, Context},
    http::HttpClient,
};
use minicbor::{Encode, Encoder};
use pallas_primitives::conway::{Tx, WitnessSet};
use reqwest::Url;
use serde::{Deserialize, Serialize};

pub struct CardanoSigner {
    client: HttpClient,
    sign_url: Url,
}

impl CardanoSigner {
    pub fn new(config: &CardanoConnectConfig) -> Result<Self> {
        let client = HttpClient::new(&config.http)?;
        let base_url = Url::parse(&config.connector.signer_url)?;
        let sign_url = base_url.join("/sign")?;
        Ok(Self { client, sign_url })
    }

    pub async fn sign(&self, address: String, transaction: &mut Tx<'_>) -> ApiResult<()> {
        let req = {
            let mut encoder = Encoder::new(vec![]);
            transaction.encode(&mut encoder, &mut ())?;
            SignTransactionRequest {
                address,
                transaction: hex::encode(encoder.into_writer()),
            }
        };
        let res: SignTransactionResponse = self
            .client
            .post(self.sign_url.clone(), &req)
            .await
            .context("Could not sign transaction")?;
        let witness_set_bytes = hex::decode(res.transaction_witness_set)?;
        let witness_set: WitnessSet = minicbor::decode(&witness_set_bytes)?;
        self.update_witness_set(transaction, witness_set)?;
        Ok(())
    }

    fn update_witness_set(&self, transaction: &mut Tx, witness_set: WitnessSet) -> Result<()> {
        let mut all_witnesses = vec![];
        if let Some(old_witnesses) = &transaction.transaction_witness_set.vkeywitness {
            all_witnesses.extend(old_witnesses.iter().cloned());
        }
        if let Some(new_witnesses) = witness_set.vkeywitness {
            all_witnesses.extend(new_witnesses.iter().cloned());
        }

        let vkeywitnesses = all_witnesses
            .try_into()
            .map_err(|_| anyhow!("No vkeywitnesses found"))?;
        transaction.transaction_witness_set.vkeywitness = Some(vkeywitnesses);

        Ok(())
    }
}

#[derive(Serialize)]
struct SignTransactionRequest {
    /// The address of the key to sign the transaction with.
    address: String,
    /// The raw CBOR-encoded transaction to sign.
    transaction: String,
}

#[derive(Deserialize)]
struct SignTransactionResponse {
    /// The CBOR-encoded transaction witness set.
    transaction_witness_set: String,
}
