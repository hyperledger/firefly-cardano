use crate::config::CardanoConnectConfig;
use anyhow::{bail, Result};
use pallas_primitives::conway::Tx;
use serde::Deserialize;
use tokio::sync::Mutex;
use utxorpc::{CardanoSubmitClient, ClientBuilder, Stage};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockchainConfig {
    pub utxorpc_address: String,
}

pub struct BlockchainClient {
    submit: Mutex<CardanoSubmitClient>,
}

impl BlockchainClient {
    pub async fn new(config: &CardanoConnectConfig) -> Result<Self> {
        let blockchain = &config.connector.blockchain;
        let submit: CardanoSubmitClient = ClientBuilder::new()
            .uri(&blockchain.utxorpc_address)?
            .build()
            .await;
        Ok(Self {
            submit: Mutex::new(submit)
        })
    }
    pub async fn submit(&self, transaction: Tx) -> Result<String> {
        let tx_bytes = {
            let mut bytes = vec![];
            minicbor::encode(transaction, &mut bytes).expect("infallible");
            bytes
        };
        
        let (txid, mut stream) = {
            let mut submit = self.submit.lock().await;
            let txid = submit.submit_tx(vec![tx_bytes]).await?.pop().expect("utxorpc response was missing id");
            let stream = submit.wait_for_tx(vec![txid.clone()]).await?;
            (txid, stream)
        };

        while let Some(event) = stream.event().await? {
            if event.r#ref == txid && event.stage == Stage::Acknowledged {
                return Ok(hex::encode(txid));
            }
        }

        bail!("Transaction was submitted, but we lost track of its state")
    }
}