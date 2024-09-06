use std::path::PathBuf;

use crate::config::CardanoConnectConfig;
use anyhow::{bail, Context, Result};
use pallas_crypto::hash::Hasher;
use pallas_network::{
    facades::NodeClient,
    miniprotocols::localtxsubmission::{EraTx, Response},
};
use pallas_primitives::conway::Tx;
use serde::Deserialize;
use tokio::sync::Mutex;

pub mod mocks;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockchainConfig {
    pub socket: PathBuf,
    pub network: Option<Network>,
    pub network_magic: Option<u64>,
    pub era: u16,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    Mainnet,
    Preview,
    PreProd,
}

impl BlockchainConfig {
    fn magic(&self) -> u64 {
        if let Some(magic) = self.network_magic {
            return magic;
        }
        match self.network.unwrap_or(Network::Mainnet) {
            Network::PreProd => 1,
            Network::Preview => 2,
            Network::Mainnet => 764824073,
        }
    }
}

pub struct BlockchainClient {
    n2c: Mutex<NodeClient>,
    era: u16,
}

impl BlockchainClient {
    pub async fn new(config: &CardanoConnectConfig) -> Result<Self> {
        let blockchain = &config.connector.blockchain;

        let n2c = {
            let client = NodeClient::connect(&blockchain.socket, blockchain.magic())
                .await
                .context("could not connect to socket")?;
            Mutex::new(client)
        };

        Ok(Self {
            n2c,
            era: blockchain.era,
        })
    }

    pub async fn submit(&self, transaction: Tx) -> Result<String> {
        let txid = {
            let txid_bytes = Hasher::<256>::hash_cbor(&transaction.transaction_body);
            hex::encode(txid_bytes)
        };
        let era_tx = {
            let mut bytes = vec![];
            minicbor::encode(transaction, &mut bytes).expect("infallible");
            EraTx(self.era, bytes)
        };
        let response = {
            let mut client = self.n2c.lock().await;
            client
                .submission()
                .submit_tx(era_tx)
                .await
                .context("could not submit transaction")?
        };
        match response {
            Response::Accepted => Ok(txid),
            Response::Rejected(reason) => {
                bail!("transaction was rejected: {}", hex::encode(&reason.0));
            }
        }
    }
}
