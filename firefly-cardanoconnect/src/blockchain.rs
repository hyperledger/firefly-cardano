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
use utxorpc::{CardanoSubmitClient, ClientBuilder, Stage};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockchainConfig {
    pub utxorpc_address: Option<String>,
    pub socket: Option<PathBuf>,
    pub network: Option<Network>,
    pub network_magic: Option<u64>,
    pub era: u16,
}

#[derive(Clone, Copy, Deserialize)]
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
    n2c: Option<Mutex<NodeClient>>,
    utxorpc: Option<Mutex<CardanoSubmitClient>>,
    era: u16,
}

impl BlockchainClient {
    pub async fn new(config: &CardanoConnectConfig) -> Result<Self> {
        let blockchain = &config.connector.blockchain;

        let n2c = if let Some(socket) = &blockchain.socket {
            let client = NodeClient::connect(&socket, blockchain.magic())
                .await
                .context("could not connect to socket")?;
            Some(Mutex::new(client))
        } else {
            None
        };

        let utxorpc = if let Some(address) = &blockchain.utxorpc_address {
            let client = ClientBuilder::new()
                .uri(address)
                .context("could not configure utxorpc")?
                .build()
                .await;
            Some(Mutex::new(client))
        } else {
            None
        };

        if n2c.is_none() && utxorpc.is_none() {
            bail!("Missing configuration in connector.blockchain");
        }

        Ok(Self {
            n2c,
            utxorpc,
            era: blockchain.era,
        })
    }

    pub async fn submit(&self, transaction: Tx) -> Result<String> {
        if let Some(client) = self.n2c.as_ref() {
            self.submit_n2c(client, transaction).await
        } else {
            self.submit_utxorpc(self.utxorpc.as_ref().unwrap(), transaction)
                .await
        }
    }

    async fn submit_n2c(&self, client: &Mutex<NodeClient>, transaction: Tx) -> Result<String> {
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
            let mut client = client.lock().await;
            client.submission().submit_tx(era_tx).await?
        };
        match response {
            Response::Accepted => Ok(txid),
            Response::Rejected(reason) => {
                panic!("not sure of format {:?}", reason);
            }
        }
    }

    async fn submit_utxorpc(
        &self,
        client: &Mutex<CardanoSubmitClient>,
        transaction: Tx,
    ) -> Result<String> {
        let tx_bytes = {
            let mut bytes = vec![];
            minicbor::encode(transaction, &mut bytes).expect("infallible");
            bytes
        };

        let (txid, mut stream) = {
            let mut submit = client.lock().await;
            let txid = submit
                .submit_tx(vec![tx_bytes])
                .await?
                .pop()
                .expect("utxorpc response was missing id");
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
