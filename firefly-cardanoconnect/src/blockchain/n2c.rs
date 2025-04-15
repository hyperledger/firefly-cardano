use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chainsync::NodeToClientChainSync;
use ledger::NodeToClientLedger;
use pallas_crypto::hash::Hasher;
use pallas_network::{
    facades::NodeClient,
    miniprotocols::localtxsubmission::{EraTx, Response},
};
use pallas_primitives::conway::Tx;
use pallas_traverse::wellknown::GenesisValues;
use tokio::time;
use tracing::warn;

use crate::utils::LazyInit;

mod chainsync;
mod ledger;

pub struct NodeToClient {
    socket: PathBuf,
    magic: u64,
    era: u16,
    genesis_values: GenesisValues,
    client: LazyInit<NodeClient>,
}

impl NodeToClient {
    pub async fn new(socket: &Path, magic: u64, era: u16, genesis_values: GenesisValues) -> Self {
        let client = Self::connect(socket, magic);
        let mut result = Self {
            socket: socket.to_path_buf(),
            magic,
            era,
            genesis_values,
            client,
        };
        if let Err(error) = result.get_client().await {
            warn!("cannot connect to local node: {error}");
        }
        result
    }

    pub async fn health(&self) -> Result<()> {
        match self.client.initialized() {
            true => Ok(()),
            false => bail!("not connected to cardano node"),
        }
    }

    pub async fn submit(&mut self, transaction: Tx<'_>, era: u16) -> Result<String> {
        let txid = {
            let txid_bytes = Hasher::<256>::hash_cbor(&transaction.transaction_body);
            hex::encode(txid_bytes)
        };
        let era_tx = {
            let mut bytes = vec![];
            minicbor::encode(transaction, &mut bytes).expect("infallible");
            EraTx(era, bytes)
        };
        let response = {
            self.get_client()
                .await?
                .submission()
                .submit_tx(era_tx)
                .await
                .context("could not submit transaction")?
        };
        match response {
            Response::Accepted => Ok(txid),
            Response::Rejected(reason) => {
                bail!("transaction was rejected: {:?}", reason);
            }
        }
    }

    pub fn open_chainsync(&self) -> Result<NodeToClientChainSync> {
        let client = Self::connect(&self.socket, self.magic);
        let genesis_values = self.genesis_values.clone();
        Ok(NodeToClientChainSync::new(client, genesis_values))
    }

    pub fn ledger(&self) -> NodeToClientLedger {
        NodeToClientLedger::new(Self::connect(&self.socket, self.magic), self.era)
    }

    async fn get_client(&mut self) -> Result<&mut NodeClient> {
        time::timeout(Duration::from_secs(10), self.client.get())
            .await
            .map_err(|_| anyhow!("could not connect to cardano node"))
    }

    fn connect(socket: &Path, magic: u64) -> LazyInit<NodeClient> {
        let socket = socket.to_path_buf();
        LazyInit::new(async move {
            let wait_time = Duration::from_secs(10);
            loop {
                match Self::try_connect(&socket, magic).await {
                    Ok(client) => break client,
                    Err(error) => {
                        warn!("{error:?}");
                        time::sleep(wait_time).await;
                    }
                }
            }
        })
    }
    async fn try_connect(socket: &Path, magic: u64) -> Result<NodeClient> {
        NodeClient::connect(socket, magic)
            .await
            .context("could not connect to socket")
    }
}
