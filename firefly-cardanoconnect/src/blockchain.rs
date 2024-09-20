use std::path::PathBuf;

use crate::{
    config::CardanoConnectConfig,
    streams::{BlockInfo, BlockReference},
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use mocks::MockChain;
use n2c::NodeToClient;
use pallas_primitives::conway::Tx;
use serde::Deserialize;
use tokio::sync::RwLock;

pub mod mocks;
mod n2c;

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

enum ClientImpl {
    NodeToClient(RwLock<NodeToClient>),
    Mock(MockChain),
}

pub struct BlockchainClient {
    client: ClientImpl,
    era: u16,
}

impl BlockchainClient {
    pub async fn new(config: &CardanoConnectConfig) -> Result<Self> {
        let blockchain = &config.connector.blockchain;

        let n2c = {
            let client = NodeToClient::new(&blockchain.socket, blockchain.magic()).await?;
            RwLock::new(client)
        };

        Ok(Self {
            client: ClientImpl::NodeToClient(n2c),
            era: blockchain.era,
        })
    }

    #[allow(unused)]
    pub fn mock() -> Self {
        let mock_chain = MockChain::new(3000);
        Self {
            client: ClientImpl::Mock(mock_chain),
            era: 0,
        }
    }

    pub async fn submit(&self, transaction: Tx) -> Result<String> {
        match &self.client {
            ClientImpl::Mock(_) => bail!("mock transaction submission not implemented"),
            ClientImpl::NodeToClient(n2c) => {
                let mut client = n2c.write().await;
                client.submit(transaction, self.era).await
            }
        }
    }

    pub async fn sync(&self) -> Result<ChainSyncClientWrapper> {
        let inner: Box<dyn ChainSyncClient + Send + Sync> = match &self.client {
            ClientImpl::Mock(mock) => Box::new(mock.sync()),
            ClientImpl::NodeToClient(n2c) => {
                let client = n2c.read().await;
                Box::new(client.open_chainsync().await?)
            }
        };
        Ok(ChainSyncClientWrapper { inner })
    }
}

#[async_trait]
pub trait ChainSyncClient {
    async fn request_next(&mut self) -> Result<RequestNextResponse>;
    async fn find_intersect(
        &mut self,
        points: &[BlockReference],
    ) -> Result<(Option<BlockReference>, BlockReference)>;
    async fn request_block(&self, block_ref: &BlockReference) -> Result<Option<BlockInfo>>;
}

pub struct ChainSyncClientWrapper {
    inner: Box<dyn ChainSyncClient + Send + Sync>,
}

#[async_trait]
impl ChainSyncClient for ChainSyncClientWrapper {
    async fn request_next(&mut self) -> Result<RequestNextResponse> {
        self.inner.request_next().await
    }
    async fn find_intersect(
        &mut self,
        points: &[BlockReference],
    ) -> Result<(Option<BlockReference>, BlockReference)> {
        self.inner.find_intersect(points).await
    }
    async fn request_block(&self, block_ref: &BlockReference) -> Result<Option<BlockInfo>> {
        self.inner.request_block(block_ref).await
    }
}

pub enum RequestNextResponse {
    RollForward(BlockInfo, #[expect(dead_code)] BlockReference),
    RollBackward(BlockReference, #[expect(dead_code)] BlockReference),
}
