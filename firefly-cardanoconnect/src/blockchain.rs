use std::path::PathBuf;

use crate::{
    blockfrost::BlockfrostClient,
    config::{CardanoConnectConfig, Secret},
    streams::{BlockInfo, BlockReference},
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use blockfrost::Blockfrost;
use mocks::MockChain;
use n2c::NodeToClient;
use pallas_primitives::conway::Tx;
use pallas_traverse::wellknown::GenesisValues;
use serde::Deserialize;
use tokio::sync::RwLock;

mod blockfrost;
pub mod mocks;
mod n2c;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockchainConfig {
    pub socket: Option<PathBuf>,
    pub blockfrost_key: Option<Secret<String>>,
    pub network: Option<Network>,
    pub network_magic: Option<u64>,
    pub genesis_hash: Option<String>,
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
    fn genesis_hash(&self) -> &str {
        if let Some(hash) = &self.genesis_hash {
            return hash;
        }
        match self.network.unwrap_or(Network::Mainnet) {
            Network::PreProd => "f28f1c1280ea0d32f8cd3143e268650d6c1a8e221522ce4a7d20d62fc09783e1",
            Network::Preview => "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761",
            Network::Mainnet => "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb",
        }
    }
    fn genesis_values(&self) -> GenesisValues {
        match self.network.unwrap_or(Network::Mainnet) {
            Network::PreProd => GenesisValues::preprod(),
            Network::Preview => GenesisValues::preview(),
            Network::Mainnet => GenesisValues::mainnet(),
        }
    }
}

enum ClientImpl {
    Blockfrost(Blockfrost),
    NodeToClient(RwLock<NodeToClient>),
    Mock(MockChain),
}

pub struct BlockchainClient {
    client: ClientImpl,
    genesis_hash: String,
    era: u16,
}

impl BlockchainClient {
    pub async fn new(
        config: &CardanoConnectConfig,
        blockfrost: Option<BlockfrostClient>,
    ) -> Result<Self> {
        let blockchain = &config.connector.blockchain;

        let client = match (&blockchain.socket, blockfrost) {
            (Some(socket), _) => {
                let client =
                    NodeToClient::new(socket, blockchain.magic(), blockchain.genesis_values())
                        .await;
                ClientImpl::NodeToClient(RwLock::new(client))
            }
            (None, Some(blockfrost)) => {
                let client = Blockfrost::new(blockfrost, blockchain.genesis_hash());
                ClientImpl::Blockfrost(client)
            }
            (None, None) => bail!("Missing blockchain configuration"),
        };
        Ok(Self {
            client,
            genesis_hash: blockchain.genesis_hash().to_string(),
            era: blockchain.era,
        })
    }

    #[allow(unused)]
    pub async fn mock() -> Self {
        let mock_chain = MockChain::new(3000);
        let genesis_hash = mock_chain.genesis_hash().await;
        Self {
            client: ClientImpl::Mock(mock_chain),
            genesis_hash,
            era: 0,
        }
    }

    pub fn genesis_hash(&self) -> String {
        self.genesis_hash.clone()
    }

    pub async fn health(&self) -> Result<()> {
        match &self.client {
            ClientImpl::Blockfrost(_) => Ok(()),
            ClientImpl::Mock(_) => Ok(()),
            ClientImpl::NodeToClient(n2c) => {
                let client = n2c.read().await;
                client.health().await
            }
        }
    }

    pub async fn submit(&self, transaction: Tx) -> Result<String> {
        match &self.client {
            ClientImpl::Blockfrost(bf) => bf.submit(transaction).await,
            ClientImpl::Mock(_) => bail!("mock transaction submission not implemented"),
            ClientImpl::NodeToClient(n2c) => {
                let mut client = n2c.write().await;
                client.submit(transaction, self.era).await
            }
        }
    }

    pub async fn sync(&self) -> Result<ChainSyncClientWrapper> {
        let inner: Box<dyn ChainSyncClient + Send + Sync> = match &self.client {
            ClientImpl::Blockfrost(bf) => Box::new(bf.open_chainsync().await?),
            ClientImpl::Mock(mock) => Box::new(mock.sync()),
            ClientImpl::NodeToClient(n2c) => {
                let client = n2c.read().await;
                Box::new(client.open_chainsync()?)
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
}

pub enum RequestNextResponse {
    RollForward(BlockInfo, #[expect(dead_code)] BlockReference),
    RollBackward(BlockReference, #[expect(dead_code)] BlockReference),
}
