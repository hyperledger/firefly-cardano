use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use pallas_crypto::hash::Hasher;
use pallas_network::{
    facades::NodeClient,
    miniprotocols::{
        chainsync::{BlockContent, NextResponse},
        localtxsubmission::{EraTx, Response},
        Point,
    },
};
use pallas_primitives::conway::Tx;
use pallas_traverse::{wellknown::GenesisValues, MultiEraBlock, MultiEraHeader};
use tokio::time;
use tracing::warn;

use crate::{
    streams::{BlockInfo, BlockReference},
    utils::LazyInit,
};

use super::{ChainSyncClient, RequestNextResponse};

pub struct NodeToClient {
    socket: PathBuf,
    magic: u64,
    genesis_values: GenesisValues,
    client: LazyInit<NodeClient>,
}

impl NodeToClient {
    pub async fn new(socket: &Path, magic: u64, genesis_values: GenesisValues) -> Self {
        let client = Self::connect(socket, magic);
        let mut result = Self {
            socket: socket.to_path_buf(),
            magic,
            genesis_values,
            client,
        };
        if let Err(error) = result.get_client().await {
            warn!("cannot connect to blockfrost: {error}");
        }
        result
    }

    pub async fn health(&self) -> Result<()> {
        match self.client.initialized() {
            true => Ok(()),
            false => bail!("not connected to cardano node"),
        }
    }

    pub async fn submit(&mut self, transaction: Tx, era: u16) -> Result<String> {
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
                bail!("transaction was rejected: {}", hex::encode(&reason.0));
            }
        }
    }

    pub fn open_chainsync(&self) -> Result<N2cChainSync> {
        let client = Self::connect(&self.socket, self.magic);
        let genesis_values = self.genesis_values.clone();
        Ok(N2cChainSync {
            client,
            genesis_values,
        })
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

pub struct N2cChainSync {
    client: LazyInit<NodeClient>,
    genesis_values: GenesisValues,
}

#[async_trait]
impl ChainSyncClient for N2cChainSync {
    async fn request_next(&mut self) -> Result<RequestNextResponse> {
        loop {
            let res = self
                .client
                .get()
                .await
                .chainsync()
                .request_or_await_next()
                .await
                .context("error waiting for next response")?;
            match res {
                NextResponse::Await => continue,
                NextResponse::RollForward(content, tip) => {
                    let info = self
                        .content_to_block_info(content)
                        .context("error parsing new block")?;
                    let tip = point_to_block_ref(tip.0);
                    return Ok(RequestNextResponse::RollForward(info, tip));
                }
                NextResponse::RollBackward(point, tip) => {
                    let point = point_to_block_ref(point);
                    let tip = point_to_block_ref(tip.0);
                    return Ok(RequestNextResponse::RollBackward(point, tip));
                }
            };
        }
    }
    async fn find_intersect(
        &mut self,
        points: &[BlockReference],
    ) -> Result<(Option<BlockReference>, BlockReference)> {
        let points = points.iter().filter_map(block_ref_to_point).collect();
        let (intersect, tip) = self
            .client
            .get()
            .await
            .chainsync()
            .find_intersect(points)
            .await?;
        let intersect = intersect.map(point_to_block_ref);
        let tip = point_to_block_ref(tip.0);

        Ok((intersect, tip))
    }
}

impl N2cChainSync {
    fn content_to_block_info(&self, content: BlockContent) -> Result<BlockInfo> {
        let block = MultiEraBlock::decode(&content.0)?;

        let (block_height, block_slot) = match block.header() {
            MultiEraHeader::EpochBoundary(x) => {
                let height = x.consensus_data.difficulty.first().cloned();
                let slot = self
                    .genesis_values
                    .relative_slot_to_absolute(x.consensus_data.epoch_id, 0);
                (height, slot)
            }
            MultiEraHeader::ShelleyCompatible(x) => {
                let height = Some(x.header_body.block_number);
                let slot = x.header_body.slot;
                (height, slot)
            }
            MultiEraHeader::BabbageCompatible(x) => {
                let height = Some(x.header_body.block_number);
                let slot = x.header_body.slot;
                (height, slot)
            }
            MultiEraHeader::Byron(x) => {
                let height = x.consensus_data.2.first().cloned();
                let slot = self
                    .genesis_values
                    .relative_slot_to_absolute(x.consensus_data.0.epoch, x.consensus_data.0.slot);
                (height, slot)
            }
        };

        let block_hash = hex::encode(block.hash());
        let parent_hash = block.header().previous_hash().map(hex::encode);
        let mut transaction_hashes = vec![];
        let mut transactions = vec![];
        for tx in block.txs() {
            transaction_hashes.push(hex::encode(tx.hash()));
            transactions.push(tx.encode());
        }
        let transaction_hashes = block
            .txs()
            .iter()
            .map(|tx| hex::encode(tx.hash()))
            .collect();
        Ok(BlockInfo {
            block_height,
            block_slot: Some(block_slot),
            block_hash,
            parent_hash,
            transaction_hashes,
            transactions,
        })
    }
}

fn block_ref_to_point(block_ref: &BlockReference) -> Option<Point> {
    match block_ref {
        BlockReference::Origin => Some(Point::Origin),
        BlockReference::Point(slot, hash) => {
            Some(Point::Specific((*slot)?, hex::decode(hash).ok()?))
        }
    }
}

fn point_to_block_ref(point: Point) -> BlockReference {
    match point {
        Point::Origin => BlockReference::Origin,
        Point::Specific(number, hash) => BlockReference::Point(Some(number), hex::encode(hash)),
    }
}
