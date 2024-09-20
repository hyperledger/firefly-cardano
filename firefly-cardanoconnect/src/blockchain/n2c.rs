use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use blockfrost::{BlockFrostSettings, BlockfrostAPI, BlockfrostError, Pagination};
use pallas_crypto::hash::Hasher;
use pallas_network::{
    facades::NodeClient,
    miniprotocols::{
        chainsync::{BlockContent, NextResponse},
        localtxsubmission::{EraTx, Response},
        Point,
    },
};
use pallas_primitives::conway::{MintedBlock, Tx};

use crate::{
    config::Secret,
    streams::{BlockInfo, BlockReference},
};

use super::{ChainSyncClient, RequestNextResponse};

pub struct NodeToClient {
    socket: PathBuf,
    magic: u64,
    genesis_hash: String,
    blockfrost: Option<BlockfrostAPI>,
    client: NodeClient,
}

impl NodeToClient {
    pub async fn new(
        socket: &Path,
        magic: u64,
        genesis_hash: &str,
        blockfrost_key: Option<&Secret<String>>,
    ) -> Result<Self> {
        let client = Self::connect(socket, magic).await?;
        let blockfrost =
            blockfrost_key.map(|key| BlockfrostAPI::new(&key.0, BlockFrostSettings::new()));
        Ok(Self {
            socket: socket.to_path_buf(),
            magic,
            genesis_hash: genesis_hash.to_string(),
            blockfrost,
            client,
        })
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
            self.client
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

    pub async fn open_chainsync(&self) -> Result<N2cChainSync> {
        let client = Self::connect(&self.socket, self.magic).await?;
        let Some(blockfrost) = self.blockfrost.clone() else {
            bail!("Cannot use node-to-client without a blockfrost key")
        };
        let genesis_hash = self.genesis_hash.clone();
        Ok(N2cChainSync {
            client,
            blockfrost,
            genesis_hash,
        })
    }

    async fn connect(socket: &Path, magic: u64) -> Result<NodeClient> {
        NodeClient::connect(socket, magic)
            .await
            .context("could not connect to socket")
    }
}

pub struct N2cChainSync {
    client: NodeClient,
    blockfrost: BlockfrostAPI,
    genesis_hash: String,
}

#[async_trait]
impl ChainSyncClient for N2cChainSync {
    async fn request_next(&mut self) -> Result<RequestNextResponse> {
        loop {
            let res = self
                .client
                .chainsync()
                .request_or_await_next()
                .await
                .context("error waiting for next response")?;
            match res {
                NextResponse::Await => continue,
                NextResponse::RollForward(content, tip) => {
                    let info = content_to_block_info(content).context("error parsing new block")?;
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
        let (intersect, tip) = self.client.chainsync().find_intersect(points).await?;
        let intersect = intersect.map(point_to_block_ref);
        let tip = point_to_block_ref(tip.0);

        Ok((intersect, tip))
    }
    async fn request_block(&self, block_ref: &BlockReference) -> Result<Option<BlockInfo>> {
        let (requested_slot, requested_hash) = match block_ref {
            BlockReference::Origin => (None, &self.genesis_hash),
            BlockReference::Point(number, hash) => (*number, hash),
        };
        let block = match self.blockfrost.blocks_by_id(requested_hash).await {
            Err(BlockfrostError::Response { reason, .. }) if reason.status_code == 404 => {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
            Ok(block) => block,
        };

        let block_hash = block.hash;
        let block_height = block.height.map(|h| h as u64);
        let block_slot = block.slot.map(|s| s as u64);
        if requested_slot.is_some_and(|s| block.slot.is_some_and(|b| b as u64 != s)) {
            bail!("requested_block returned a block in the wrong slot");
        }

        let transaction_hashes = self
            .blockfrost
            .blocks_txs(requested_hash, Pagination::all())
            .await?;

        let info = BlockInfo {
            block_hash,
            block_height,
            block_slot,
            parent_hash: block.previous_block,
            transaction_hashes,
        };

        Ok(Some(info))
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

fn content_to_block_info(content: BlockContent) -> Result<BlockInfo> {
    type BlockWrapper<'b> = (u16, MintedBlock<'b>);

    let (_, block): BlockWrapper = minicbor::decode(&content.0)?;
    let block_hash = {
        let header = block.header.raw_cbor();
        let hash = Hasher::<256>::hash(header);
        hex::encode(hash)
    };
    let header = &block.header.header_body;
    let parent_hash = header.prev_hash.map(hex::encode);
    let transaction_hashes = block
        .transaction_bodies
        .iter()
        .map(|t| {
            let body = t.raw_cbor();
            let hash = Hasher::<256>::hash(body);
            hex::encode(hash)
        })
        .collect();
    Ok(BlockInfo {
        block_height: Some(header.block_number),
        block_slot: Some(header.slot),
        block_hash,
        parent_hash,
        transaction_hashes,
    })
}
