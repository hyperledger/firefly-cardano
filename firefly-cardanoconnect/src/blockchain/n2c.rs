use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Ok, Result};
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
use pallas_primitives::conway::{MintedBlock, Tx};

use crate::streams::{BlockInfo, BlockReference};

use super::{ChainSyncClient, RequestNextResponse};

pub struct NodeToClient {
    socket: PathBuf,
    magic: u64,
    client: NodeClient,
}

impl NodeToClient {
    pub async fn new(socket: &Path, magic: u64) -> Result<Self> {
        let client = Self::connect(socket, magic).await?;
        Ok(Self {
            socket: socket.to_path_buf(),
            magic,
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
        Ok(N2cChainSync { client })
    }

    async fn connect(socket: &Path, magic: u64) -> Result<NodeClient> {
        NodeClient::connect(socket, magic)
            .await
            .context("could not connect to socket")
    }
}

pub struct N2cChainSync {
    client: NodeClient,
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
        let points: Result<Vec<Point>> = points.iter().map(block_ref_to_point).collect();
        let (intersect, tip) = self.client.chainsync().find_intersect(points?).await?;
        let intersect = intersect.map(point_to_block_ref);
        let tip = point_to_block_ref(tip.0);

        Ok((intersect, tip))
    }
    async fn request_block(&self, _block_ref: &BlockReference) -> Result<Option<BlockInfo>> {
        bail!("request_block not implemented")
    }
}

fn block_ref_to_point(block_ref: &BlockReference) -> Result<Point> {
    match block_ref {
        BlockReference::Origin => Ok(Point::Origin),
        BlockReference::Point(number, hash) => Ok(Point::Specific(*number, hex::decode(hash)?)),
    }
}

fn point_to_block_ref(point: Point) -> BlockReference {
    match point {
        Point::Origin => BlockReference::Origin,
        Point::Specific(number, hash) => BlockReference::Point(number, hex::encode(hash)),
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
    let parent_hash = header.prev_hash.map(hex::encode).unwrap_or_default();
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
        block_number: header.block_number,
        block_hash,
        parent_hash,
        transaction_hashes,
    })
}
