use anyhow::{Context as _, Result};
use async_trait::async_trait;
use pallas_network::{
    facades::NodeClient,
    miniprotocols::{
        chainsync::{BlockContent, NextResponse},
        Point,
    },
};
use pallas_traverse::{wellknown::GenesisValues, MultiEraBlock, MultiEraHeader};

use crate::{
    blockchain::{ChainSyncClient, RequestNextResponse},
    streams::{BlockInfo, BlockReference},
    utils::LazyInit,
};

pub struct NodeToClientChainSync {
    client: LazyInit<NodeClient>,
    genesis_values: GenesisValues,
}

impl NodeToClientChainSync {
    pub fn new(client: LazyInit<NodeClient>, genesis_values: GenesisValues) -> Self {
        Self {
            client,
            genesis_values,
        }
    }
}

#[async_trait]
impl ChainSyncClient for NodeToClientChainSync {
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

impl NodeToClientChainSync {
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
