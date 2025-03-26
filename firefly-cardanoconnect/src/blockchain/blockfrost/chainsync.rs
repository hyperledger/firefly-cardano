use anyhow::{bail, Context as _, Result};
use async_trait::async_trait;
use blockfrost_openapi::models::BlockContent;
use futures::future::try_join_all;
use std::{collections::VecDeque, time::Duration};
use tokio::time;

use blockfrost::Pagination;

use crate::{
    blockchain::{ChainSyncClient, RequestNextResponse},
    streams::{BlockInfo, BlockReference},
};

use super::client::BlockfrostClient;

pub struct BlockfrostChainSync {
    client: BlockfrostClient,
    tip: BlockContent,
    prev: VecDeque<Point>,
    head: Point,
    next: VecDeque<BlockContent>,
    genesis_hash: String,
}

#[async_trait]
impl ChainSyncClient for BlockfrostChainSync {
    async fn request_next(&mut self) -> Result<RequestNextResponse> {
        match self.fetch_next().await? {
            Some(block) => {
                // roll forward
                Ok(RequestNextResponse::RollForward(
                    block,
                    parse_reference(&self.tip),
                ))
            }
            _ => {
                // roll backward
                let new_head = self.roll_back().await?;
                Ok(RequestNextResponse::RollBackward(
                    new_head,
                    parse_reference(&self.tip),
                ))
            }
        }
    }

    async fn find_intersect(
        &mut self,
        points: &[BlockReference],
    ) -> Result<(Option<BlockReference>, BlockReference)> {
        self.prev.clear();
        self.next.clear();
        for point in points {
            let Some(point) = self.request_point(point).await? else {
                continue;
            };

            let history = self
                .client
                .blocks_previous(
                    &point.hash,
                    Pagination {
                        count: 20,
                        ..Pagination::default()
                    },
                )
                .await?;

            for block in history {
                self.prev.push_back(parse_point(&block));
            }
            let head = point.as_reference();
            self.head = point;

            return Ok((Some(head), parse_reference(&self.tip)));
        }
        self.head = Point {
            slot: None,
            hash: self.genesis_hash.clone(),
        };
        Ok((None, parse_reference(&self.tip)))
    }
}

impl BlockfrostChainSync {
    pub async fn new(client: BlockfrostClient, genesis_hash: String) -> Result<Self> {
        let tip = client.blocks_latest().await?;
        Ok(Self {
            client,
            tip,
            prev: VecDeque::new(),
            head: Point {
                slot: None,
                hash: genesis_hash.clone(),
            },
            next: VecDeque::new(),
            genesis_hash,
        })
    }

    async fn fetch_next(&mut self) -> Result<Option<BlockInfo>> {
        while self.next.is_empty() {
            let Some(next_blocks) = self
                .client
                .try_blocks_next(&self.head.hash, Pagination::default())
                .await?
            else {
                // our head is gone, time to roll back
                return Ok(None);
            };

            for block in next_blocks.into_iter() {
                self.next.push_back(block);
            }

            if let Some(latest) = self.next.back() {
                // Update the tip if we've fetched newer blocks
                if latest.time > self.tip.time {
                    self.tip = latest.clone();
                }
            } else {
                // If next is empty at this point, we're at the tip.
                // We're now polling until something new appears.
                time::sleep(Duration::from_secs(10)).await;
            }
        }

        // We definitely have the next block to return now
        let next = self.next.pop_front().unwrap();
        let next = parse_block(&self.client, next).await?;

        // update prev to point to the next
        self.prev.push_back(self.head.clone());
        self.prev.pop_front();
        self.head = Point {
            slot: next.block_slot,
            hash: next.block_hash.clone(),
        };

        Ok(Some(next))
    }

    async fn roll_back(&mut self) -> Result<BlockReference> {
        let Some(oldest) = self.prev.pop_front() else {
            bail!("chain has rolled too far back!");
        };

        let mut new_history = self
            .client
            .blocks_next(&oldest.hash, Pagination::default())
            .await?;
        // find where history diverged
        let split_index = new_history
            .iter()
            .zip(&self.prev)
            .take_while(|(new, old)| new.hash == old.hash)
            .count();
        let new_next = new_history.split_off(split_index);

        // everything before that point is the past
        self.prev.clear();
        self.prev.push_back(oldest);
        for block in new_history {
            self.prev.push_back(parse_point(&block));
        }

        // whatever was at that point is our new head
        self.head = self.prev.pop_back().unwrap();

        // any blocks left after that point are the future
        self.next.clear();
        for block in new_next {
            self.next.push_back(block);
        }

        Ok(self.head.as_reference())
    }

    async fn request_point(&self, block_ref: &BlockReference) -> Result<Option<Point>> {
        let (requested_slot, requested_hash) = match block_ref {
            BlockReference::Origin => (None, &self.genesis_hash),
            BlockReference::Point(number, hash) => (*number, hash),
        };
        let Some(block) = self.client.try_blocks_by_id(requested_hash).await? else {
            return Ok(None);
        };

        if requested_slot.is_some_and(|s| block.slot.is_some_and(|b| b as u64 != s)) {
            bail!("requested_block returned a block in the wrong slot");
        }

        Ok(Some(parse_point(&block)))
    }
}

#[derive(Clone)]
struct Point {
    slot: Option<u64>,
    hash: String,
}
impl Point {
    fn as_reference(&self) -> BlockReference {
        BlockReference::Point(self.slot, self.hash.clone())
    }
}

fn parse_point(block: &BlockContent) -> Point {
    Point {
        slot: block.slot.map(|s| s as u64),
        hash: block.hash.clone(),
    }
}

fn parse_reference(block: &BlockContent) -> BlockReference {
    let point = parse_point(block);
    BlockReference::Point(point.slot, point.hash)
}

async fn parse_block(client: &BlockfrostClient, block: BlockContent) -> Result<BlockInfo> {
    let block_hash = block.hash;
    let block_height = block.height.map(|h| h as u64);
    let block_slot = block.slot.map(|s| s as u64);

    let transaction_hashes = client.blocks_txs(&block_hash).await?;

    let tx_body_requests = transaction_hashes.iter().map(|hash| fetch_tx(client, hash));
    let transactions = try_join_all(tx_body_requests).await?;

    let info = BlockInfo {
        block_hash,
        block_height,
        block_slot,
        parent_hash: block.previous_block,
        transaction_hashes,
        transactions,
    };
    Ok(info)
}

async fn fetch_tx(blockfrost: &BlockfrostClient, hash: &str) -> Result<Vec<u8>> {
    let tx_body = blockfrost
        .transactions_cbor(hash)
        .await
        .context("could not fetch tx body")?;
    let bytes = hex::decode(&tx_body.cbor)?;
    Ok(bytes)
}
