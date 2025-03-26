use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaChaRng;
use tokio::{
    sync::{Notify, RwLock, RwLockReadGuard},
    time,
};

use crate::streams::{BlockInfo, BlockReference};

use super::{ChainSyncClient, RequestNextResponse};

pub struct MockChainSync {
    chain: MockChain,
    consumer_tip: BlockReference,
}

#[async_trait]
impl ChainSyncClient for MockChainSync {
    async fn request_next(&mut self) -> Result<RequestNextResponse> {
        loop {
            let chain = self.chain.read_lock().await;
            let tip = chain.last().map(|b| b.as_reference()).unwrap_or_default();

            // If they need ro roll back, let em know
            if let Some(rollback_to) = self.chain.find_rollback(&self.consumer_tip) {
                self.consumer_tip = rollback_to.clone();
                return Ok(RequestNextResponse::RollBackward(rollback_to, tip));
            }

            // what are you waiting for?
            let next_index = match &self.consumer_tip {
                BlockReference::Origin => Some(1),
                BlockReference::Point(_, hash) => self.chain.indexes.get(hash).map(|i| *i + 1),
            };

            // if we have it, give it
            if let Some(info) = next_index.and_then(|i| chain.get(i)) {
                self.consumer_tip = BlockReference::Point(info.block_slot, info.block_hash.clone());
                return Ok(RequestNextResponse::RollForward(info.clone(), tip));
            }

            // and now we wait until the chain changes and try again
            drop(chain);
            self.chain.wait_for_new_block().await;
        }
    }

    async fn find_intersect(
        &mut self,
        points: &[BlockReference],
    ) -> Result<(Option<BlockReference>, BlockReference)> {
        let chain = self.chain.read_lock().await;
        let intersect = points.iter().find_map(|point| match point {
            BlockReference::Origin => chain.first(),
            BlockReference::Point(_, hash) => {
                self.chain.indexes.get(hash).and_then(|i| chain.get(*i))
            }
        });
        self.consumer_tip = intersect.map(|b| b.as_reference()).unwrap_or_default();
        let tip = chain.last().map(|b| b.as_reference()).unwrap_or_default();
        Ok((intersect.map(|i| i.as_reference()), tip))
    }
}

// Mock implementation of something which can query the chain
#[derive(Clone)]
pub struct MockChain {
    // the real implementation of course won't be in memory
    chain: Arc<RwLock<Vec<BlockInfo>>>,
    indexes: Arc<DashMap<String, usize>>,
    new_block: Arc<Notify>,
    rolled_back: Arc<DashMap<BlockReference, BlockReference>>,
}

impl MockChain {
    pub fn new(initial_height: usize) -> Self {
        let chain = Arc::new(RwLock::new(vec![]));
        let indexes = Arc::new(DashMap::new());
        let new_block = Arc::new(Notify::new());
        let rolled_back = Arc::new(DashMap::new());
        tokio::spawn(Self::generate(
            chain.clone(),
            indexes.clone(),
            new_block.clone(),
            rolled_back.clone(),
            initial_height,
        ));

        Self {
            chain,
            indexes,
            new_block,
            rolled_back,
        }
    }

    pub async fn genesis_hash(&self) -> String {
        self.new_block.notified().await;
        self.chain.read().await.first().unwrap().block_hash.clone()
    }

    pub fn sync(&self) -> MockChainSync {
        MockChainSync {
            chain: self.clone(),
            consumer_tip: BlockReference::Origin,
        }
    }

    async fn read_lock(&self) -> RwLockReadGuard<Vec<BlockInfo>> {
        self.chain.read().await
    }

    async fn wait_for_new_block(&self) {
        self.new_block.notified().await;
    }

    fn find_rollback(&self, tip: &BlockReference) -> Option<BlockReference> {
        let rollback_to = self.rolled_back.get(tip)?;

        let mut final_rollback_target = rollback_to.clone();
        while let Some(rollback_to) = self.rolled_back.get(&final_rollback_target) {
            final_rollback_target = rollback_to.clone();
        }

        Some(final_rollback_target)
    }

    // TODO: roll back sometimes
    async fn generate(
        chain: Arc<RwLock<Vec<BlockInfo>>>,
        indexes: Arc<DashMap<String, usize>>,
        new_block: Arc<Notify>,
        _rolled_back: Arc<DashMap<BlockReference, BlockReference>>,
        initial_height: usize,
    ) {
        let mut rng = ChaChaRng::from_seed([0; 32]);
        {
            let mut lock = chain.write().await;
            for _ in 0..initial_height {
                Self::generate_block(&mut rng, &mut lock, &indexes);
            }
        }
        loop {
            time::sleep(Duration::from_secs(1)).await;
            let mut lock = chain.write().await;
            Self::generate_block(&mut rng, &mut lock, &indexes);
            new_block.notify_waiters();
        }
    }

    fn generate_block(
        rng: &mut ChaChaRng,
        chain: &mut Vec<BlockInfo>,
        indexes: &DashMap<String, usize>,
    ) {
        let (block_height, parent_hash) = match chain.last() {
            Some(block) => (Some(chain.len() as u64), Some(block.block_hash.clone())),
            None => (None, None),
        };

        let mut transaction_hashes = vec![];
        for _ in 0..rng.random_range(0..10) {
            transaction_hashes.push(Self::generate_hash(rng));
        }
        let block = BlockInfo {
            block_height,
            block_slot: block_height,
            block_hash: Self::generate_hash(rng),
            parent_hash,
            transaction_hashes,
            transactions: vec![],
        };
        indexes.insert(block.block_hash.clone(), chain.len());
        chain.push(block);
    }

    fn generate_hash(rng: &mut ChaChaRng) -> String {
        let bytes: [u8; 32] = rng.random();
        hex::encode(bytes)
    }
}
