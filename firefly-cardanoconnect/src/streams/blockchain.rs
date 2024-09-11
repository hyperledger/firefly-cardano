use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::{bail, Result};
use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::blockchain::mocks::{MockChain, MockChainSync, RequestNextResponse};

use super::{BlockInfo, BlockReference, ListenerId};

#[derive(Debug)]
pub enum ListenerEvent {
    Process(BlockInfo),
    Rollback(BlockInfo),
}

pub struct DataSource {
    chain: Arc<MockChain>,
    db: Arc<BlockDatabase>,
}

impl Default for DataSource {
    fn default() -> Self {
        Self::new()
    }
}

const APPROXIMATELY_IMMUTABLE_LENGTH: usize = 20;

impl DataSource {
    pub fn new() -> Self {
        Self {
            chain: Arc::new(MockChain::new(3000)),
            db: Arc::new(BlockDatabase::default()),
        }
    }

    pub async fn listen(
        &self,
        id: ListenerId,
        from: Option<&BlockReference>,
    ) -> Result<ChainListener> {
        ChainListener::init(id, &self.chain, &self.db, from).await
    }
}

#[derive(Debug)]
pub struct ChainListener {
    history: VecDeque<BlockInfo>,
    rollbacks: HashMap<BlockReference, BlockInfo>,
    sync_event_source: mpsc::Receiver<ChainSyncEvent>,
    block_record_sink: mpsc::UnboundedSender<BlockRecord>,
}

impl ChainListener {
    async fn init(
        id: ListenerId,
        chain: &MockChain,
        db: &Arc<BlockDatabase>,
        from: Option<&BlockReference>,
    ) -> Result<Self> {
        if let Some(records) = db.load_history(&id).await {
            Self::init_existing(id, chain, db, records).await
        } else {
            Self::init_new(id, chain, db, from).await
        }
    }

    pub fn get_tip(&self) -> BlockReference {
        self.history.front().unwrap().as_reference()
    }

    pub fn get_event(&mut self, block_ref: &BlockReference) -> ListenerEvent {
        let (target_number, target_hash) = match block_ref {
            BlockReference::Origin => (0, None),
            BlockReference::Point(number, hash) => (*number, Some(hash.clone())),
        };

        if self
            .history
            .front()
            .is_some_and(|b| b.block_number > target_number)
        {
            panic!("Caller requested a block which was too old for us to know about")
        }

        // if we haven't seen enough blocks to be "sure" that this one is immutable, apply all pending updates synchronously
        if self.history.back().is_some_and(|tip| {
            tip.block_number < target_number + APPROXIMATELY_IMMUTABLE_LENGTH as u64
        }) {
            while let Ok(sync_event) = self.sync_event_source.try_recv() {
                self.handle_sync_event(sync_event);
            }
        }

        // If we already know this block has been rolled back, just say so
        if let Some(rollback) = self.rollbacks.get(block_ref) {
            return ListenerEvent::Rollback(rollback.clone());
        }

        // If we have it already, return it
        if self
            .history
            .back()
            .is_some_and(|tip| tip.block_number >= target_number)
        {
            let block = self
                .history
                .iter()
                .rev()
                .find(|b| b.block_number == target_number)
                .expect("our persisted history has gaps");
            if target_hash.is_some_and(|h| h != block.block_hash) {
                panic!("Caller requested a nonexistent block, and we never heard that it was rolled back");
            }
            return ListenerEvent::Process(block.clone());
        }

        panic!("Caller asked about an event far in the future");
    }

    pub async fn get_next(&mut self, block_ref: &BlockReference) -> Option<BlockReference> {
        let (target_number, prev_hash) = match block_ref {
            BlockReference::Origin => (1, None),
            BlockReference::Point(number, hash) => (*number + 1, Some(hash.clone())),
        };

        loop {
            // Have we rolled back to before this block? If so, don't wait for its successor.
            // That successor will never come, and even if it did, we'd ignore it.
            if self.rollbacks.contains_key(block_ref) {
                return None;
            }

            // Have we already seen the next block? Great, return it
            if self
                .history
                .back()
                .is_some_and(|b| b.block_number >= target_number)
            {
                let block = self
                    .history
                    .iter()
                    .rev()
                    .find(|b| b.block_number == target_number)
                    .expect("our persisted history has gaps");
                if prev_hash.as_ref().is_some_and(|h| *h != block.parent_hash) {
                    panic!("The next block in the chain is from a different fork, and we never heard this block was rolled back");
                }
                return Some(block.as_reference());
            }

            // We don't have it, wait until the chain has progressed before checking again
            let mut sync_events = vec![];
            if self.sync_event_source.recv_many(&mut sync_events, 32).await == 0 {
                panic!("data source has been shut down")
            }
            for sync_event in sync_events {
                self.handle_sync_event(sync_event);
            }
        }
    }

    pub fn get_next_rollback(&mut self, block_ref: &BlockReference) -> BlockReference {
        let Some(rollback_to) = self.rollbacks.get(block_ref) else {
            panic!("caller is trying to roll back when we didn't tell them to");
        };
        rollback_to.as_reference()
    }

    fn handle_sync_event(&mut self, sync_event: ChainSyncEvent) {
        match sync_event {
            ChainSyncEvent::RollForward(block) => {
                assert!(self
                    .history
                    .back()
                    .is_some_and(|n| n.block_number + 1 == block.block_number));
                let record = BlockRecord {
                    block: block.clone(),
                    rolled_back: false,
                };
                self.block_record_sink.send(record).unwrap();
                self.history.push_back(block);
            }
            ChainSyncEvent::RollBackward(rollback_to) => {
                let (target_number, target_hash) = match rollback_to {
                    BlockReference::Origin => (0, None),
                    BlockReference::Point(number, hash) => (number, Some(hash)),
                };
                while self
                    .history
                    .back()
                    .is_some_and(|i| i.block_number > target_number)
                {
                    let rolled_back = self.history.pop_back().unwrap();
                    let record = BlockRecord {
                        block: rolled_back.clone(),
                        rolled_back: true,
                    };
                    self.block_record_sink.send(record).unwrap();
                    self.rollbacks
                        .insert(rolled_back.as_reference(), rolled_back);
                }
                let new_tip = self
                    .history
                    .back()
                    .expect("cannot roll back past recorded history");
                if new_tip.block_number != target_number {
                    panic!("we have a gap in blockchain history");
                }
                if target_hash.is_some_and(|h| *h != new_tip.block_hash) {
                    panic!("we rolled back to the wrong point");
                }
            }
        }
    }

    async fn init_existing(
        id: ListenerId,
        chain: &MockChain,
        db: &Arc<BlockDatabase>,
        records: Vec<BlockRecord>,
    ) -> Result<Self> {
        let mut history = Vec::new();
        let mut rollbacks = HashMap::new();
        for record in records {
            if record.rolled_back {
                rollbacks.insert(record.block.as_reference(), record.block.clone());
            } else {
                history.push(record.block.clone());
            }
        }
        history.sort_by_key(|b| b.block_number);
        let mut history: VecDeque<BlockInfo> = history.into();

        let mut sync = chain.sync();
        let points: Vec<_> = history.iter().rev().map(|b| b.as_reference()).collect();
        let (head_ref, _) = sync.find_intersect(&points).await;
        let Some(head_ref) = head_ref else {
            // The chain didn't recognize any of the blocks we saved from this chain.
            // We have no way to recover.
            bail!("listener {id} is on a fork which no longer exists");
        };
        let Some(head) = chain.request_block(&head_ref).await else {
            bail!("listener {id} is on a fork which no longer exists");
        };

        let mut rollbacks = HashMap::new();
        while history
            .back()
            .is_some_and(|i| i.block_number > head.block_number)
        {
            let rolled_back = history.pop_back().unwrap();
            rollbacks.insert(rolled_back.as_reference(), rolled_back);
        }

        let (sync_event_sink, sync_event_source) = mpsc::channel(16);
        let (block_record_sink, block_record_source) = mpsc::unbounded_channel();
        tokio::spawn(Self::stay_in_sync(sync, sync_event_sink));
        tokio::spawn(Self::persist_blocks(id, db.clone(), block_record_source));
        Ok(Self {
            history,
            rollbacks,
            sync_event_source,
            block_record_sink,
        })
    }

    async fn init_new(
        id: ListenerId,
        chain: &MockChain,
        db: &Arc<BlockDatabase>,
        from: Option<&BlockReference>,
    ) -> Result<Self> {
        let mut sync = chain.sync();
        let head_ref = match from {
            Some(block_ref) => {
                // If the caller passed a block reference, they're starting from either the origin or a specific point
                let (head, _) = sync.find_intersect(&[block_ref.clone()]).await;
                let Some(head) = head else {
                    // Trying to init a fresh listener from a ref which does not exist
                    bail!("could not start listening from {from:?}, as it does not exist on-chain");
                };
                head
            }
            None => {
                // Otherwise, they just want to follow from the tip
                let (_, tip) = sync.find_intersect(&[]).await;
                // Call find_intersect again so the chainsync protocol knows we're following from the tip
                let (head, _) = sync.find_intersect(&[tip.clone()]).await;
                if !head.is_some_and(|h| h == tip) {
                    bail!("could not start listening from latest: rollback occurred while we were connecting");
                };
                tip
            }
        };
        let Some(head) = chain.request_block(&head_ref).await else {
            // Trying to init a fresh listener from a ref which does not exist
            bail!("could not start listening from {from:?}, as it does not exist on-chain");
        };

        let mut oldest_number = head.block_number;
        let mut prev_hash = head.parent_hash.clone();
        let mut history = VecDeque::new();
        history.push_back(head);

        for _ in 0..APPROXIMATELY_IMMUTABLE_LENGTH {
            if oldest_number == 0 {
                break;
            }
            let prev_ref = BlockReference::Point(oldest_number - 1, prev_hash);
            let Some(prev_block) = chain.request_block(&prev_ref).await else {
                // The chain rolled back while we were building up history
                bail!("block {from:?} was rolled back before we could finish setting it up");
            };

            oldest_number = prev_block.block_number;
            prev_hash = prev_block.parent_hash.clone();
            history.push_front(prev_block);
        }

        let (sync_event_sink, sync_event_source) = mpsc::channel(16);
        let (block_record_sink, block_record_source) = mpsc::unbounded_channel();
        tokio::spawn(Self::stay_in_sync(sync, sync_event_sink));
        tokio::spawn(Self::persist_blocks(id, db.clone(), block_record_source));
        Ok(Self {
            history,
            rollbacks: HashMap::new(),
            sync_event_source,
            block_record_sink,
        })
    }

    async fn stay_in_sync(mut sync: MockChainSync, sync_event_sink: mpsc::Sender<ChainSyncEvent>) {
        loop {
            let next_event = match sync.request_next().await {
                RequestNextResponse::RollForward(tip, _) => ChainSyncEvent::RollForward(tip),
                RequestNextResponse::RollBackward(rollback_to, _) => {
                    ChainSyncEvent::RollBackward(rollback_to)
                }
            };
            if sync_event_sink.send(next_event).await.is_err() {
                // the caller has disconnected, and so can we
                break;
            }
        }
    }

    async fn persist_blocks(
        id: ListenerId,
        db: Arc<BlockDatabase>,
        mut block_record_source: mpsc::UnboundedReceiver<BlockRecord>,
    ) {
        while let Some(record) = block_record_source.recv().await {
            db.save_block_record(&id, record).await;
        }
    }
}

enum ChainSyncEvent {
    RollForward(BlockInfo),
    RollBackward(BlockReference),
}

#[derive(Default)]
struct BlockDatabase {
    block_records: Arc<DashMap<ListenerId, HashMap<String, BlockRecord>>>,
}

impl BlockDatabase {
    async fn load_history(&self, listener: &ListenerId) -> Option<Vec<BlockRecord>> {
        let records = self.block_records.get(listener)?;
        Some(records.values().cloned().collect())
    }

    async fn save_block_record(&self, listener: &ListenerId, record: BlockRecord) {
        let mut records = self.block_records.entry(listener.clone()).or_default();
        records.insert(record.block.block_hash.clone(), record);
    }
}

#[derive(Clone)]
struct BlockRecord {
    block: BlockInfo,
    rolled_back: bool,
}
