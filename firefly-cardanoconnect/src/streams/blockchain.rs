use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::{bail, Result};
use tokio::sync::mpsc;
use tracing::warn;

use crate::{
    blockchain::{BlockchainClient, ChainSyncClient, RequestNextResponse},
    persistence::Persistence,
};

use super::{BlockInfo, BlockRecord, BlockReference, ListenerId};

#[derive(Debug)]
pub enum ListenerEvent {
    Process(BlockInfo),
    Rollback(BlockInfo),
}

pub struct DataSource {
    blockchain: Arc<BlockchainClient>,
    persistence: Arc<dyn Persistence>,
}

const APPROXIMATELY_IMMUTABLE_LENGTH: u64 = 20;

impl DataSource {
    pub fn new(blockchain: Arc<BlockchainClient>, persistence: Arc<dyn Persistence>) -> Self {
        Self {
            blockchain,
            persistence,
        }
    }

    pub async fn listen(
        &self,
        id: ListenerId,
        from: Option<&BlockReference>,
    ) -> Result<ChainListener> {
        ChainListener::init(id, &self.blockchain, &self.persistence, from).await
    }
}

#[derive(Debug)]
pub struct ChainListener {
    history: VecDeque<BlockInfo>,
    rollbacks: HashMap<BlockReference, BlockInfo>,
    sync_event_source: mpsc::Receiver<ChainSyncEvent>,
    block_record_sink: mpsc::UnboundedSender<BlockRecord>,
    genesis_hash: String,
}

impl ChainListener {
    async fn init(
        id: ListenerId,
        chain: &BlockchainClient,
        persistence: &Arc<dyn Persistence>,
        from: Option<&BlockReference>,
    ) -> Result<Self> {
        let records = persistence.load_history(&id).await?;
        if !records.is_empty() {
            Self::init_existing(id, chain, persistence, records).await
        } else {
            Self::init_new(id, chain, persistence, from).await
        }
    }

    pub fn get_tip(&self) -> BlockReference {
        self.history.back().unwrap().as_reference()
    }

    pub fn get_event(&mut self, block_ref: &BlockReference) -> Option<ListenerEvent> {
        let (target_slot, target_hash) = match block_ref {
            BlockReference::Origin => (None, self.genesis_hash.clone()),
            BlockReference::Point(slot, hash) => (*slot, hash.clone()),
        };

        if let Some(slot) = target_slot {
            // if we haven't seen enough blocks to be "sure" that this one is immutable, apply all pending updates synchronously
            if self
                .history
                .iter()
                .rev()
                .find_map(|b| b.block_slot)
                .is_some_and(|tip| tip < slot + APPROXIMATELY_IMMUTABLE_LENGTH)
            {
                while let Ok(sync_event) = self.sync_event_source.try_recv() {
                    self.handle_sync_event(sync_event);
                }
            }
        }

        // If we already know this block has been rolled back, just say so
        if let Some(rollback) = self.rollbacks.get(block_ref) {
            return Some(ListenerEvent::Rollback(rollback.clone()));
        }

        // If we have it already, return it.
        // If we don't, no big deal, some other consumer is running at a different point in history
        self.history
            .iter()
            .rev()
            .take_while(|b| {
                !b.block_slot
                    .is_some_and(|b| target_slot.is_some_and(|t| b < t))
            })
            .find(|b| b.block_hash == target_hash)
            .cloned()
            .map(ListenerEvent::Process)
    }

    pub async fn get_next(&mut self, block_ref: &BlockReference) -> Option<BlockReference> {
        let (prev_slot, prev_hash) = match block_ref {
            BlockReference::Origin => (None, self.genesis_hash.clone()),
            BlockReference::Point(slot, hash) => (*slot, hash.clone()),
        };

        loop {
            // Have we rolled back to before this block? If so, don't wait for its successor.
            // That successor will never come, and even if it did, we'd ignore it.
            if self.rollbacks.contains_key(block_ref) {
                return None;
            }

            for (index, block) in self.history.iter().enumerate().rev() {
                if block.block_hash == prev_hash {
                    if let Some(next) = self.history.get(index + 1) {
                        // we already have the block which comes after this!
                        return Some(next.as_reference());
                    } else {
                        // we don't have that block yet, so process events until we do
                        break;
                    }
                }
                // If we can tell by the slots we've gone too far back, break early
                if block
                    .block_slot
                    .is_some_and(|slot| prev_slot.is_some_and(|target| slot < target))
                {
                    break;
                }
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
                let record = BlockRecord {
                    block: block.clone(),
                    rolled_back: false,
                };
                self.block_record_sink.send(record).unwrap();
                self.history.push_back(block);
            }
            ChainSyncEvent::RollBackward(rollback_to) => {
                let target_hash = match rollback_to {
                    BlockReference::Origin => self.genesis_hash.clone(),
                    BlockReference::Point(_, hash) => hash,
                };
                while self
                    .history
                    .back()
                    .is_some_and(|i| i.block_hash != target_hash)
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
                assert!(
                    !self.history.is_empty(),
                    "tried rolling back past recorded history"
                );
            }
        }
    }

    async fn init_existing(
        id: ListenerId,
        chain: &BlockchainClient,
        persistence: &Arc<dyn Persistence>,
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
        let mut history: VecDeque<BlockInfo> = history.into();

        let mut sync = chain.sync().await?;
        let points: Vec<_> = history.iter().rev().map(|b| b.as_reference()).collect();
        let (head_ref, _) = sync.find_intersect(&points).await?;
        let Some(head_ref) = head_ref else {
            // The chain didn't recognize any of the blocks we saved from this chain.
            // We have no way to recover.
            bail!("listener {id} is on a fork which no longer exists");
        };
        let Some(head) = sync.request_block(&head_ref).await? else {
            bail!("listener {id} is on a fork which no longer exists");
        };

        let mut rollbacks = HashMap::new();
        while history
            .back()
            .is_some_and(|i| i.block_hash != head.block_hash)
        {
            let rolled_back = history.pop_back().unwrap();
            rollbacks.insert(rolled_back.as_reference(), rolled_back);
        }

        let (sync_event_sink, sync_event_source) = mpsc::channel(16);
        let (block_record_sink, block_record_source) = mpsc::unbounded_channel();
        tokio::spawn(Self::stay_in_sync(sync, sync_event_sink));
        tokio::spawn(Self::persist_blocks(
            id,
            persistence.clone(),
            block_record_source,
        ));
        Ok(Self {
            history,
            rollbacks,
            genesis_hash: chain.genesis_hash(),
            sync_event_source,
            block_record_sink,
        })
    }

    async fn init_new(
        id: ListenerId,
        chain: &BlockchainClient,
        persistence: &Arc<dyn Persistence>,
        from: Option<&BlockReference>,
    ) -> Result<Self> {
        let mut sync = chain.sync().await?;
        let head_ref = match from {
            Some(block_ref) => {
                // If the caller passed a block reference, they're starting from either the origin or a specific point
                let (head, _) = sync.find_intersect(&[block_ref.clone()]).await?;
                let Some(head) = head else {
                    // Trying to init a fresh listener from a ref which does not exist
                    bail!("could not start listening from {from:?}, as it does not exist on-chain");
                };
                head
            }
            None => {
                // Otherwise, they just want to follow from the tip
                let (_, tip) = sync.find_intersect(&[]).await?;
                // Call find_intersect again so the chainsync protocol knows we're following from the tip
                let (head, _) = sync.find_intersect(&[tip.clone()]).await?;
                if !head.is_some_and(|h| h == tip) {
                    bail!("could not start listening from latest: rollback occurred while we were connecting");
                };
                tip
            }
        };
        let Some(head) = sync.request_block(&head_ref).await? else {
            // Trying to init a fresh listener from a ref which does not exist
            bail!("could not start listening from {from:?}, as it does not exist on-chain");
        };

        let mut prev_hash = head.parent_hash.clone();
        let mut history = VecDeque::new();
        history.push_back(head);

        for _ in 0..APPROXIMATELY_IMMUTABLE_LENGTH {
            let Some(prev) = prev_hash else {
                break;
            };
            let prev_ref = BlockReference::Point(None, prev);
            let prev_block = match sync.request_block(&prev_ref).await {
                Err(err) => {
                    warn!("could not populate a history for this listener, it may not be able to recover from rollback: {}", err);
                    break;
                }
                Ok(None) => {
                    bail!("block {from:?} was rolled back before we could finish setting it up")
                }
                Ok(Some(prev_block)) => prev_block,
            };

            prev_hash = prev_block.parent_hash.clone();
            history.push_front(prev_block);
        }

        let records: Vec<_> = history
            .iter()
            .map(|block| BlockRecord {
                block: block.clone(),
                rolled_back: false,
            })
            .collect();
        persistence.save_block_records(&id, records).await?;

        let (sync_event_sink, sync_event_source) = mpsc::channel(16);
        let (block_record_sink, block_record_source) = mpsc::unbounded_channel();
        tokio::spawn(Self::stay_in_sync(sync, sync_event_sink));
        tokio::spawn(Self::persist_blocks(
            id,
            persistence.clone(),
            block_record_source,
        ));
        Ok(Self {
            history,
            rollbacks: HashMap::new(),
            genesis_hash: chain.genesis_hash(),
            sync_event_source,
            block_record_sink,
        })
    }

    async fn stay_in_sync(
        mut sync: impl ChainSyncClient,
        sync_event_sink: mpsc::Sender<ChainSyncEvent>,
    ) {
        loop {
            let next_response = match sync.request_next().await {
                Ok(response) => response,
                Err(error) => {
                    warn!("Error syncing with chain: {:#}", error);
                    break;
                }
            };
            let next_event = match next_response {
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
        db: Arc<dyn Persistence>,
        mut block_record_source: mpsc::UnboundedReceiver<BlockRecord>,
    ) {
        let mut records = vec![];
        while block_record_source.recv_many(&mut records, 256).await > 0 {
            if let Err(error) = db.save_block_records(&id, records).await {
                warn!("could not save records: {error:#}");
            }
            records = vec![];
        }
        warn!("stopped saving records?");
    }
}

enum ChainSyncEvent {
    RollForward(BlockInfo),
    RollBackward(BlockReference),
}
