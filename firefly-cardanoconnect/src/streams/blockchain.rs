use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::Arc,
};

use anyhow::{anyhow, bail, Result};
use tokio::sync::mpsc;
use tracing::warn;

use crate::{
    blockchain::{BlockchainClient, ChainSyncClient, RequestNextResponse},
    persistence::Persistence,
    utils::LazyInit,
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

    pub fn listen(&self, id: ListenerId, from: Option<&BlockReference>) -> ChainListener {
        let inner = ChainListenerImpl::init(
            id,
            self.blockchain.clone(),
            self.persistence.clone(),
            from.cloned(),
        );
        ChainListener(LazyInit::new(inner))
    }
}

#[derive(Debug)]
pub struct ChainListener(LazyInit<Result<ChainListenerImpl>>);
impl ChainListener {
    pub async fn get_tip(&mut self) -> Result<BlockReference> {
        Ok(self.get_impl().await?.get_tip())
    }

    pub fn try_get_next_event(&mut self, block_ref: &BlockReference) -> Option<ListenerEvent> {
        let l = self.0.try_get()?.as_mut().ok()?;
        l.try_get_next_event(block_ref)
    }

    pub async fn wait_for_next_event(&mut self, block_ref: &BlockReference) -> ListenerEvent {
        let l = self.get_impl().await.unwrap();
        loop {
            if let Some(event) = l.try_get_next_event(block_ref) {
                return event;
            }
            l.wait_for_more_events().await;
        }
    }

    async fn get_impl(&mut self) -> Result<&mut ChainListenerImpl> {
        match self.0.get().await {
            Ok(res) => Ok(res),
            Err(e) => Err(anyhow!("could not init chain listener: {e:?}")),
        }
    }
}

#[derive(Debug)]
pub struct ChainListenerImpl {
    history: VecDeque<BlockInfo>,
    start: BlockReference,
    rollbacks: HashMap<BlockReference, BlockInfo>,
    sync_event_source: mpsc::Receiver<ChainSyncEvent>,
    block_record_sink: mpsc::UnboundedSender<BlockRecord>,
    genesis_hash: String,
}

impl ChainListenerImpl {
    async fn init(
        id: ListenerId,
        chain: Arc<BlockchainClient>,
        persistence: Arc<dyn Persistence>,
        from: Option<BlockReference>,
    ) -> Result<Self> {
        let records = persistence.load_history(&id).await?;
        if !records.is_empty() {
            Self::init_existing(id, &chain, persistence, records).await
        } else {
            Self::init_new(id, &chain, persistence, from).await
        }
    }

    pub fn get_tip(&self) -> BlockReference {
        self.history
            .back()
            .map(|b| b.as_reference())
            .unwrap_or(self.start.clone())
    }

    pub fn try_get_next_event(&mut self, block_ref: &BlockReference) -> Option<ListenerEvent> {
        let (prev_slot, prev_hash) = match block_ref {
            BlockReference::Origin => (None, self.genesis_hash.clone()),
            BlockReference::Point(slot, hash) => (*slot, hash.clone()),
        };

        // if we haven't seen enough blocks to be "sure" that this one is immutable, apply all pending updates synchronously
        if prev_slot.is_none_or(|slot| {
            self.history
                .iter()
                .rev()
                .find_map(|b| b.block_slot)
                .is_none_or(|tip| tip < slot + APPROXIMATELY_IMMUTABLE_LENGTH)
        }) {
            while let Ok(sync_event) = self.sync_event_source.try_recv() {
                self.handle_sync_event(sync_event);
            }
        }

        // Check if we've rolled back already
        if let Some(rollback_to) = self.rollbacks.get(block_ref) {
            return Some(ListenerEvent::Rollback(rollback_to.clone()));
        }

        for block in self.history.iter().rev() {
            // If this block's parent is the one we're starting from, this is the next block to process.
            if block.parent_hash.as_ref().is_some_and(|h| *h == prev_hash) {
                return Some(ListenerEvent::Process(block.clone()));
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
        None
    }

    pub async fn wait_for_more_events(&mut self) {
        let mut sync_events = vec![];
        if self.sync_event_source.recv_many(&mut sync_events, 32).await == 0 {
            panic!("data source has been shut down")
        }
        for sync_event in sync_events {
            self.handle_sync_event(sync_event);
        }
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
        persistence: Arc<dyn Persistence>,
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
        let Some(BlockReference::Point(_, head_hash)) = head_ref else {
            // The chain didn't recognize any of the blocks we saved from this chain.
            // We have no way to recover.
            bail!("listener {id} is on a fork which no longer exists");
        };

        let mut rollbacks = HashMap::new();
        while history.back().is_some_and(|i| i.block_hash != head_hash) {
            let rolled_back = history.pop_back().unwrap();
            rollbacks.insert(rolled_back.as_reference(), rolled_back);
        }
        let start = history.back().unwrap().as_reference();

        let (sync_event_sink, sync_event_source) = mpsc::channel(16);
        let (block_record_sink, block_record_source) = mpsc::unbounded_channel();
        tokio::spawn(Self::stay_in_sync(sync, sync_event_sink));
        tokio::spawn(Self::persist_blocks(id, persistence, block_record_source));
        Ok(Self {
            history,
            start,
            rollbacks,
            genesis_hash: chain.genesis_hash(),
            sync_event_source,
            block_record_sink,
        })
    }

    async fn init_new(
        id: ListenerId,
        chain: &BlockchainClient,
        persistence: Arc<dyn Persistence>,
        from: Option<BlockReference>,
    ) -> Result<Self> {
        let mut sync = chain.sync().await?;
        let start = if let Some(block_ref) = &from {
            let (Some(head), _) = sync.find_intersect(&[block_ref.clone()]).await? else {
                // Trying to init a fresh listener from a ref which does not exist
                bail!("could not start listening from {from:?}, as it does not exist on-chain");
            };
            head
        } else {
            // Otherwise, they just want to follow from the tip
            let (_, tip) = sync.find_intersect(&[]).await?;
            // Call find_intersect again so the chainsync protocol knows we're following from the tip
            let (head, _) = sync.find_intersect(&[tip.clone()]).await?;
            if !head.is_some_and(|h| h == tip) {
                bail!("could not start listening from latest: rollback occurred while we were connecting");
            };
            tip
        };

        let (sync_event_sink, sync_event_source) = mpsc::channel(16);
        let (block_record_sink, block_record_source) = mpsc::unbounded_channel();
        tokio::spawn(Self::stay_in_sync(sync, sync_event_sink));
        tokio::spawn(Self::persist_blocks(id, persistence, block_record_source));
        Ok(Self {
            history: VecDeque::new(),
            start,
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
