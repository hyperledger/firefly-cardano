use std::{cmp::Ordering, collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use dashmap::{DashMap, Entry};
use firefly_server::apitypes::ToAnyhow;
use rand::Rng;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use tokio::{
    select,
    sync::{mpsc, oneshot, Notify, RwLock},
    time,
};

use crate::persistence::Persistence;

use super::{
    BlockEvent, BlockInfo, BlockReference, Listener, ListenerId, Stream, StreamCheckpoint, StreamId,
};

#[derive(Clone)]
pub struct Multiplexer {
    dispatchers: Arc<DashMap<StreamId, StreamDispatcher>>,
    stream_ids_by_topic: Arc<DashMap<String, StreamId>>,
    persistence: Arc<Persistence>,
    data_source: DataSource,
}

impl Multiplexer {
    pub async fn new(persistence: Arc<Persistence>) -> Result<Self> {
        let data_source = DataSource::new();

        let dispatchers = DashMap::new();
        let stream_ids_by_topic = DashMap::new();
        for stream in persistence.list_streams(None, None).await? {
            let topic = stream.name.clone();
            stream_ids_by_topic.insert(topic.clone(), stream.id.clone());

            let dispatcher =
                StreamDispatcher::new(&stream, persistence.clone(), data_source.clone()).await?;
            dispatchers.insert(stream.id, dispatcher);
        }
        Ok(Self {
            dispatchers: Arc::new(dispatchers),
            stream_ids_by_topic: Arc::new(stream_ids_by_topic),
            persistence,
            data_source,
        })
    }

    pub async fn handle_stream_write(&self, stream: &Stream) -> Result<()> {
        match self.dispatchers.entry(stream.id.clone()) {
            Entry::Occupied(entry) => {
                entry.get().update_settings(stream).await?;
            }
            Entry::Vacant(entry) => {
                self.stream_ids_by_topic
                    .insert(stream.name.clone(), stream.id.clone());

                entry.insert(
                    StreamDispatcher::new(
                        stream,
                        self.persistence.clone(),
                        self.data_source.clone(),
                    )
                    .await?,
                );
            }
        }
        Ok(())
    }

    pub async fn handle_stream_delete(&self, id: &StreamId) {
        self.dispatchers.remove(id);
    }

    pub async fn handle_listener_write(&self, listener: &Listener) -> Result<()> {
        let Some(dispatcher) = self.dispatchers.get(&listener.stream_id) else {
            bail!("new listener created for stream we haven't heard of");
        };
        dispatcher
            .add_listener(listener, BlockReference::Origin)
            .await
    }

    pub async fn handle_listener_delete(
        &self,
        stream_id: &StreamId,
        listener_id: &ListenerId,
    ) -> Result<()> {
        let Some(dispatcher) = self.dispatchers.get(stream_id) else {
            eprintln!("listener deleted for stream we haven't heard of");
            return Ok(());
        };
        dispatcher.remove_listener(listener_id).await
    }

    pub async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Batch>> {
        let Some(stream_id) = self.stream_ids_by_topic.get(topic) else {
            bail!("no stream found for topic {topic}");
        };
        let Some(dispatcher) = self.dispatchers.get(&stream_id) else {
            bail!("stream for topic {topic} has been deleted");
        };
        dispatcher.subscribe().await
    }
}

struct StreamDispatcher {
    state_change_sink: mpsc::Sender<StreamDispatcherStateChange>,
}

impl StreamDispatcher {
    pub async fn new(
        stream: &Stream,
        persistence: Arc<Persistence>,
        data_source: DataSource,
    ) -> Result<Self> {
        let listeners = persistence
            .list_listeners(&stream.id, None, None)
            .await
            .to_anyhow()?;
        let checkpoint = persistence.read_checkpoint(&stream.id).await.to_anyhow()?;
        let old_hwms = checkpoint.map(|cp| cp.listeners).unwrap_or_default();

        let mut hwms = BTreeMap::new();
        for listener in listeners {
            let hwm = old_hwms
                .get(&listener.id)
                .cloned()
                .unwrap_or(BlockReference::Origin);
            hwms.insert(listener.id, hwm);
        }

        let worker = StreamDispatcherWorker {
            stream_id: stream.id.clone(),
            batch_size: stream.batch_size,
            batch_timeout: stream.batch_timeout,
            batch_number: 0,
            hwms,
            data_source,
            persistence,
        };
        let (state_change_sink, state_change_source) = mpsc::channel(16);
        tokio::spawn(worker.run(state_change_source));
        Ok(Self { state_change_sink })
    }

    pub async fn update_settings(&self, stream: &Stream) -> Result<()> {
        let settings = StreamDispatcherSettings {
            batch_size: stream.batch_size,
            batch_timeout: stream.batch_timeout,
        };
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewSettings(settings))
            .await
            .context("could not update stream settings")?;
        Ok(())
    }

    pub async fn add_listener(&self, listener: &Listener, hwm: BlockReference) -> Result<()> {
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewListener(
                listener.id.clone(),
                hwm,
            ))
            .await
            .context("could not add listener")?;
        Ok(())
    }

    pub async fn remove_listener(&self, listener_id: &ListenerId) -> Result<()> {
        self.state_change_sink
            .send(StreamDispatcherStateChange::RemovedListener(
                listener_id.clone(),
            ))
            .await
            .context("could not remove listener")?;
        Ok(())
    }

    pub async fn subscribe(&self) -> Result<mpsc::Receiver<Batch>> {
        let (batch_sink, batch_source) = mpsc::channel(1);
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewBatchSink(batch_sink))
            .await
            .context("could not subscribe to stream")?;
        Ok(batch_source)
    }
}

struct StreamDispatcherWorker {
    stream_id: StreamId,
    batch_size: usize,
    batch_timeout: Duration,
    batch_number: u64,
    hwms: BTreeMap<ListenerId, BlockReference>,
    data_source: DataSource,
    persistence: Arc<Persistence>,
}

impl StreamDispatcherWorker {
    async fn run(mut self, mut state_change_source: mpsc::Receiver<StreamDispatcherStateChange>) {
        let mut batch_sink = None;
        loop {
            let (batch, ack_rx, hwms) = select! {
                batch = self.build_batch(), if batch_sink.is_some() && !self.hwms.is_empty() => batch,
                Some(change) = state_change_source.recv() => {
                    match change {
                        StreamDispatcherStateChange::NewSettings(settings) => {
                            self.batch_size = settings.batch_size;
                            self.batch_timeout = settings.batch_timeout;
                        }
                        StreamDispatcherStateChange::NewListener(listener_id, hwm) => {
                            self.hwms.insert(listener_id, hwm);
                        }
                        StreamDispatcherStateChange::RemovedListener(listener_id) => {
                            self.hwms.remove(&listener_id);
                        }
                        StreamDispatcherStateChange::NewBatchSink(sink) => {
                            batch_sink = Some(sink);
                        }
                    }
                    continue;
                }
                else => { panic!("stream dispatch worker in invalid state"); }
            };
            let sink = batch_sink.as_ref().unwrap();
            if let Err(err) = sink.send(batch).await {
                eprintln!("could not send batch: {err}");
                batch_sink = None;
            }
            match ack_rx.await {
                Ok(()) => {
                    self.hwms = hwms.clone();
                    self.persistence
                        .write_checkpoint(&StreamCheckpoint {
                            stream_id: self.stream_id.clone(),
                            listeners: hwms,
                        })
                        .await
                        .expect("error persisting checkpoint");
                    self.batch_number += 1;
                }
                Err(err) => {
                    eprintln!("error dispatching messages to stream: {}", err);
                }
            }
        }
    }

    async fn build_batch(
        &mut self,
    ) -> (
        Batch,
        oneshot::Receiver<()>,
        BTreeMap<ListenerId, BlockReference>,
    ) {
        let mut hwms = self.hwms.clone();
        loop {
            let batch_timeout_at = time::sleep(self.batch_timeout);
            tokio::pin!(batch_timeout_at);

            let mut events = vec![];
            loop {
                select! {
                    () = &mut batch_timeout_at => { break; }
                    event = self.next_event(&hwms) => {
                        hwms.insert(event.listener_id.clone(), BlockReference::Point(
                            event.block_info.block_number,
                            event.block_info.block_hash.clone(),
                        ));
                        events.push(event);
                        if events.len() >= self.batch_size {
                            break;
                        }
                    }
                }
            }

            if events.is_empty() {
                continue;
            }

            let (ack_tx, ack_rx) = oneshot::channel();
            let batch = Batch {
                batch_number: self.batch_number,
                events,
                ack_tx,
            };
            return (batch, ack_rx, hwms);
        }
    }

    async fn next_event(&mut self, hwms: &BTreeMap<ListenerId, BlockReference>) -> BlockEvent {
        assert!(!hwms.is_empty(), "no listeners to produce events!");
        let (mut listener_id, mut min_hwm) = hwms.first_key_value().unwrap();
        for (id, hwm) in hwms.iter().skip(1) {
            if let Some(Ordering::Less) = hwm.partial_cmp(min_hwm) {
                listener_id = id;
                min_hwm = hwm;
            }
        }
        let block_info = self.data_source.next_block(min_hwm).await;
        BlockEvent {
            listener_id: listener_id.clone(),
            block_info,
        }
    }
}

enum StreamDispatcherStateChange {
    NewSettings(StreamDispatcherSettings),
    NewListener(ListenerId, BlockReference),
    RemovedListener(ListenerId),
    NewBatchSink(mpsc::Sender<Batch>),
}

struct StreamDispatcherSettings {
    batch_size: usize,
    batch_timeout: Duration,
}

pub struct Batch {
    pub batch_number: u64,
    pub events: Vec<BlockEvent>,
    ack_tx: oneshot::Sender<()>,
}
impl Batch {
    pub fn ack(self) {
        let _ = self.ack_tx.send(());
    }
}

// Mock implementation of something which can query the chain
#[derive(Clone)]
struct DataSource {
    // the real implementation of course won't be in memory
    chain: Arc<RwLock<Vec<BlockInfo>>>,
    new_block: Arc<Notify>,
}

impl Default for DataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl DataSource {
    pub fn new() -> Self {
        let chain = Arc::new(RwLock::new(vec![]));
        let new_block = Arc::new(Notify::new());
        tokio::spawn(Self::generate(chain.clone(), new_block.clone()));
        Self { chain, new_block }
    }

    // TODO: sometimes there are rollbacks
    pub async fn next_block(&mut self, last: &BlockReference) -> BlockInfo {
        let block_number = match last {
            BlockReference::Origin => 0,
            BlockReference::Point(last_block_number, _) => *last_block_number as usize + 1,
        };
        loop {
            {
                let lock = self.chain.read().await;
                if lock.len() > block_number {
                    return lock[block_number].clone();
                }
            }
            self.new_block.notified().await;
        }
    }

    async fn generate(chain: Arc<RwLock<Vec<BlockInfo>>>, new_block: Arc<Notify>) {
        let mut rng = ChaChaRng::from_seed([0; 32]);
        loop {
            time::sleep(Duration::from_secs(1)).await;
            let mut lock = chain.write().await;
            Self::generate_block(&mut rng, &mut lock);
            new_block.notify_waiters();
        }
    }

    fn generate_block(rng: &mut ChaChaRng, chain: &mut Vec<BlockInfo>) {
        let (block_number, parent_hash) = match chain.last() {
            Some(block) => (block.block_number + 1, block.block_hash.clone()),
            None => (0, "".into()),
        };

        let mut transaction_hashes = vec![];
        for _ in 0..rng.gen_range(0..10) {
            transaction_hashes.push(Self::generate_hash(rng));
        }
        let block = BlockInfo {
            block_number,
            block_hash: Self::generate_hash(rng),
            parent_hash,
            transaction_hashes,
        };
        chain.push(block);
    }

    fn generate_hash(rng: &mut ChaChaRng) -> String {
        let bytes: [u8; 32] = rng.gen();
        hex::encode(bytes)
    }
}
