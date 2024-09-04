use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{bail, Result};
use dashmap::{DashMap, Entry};
use firefly_server::apitypes::ToAnyhow;
use rand::{rngs::ThreadRng, thread_rng, Rng};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    time,
};

use crate::persistence::Persistence;

use super::{BlockEvent, BlockInfo, Listener, ListenerId, Stream, StreamId};

#[derive(Clone)]
pub struct Multiplexer {
    dispatchers: Arc<DashMap<StreamId, StreamDispatcher>>,
    stream_ids_by_topic: Arc<DashMap<String, StreamId>>,
    block_sinks: Arc<Mutex<Vec<mpsc::Sender<BlockInfo>>>>,
}

impl Multiplexer {
    pub async fn new(persistence: Arc<Persistence>) -> Result<Self> {
        let dispatchers = DashMap::new();
        let stream_ids_by_topic = DashMap::new();
        let mut block_sinks = vec![];
        for stream in persistence.list_streams(None, None).await? {
            let topic = stream.name.clone();
            stream_ids_by_topic.insert(topic.clone(), stream.id.clone());

            let listeners = persistence
                .list_listeners(&stream.id, None, None)
                .await
                .to_anyhow()?;

            let (block_sink, block_source) = mpsc::channel(1);
            block_sinks.push(block_sink);

            let dispatcher = StreamDispatcher::new(&stream, listeners, DataSource { block_source });
            dispatchers.insert(stream.id, dispatcher);
        }
        let me = Self {
            dispatchers: Arc::new(dispatchers),
            stream_ids_by_topic: Arc::new(stream_ids_by_topic),
            block_sinks: Arc::new(Mutex::new(block_sinks)),
        };
        tokio::spawn(me.clone().run());
        Ok(me)
    }

    pub async fn run(self) {
        // TODO: we won't be pushing new blocks to the dispatchers, they should be requesting them
        let block_sinks = self.block_sinks;
        let mut block_number = 0;
        let mut last_block_hash = "".to_string();
        loop {
            time::sleep(Duration::from_secs(5)).await;
            let block = {
                let mut rng = thread_rng();
                let mut transaction_hashes = vec![];
                for _ in 0..rng.gen_range(0..10) {
                    transaction_hashes.push(Self::fake_hash(&mut rng));
                }
                BlockInfo {
                    block_number,
                    block_hash: Self::fake_hash(&mut rng),
                    parent_hash: last_block_hash.clone(),
                    transaction_hashes,
                }
            };
            block_number += 1;
            last_block_hash = block.block_hash.clone();

            let mut sink_lock = block_sinks.lock().await;
            for sink in sink_lock.iter_mut() {
                let _ = sink.try_send(block.clone());
            }
        }
    }

    fn fake_hash(rng: &mut ThreadRng) -> String {
        let bytes: [u8; 32] = rng.gen();
        hex::encode(bytes)
    }

    pub async fn handle_stream_write(&self, stream: &Stream) {
        match self.dispatchers.entry(stream.id.clone()) {
            Entry::Occupied(entry) => {
                entry.get().update_settings(stream).await;
            }
            Entry::Vacant(entry) => {
                let (block_sink, block_source) = mpsc::channel(1);
                entry.insert(StreamDispatcher::new(
                    stream,
                    vec![],
                    DataSource { block_source },
                ));

                self.stream_ids_by_topic
                    .insert(stream.name.clone(), stream.id.clone());
                self.block_sinks.lock().await.push(block_sink);
            }
        }
    }

    pub async fn handle_stream_delete(&self, id: &StreamId) {
        self.dispatchers.remove(id);
    }

    pub async fn handle_listener_write(&self, listener: &Listener) {
        let Some(dispatcher) = self.dispatchers.get(&listener.stream_id) else {
            eprintln!("new listener created for stream we haven't heard of");
            return;
        };
        dispatcher.add_listener(listener).await;
    }

    pub async fn handle_listener_delete(&self, stream_id: &StreamId, listener_id: &ListenerId) {
        let Some(dispatcher) = self.dispatchers.get(stream_id) else {
            eprintln!("listener deleted for stream we haven't heard of");
            return;
        };
        dispatcher.remove_listener(listener_id).await;
    }

    pub async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Batch>> {
        let Some(stream_id) = self.stream_ids_by_topic.get(topic) else {
            bail!("no stream found for topic {topic}");
        };
        let Some(dispatcher) = self.dispatchers.get(&stream_id) else {
            bail!("stream for topic {topic} has been deleted");
        };
        Ok(dispatcher.subscribe().await)
    }
}

struct StreamDispatcher {
    state_change_sink: mpsc::Sender<StreamDispatcherStateChange>,
}

impl StreamDispatcher {
    pub fn new(stream: &Stream, listeners: Vec<Listener>, data_source: DataSource) -> Self {
        let worker = StreamDispatcherWorker {
            batch_size: stream.batch_size,
            batch_timeout: stream.batch_timeout,
            batch_number: 0,
            listeners: listeners.into_iter().map(|it| it.id).collect(),
            data_source,
        };
        let (state_change_sink, state_change_source) = mpsc::channel(16);
        tokio::spawn(worker.run(state_change_source));
        Self { state_change_sink }
    }

    pub async fn update_settings(&self, stream: &Stream) {
        let settings = StreamDispatcherSettings {
            batch_size: stream.batch_size,
            batch_timeout: stream.batch_timeout,
        };
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewSettings(settings))
            .await
            .unwrap();
    }

    pub async fn add_listener(&self, listener: &Listener) {
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewListener(
                listener.id.clone(),
            ))
            .await
            .unwrap();
    }

    pub async fn remove_listener(&self, listener_id: &ListenerId) {
        self.state_change_sink
            .send(StreamDispatcherStateChange::RemovedListener(
                listener_id.clone(),
            ))
            .await
            .unwrap();
    }

    pub async fn subscribe(&self) -> mpsc::Receiver<Batch> {
        let (batch_sink, batch_source) = mpsc::channel(1);
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewBatchSink(batch_sink))
            .await
            .unwrap();
        batch_source
    }
}

struct StreamDispatcherWorker {
    batch_size: usize,
    batch_timeout: Duration,
    batch_number: u64,
    listeners: BTreeSet<ListenerId>,
    data_source: DataSource,
}

impl StreamDispatcherWorker {
    async fn run(mut self, mut state_change_source: mpsc::Receiver<StreamDispatcherStateChange>) {
        let mut batch_sink = None;
        loop {
            let (batch, ack_rx) = select! {
                batch = self.build_batch(), if batch_sink.is_some() => batch,
                Some(change) = state_change_source.recv() => {
                    match change {
                        StreamDispatcherStateChange::NewSettings(settings) => {
                            self.batch_size = settings.batch_size;
                            self.batch_timeout = settings.batch_timeout;
                        }
                        StreamDispatcherStateChange::NewListener(listener) => {
                            self.listeners.insert(listener);
                        }
                        StreamDispatcherStateChange::RemovedListener(listener) => {
                            self.listeners.remove(&listener);
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
            sink.send(batch).await.unwrap();
            match ack_rx.await {
                Ok(()) => {
                    // TODO: ack
                    self.batch_number += 1;
                }
                Err(err) => {
                    eprintln!("error dispatching messages to stream: {}", err);
                    // TODO: nack
                }
            }
        }
    }

    async fn build_batch(&mut self) -> (Batch, oneshot::Receiver<()>) {
        loop {
            let batch_timeout_at = time::sleep(self.batch_timeout);
            tokio::pin!(batch_timeout_at);

            let mut events = vec![];
            loop {
                select! {
                    () = &mut batch_timeout_at => { break; }
                    block = self.data_source.next_block() => {
                        for listener_id in self.listeners.iter() {
                            events.push(BlockEvent {
                                listener_id: listener_id.clone(),
                                block_info: block.clone(),
                            });
                        }
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
            return (batch, ack_rx);
        }
    }
}

enum StreamDispatcherStateChange {
    NewSettings(StreamDispatcherSettings),
    NewListener(ListenerId),
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

struct DataSource {
    block_source: mpsc::Receiver<BlockInfo>,
}

impl DataSource {
    async fn next_block(&mut self) -> BlockInfo {
        self.block_source.recv().await.unwrap()
    }
}
