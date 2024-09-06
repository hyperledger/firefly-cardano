use std::{
    cmp::Ordering,
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Context, Result};
use dashmap::{DashMap, Entry};
use firefly_server::apitypes::ToAnyhow;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time,
};
use tracing::warn;

use crate::{
    persistence::Persistence,
    streams::{blockchain::ListenerEvent, EventData, EventId},
};

use super::{
    blockchain::DataSource, BlockInfo, BlockReference, Event, EventReference, Listener,
    ListenerFilter, ListenerId, Stream, StreamCheckpoint, StreamId,
};

#[derive(Clone)]
pub struct Multiplexer {
    dispatchers: Arc<DashMap<StreamId, StreamDispatcher>>,
    stream_ids_by_topic: Arc<DashMap<String, StreamId>>,
    persistence: Arc<Persistence>,
    data_source: Arc<DataSource>,
}

impl Multiplexer {
    pub async fn new(persistence: Arc<Persistence>) -> Result<Self> {
        let data_source = Arc::new(DataSource::new());

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
        let hwm = EventReference::default(); // TODO: caller can provide this
        self.data_source
            .init_listener(listener.id.clone(), &hwm.block)
            .await?;
        let Some(dispatcher) = self.dispatchers.get(&listener.stream_id) else {
            bail!("new listener created for stream we haven't heard of");
        };
        dispatcher
            .add_listener(listener, EventReference::default())
            .await
    }

    pub async fn handle_listener_delete(
        &self,
        stream_id: &StreamId,
        listener_id: &ListenerId,
    ) -> Result<()> {
        let Some(dispatcher) = self.dispatchers.get(stream_id) else {
            warn!(%stream_id, %listener_id, "listener deleted for stream we haven't heard of");
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
        data_source: Arc<DataSource>,
    ) -> Result<Self> {
        let all_listeners = persistence
            .list_listeners(&stream.id, None, None)
            .await
            .to_anyhow()?;
        let checkpoint = persistence.read_checkpoint(&stream.id).await.to_anyhow()?;
        let old_hwms = checkpoint.map(|cp| cp.listeners).unwrap_or_default();

        let mut listeners = BTreeMap::new();
        for listener in all_listeners {
            let hwm = old_hwms.get(&listener.id).cloned().unwrap_or_default();
            listeners.insert(
                listener.id.clone(),
                ListenerState {
                    id: listener.id,
                    hwm,
                    filters: listener.filters,
                },
            );
        }

        let worker = StreamDispatcherWorker {
            stream_id: stream.id.clone(),
            batch_size: stream.batch_size,
            batch_timeout: stream.batch_timeout,
            batch_number: 0,
            listeners,
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

    pub async fn add_listener(&self, listener: &Listener, hwm: EventReference) -> Result<()> {
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewListener(
                listener.id.clone(),
                listener.filters.clone(),
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
    listeners: BTreeMap<ListenerId, ListenerState>,
    data_source: Arc<DataSource>,
    persistence: Arc<Persistence>,
}

impl StreamDispatcherWorker {
    async fn run(mut self, mut state_change_source: mpsc::Receiver<StreamDispatcherStateChange>) {
        let mut batch_sink = None;
        loop {
            let (batch, ack_rx, listeners) = select! {
                batch = self.build_batch(), if batch_sink.is_some() && !self.listeners.is_empty() => batch,
                Some(change) = state_change_source.recv() => {
                    match change {
                        StreamDispatcherStateChange::NewSettings(settings) => {
                            self.batch_size = settings.batch_size;
                            self.batch_timeout = settings.batch_timeout;
                        }
                        StreamDispatcherStateChange::NewListener(listener_id, filters, hwm) => {
                            self.listeners.insert(listener_id.clone(), ListenerState {
                                id: listener_id,
                                hwm,
                                filters,
                            });
                        }
                        StreamDispatcherStateChange::RemovedListener(listener_id) => {
                            self.listeners.remove(&listener_id);
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
                warn!(%self.stream_id, self.batch_number, "could not send batch: {err}");
                batch_sink = None;
            }
            match ack_rx.await {
                Ok(()) => {
                    self.listeners = listeners;
                    let mut hwms = BTreeMap::new();
                    for (listener_id, listener) in self.listeners.iter() {
                        hwms.insert(listener_id.clone(), listener.hwm.clone());
                    }
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
                    warn!(%self.stream_id, self.batch_number, "error dispatching messages to stream: {}", err);
                }
            }
        }
    }

    async fn build_batch(
        &mut self,
    ) -> (
        Batch,
        oneshot::Receiver<()>,
        BTreeMap<ListenerId, ListenerState>,
    ) {
        let mut listeners = self.listeners.clone();

        loop {
            let batch_timeout_at = time::sleep(self.batch_timeout);
            tokio::pin!(batch_timeout_at);

            let mut events = vec![];
            select! {
                () = &mut batch_timeout_at => {}
                () = self.collect_events(&mut listeners, &mut events) => {}
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
            return (batch, ack_rx, listeners);
        }
    }

    async fn collect_events(
        &mut self,
        listeners: &mut BTreeMap<ListenerId, ListenerState>,
        events: &mut Vec<Event>,
    ) {
        assert!(!listeners.is_empty(), "no listeners to produce events!");
        loop {
            let block_refs: Vec<_> = listeners
                .values()
                .map(|l| (l.id.clone(), l.hwm.block.clone()))
                .collect();
            let listener_event = self.data_source.current_event(&block_refs);
            match &listener_event {
                ListenerEvent::Process(block) => {
                    self.collect_forward_events(listeners, events, block);
                }
                ListenerEvent::Rollback(block) => {
                    self.collect_backward_events(listeners, events, block)
                }
            }
            if events.len() >= self.batch_size {
                break;
            }
            if let Some(ref_update) = self
                .data_source
                .update_refs(&block_refs, &listener_event)
                .await
            {
                for listener in listeners.values_mut() {
                    if listener.hwm.block == ref_update.from {
                        listener.hwm = EventReference {
                            block: ref_update.to.clone(),
                            rollback: ref_update.rollback,
                            tx_index: None,
                            log_index: None,
                        };
                    }
                }
            }
        }
    }

    fn collect_forward_events(
        &self,
        listeners: &mut BTreeMap<ListenerId, ListenerState>,
        events: &mut Vec<Event>,
        block: &BlockInfo,
    ) {
        for listener in listeners.values_mut() {
            if !Self::matches_ref(block, &listener.hwm.block) {
                continue;
            }
            for event in self.find_forward_events(listener, block) {
                listener.hwm = EventReference {
                    block: listener.hwm.block.clone(),
                    rollback: false,
                    tx_index: Some(event.id.transaction_index),
                    log_index: Some(event.id.log_index),
                };
                events.push(event);
                if events.len() >= self.batch_size {
                    return;
                }
            }
        }
    }

    fn collect_backward_events(
        &self,
        listeners: &mut BTreeMap<ListenerId, ListenerState>,
        events: &mut Vec<Event>,
        block: &BlockInfo,
    ) {
        for listener in listeners.values_mut().rev() {
            if !Self::matches_ref(block, &listener.hwm.block) {
                continue;
            }
            for event in self.find_backward_events(listener, block) {
                listener.hwm = EventReference {
                    block: listener.hwm.block.clone(),
                    rollback: true,
                    tx_index: Some(event.id.transaction_index),
                    log_index: Some(event.id.log_index),
                };
                events.push(event);
                if events.len() >= self.batch_size {
                    return;
                }
            }
        }
    }

    fn matches_ref(block: &BlockInfo, block_ref: &BlockReference) -> bool {
        match block_ref {
            BlockReference::Origin => false,
            BlockReference::Point(number, hash) => {
                block.block_number == *number && block.block_hash == *hash
            }
        }
    }

    fn find_forward_events(&self, listener: &ListenerState, block: &BlockInfo) -> Vec<Event> {
        if listener.hwm.rollback {
            // If we were rolling back before, and now we've started rolling forward,
            // we rolled back onto a block we've already completely processed.
            return vec![];
        }
        let mut events = self.find_events(listener, block);
        events.retain(|e| {
            // throw out any events which came before our high-water mark
            let hwm = &listener.hwm;
            let tx_cmp = hwm
                .tx_index
                .as_ref()
                .map(|tx| e.id.transaction_index.cmp(tx));
            let log_cmp = hwm.log_index.as_ref().map(|log| e.id.log_index.cmp(log));
            match (tx_cmp, log_cmp) {
                (Some(Ordering::Less), _) => false,
                (Some(Ordering::Equal), Some(Ordering::Less)) => false,
                (_, _) => true,
            }
        });
        events
    }

    fn find_backward_events(&self, listener: &ListenerState, block: &BlockInfo) -> Vec<Event> {
        self.find_events(listener, block)
            .into_iter()
            .rev()
            .filter(|e| {
                let hwm = &listener.hwm;
                let tx_cmp = hwm
                    .tx_index
                    .as_ref()
                    .map(|tx| e.id.transaction_index.cmp(tx));
                let log_cmp = hwm.log_index.as_ref().map(|log| e.id.log_index.cmp(log));
                if hwm.rollback {
                    // if we were rolling back already, throw out events we've already rolled back past
                    match (tx_cmp, log_cmp) {
                        (Some(Ordering::Greater), _) => false,
                        (Some(Ordering::Equal), Some(Ordering::Greater)) => false,
                        (_, _) => true,
                    }
                } else {
                    // otherwise, just keep any events that rolling forward would have kept
                    match (tx_cmp, log_cmp) {
                        (Some(Ordering::Less), _) => true,
                        (Some(Ordering::Equal), Some(Ordering::Less)) => true,
                        (_, _) => false,
                    }
                }
            })
            .map(|e| e.into_rollback())
            .collect()
    }

    fn find_events(&self, listener: &ListenerState, block: &BlockInfo) -> Vec<Event> {
        let mut events = vec![];
        for (tx_idx, tx_hash) in block.transaction_hashes.iter().enumerate() {
            let tx_idx = tx_idx as u64;
            for filter in &listener.filters {
                if Self::matches_filter(tx_hash, filter) {
                    let id = EventId {
                        listener_id: listener.id.clone(),
                        block_hash: block.block_hash.clone(),
                        block_number: block.block_number,
                        transaction_hash: tx_hash.clone(),
                        transaction_index: tx_idx,
                        log_index: 0,
                        timestamp: Some(SystemTime::now()),
                    };
                    events.push(Event {
                        id,
                        data: EventData::TransactionAccepted,
                    })
                }
            }
        }
        events
    }

    fn matches_filter(tx: &str, filter: &ListenerFilter) -> bool {
        match filter {
            ListenerFilter::TransactionId(id) => id == tx || id == "any",
        }
    }
}

#[derive(Debug, Clone)]
struct ListenerState {
    id: ListenerId,
    hwm: EventReference,
    filters: Vec<ListenerFilter>,
}

enum StreamDispatcherStateChange {
    NewSettings(StreamDispatcherSettings),
    NewListener(ListenerId, Vec<ListenerFilter>, EventReference),
    RemovedListener(ListenerId),
    NewBatchSink(mpsc::Sender<Batch>),
}

struct StreamDispatcherSettings {
    batch_size: usize,
    batch_timeout: Duration,
}

pub struct Batch {
    pub batch_number: u64,
    pub events: Vec<Event>,
    ack_tx: oneshot::Sender<()>,
}
impl Batch {
    pub fn ack(self) {
        let _ = self.ack_tx.send(());
    }
}
