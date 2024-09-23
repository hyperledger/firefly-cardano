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
    blockchain::BlockchainClient,
    persistence::Persistence,
    streams::{blockchain::ListenerEvent, EventData, EventId},
};

use super::{
    blockchain::{ChainListener, DataSource},
    BlockInfo, BlockReference, Event, EventReference, Listener, ListenerFilter, ListenerId, Stream,
    StreamCheckpoint, StreamId,
};

#[derive(Clone)]
pub struct Multiplexer {
    dispatchers: Arc<DashMap<StreamId, StreamDispatcher>>,
    stream_ids_by_topic: Arc<DashMap<String, StreamId>>,
    persistence: Arc<Persistence>,
    data_source: Arc<DataSource>,
}

impl Multiplexer {
    pub async fn new(
        persistence: Arc<Persistence>,
        blockchain: Arc<BlockchainClient>,
    ) -> Result<Self> {
        let data_source = Arc::new(DataSource::new(blockchain));

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

    pub async fn handle_listener_write(
        &self,
        listener: &Listener,
        from_block: Option<BlockReference>,
    ) -> Result<()> {
        let sync = self
            .data_source
            .listen(listener.id.clone(), from_block.as_ref())
            .await?;
        let block = from_block.unwrap_or(sync.get_tip());
        let hwm = EventReference {
            block,
            rollback: false,
            tx_index: None,
            log_index: None,
        };

        let Some(dispatcher) = self.dispatchers.get(&listener.stream_id) else {
            bail!("new listener created for stream we haven't heard of");
        };
        dispatcher.add_listener(listener, sync, hwm).await
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
        let mut hwms = BTreeMap::new();
        for listener in all_listeners {
            let hwm = old_hwms.get(&listener.id).cloned().unwrap_or_default();
            let stream = data_source
                .listen(listener.id.clone(), Some(&hwm.block))
                .await?;

            hwms.insert(listener.id.clone(), hwm);
            listeners.insert(
                listener.id.clone(),
                ListenerState {
                    id: listener.id,
                    sync: stream,
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
            hwms,
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

    pub async fn add_listener(
        &self,
        listener: &Listener,
        sync: ChainListener,
        hwm: EventReference,
    ) -> Result<()> {
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewListener(
                listener.id.clone(),
                listener.filters.clone(),
                sync,
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
    hwms: BTreeMap<ListenerId, EventReference>,
    persistence: Arc<Persistence>,
}

impl StreamDispatcherWorker {
    async fn run(mut self, mut state_change_source: mpsc::Receiver<StreamDispatcherStateChange>) {
        let mut batch_sink = None;
        loop {
            let (batch, ack_rx, hwms) = select! {
                batch = self.build_batch(), if batch_sink.is_some() && !self.listeners.is_empty() => batch,
                Some(change) = state_change_source.recv() => {
                    match change {
                        StreamDispatcherStateChange::NewSettings(settings) => {
                            self.batch_size = settings.batch_size;
                            self.batch_timeout = settings.batch_timeout;
                        }
                        StreamDispatcherStateChange::NewListener(listener_id, filters, sync, hwm) => {
                            self.listeners.insert(listener_id.clone(), ListenerState {
                                id: listener_id.clone(),
                                sync,
                                filters,
                            });
                            self.hwms.insert(listener_id, hwm);
                        }
                        StreamDispatcherStateChange::RemovedListener(listener_id) => {
                            self.listeners.remove(&listener_id);
                            self.hwms.remove(&listener_id);
                        }
                        StreamDispatcherStateChange::NewBatchSink(sink) => {
                            batch_sink = Some(sink);
                        }
                    }
                    continue;
                }
                else => {
                    warn!("stream dispatch worker is shutting down");
                    return;
                }
            };
            let sink = batch_sink.as_ref().unwrap();
            if let Err(err) = sink.send(batch).await {
                warn!(%self.stream_id, self.batch_number, "could not send batch: {err}");
                batch_sink = None;
            }
            match ack_rx.await {
                Ok(()) => {
                    self.hwms = hwms;
                    self.persistence
                        .write_checkpoint(&StreamCheckpoint {
                            stream_id: self.stream_id.clone(),
                            listeners: self.hwms.clone(),
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
        BTreeMap<ListenerId, EventReference>,
    ) {
        let mut hwms = self.hwms.clone();

        loop {
            let batch_timeout_at = time::sleep(self.batch_timeout);
            tokio::pin!(batch_timeout_at);

            let mut events = vec![];
            select! {
                () = &mut batch_timeout_at => {}
                () = self.collect_events(&mut hwms, &mut events) => {}
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

    async fn collect_events(
        &mut self,
        hwms: &mut BTreeMap<ListenerId, EventReference>,
        events: &mut Vec<Event>,
    ) {
        assert!(
            !self.listeners.is_empty(),
            "no listeners to produce events!"
        );
        loop {
            // find the next event to process
            let mut sync_events = vec![];
            for listener in self.listeners.values_mut() {
                let hwm = hwms.get(&listener.id).unwrap();
                if let Some(sync_event) = listener.sync.get_event(&hwm.block) {
                    sync_events.push((listener.id.clone(), hwm.block.clone(), sync_event));
                }
            }
            let (listener_id, block_ref, sync_event) = sync_events
                .into_iter()
                .max_by(|l, r| Self::compare_stream_event_priority(&l.2, &r.2))
                .expect("no listeners had anything to do on this event!");

            // process it, updating our high water marks as we go
            match &sync_event {
                ListenerEvent::Process(block) => {
                    self.collect_forward_events(hwms, events, block);
                }
                ListenerEvent::Rollback(block) => {
                    self.collect_backward_events(hwms, events, block);
                }
            }

            // if we still need to fill this batch, update our high water marks to look at the next block
            if events.len() >= self.batch_size {
                break;
            }
            let listener = self.listeners.get_mut(&listener_id).unwrap();
            let (update_to, rollback) = match sync_event {
                ListenerEvent::Process(_) => {
                    let update_to = listener.sync.get_next(&block_ref).await;
                    (update_to, false)
                }
                ListenerEvent::Rollback(_) => {
                    let update_to = Some(listener.sync.get_next_rollback(&block_ref));
                    (update_to, true)
                }
            };
            if let Some(next_ref) = update_to {
                for hwm in hwms.values_mut() {
                    if hwm.block == block_ref {
                        *hwm = EventReference {
                            block: next_ref.clone(),
                            rollback,
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
        hwms: &mut BTreeMap<ListenerId, EventReference>,
        events: &mut Vec<Event>,
        block: &BlockInfo,
    ) {
        for (id, hwm) in hwms.iter_mut() {
            if !Self::matches_ref(block, &hwm.block) {
                continue;
            }
            let listener = self.listeners.get(id).unwrap();
            for event in self.find_forward_events(listener, hwm, block) {
                *hwm = EventReference {
                    block: hwm.block.clone(),
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
        hwms: &mut BTreeMap<ListenerId, EventReference>,
        events: &mut Vec<Event>,
        block: &BlockInfo,
    ) {
        for (id, hwm) in hwms.iter_mut().rev() {
            if !Self::matches_ref(block, &hwm.block) {
                continue;
            }
            let listener = self.listeners.get(id).unwrap();
            for event in self.find_backward_events(listener, hwm, block) {
                *hwm = EventReference {
                    block: hwm.block.clone(),
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
            BlockReference::Origin => block.block_slot.is_none() && block.block_height.is_none(),
            BlockReference::Point(slot, hash) => {
                block.block_slot == *slot && block.block_hash == *hash
            }
        }
    }

    fn find_forward_events(
        &self,
        listener: &ListenerState,
        hwm: &EventReference,
        block: &BlockInfo,
    ) -> Vec<Event> {
        if hwm.rollback {
            // If we were rolling back before, and now we've started rolling forward,
            // we rolled back onto a block we've already completely processed.
            return vec![];
        }
        let mut events = self.find_events(listener, block);
        events.retain(|e| {
            // throw out any events which came at or before our high-water mark
            let tx_cmp = hwm
                .tx_index
                .as_ref()
                .map(|tx| e.id.transaction_index.cmp(tx));
            let log_cmp = hwm.log_index.as_ref().map(|log| e.id.log_index.cmp(log));
            match (tx_cmp, log_cmp) {
                (Some(Ordering::Less), _) => false,
                (Some(Ordering::Equal), Some(Ordering::Less | Ordering::Equal)) => false,
                (_, _) => true,
            }
        });
        events
    }

    fn find_backward_events(
        &self,
        listener: &ListenerState,
        hwm: &EventReference,
        block: &BlockInfo,
    ) -> Vec<Event> {
        self.find_events(listener, block)
            .into_iter()
            .rev()
            .filter(|e| {
                let tx_cmp = hwm
                    .tx_index
                    .as_ref()
                    .map(|tx| e.id.transaction_index.cmp(tx));
                let log_cmp = hwm.log_index.as_ref().map(|log| e.id.log_index.cmp(log));
                if hwm.rollback {
                    // if we were rolling back already, throw out events we've already rolled back past
                    match (tx_cmp, log_cmp) {
                        (Some(Ordering::Greater), _) => false,
                        (Some(Ordering::Equal), Some(Ordering::Greater | Ordering::Equal)) => false,
                        (_, _) => true,
                    }
                } else {
                    // otherwise, just keep any events that rolling forward would have kept
                    match (tx_cmp, log_cmp) {
                        (Some(Ordering::Less), _) => true,
                        (Some(Ordering::Equal), Some(Ordering::Less | Ordering::Equal)) => true,
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
                        block_number: block.block_height,
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

    // "rollback" events are higher priority than "process" events
    // "process" events are higher priority the older they are
    // "rollback" events are higher priority the newer they are
    fn compare_stream_event_priority(lhs: &ListenerEvent, rhs: &ListenerEvent) -> Ordering {
        match (lhs, rhs) {
            (ListenerEvent::Process(_), ListenerEvent::Rollback(_)) => Ordering::Less,
            (ListenerEvent::Rollback(_), ListenerEvent::Process(_)) => Ordering::Greater,
            (ListenerEvent::Process(l), ListenerEvent::Process(r)) => {
                r.block_slot.cmp(&l.block_slot)
            }
            (ListenerEvent::Rollback(l), ListenerEvent::Rollback(r)) => {
                l.block_slot.cmp(&r.block_slot)
            }
        }
    }

    fn matches_filter(tx: &str, filter: &ListenerFilter) -> bool {
        match filter {
            ListenerFilter::TransactionId(id) => id == tx || id == "any",
        }
    }
}

#[derive(Debug)]
struct ListenerState {
    id: ListenerId,
    sync: ChainListener,
    filters: Vec<ListenerFilter>,
}

enum StreamDispatcherStateChange {
    NewSettings(StreamDispatcherSettings),
    NewListener(
        ListenerId,
        Vec<ListenerFilter>,
        ChainListener,
        EventReference,
    ),
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
