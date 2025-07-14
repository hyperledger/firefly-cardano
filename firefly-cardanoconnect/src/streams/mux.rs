use std::{cmp::Ordering, collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use dashmap::{DashMap, Entry};
use firefly_server::apitypes::ToAnyhow;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::{
    select,
    sync::{mpsc, oneshot, watch},
    time,
};
use tracing::warn;

use crate::{
    blockchain::BlockchainClient,
    contracts::{ContractListener, ContractManager},
    operations::{Operation, OperationUpdateId},
    persistence::Persistence,
};

use super::{
    BlockReference, ContractEvent, EventReference, Listener, ListenerFilter, ListenerId, Stream,
    StreamCheckpoint, StreamId,
    blockchain::{ChainListener, DataSource},
    events::ChainEventStream,
};

#[derive(Clone)]
pub struct Multiplexer {
    dispatchers: Arc<DashMap<StreamId, StreamDispatcher>>,
    stream_ids_by_topic: Arc<DashMap<String, StreamId>>,
    operation_update_sink: watch::Sender<Option<OperationUpdateId>>,
    contracts: Arc<ContractManager>,
    persistence: Arc<dyn Persistence>,
    data_source: Arc<DataSource>,
}

impl Multiplexer {
    pub async fn new(
        blockchain: Arc<BlockchainClient>,
        contracts: Arc<ContractManager>,
        persistence: Arc<dyn Persistence>,
        operation_update_sink: watch::Sender<Option<OperationUpdateId>>,
    ) -> Result<Self> {
        let data_source = Arc::new(DataSource::new(blockchain, persistence.clone()));

        let dispatchers = DashMap::new();
        let stream_ids_by_topic = DashMap::new();
        for stream in persistence.list_streams(None, None).await? {
            let topic = stream.name.clone();
            stream_ids_by_topic.insert(topic.clone(), stream.id.clone());

            let dispatcher = StreamDispatcher::new(
                &stream,
                contracts.clone(),
                persistence.clone(),
                data_source.clone(),
                operation_update_sink.subscribe(),
            )
            .await?;
            dispatchers.insert(stream.id, dispatcher);
        }
        Ok(Self {
            dispatchers: Arc::new(dispatchers),
            stream_ids_by_topic: Arc::new(stream_ids_by_topic),
            operation_update_sink,
            contracts,
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
                        self.contracts.clone(),
                        self.persistence.clone(),
                        self.data_source.clone(),
                        self.operation_update_sink.subscribe(),
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
        let mut sync = self
            .data_source
            .listen(listener.id.clone(), from_block.as_ref());
        let block = from_block.unwrap_or(sync.get_tip().await?);
        let contract = self.contracts.listen(listener).await;
        let hwm = EventReference {
            block,
            rollback: false,
            tx_index: None,
            log_index: None,
        };

        let Some(dispatcher) = self.dispatchers.get(&listener.stream_id) else {
            bail!("new listener created for stream we haven't heard of");
        };
        dispatcher.add_listener(listener, sync, contract, hwm).await
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

    pub async fn subscribe(&self, topic: &str) -> Result<StreamSubscription> {
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
        contracts: Arc<ContractManager>,
        persistence: Arc<dyn Persistence>,
        data_source: Arc<DataSource>,
        operation_update_source: watch::Receiver<Option<OperationUpdateId>>,
    ) -> Result<Self> {
        let (state_change_sink, state_change_source) = mpsc::channel(16);

        let all_listeners = persistence
            .list_listeners(&stream.id, None, None)
            .await
            .to_anyhow()?;
        let checkpoint = persistence.read_checkpoint(&stream.id).await.to_anyhow()?;
        let last_operation_id = checkpoint
            .as_ref()
            .and_then(|c| c.last_operation_id.clone());
        let old_hwms = checkpoint.map(|cp| cp.listeners).unwrap_or_default();

        let stream = stream.clone();
        tokio::spawn(async move {
            let mut listeners = BTreeMap::new();
            let mut hwms = BTreeMap::new();
            for listener in all_listeners {
                let hwm = old_hwms.get(&listener.id).cloned().unwrap_or_default();
                let sync = data_source.listen(listener.id.clone(), Some(&hwm.block));
                let contract = contracts.listen(&listener).await;

                hwms.insert(listener.id.clone(), hwm);
                listeners.insert(
                    listener.id.clone(),
                    ListenerState {
                        id: listener.id.clone(),
                        stream: ChainEventStream::new(
                            listener.id,
                            listener.filters,
                            sync,
                            contract,
                        ),
                    },
                );
            }

            let worker = StreamDispatcherWorker {
                stream_id: stream.id,
                batch_size: stream.batch_size,
                batch_timeout: stream.batch_timeout,
                batch_number: 1,
                listeners,
                last_operation_id,
                hwms,
                persistence,
            };
            worker
                .run(state_change_source, operation_update_source)
                .await;
        });
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
        contract: ContractListener,
        hwm: EventReference,
    ) -> Result<()> {
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewListener(
                listener.id.clone(),
                listener.filters.clone(),
                sync,
                contract,
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

    pub async fn subscribe(&self) -> Result<StreamSubscription> {
        let (batch_sink, batch_source) = mpsc::channel(1);
        self.state_change_sink
            .send(StreamDispatcherStateChange::NewBatchSink(batch_sink))
            .await
            .context("could not subscribe to stream")?;
        Ok(StreamSubscription {
            batch_receiver: batch_source,
        })
    }
}

struct StreamDispatcherWorker {
    stream_id: StreamId,
    batch_size: usize,
    batch_timeout: Duration,
    batch_number: u64,
    listeners: BTreeMap<ListenerId, ListenerState>,
    last_operation_id: Option<OperationUpdateId>,
    hwms: BTreeMap<ListenerId, EventReference>,
    persistence: Arc<dyn Persistence>,
}

impl StreamDispatcherWorker {
    async fn run(
        mut self,
        mut state_change_source: mpsc::Receiver<StreamDispatcherStateChange>,
        mut operation_update_source: watch::Receiver<Option<OperationUpdateId>>,
    ) {
        let mut batch_sink = None;
        loop {
            let (batch, ack_rx, last_op_id, hwms) = select! {
                batch = self.build_batch(&mut operation_update_source), if batch_sink.is_some() => batch,
                Some(change) = state_change_source.recv() => {
                    match change {
                        StreamDispatcherStateChange::NewSettings(settings) => {
                            self.batch_size = settings.batch_size;
                            self.batch_timeout = settings.batch_timeout;
                        }
                        StreamDispatcherStateChange::NewListener(listener_id, filters, sync, contract, hwm) => {
                            self.listeners.insert(listener_id.clone(), ListenerState {
                                id: listener_id.clone(),
                                stream: ChainEventStream::new(listener_id.clone(), filters, sync, contract),
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
                    self.last_operation_id = last_op_id;
                    self.hwms = hwms;
                    self.persistence
                        .write_checkpoint(&StreamCheckpoint {
                            stream_id: self.stream_id.clone(),
                            last_operation_id: self.last_operation_id.clone(),
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
        operation_update_source: &mut watch::Receiver<Option<OperationUpdateId>>,
    ) -> (
        Batch,
        oneshot::Receiver<()>,
        Option<OperationUpdateId>,
        BTreeMap<ListenerId, EventReference>,
    ) {
        let mut last_op_id = self.last_operation_id.clone();
        let mut hwms = self.hwms.clone();

        loop {
            let batch_timeout_at = time::sleep(self.batch_timeout);
            tokio::pin!(batch_timeout_at);

            let mut events = vec![];
            select! {
                () = &mut batch_timeout_at => {}
                () = self.collect_events(&mut last_op_id, &mut hwms, &mut events, operation_update_source) => {}
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
            return (batch, ack_rx, last_op_id, hwms);
        }
    }

    async fn collect_events(
        &mut self,
        last_op: &mut Option<OperationUpdateId>,
        hwms: &mut BTreeMap<ListenerId, EventReference>,
        events: &mut Vec<BatchEvent>,
        operation_update_source: &mut watch::Receiver<Option<OperationUpdateId>>,
    ) {
        if *operation_update_source.borrow() > *last_op {
            self.collect_operation_events(last_op, events).await;
        }
        while events.len() < self.batch_size {
            select! {
                _ = self.collect_contract_events(hwms, events), if !self.listeners.is_empty() => {}
                Ok(()) = operation_update_source.changed() => {
                    self.collect_operation_events(last_op, events).await;
                }
            }
        }
    }

    async fn collect_operation_events(
        &self,
        last_op: &mut Option<OperationUpdateId>,
        events: &mut Vec<BatchEvent>,
    ) {
        let Ok(updates) = self
            .persistence
            .list_operation_updates(last_op.as_ref(), self.batch_size - events.len())
            .await
        else {
            return;
        };
        for update in updates {
            events.push(BatchEvent::Receipt(update.operation));
            last_op.replace(update.update_id);
            if events.len() >= self.batch_size {
                break;
            }
        }
    }

    async fn collect_contract_events(
        &mut self,
        hwms: &mut BTreeMap<ListenerId, EventReference>,
        events: &mut Vec<BatchEvent>,
    ) {
        assert!(
            !self.listeners.is_empty(),
            "no listeners to produce events!"
        );
        while events.len() < self.batch_size {
            let mut new_events = vec![];
            let mut next_event_future = FuturesUnordered::new();
            for listener in self.listeners.values_mut() {
                let hwm = hwms.get(&listener.id).unwrap();
                match listener.stream.try_get_next_event(hwm).await {
                    Some((event_ref, event)) => {
                        // This listener already has an event waiting to surface.
                        new_events.push((listener.id.clone(), event_ref, event));
                    }
                    _ => {
                        // This listener must already be at the tip. We might have to wait a while for more events.
                        next_event_future.push(async move {
                            let (event_ref, event) = listener.stream.wait_for_next_event(hwm).await;
                            (listener.id.clone(), event_ref, event)
                        });
                    }
                }
            }
            // If any listeners already had events waiting for us, choose the highest-priority event
            let next_event = new_events
                .into_iter()
                .max_by(|l, r| Self::compare_event_priority(&l.1, &r.1));
            let (listener_id, event_ref, new_event) = if let Some(event) = next_event {
                event
            } else {
                // Block until some listener has a new event, then use that
                next_event_future.next().await.unwrap()
            };
            drop(next_event_future);
            hwms.insert(listener_id, event_ref);
            events.push(BatchEvent::ContractEvent(new_event));
        }
    }

    // "rollback" events are higher priority than "process" events
    // "process" events are higher priority the older they are
    // "rollback" events are higher priority the newer they are
    fn compare_event_priority(lhs: &EventReference, rhs: &EventReference) -> Ordering {
        if lhs.rollback && !rhs.rollback {
            return Ordering::Less;
        }
        if !lhs.rollback && rhs.rollback {
            return Ordering::Greater;
        }

        let event_ordering = lhs
            .block
            .partial_cmp(&rhs.block)
            .unwrap_or(Ordering::Equal)
            .then(lhs.tx_index.cmp(&rhs.tx_index))
            .then(lhs.log_index.cmp(&rhs.log_index));
        if lhs.rollback && rhs.rollback {
            event_ordering.reverse()
        } else {
            event_ordering
        }
    }
}

struct ListenerState {
    id: ListenerId,
    stream: ChainEventStream,
}

#[allow(clippy::large_enum_variant)]
enum StreamDispatcherStateChange {
    NewSettings(StreamDispatcherSettings),
    NewListener(
        ListenerId,
        Vec<ListenerFilter>,
        ChainListener,
        ContractListener,
        EventReference,
    ),
    RemovedListener(ListenerId),
    NewBatchSink(mpsc::Sender<Batch>),
}

struct StreamDispatcherSettings {
    batch_size: usize,
    batch_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchEvent {
    ContractEvent(ContractEvent),
    Receipt(Operation),
}

pub struct Batch {
    pub batch_number: u64,
    pub events: Vec<BatchEvent>,
    ack_tx: oneshot::Sender<()>,
}
impl Batch {
    pub fn ack(self) {
        let _ = self.ack_tx.send(());
    }
}

pub struct StreamSubscription {
    batch_receiver: mpsc::Receiver<Batch>,
}

impl StreamSubscription {
    pub async fn recv(&mut self) -> Option<Batch> {
        self.batch_receiver.recv().await
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{
        blockchain::BlockchainClient,
        contracts::ContractManager,
        operations::{Operation, OperationId, OperationStatus},
        persistence,
        streams::{
            BatchEvent, BlockReference, Listener, ListenerFilter, ListenerType, Stream,
            mux::Multiplexer,
        },
    };
    use firefly_server::apitypes::ApiError;
    use tokio::sync::watch;

    #[tokio::test]
    async fn should_ack_events() -> Result<(), ApiError> {
        let blockchain = Arc::new(BlockchainClient::mock().await);
        let contracts = Arc::new(ContractManager::none());
        let persistence = persistence::init(&persistence::PersistenceConfig::Mock).await?;
        let operation_update_sink = watch::Sender::new(None);
        let mux = Multiplexer::new(
            blockchain.clone(),
            contracts,
            persistence.clone(),
            operation_update_sink,
        )
        .await?;

        let stream = Stream {
            id: "stream_id".to_string().into(),
            name: "Some Stream".into(),
            batch_size: 5,
            batch_timeout: Duration::from_millis(100),
        };
        persistence.write_stream(&stream).await?;
        mux.handle_stream_write(&stream).await?;

        let listener = Listener {
            id: "listener_id".to_string().into(),
            stream_id: stream.id.clone(),
            name: "Some Listener".into(),
            listener_type: ListenerType::Blocks,
            filters: vec![ListenerFilter::TransactionId("any".into())],
        };
        persistence.write_listener(&listener).await?;
        mux.handle_listener_write(&listener, Some(BlockReference::Origin))
            .await?;

        let mut subscription = mux.subscribe("Some Stream").await?;

        let batch = subscription.recv().await.unwrap();
        assert_eq!(batch.batch_number, 1);
        assert_eq!(batch.events.len(), 5);
        let old_events = batch.events.clone();
        batch.ack();

        let batch = subscription.recv().await.unwrap();
        assert_eq!(batch.batch_number, 2);
        assert_eq!(batch.events.len(), 5);
        assert_ne!(batch.events, old_events);

        Ok(())
    }

    #[tokio::test]
    async fn should_nack_events() -> Result<(), ApiError> {
        let blockchain = Arc::new(BlockchainClient::mock().await);
        let contracts = Arc::new(ContractManager::none());
        let persistence = persistence::init(&persistence::PersistenceConfig::Mock).await?;
        let operation_update_sink = watch::Sender::new(None);
        let mux = Multiplexer::new(
            blockchain.clone(),
            contracts,
            persistence.clone(),
            operation_update_sink,
        )
        .await?;

        let stream = Stream {
            id: "stream_id".to_string().into(),
            name: "Some Stream".into(),
            batch_size: 5,
            batch_timeout: Duration::from_millis(100),
        };
        persistence.write_stream(&stream).await?;
        mux.handle_stream_write(&stream).await?;

        let listener = Listener {
            id: "listener_id".to_string().into(),
            stream_id: stream.id.clone(),
            name: "Some Listener".into(),
            listener_type: ListenerType::Blocks,
            filters: vec![ListenerFilter::TransactionId("any".into())],
        };
        persistence.write_listener(&listener).await?;
        mux.handle_listener_write(&listener, Some(BlockReference::Origin))
            .await?;

        let mut subscription = mux.subscribe("Some Stream").await?;

        let batch = subscription.recv().await.unwrap();
        assert_eq!(batch.batch_number, 1);
        assert_eq!(batch.events.len(), 5);
        let old_events = batch.events.clone();
        drop(batch);

        let batch = subscription.recv().await.unwrap();
        assert_eq!(batch.batch_number, 1);
        assert_eq!(batch.events.len(), 5);
        assert_eq!(batch.events, old_events);

        Ok(())
    }

    #[tokio::test]
    async fn should_surface_operation_updates_without_listeners() -> Result<(), ApiError> {
        let blockchain = Arc::new(BlockchainClient::mock().await);
        let contracts = Arc::new(ContractManager::none());
        let persistence = persistence::init(&persistence::PersistenceConfig::Mock).await?;
        let operation_update_sink = watch::Sender::new(None);
        let mux = Multiplexer::new(
            blockchain.clone(),
            contracts,
            persistence.clone(),
            operation_update_sink.clone(),
        )
        .await?;

        let stream = Stream {
            id: "stream_id".to_string().into(),
            name: "Some Stream".into(),
            batch_size: 5,
            batch_timeout: Duration::from_millis(100),
        };
        persistence.write_stream(&stream).await?;
        mux.handle_stream_write(&stream).await?;

        let mut subscription = mux.subscribe("Some Stream").await?;

        let operation = Operation {
            id: OperationId::from("op".to_string()),
            status: OperationStatus::Pending,
            tx_id: None,
            contract_address: None,
        };

        let update_id = persistence.write_operation(&operation).await?;
        operation_update_sink.send_replace(Some(update_id));

        let batch = subscription.recv().await.unwrap();
        assert_eq!(batch.batch_number, 1);
        assert_eq!(batch.events, vec![BatchEvent::Receipt(operation)]);

        Ok(())
    }
}
