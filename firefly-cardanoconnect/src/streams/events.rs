use std::{collections::HashMap, time::SystemTime};

use serde_json::json;

use super::{
    blockchain::{ChainListener, ListenerEvent},
    BlockInfo, BlockReference, Event, EventId, EventReference, ListenerFilter, ListenerId,
};

#[derive(Debug)]
pub struct ChainEventStream {
    id: ListenerId,
    filters: Vec<ListenerFilter>,
    sync: ChainListener,
    cache: HashMap<BlockReference, Vec<(EventReference, Event)>>,
}

impl ChainEventStream {
    pub fn new(id: ListenerId, filters: Vec<ListenerFilter>, sync: ChainListener) -> Self {
        Self {
            id,
            filters,
            sync,
            cache: HashMap::new(),
        }
    }

    pub fn try_get_next_event(&mut self, hwm: &EventReference) -> Option<(EventReference, Event)> {
        let mut next_hwm = hwm.clone();
        loop {
            if let Some(result) = self.next_event_in_memory(&next_hwm) {
                return Some(result);
            }
            let (rollbacks, block) = self.try_get_next_block(&next_hwm.block)?;
            let block_ref = block.as_reference();
            if let Some(event) = self.collect_events(rollbacks, block) {
                return Some(event);
            }
            next_hwm = EventReference {
                block: block_ref,
                rollback: false,
                tx_index: None,
                log_index: None,
            }
        }
    }

    pub async fn wait_for_next_event(&mut self, hwm: &EventReference) -> (EventReference, Event) {
        let mut next_hwm = hwm.clone();
        loop {
            if let Some(result) = self.next_event_in_memory(&next_hwm) {
                return result;
            }
            let (rollbacks, block) = self.wait_for_next_block(&next_hwm.block).await;
            let block_ref = block.as_reference();
            if let Some(event) = self.collect_events(rollbacks, block) {
                return event;
            }
            next_hwm = EventReference {
                block: block_ref,
                rollback: false,
                tx_index: None,
                log_index: None,
            }
        }
    }

    fn next_event_in_memory(&mut self, hwm: &EventReference) -> Option<(EventReference, Event)> {
        let cached = self.cache.get(&hwm.block)?;
        if hwm.tx_index.is_none() && hwm.log_index.is_none() {
            // We haven't processed any events from this block yet, so just process the first
            return cached.first().cloned();
        }
        let current_index = cached.iter().position(|(e_ref, _)| e_ref == hwm)?;
        cached.get(current_index + 1).cloned()
    }

    fn try_get_next_block(
        &mut self,
        block_ref: &BlockReference,
    ) -> Option<(Vec<BlockInfo>, BlockInfo)> {
        let mut rollbacks = vec![];
        let mut at = block_ref.clone();
        loop {
            match self.sync.try_get_next_event(&at)? {
                ListenerEvent::Rollback(block) => {
                    at = block.as_reference();
                    rollbacks.push(block);
                }
                ListenerEvent::Process(block) => {
                    return Some((rollbacks, block));
                }
            }
        }
    }

    async fn wait_for_next_block(
        &mut self,
        block_ref: &BlockReference,
    ) -> (Vec<BlockInfo>, BlockInfo) {
        let mut rollbacks = vec![];
        let mut at = block_ref.clone();
        loop {
            match self.sync.wait_for_next_event(&at).await {
                ListenerEvent::Rollback(block) => {
                    at = block.as_reference();
                    rollbacks.push(block);
                }
                ListenerEvent::Process(block) => {
                    return (rollbacks, block);
                }
            }
        }
    }

    fn collect_events(
        &mut self,
        rollbacks: Vec<BlockInfo>,
        block: BlockInfo,
    ) -> Option<(EventReference, Event)> {
        let mut result = None;
        for rollback in rollbacks {
            let Some(forwards) = self.cache.get(&rollback.as_reference()) else {
                continue;
            };
            let backwards: Vec<_> = forwards
                .iter()
                .map(|(_, e)| {
                    let event_ref = EventReference {
                        block: block.as_reference(),
                        rollback: true,
                        tx_index: Some(e.id.transaction_index),
                        log_index: Some(e.id.log_index),
                    };
                    let event = e.clone().into_rollback();
                    (event_ref, event)
                })
                .collect();
            result = result.or(backwards.first().cloned());
            self.cache.insert(rollback.as_reference(), backwards);
        }
        let events = self.collect_forward_tx_events(&block);
        result = result.or(events.first().cloned());
        self.cache.insert(block.as_reference(), events);
        result
    }

    fn collect_forward_tx_events(&self, block: &BlockInfo) -> Vec<(EventReference, Event)> {
        let mut events = vec![];
        for (tx_idx, tx_hash) in block.transaction_hashes.iter().enumerate() {
            let tx_idx = tx_idx as u64;
            if self.matches_tx_filter(tx_hash) {
                let id = EventId {
                    listener_id: self.id.clone(),
                    signature: "TransactionAccepted(string, string, string)".into(),
                    block_hash: block.block_hash.clone(),
                    block_number: block.block_height,
                    transaction_hash: tx_hash.clone(),
                    transaction_index: tx_idx,
                    log_index: 0,
                    timestamp: Some(SystemTime::now()),
                };
                let event = Event {
                    id,
                    data: json!({}),
                };
                let event_ref = EventReference {
                    block: block.as_reference(),
                    rollback: false,
                    tx_index: Some(tx_idx),
                    log_index: Some(0),
                };
                events.push((event_ref, event));
            }
        }
        events
    }

    fn matches_tx_filter(&self, tx_hash: &str) -> bool {
        for filter in &self.filters {
            let ListenerFilter::TransactionId(id) = filter;
            if id == tx_hash || id == "any" {
                return true;
            }
        }
        false
    }
}
