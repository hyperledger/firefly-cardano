use std::{collections::HashMap, time::SystemTime};

use serde_json::json;

use crate::contracts::ContractListener;

use super::{
    blockchain::{ChainListener, ListenerEvent},
    BlockInfo, BlockReference, Event, EventId, EventReference, ListenerFilter, ListenerId,
};

pub struct ChainEventStream {
    id: ListenerId,
    filters: Vec<ListenerFilter>,
    sync: ChainListener,
    contract: ContractListener,
    cache: HashMap<BlockReference, Vec<(EventReference, Event)>>,
}

impl ChainEventStream {
    pub fn new(
        id: ListenerId,
        filters: Vec<ListenerFilter>,
        sync: ChainListener,
        contract: ContractListener,
    ) -> Self {
        Self {
            id,
            filters,
            sync,
            contract,
            cache: HashMap::new(),
        }
    }

    pub async fn try_get_next_event(
        &mut self,
        hwm: &EventReference,
    ) -> Option<(EventReference, Event)> {
        let mut next_hwm = hwm.clone();
        loop {
            if let Some(result) = self.next_event_in_memory(&next_hwm) {
                return Some(result);
            }
            let (rollbacks, block) = self.try_get_next_block(&next_hwm.block)?;
            let block_ref = block.as_reference();
            if let Some(event) = self.collect_events(rollbacks, block).await {
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
            if let Some(event) = self.collect_events(rollbacks, block).await {
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

    async fn collect_events(
        &mut self,
        rollbacks: Vec<BlockInfo>,
        block: BlockInfo,
    ) -> Option<(EventReference, Event)> {
        if self.cache.contains_key(&block.as_reference()) {
            // we already gathered these events
            return None;
        }
        self.contract.gather_events(&rollbacks, &block).await;
        let mut result = None;
        for rollback in rollbacks {
            let rollback_ref = rollback.as_reference();
            let mut backwards = self.collect_backward_tx_events(&rollback);
            for event in self.contract.events_for(&rollback_ref).await {
                let event_ref = EventReference {
                    block: rollback_ref.clone(),
                    rollback: true,
                    tx_index: Some(event.id.transaction_index),
                    log_index: Some(event.id.log_index),
                };
                backwards.push((event_ref, event));
            }
            result = result.or(backwards.first().cloned());
            self.cache.insert(rollback_ref, backwards);
        }
        let block_ref = block.as_reference();
        let mut events = self.collect_forward_tx_events(&block);
        for event in self.contract.events_for(&block_ref).await {
            let event_ref = EventReference {
                block: block_ref.clone(),
                rollback: false,
                tx_index: Some(event.id.transaction_index),
                log_index: Some(event.id.log_index),
            };
            events.push((event_ref, event));
        }
        result = result.or(events.first().cloned());
        self.cache.insert(block_ref, events);
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

    fn collect_backward_tx_events(&self, block: &BlockInfo) -> Vec<(EventReference, Event)> {
        let mut events = vec![];
        for (tx_idx, tx_hash) in block.transaction_hashes.iter().enumerate().rev() {
            let tx_idx = tx_idx as u64;
            if self.matches_tx_filter(tx_hash) {
                let id = EventId {
                    listener_id: self.id.clone(),
                    signature: "TransactionRolledBack(string, string, string)".into(),
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
            let ListenerFilter::TransactionId(id) = filter else {
                continue;
            };
            if id == tx_hash || id == "any" {
                return true;
            }
        }
        false
    }
}
