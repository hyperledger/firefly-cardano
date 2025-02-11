use std::{collections::HashMap, marker::PhantomData};

use balius_sdk::{
    txbuilder::{primitives::TransactionInput, BuildContext, BuildError, InputExpr, UtxoSource},
    wit, Ack, Params, Utxo, Worker, WorkerResult,
    _internal::Handler,
};
use serde::{Deserialize, Serialize};

pub struct CoinSelectionInput(pub UtxoSource, pub u64);

// TODO: this is a naive approach to coin selection,
// balius can probably help with a better one
impl InputExpr for CoinSelectionInput {
    fn eval(&self, ctx: &BuildContext) -> Result<Vec<TransactionInput>, BuildError> {
        let utxos = self.0.resolve(ctx)?;
        // If we know the fee, add it to the target amount.
        // If not, overestimate the fee so we pick at least as many TXOs as needed.
        let target_lovelace = self.1
            + if ctx.estimated_fee == 0 {
                2_000_000
            } else {
                ctx.estimated_fee
            };

        let mut inputs = vec![];
        let mut lovelace_so_far = 0;
        let mut pairs: Vec<_> = utxos.iter().collect();
        pairs.sort_by_key(|(ref_, _)| (ref_.hash, ref_.index));
        for (ref_, txo) in pairs {
            let coin = txo.value().coin();
            if coin == 0 {
                continue;
            }
            inputs.push(TransactionInput {
                transaction_id: ref_.hash,
                index: ref_.index,
            });
            lovelace_so_far += coin;
            if lovelace_so_far >= target_lovelace {
                break;
            }
        }
        if lovelace_so_far >= target_lovelace {
            Ok(inputs)
        } else {
            Err(BuildError::OutputsTooHigh)
        }
    }
}

#[derive(Deserialize)]
pub struct SubmittedTx {
    pub method: String,
    pub hash: String,
}

pub struct SubmittedTxHandler<F, C>
where
    F: Fn(C, SubmittedTx) -> WorkerResult<Ack> + 'static,
    C: TryFrom<wit::Config>,
{
    func: F,
    phantom: PhantomData<C>,
}

impl<F, C> From<F> for SubmittedTxHandler<F, C>
where
    F: Fn(C, SubmittedTx) -> WorkerResult<Ack> + 'static,
    C: TryFrom<wit::Config>,
{
    fn from(func: F) -> Self {
        Self {
            func,
            phantom: PhantomData,
        }
    }
}

impl<F, C> Handler for SubmittedTxHandler<F, C>
where
    F: Fn(C, SubmittedTx) -> WorkerResult<Ack> + Send + Sync + 'static,
    C: TryFrom<wit::Config, Error = balius_sdk::Error> + Send + Sync + 'static,
{
    fn handle(
        &self,
        config: wit::Config,
        event: wit::Event,
    ) -> Result<wit::Response, wit::HandleError> {
        let config: C = config.try_into()?;
        let event: Params<SubmittedTx> = event.try_into()?;
        let response = (self.func)(config, event.0)?;
        Ok(response.try_into()?)
    }
}

pub trait WorkerExt {
    fn with_tx_submitted_handler<C, F>(self, func: F) -> Self
    where
        C: TryFrom<wit::Config, Error = balius_sdk::Error> + Send + Sync + 'static,
        F: Fn(C, SubmittedTx) -> WorkerResult<Ack> + Send + Sync + 'static;
}

impl WorkerExt for Worker {
    fn with_tx_submitted_handler<C, F>(self, func: F) -> Self
    where
        C: TryFrom<wit::Config, Error = balius_sdk::Error> + Send + Sync + 'static,
        F: Fn(C, SubmittedTx) -> WorkerResult<Ack> + Send + Sync + 'static,
    {
        self.with_request_handler("__tx_submitted", SubmittedTxHandler::from(func))
    }
}

pub mod kv {
    use balius_sdk::{wit::balius::app::kv, WorkerResult};
    use serde::{Deserialize, Serialize};

    pub fn get<D: for<'a> Deserialize<'a>>(key: &str) -> WorkerResult<Option<D>> {
        match kv::get_value(key) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(kv::KvError::NotFound(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub fn set<S: Serialize>(key: &str, value: &S) -> WorkerResult<()> {
        kv::set_value(key, &serde_json::to_vec(value)?)?;
        Ok(())
    }
}

pub trait EventData: Serialize {
    fn signature(&self) -> String;
}

#[derive(Serialize, Deserialize)]
pub struct Event {
    pub block_hash: Vec<u8>,
    pub tx_hash: Vec<u8>,
    pub signature: String,
    pub data: serde_json::Value,
}

impl Event {
    pub fn new<D, E: EventData>(utxo: &Utxo<D>, data: &E) -> WorkerResult<Self> {
        Ok(Self {
            block_hash: utxo.block_hash.clone(),
            tx_hash: utxo.tx_hash.clone(),
            signature: data.signature(),
            data: serde_json::to_value(data)?,
        })
    }
}

pub fn emit_events(events: Vec<Event>) -> WorkerResult<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut block_events: HashMap<String, Vec<Event>> = HashMap::new();
    for event in events {
        let block_hash = hex::encode(&event.block_hash);
        block_events.entry(block_hash).or_default().push(event);
    }

    for (block, mut events) in block_events {
        let events_key = format!("__events_{block}");
        let mut all_events: Vec<Event> = kv::get(&events_key)?.unwrap_or_default();
        all_events.append(&mut events);
        kv::set(&events_key, &all_events)?;
    }

    Ok(())
}
