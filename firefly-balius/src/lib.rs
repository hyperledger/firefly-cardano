use std::marker::PhantomData;

use balius_sdk::{
    txbuilder::{primitives::TransactionInput, BuildContext, BuildError, InputExpr, UtxoSource},
    wit, Ack, Params, Worker, WorkerResult,
    _internal::Handler,
};
use serde::Deserialize;

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
