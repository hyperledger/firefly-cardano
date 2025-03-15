use std::marker::PhantomData;

use balius_sdk::{wit, Ack, Params, Worker, WorkerResult, _internal::Handler};
use serde::Deserialize;

/// Parameters to the [WorkerExt::with_tx_submitted_handler] callback.
#[derive(Deserialize)]
pub struct SubmittedTx {
    /// The method which submitted this TX.
    pub method: String,
    /// The hash of the submitted transaction.
    pub hash: String,
}

struct SubmittedTxHandler<F, C>
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

/// Helper methods on the Balius Worker.
pub trait WorkerExt {
    /// Register a callback to run when a transaction has been submitted.
    /// The callback will be passed a [SubmittedTx] describing the transaction.
    fn with_tx_submitted_handler<C, F>(self, func: F) -> Self
    where
        C: TryFrom<wit::Config, Error = balius_sdk::Error> + Send + Sync + 'static,
        F: Fn(C, SubmittedTx) -> WorkerResult<Ack> + Send + Sync + 'static;

    /// Register a callback to run whenever a new UTXO appears on-chain.
    fn with_new_txo_handler(self, handler: impl Handler) -> Self;
}

impl WorkerExt for Worker {
    fn with_tx_submitted_handler<C, F>(self, func: F) -> Self
    where
        C: TryFrom<wit::Config, Error = balius_sdk::Error> + Send + Sync + 'static,
        F: Fn(C, SubmittedTx) -> WorkerResult<Ack> + Send + Sync + 'static,
    {
        self.with_request_handler("__tx_submitted", SubmittedTxHandler::from(func))
    }

    fn with_new_txo_handler(self, handler: impl Handler) -> Self {
        self.with_utxo_handler(
            wit::balius::app::driver::UtxoPattern {
                address: None,
                token: None,
            },
            handler,
        )
    }
}
