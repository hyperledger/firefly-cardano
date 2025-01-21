use std::collections::HashSet;

use balius_sdk::{
    txbuilder::{
        AddressPattern, BuildError, FeeChangeReturn, OutputBuilder, TxBuilder, UtxoPattern,
        UtxoSource,
    },
    Ack, Config, FnHandler, NewTx, Params, Worker, WorkerResult,
};
use firefly_balius::{kv, CoinSelectionInput, SubmittedTx, WorkerExt};
use pallas_addresses::Address;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendAdaRequest {
    pub from_address: String,
    pub to_address: String,
    pub amount: u64,
}

fn send_ada(_: Config<()>, req: Params<SendAdaRequest>) -> WorkerResult<NewTx> {
    let mut tx = TxBuilder::new();

    let from_address =
        Address::from_bech32(&req.from_address).map_err(|_| BuildError::MalformedAddress)?;

    let address_source = UtxoSource::Search(UtxoPattern {
        address: Some(AddressPattern {
            exact_address: from_address.to_vec(),
        }),
        ..UtxoPattern::default()
    });

    tx = tx
        .with_input(CoinSelectionInput(address_source.clone(), req.amount))
        .with_output(
            OutputBuilder::new()
                .address(req.to_address.clone())
                .with_value(req.amount),
        )
        .with_output(FeeChangeReturn(address_source));

    Ok(NewTx(Box::new(tx)))
}

fn handle_submit(_: Config<()>, tx: SubmittedTx) -> WorkerResult<Ack> {
    // A TX which we produced before was submitted to the blockchain.
    // It hasn't reached the chain yet, but track it so we can react when it does.
    let mut submitted_txs: HashSet<String> = kv::get("submitted_txs")?.unwrap_or_default();
    submitted_txs.insert(tx.hash);
    kv::set("submitted_txs", &submitted_txs)?;

    Ok(Ack)
}

#[balius_sdk::main]
fn main() -> Worker {
    Worker::new()
        .with_request_handler("send_ada", FnHandler::from(send_ada))
        .with_tx_submitted_handler(handle_submit)
}
