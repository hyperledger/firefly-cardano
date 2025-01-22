use std::collections::{BTreeMap, HashSet};

use balius_sdk::{
    txbuilder::{
        AddressPattern, BuildError, FeeChangeReturn, OutputBuilder, TxBuilder, UtxoPattern,
        UtxoSource,
    },
    Ack, Config, FnHandler, NewTx, Params, Utxo, Worker, WorkerResult,
};
use firefly_balius::{emit_events, kv, CoinSelectionInput, Event, EventData, SubmittedTx, WorkerExt as _};
use pallas_addresses::Address;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendAdaRequest {
    pub from_address: String,
    pub to_address: String,
    pub amount: u64,
}

#[derive(Serialize)]
struct TxSubmittedEventData {
    pub tx_id: String,
}
impl EventData for TxSubmittedEventData {
    fn signature(&self) -> String {
        "TransactionSubmitted(String)".into()        
    }
}

#[derive(Serialize)]
struct TxFinalizedEventData {
    pub tx_id: String,
}
impl EventData for TxFinalizedEventData {
    fn signature(&self) -> String {
        "TransactionFinalized(String)".into()
    }
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

fn handle_utxo(_: Config<()>, utxo: Utxo<()>) -> WorkerResult<Ack> {
    let mut events = vec![];

    // If we see a submitted block on the chain, track that we see it
    let mut submitted_txs: HashSet<String> = kv::get("submitted_txs")?.unwrap_or_default();
    let tx_id = hex::encode(&utxo.tx_hash);
    if submitted_txs.remove(&tx_id) {
        events.push(Event::new(&utxo, &TxSubmittedEventData { tx_id: tx_id.clone() })?);

        kv::set("submitted_txs", &submitted_txs)?;
        let mut pending_txs: BTreeMap<u64, Vec<String>> = kv::get("pending_txs")?.unwrap_or_default();
        pending_txs.entry(utxo.block_height).or_default().push(tx_id.clone());
        kv::set("pending_txs", &pending_txs)?;
    }

    // If any submitted transactions have been waiting on the chain long enough,
    // emit an event that says they've been "finalized"
    let mut pending_txs: BTreeMap<u64, Vec<String>> = kv::get("pending_txs")?.unwrap_or_default();
    let mut txs_processed = false;
    while let Some((&height, _)) = pending_txs.first_key_value() {
        if height > utxo.block_height - 4 {
            break;
        }
        txs_processed = true;
        for tx_id in pending_txs.remove(&height).unwrap() {
            events.push(Event::new(&utxo, &TxFinalizedEventData { tx_id: tx_id.clone() })?);
        }
    }
    if txs_processed {
        kv::set("pending_txs", &pending_txs)?;
    }

    emit_events(events)?;
    Ok(Ack)
}

#[balius_sdk::main]
fn main() -> Worker {
    Worker::new()
        .with_request_handler("send_ada", FnHandler::from(send_ada))
        .with_tx_submitted_handler(handle_submit)
        .with_utxo_handler(
            balius_sdk::wit::balius::app::driver::UtxoPattern {
                address: None,
                token: None,
            },
            FnHandler::from(handle_utxo),
        )
}
