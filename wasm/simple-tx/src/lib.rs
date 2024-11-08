use balius_sdk::{
    txbuilder::{FeeChangeReturn, Hash, OutputBuilder, TxBuilder, TxoRef},
    wit::balius::app::ledger::{AddressPattern, UtxoPattern},
    Config, FnHandler, NewTx, Params, Worker, WorkerResult,
};
use pallas_addresses::Address;
use pallas_traverse::MultiEraOutput;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferRequest {
    pub from_address: String,
    pub to_address: String,
    pub amount: u64,
}

// TODO: this is a naive approach to coin selection,
// balius can probably help with a better one
fn select_inputs(from_address: &Address, amount: u64) -> WorkerResult<Vec<TxoRef>> {
    let from_pattern = UtxoPattern {
        address: Some(AddressPattern {
            exact_address: from_address.to_vec(),
        }),
        asset: None,
    };

    let mut refs = vec![];
    let mut total_so_far = 0;
    let mut utxo_page = Some(balius_sdk::wit::balius::app::ledger::search_utxos(
        &from_pattern,
        None,
        16,
    )?);
    while let Some(page) = utxo_page.take() {
        for utxo in page.utxos {
            let body = MultiEraOutput::decode(pallas_traverse::Era::Conway, &utxo.body)
                .map_err(|e| balius_sdk::Error::Internal(e.to_string()))?;
            let value = body.value();
            if value.coin() == 0 {
                continue;
            }

            let hash = Hash::<32>::from(utxo.ref_.tx_hash.as_slice());
            let ref_ = TxoRef::new(hash, utxo.ref_.tx_index as u64);
            refs.push(ref_);

            total_so_far += value.coin();
            if total_so_far >= amount {
                return Ok(refs);
            }
        }
        if let Some(token) = page.next_token {
            utxo_page = Some(balius_sdk::wit::balius::app::ledger::search_utxos(
                &from_pattern,
                Some(&token),
                16,
            )?)
        }
    }
    Err(balius_sdk::Error::Internal(format!(
        "not enough funds (need {}, have {})",
        amount, total_so_far
    )))
}

fn send_ada(_: Config<()>, req: Params<TransferRequest>) -> WorkerResult<NewTx> {
    let mut tx = TxBuilder::new();

    // TODO: return an error if this address is invalid
    let from_address = Address::from_bech32(&req.from_address).unwrap();
    let to_address = Address::from_bech32(&req.to_address).unwrap();

    let refs = select_inputs(&from_address, req.amount)?;
    for input in &refs {
        tx = tx.with_input(input.clone());
    }

    tx = tx
        .with_output(
            OutputBuilder::new()
                .address(to_address)
                .with_value(req.amount),
        )
        .with_output(FeeChangeReturn(balius_sdk::txbuilder::UtxoSource::Refs(
            refs,
        )));

    Ok(NewTx(Box::new(tx)))
}

#[balius_sdk::main]
fn main() -> Worker {
    Worker::new().with_request_handler("send_ada", FnHandler::from(send_ada))
}
