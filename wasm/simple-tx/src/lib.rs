use balius_sdk::{
    txbuilder::{
        primitives::TransactionInput, AddressPattern, BuildContext, BuildError, FeeChangeReturn,
        InputExpr, OutputBuilder, TxBuilder, UtxoPattern, UtxoSource,
    },
    Config, FnHandler, NewTx, Params, Worker, WorkerResult,
};
use pallas_addresses::Address;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferRequest {
    pub from_address: String,
    pub to_address: String,
    pub amount: u64,
}

struct CoinSelectionInput(UtxoSource, u64);

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

fn send_ada(_: Config<()>, req: Params<TransferRequest>) -> WorkerResult<NewTx> {
    let mut tx = TxBuilder::new();

    let from_address = Address::from_bech32(&req.from_address).map_err(|_| BuildError::MalformedAddress)?;
    let to_address = Address::from_bech32(&req.to_address).map_err(|_| BuildError::MalformedAddress)?;

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
                .address(to_address)
                .with_value(req.amount),
        )
        .with_output(FeeChangeReturn(address_source));

    Ok(NewTx(Box::new(tx)))
}

#[balius_sdk::main]
fn main() -> Worker {
    Worker::new().with_request_handler("send_ada", FnHandler::from(send_ada))
}
