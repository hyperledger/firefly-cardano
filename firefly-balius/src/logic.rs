use balius_sdk::txbuilder::{
    primitives::TransactionInput, BuildContext, BuildError, InputExpr, UtxoSource,
};

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
