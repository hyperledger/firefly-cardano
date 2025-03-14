use std::collections::HashMap;

use balius_sdk::txbuilder::{
    primitives::TransactionInput, BuildContext, BuildError, InputExpr, UtxoSource, Value, ValueExpr
};

/// When used in a transaction, adds a minimal set of UTXOs with at least the given value (plus fees).
pub struct CoinSelectionInput<V: ValueExpr>(pub UtxoSource, pub V);

// TODO: this is a naive approach to coin selection,
// balius can probably help with a better one
impl<V: ValueExpr> InputExpr for CoinSelectionInput<V> {
    fn eval(&self, ctx: &BuildContext) -> Result<Vec<TransactionInput>, BuildError> {
        let utxos = self.0.resolve(ctx)?;
        let target_amount = self.1.eval(ctx)?;
        
        // If we know the fee, add it to the target amount.
        // If not, overestimate the fee so we pick at least as many TXOs as needed.
        let fee = if ctx.estimated_fee == 0 {
                2_000_000
            } else {
                ctx.estimated_fee
            };
        let (target_lovelace, mut target_assets) = match target_amount {
            Value::Coin(c) => (c + fee, HashMap::new()),
            Value::Multiasset(c, assets) => {
                let asset_map = assets.into_iter()
                    .map(|(policy_id, assets)| (policy_id, assets.into_iter().map(|(name, coin)| (name.to_vec(), u64::from(coin))).collect::<HashMap<_, _>>()))
                    .collect();
                (c + fee, asset_map)
            }
        };

        let mut inputs = vec![];
        let mut lovelace_so_far = 0;
        let mut pairs: Vec<_> = utxos.iter().collect();
        pairs.sort_by_key(|(ref_, _)| (ref_.hash, ref_.index));
        for (ref_, txo) in pairs {
            let coin = txo.value().coin();
            let mut useful = coin != 0 && lovelace_so_far < target_lovelace;
            for policy_assets in txo.value().assets() {
                let Some(target_policy_assets) = target_assets.get_mut(policy_assets.policy()) else {
                    continue;
                };
                for asset in policy_assets.assets() {
                    let Some(target_asset) = target_policy_assets.get_mut(asset.name()) else {
                        continue;
                    };
                    let Some(amount) = asset.output_coin() else {
                        continue;
                    };
                    if amount == 0 {
                        continue;
                    }
                    useful = true;
                    if amount < *target_asset {
                        *target_asset -= amount;
                    } else {
                        target_policy_assets.remove(asset.name());
                    }
                }
                if target_policy_assets.is_empty() {
                    target_assets.remove(policy_assets.policy());
                }
            }
            if !useful {
                continue;
            }
            inputs.push(TransactionInput {
                transaction_id: ref_.hash,
                index: ref_.index,
            });
            lovelace_so_far += coin;
            if lovelace_so_far >= target_lovelace && target_assets.is_empty() {
                break;
            }
        }
        if lovelace_so_far >= target_lovelace && target_assets.is_empty() {
            Ok(inputs)
        } else {
            Err(BuildError::OutputsTooHigh)
        }
    }
}
