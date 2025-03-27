use anyhow::Result;
use async_trait::async_trait;
use balius_runtime::ledgers::{TxoRef, Utxo, UtxoPage};
use pallas_codec::utils::{CborWrap, TagWrap};
use pallas_network::{
    facades::NodeClient,
    miniprotocols::localstate::{
        self,
        queries_v16::{
            RationalNumber, TransactionInput, TransactionOutput, TxIns, Value, get_current_pparams,
            get_utxo_by_address, get_utxo_by_txin,
        },
    },
};
use pallas_primitives::{Bytes, NonEmptyKeyValuePairs, PositiveCoin, alonzo, conway};
use utxorpc_spec::utxorpc::v1alpha::cardano::{
    CostModel, CostModels, ExPrices, ExUnits, PParams, ProtocolVersion, VotingThresholds,
};

use crate::{blockchain::BaliusLedger, utils::LazyInit};

pub struct NodeToClientLedger {
    client: LazyInit<NodeClient>,
    era: u16,
}

impl NodeToClientLedger {
    pub fn new(client: LazyInit<NodeClient>, era: u16) -> Self {
        Self { client, era }
    }

    async fn query_client(&mut self) -> Result<(&mut localstate::Client, u16)> {
        let client = self.client.get().await;
        let query = client.statequery();
        query.acquire(None).await?;
        Ok((query, self.era))
    }
}

#[async_trait]
impl BaliusLedger for NodeToClientLedger {
    async fn get_utxos(&mut self, refs: &[TxoRef]) -> Result<Vec<Utxo>> {
        let mut txins = TxIns::new();
        for ref_ in refs {
            txins.insert(TransactionInput {
                transaction_id: ref_.tx_hash[..].into(),
                index: ref_.tx_index as u64,
            });
        }

        let txos = {
            let (query, era) = self.query_client().await?;
            let result = get_utxo_by_txin(query, era, txins).await;
            query.send_release().await?;
            result?
        };

        let mut result = vec![];
        for (address, output) in txos.iter() {
            let body = encode_transaction_output(output)?;
            result.push(Utxo {
                ref_: TxoRef {
                    tx_hash: address.transaction_id.to_vec(),
                    tx_index: u64::from(address.index) as u32,
                },
                body,
            });
        }
        Ok(result)
    }

    async fn get_utxos_by_address(
        &mut self,
        address: Vec<u8>,
        start: Option<String>,
        max: usize,
    ) -> Result<UtxoPage> {
        let txos = {
            let (query, era) = self.query_client().await?;
            let result = get_utxo_by_address(query, era, vec![address.into()]).await;
            query.send_release().await?;
            result?
        };

        let start = start.and_then(|s| s.parse().ok()).unwrap_or(0usize);

        let mut result = vec![];
        let mut next_token = None;
        for (address, output) in txos.iter().skip(start) {
            if result.len() == max {
                next_token = Some((start + max).to_string());
                break;
            }
            let body = encode_transaction_output(output)?;
            result.push(Utxo {
                ref_: TxoRef {
                    tx_hash: address.transaction_id.to_vec(),
                    tx_index: u64::from(address.index) as u32,
                },
                body,
            });
        }

        Ok(UtxoPage {
            utxos: result,
            next_token,
        })
    }

    async fn get_params(&mut self) -> Result<PParams> {
        let pparams = {
            let (query, era) = self.query_client().await?;
            let result = get_current_pparams(query, era).await;
            query.send_release().await?;
            result?
        };

        let mut result = PParams::default();
        for param in std::iter::once(pparams) {
            if let Some(ada) = param.ada_per_utxo_byte {
                result.coins_per_utxo_byte = ada.into();
            }
            if let Some(size) = param.max_transaction_size {
                result.max_tx_size = size as u64;
            }
            if let Some(a) = param.minfee_a {
                result.min_fee_coefficient = a as u64;
            }
            if let Some(b) = param.minfee_b {
                result.min_fee_constant = b as u64;
            }
            if let Some(size) = param.max_block_body_size {
                result.max_block_body_size = size as u64;
            }
            if let Some(size) = param.max_block_header_size {
                result.max_block_header_size = size as u64;
            }
            if let Some(deposit) = param.key_deposit {
                result.stake_key_deposit = deposit.into();
            }
            if let Some(deposit) = param.pool_deposit {
                result.pool_deposit = deposit.into();
            }
            if let Some(bound) = param.maximum_epoch {
                result.pool_retirement_epoch_bound = bound;
            }
            if let Some(pools) = param.desired_number_of_stake_pools {
                result.desired_number_of_pools = pools as u64;
            }
            if let Some(i) = param.pool_pledge_influence {
                result.pool_influence = Some(map_rational(i));
            }
            if let Some(e) = param.expansion_rate {
                result.monetary_expansion = Some(map_rational(e));
            }
            if let Some(e) = param.treasury_growth_rate {
                result.treasury_expansion = Some(map_rational(e));
            }
            if let Some(c) = param.min_pool_cost {
                result.min_pool_cost = c.into();
            }
            if let Some((major, minor)) = param.protocol_version {
                result.protocol_version = Some(ProtocolVersion {
                    major: major as u32,
                    minor: minor as u32,
                });
            }
            if let Some(s) = param.max_value_size {
                result.max_value_size = s as u64;
            }
            if let Some(p) = param.collateral_percentage {
                result.collateral_percentage = p as u64;
            }
            if let Some(i) = param.max_collateral_inputs {
                result.max_collateral_inputs = i as u64;
            }
            if let Some(models) = param.cost_models_for_script_languages {
                result.cost_models = Some(CostModels {
                    plutus_v1: models.plutus_v1.map(|values| CostModel { values }),
                    plutus_v2: models.plutus_v2.map(|values| CostModel { values }),
                    plutus_v3: models.plutus_v3.map(|values| CostModel { values }),
                });
            }
            if let Some(prices) = param.execution_costs {
                result.prices = Some(ExPrices {
                    steps: Some(map_rational(prices.step_price)),
                    memory: Some(map_rational(prices.mem_price)),
                });
            }
            if let Some(ex) = param.max_tx_ex_units {
                result.max_execution_units_per_transaction = Some(ExUnits {
                    steps: ex.steps,
                    memory: ex.mem as u64,
                });
            }
            if let Some(ex) = param.max_block_ex_units {
                result.max_execution_units_per_block = Some(ExUnits {
                    steps: ex.steps,
                    memory: ex.mem as u64,
                });
            }
            if let Some(c) = param.min_fee_ref_script_cost_per_byte {
                result.min_fee_script_ref_cost_per_byte = Some(map_rational(c));
            }
            if let Some(t) = param.pool_voting_thresholds {
                result.pool_voting_thresholds = Some(VotingThresholds {
                    thresholds: vec![
                        map_rational(t.motion_no_confidence),
                        map_rational(t.committee_normal),
                        map_rational(t.committee_no_confidence),
                        map_rational(t.hard_fork_initiation),
                        map_rational(t.pp_security_group),
                    ],
                });
            }
            if let Some(t) = param.drep_voting_thresholds {
                result.drep_voting_thresholds = Some(VotingThresholds {
                    thresholds: vec![
                        map_rational(t.motion_no_confidence),
                        map_rational(t.committee_normal),
                        map_rational(t.committee_no_confidence),
                        map_rational(t.update_to_constitution),
                        map_rational(t.hard_fork_initiation),
                        map_rational(t.pp_network_group),
                        map_rational(t.pp_economic_group),
                        map_rational(t.pp_technical_group),
                        map_rational(t.pp_gov_group),
                        map_rational(t.treasury_withdrawal),
                    ],
                });
            }
            if let Some(s) = param.committee_min_size {
                result.min_committee_size = s as u32;
            }
            if let Some(l) = param.committee_max_term_length {
                result.committee_term_limit = l;
            }
            if let Some(g) = param.gov_action_lifetime {
                result.governance_action_validity_period = g;
            }
            if let Some(g) = param.gov_action_deposit {
                result.governance_action_deposit = g.into();
            }
            if let Some(d) = param.drep_deposit {
                result.drep_deposit = d.into();
            }
            if let Some(d) = param.drep_activity {
                result.drep_inactivity_period = d;
            }
        }
        Ok(result)
    }
}

fn encode_transaction_output(output: &TransactionOutput) -> Result<Vec<u8>> {
    let txo = match output {
        TransactionOutput::Legacy(o) => {
            conway::TransactionOutput::Legacy(conway::LegacyTransactionOutput {
                address: o.address.clone(),
                amount: map_alonzo_value(&o.amount),
                datum_hash: o.datum_hash,
            })
        }
        TransactionOutput::Current(o) => {
            conway::TransactionOutput::PostAlonzo(conway::PostAlonzoTransactionOutput {
                address: o.address.clone(),
                value: map_post_alonzo_value(&o.amount),
                datum_option: o.inline_datum.as_ref().map(map_datum).transpose()?,
                script_ref: o.script_ref.as_ref().map(map_script_ref).transpose()?,
            })
        }
    };
    let mut buffer = vec![];
    minicbor::encode(&txo, &mut buffer).expect("infallible");
    Ok(buffer)
}

fn map_alonzo_value(v: &Value) -> alonzo::Value {
    match v {
        Value::Coin(coin) => alonzo::Value::Coin(coin.into()),
        Value::Multiasset(coin, assets) => {
            let coin = coin.into();
            let assets = assets
                .clone()
                .to_vec()
                .into_iter()
                .map(|(policy_id, assets)| {
                    let assets = assets
                        .to_vec()
                        .into_iter()
                        .map(|(asset_name, value)| (asset_name, value.into()))
                        .collect();
                    (policy_id, assets)
                })
                .collect();
            alonzo::Value::Multiasset(coin, assets)
        }
    }
}

fn map_post_alonzo_value(v: &Value) -> conway::Value {
    match v {
        Value::Coin(coin) => conway::Value::Coin(coin.into()),
        Value::Multiasset(coin, assets) => {
            let coin = coin.into();
            let assets = assets
                .clone()
                .to_vec()
                .into_iter()
                .filter_map(|(policy_id, assets)| {
                    let assets = assets
                        .to_vec()
                        .into_iter()
                        .filter_map(|(asset_name, value)| {
                            let coin = PositiveCoin::try_from(u64::from(value)).ok()?;
                            Some((asset_name, coin))
                        })
                        .collect();
                    Some((policy_id, NonEmptyKeyValuePairs::from_vec(assets)?))
                })
                .collect();
            if let Some(assets) = NonEmptyKeyValuePairs::from_vec(assets) {
                conway::Value::Multiasset(coin, assets)
            } else {
                conway::Value::Coin(coin)
            }
        }
    }
}

fn map_datum(d: &(u16, TagWrap<Bytes, 24>)) -> Result<conway::DatumOption> {
    Ok(minicbor::decode(&d.1)?)
}

fn map_script_ref(r: &TagWrap<Bytes, 24>) -> Result<CborWrap<conway::ScriptRef>> {
    Ok(CborWrap(minicbor::decode(&r.0)?))
}

fn map_rational(r: RationalNumber) -> utxorpc_spec::utxorpc::v1alpha::cardano::RationalNumber {
    utxorpc_spec::utxorpc::v1alpha::cardano::RationalNumber {
        numerator: r.numerator as i32,
        denominator: r.denominator as u32,
    }
}
