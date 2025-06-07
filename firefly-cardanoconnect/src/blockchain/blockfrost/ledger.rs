use std::{
    collections::{HashMap, hash_map::Entry},
    str::FromStr,
};

use anyhow::Result;
use async_trait::async_trait;
use balius_runtime::ledgers::{LedgerError, TxoRef, Utxo, UtxoPage};
use blockfrost::Pagination;
use num_rational::Rational32;
use num_traits::FromPrimitive as _;
use pallas_traverse::MultiEraTx;
use utxorpc_spec::utxorpc::v1alpha::cardano::{
    CostModel, CostModels, ExPrices, ExUnits, PParams, ProtocolVersion, RationalNumber,
    VotingThresholds,
};

use crate::blockchain::BaliusLedger;

use super::client::BlockfrostClient;

pub struct BlockfrostLedger {
    client: BlockfrostClient,
}

impl BlockfrostLedger {
    pub fn new(client: BlockfrostClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl BaliusLedger for BlockfrostLedger {
    async fn get_utxos(&mut self, refs: &[TxoRef]) -> Result<Vec<Utxo>> {
        let mut txs = TxDict::new(&mut self.client);
        let mut result = vec![];
        for ref_ in refs {
            let tx_bytes = txs.get_bytes(hex::encode(&ref_.tx_hash)).await?;
            let tx = TxDict::decode_tx(tx_bytes)?;
            if let Some(txo) = tx.output_at(ref_.tx_index as usize) {
                result.push(Utxo {
                    ref_: ref_.clone(),
                    body: txo.encode(),
                });
            }
        }
        Ok(result)
    }

    async fn get_utxos_by_address(
        &mut self,
        address: Vec<u8>,
        start: Option<String>,
        max: usize,
    ) -> Result<UtxoPage> {
        let address = pallas_addresses::Address::from_bytes(&address)
            .and_then(|a| a.to_bech32())
            .map_err(|err| LedgerError::Internal(err.to_string()))?;
        let page = match start {
            Some(s) => s
                .parse::<usize>()
                .map_err(|e| LedgerError::Internal(e.to_string()))?,
            None => 1,
        };

        let pagination = Pagination::new(blockfrost::Order::Asc, page, max);
        let query = self
            .client
            .addresses_utxos(&address, pagination)
            .await
            .map_err(|err| LedgerError::Upstream(err.to_string()))?;

        let mut utxos = vec![];
        let mut txs = TxDict::new(&mut self.client);
        for utxo in query {
            let raw_tx_hash =
                hex::decode(&utxo.tx_hash).map_err(|e| LedgerError::Upstream(e.to_string()))?;
            let ref_ = TxoRef {
                tx_hash: raw_tx_hash,
                tx_index: utxo.tx_index as u32,
            };

            let tx_bytes = txs.get_bytes(utxo.tx_hash.clone()).await?;
            let tx = TxDict::decode_tx(tx_bytes)?;
            if let Some(txo) = tx.output_at(utxo.tx_index as usize) {
                utxos.push(Utxo {
                    ref_,
                    body: txo.encode(),
                });
            };
        }

        let next_token = if utxos.len() == max {
            Some((page + 1).to_string())
        } else {
            None
        };

        Ok(UtxoPage { utxos, next_token })
    }

    async fn get_params(&mut self) -> Result<PParams> {
        let raw_params = self
            .client
            .epochs_latest_parameters()
            .await
            .map_err(|e| LedgerError::Upstream(e.to_string()))?;
        Ok(PParams {
            coins_per_utxo_byte: string_to_num(raw_params.coins_per_utxo_size),
            max_tx_size: raw_params.max_tx_size as u64,
            min_fee_coefficient: raw_params.min_fee_a as u64,
            min_fee_constant: raw_params.min_fee_b as u64,
            max_block_body_size: raw_params.max_block_size as u64,
            max_block_header_size: raw_params.max_block_header_size as u64,
            stake_key_deposit: raw_params.key_deposit.parse().unwrap(),
            pool_deposit: raw_params.pool_deposit.parse().unwrap(),
            pool_retirement_epoch_bound: raw_params.e_max as u64,
            desired_number_of_pools: raw_params.n_opt as u64,
            pool_influence: Some(f64_to_rational(raw_params.a0)),
            monetary_expansion: Some(f64_to_rational(raw_params.rho)),
            treasury_expansion: Some(f64_to_rational(raw_params.tau)),
            min_pool_cost: raw_params.min_pool_cost.parse().unwrap(),
            protocol_version: Some(ProtocolVersion {
                major: raw_params.protocol_major_ver as u32,
                minor: raw_params.protocol_minor_ver as u32,
            }),
            max_value_size: string_to_num(raw_params.max_val_size),
            collateral_percentage: raw_params
                .collateral_percent
                .map(|v| v as u64)
                .unwrap_or_default(),
            max_collateral_inputs: raw_params
                .max_collateral_inputs
                .map(|v| v as u64)
                .unwrap_or_default(),
            cost_models: raw_params.cost_models_raw.flatten().map(|models| {
                let extract_model = |name| {
                    let val = models.get(name)?;
                    let array = val.as_array()?;
                    let values = array.iter().map(|v| v.as_i64().unwrap()).collect();
                    Some(CostModel { values })
                };
                CostModels {
                    plutus_v1: extract_model("PlutusV1"),
                    plutus_v2: extract_model("PlutusV2"),
                    plutus_v3: extract_model("PlutusV3"),
                }
            }),
            prices: Some(ExPrices {
                steps: raw_params.price_step.map(f64_to_rational),
                memory: raw_params.price_mem.map(f64_to_rational),
            }),
            max_execution_units_per_transaction: ex_units(
                raw_params.max_tx_ex_steps,
                raw_params.max_tx_ex_mem,
            ),
            max_execution_units_per_block: ex_units(
                raw_params.max_block_ex_steps,
                raw_params.max_block_ex_mem,
            ),
            min_fee_script_ref_cost_per_byte: raw_params
                .min_fee_ref_script_cost_per_byte
                .map(f64_to_rational),
            pool_voting_thresholds: voting_thresholds([
                raw_params.pvt_motion_no_confidence,
                raw_params.pvt_committee_normal,
                raw_params.pvt_committee_no_confidence,
                raw_params.pvt_hard_fork_initiation,
                raw_params.pvt_p_p_security_group,
            ]),
            drep_voting_thresholds: voting_thresholds([
                raw_params.dvt_motion_no_confidence,
                raw_params.dvt_committee_normal,
                raw_params.dvt_committee_no_confidence,
                raw_params.dvt_update_to_constitution,
                raw_params.dvt_hard_fork_initiation,
                raw_params.dvt_p_p_network_group,
                raw_params.dvt_p_p_economic_group,
                raw_params.dvt_p_p_technical_group,
                raw_params.dvt_p_p_gov_group,
                raw_params.dvt_treasury_withdrawal,
            ]),
            min_committee_size: string_to_num(raw_params.committee_min_size),
            committee_term_limit: string_to_num(raw_params.committee_max_term_length),
            governance_action_validity_period: string_to_num(raw_params.gov_action_lifetime),
            governance_action_deposit: string_to_num(raw_params.gov_action_deposit),
            drep_deposit: string_to_num(raw_params.drep_deposit),
            drep_inactivity_period: string_to_num(raw_params.drep_activity),
        })
    }
}

struct TxDict<'a> {
    client: &'a mut BlockfrostClient,
    txs: HashMap<String, Vec<u8>>,
}
impl<'a> TxDict<'a> {
    fn new(client: &'a mut BlockfrostClient) -> Self {
        Self {
            client,
            txs: HashMap::new(),
        }
    }

    async fn get_bytes(&mut self, hash: String) -> Result<&Vec<u8>, LedgerError> {
        match self.txs.entry(hash) {
            Entry::Occupied(tx) => Ok(tx.into_mut()),
            Entry::Vacant(entry) => {
                let tx = self
                    .client
                    .transactions_cbor(entry.key())
                    .await
                    .map_err(|e| LedgerError::Upstream(e.to_string()))?;
                let bytes =
                    hex::decode(&tx.cbor).map_err(|e| LedgerError::Internal(e.to_string()))?;

                Ok(entry.insert(bytes))
            }
        }
    }

    fn decode_tx(bytes: &[u8]) -> Result<MultiEraTx<'_>, LedgerError> {
        MultiEraTx::decode(bytes).map_err(|e| LedgerError::Internal(e.to_string()))
    }
}

fn string_to_num<T: FromStr + Default>(string: Option<String>) -> T {
    string.and_then(|v| v.parse().ok()).unwrap_or_default()
}

fn f64_to_rational(value: f64) -> RationalNumber {
    let ratio = Rational32::from_f64(value).unwrap();
    RationalNumber {
        numerator: *ratio.numer(),
        denominator: *ratio.denom() as u32,
    }
}

fn ex_units(step: Option<String>, mem: Option<String>) -> Option<ExUnits> {
    Some(ExUnits {
        steps: step?.parse().ok()?,
        memory: mem?.parse().ok()?,
    })
}

fn voting_thresholds(
    thresholds: impl IntoIterator<Item = Option<f64>>,
) -> Option<VotingThresholds> {
    Some(VotingThresholds {
        thresholds: thresholds
            .into_iter()
            .map(|t| t.map(f64_to_rational))
            .collect::<Option<Vec<_>>>()?,
    })
}
