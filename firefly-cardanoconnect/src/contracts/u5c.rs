use std::collections::HashMap;

use crate::streams::BlockInfo;
use anyhow::Result;
use balius_runtime::ledgers::{CustomLedger, Ledger, TxoRef};
use pallas_crypto::hash::Hasher;
use pallas_primitives::conway;
use pallas_traverse::ComputeHash as _;
use tokio::sync::Mutex;
use utxorpc_spec::utxorpc::v1alpha::cardano;

pub struct UtxorpcAdapter {
    ledger: Option<Mutex<Ledger>>,
}

impl UtxorpcAdapter {
    pub fn new(ledger: Option<Ledger>) -> Self {
        Self {
            ledger: ledger.map(Mutex::new),
        }
    }

    pub async fn convert_block(&self, info: &BlockInfo) -> Result<balius_runtime::Block> {
        let header = cardano::BlockHeader {
            slot: info.block_slot.unwrap_or_default(),
            hash: hex::decode(&info.block_hash).unwrap().into(),
            height: info.block_height.unwrap_or_default(),
        };
        let mut body = cardano::BlockBody {
            tx: info.transactions.iter().map(|tx| convert_tx(tx)).collect(),
        };
        self.populate_tx_inputs(&mut body).await?;
        let block = cardano::Block {
            header: Some(header),
            body: Some(body),
        };
        Ok(balius_runtime::Block::Cardano(block))
    }

    async fn populate_tx_inputs(&self, body: &mut cardano::BlockBody) -> Result<()> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut inputs: HashMap<_, Vec<_>> = HashMap::new();
        for input in body.tx.iter_mut().flat_map(|tx| tx.inputs.iter_mut()) {
            let txo_ref = (input.tx_hash.to_vec(), input.output_index);
            inputs.entry(txo_ref).or_default().push(input);
        }
        let refs = inputs
            .keys()
            .map(|(hash, index)| TxoRef {
                tx_hash: hash.clone(),
                tx_index: *index,
            })
            .collect();
        let outputs = {
            let mut lock = ledger.lock().await;
            lock.read_utxos(refs).await?
        };
        for output in outputs {
            let key = (output.ref_.tx_hash, output.ref_.tx_index);
            if let Some(inps) = inputs.get_mut(&key) {
                let tx_output: conway::TransactionOutput = minicbor::decode(&output.body)?;
                for inp in inps {
                    inp.as_output = Some(convert_txo(&tx_output));
                }
            }
        }
        Ok(())
    }
}

fn hash_tx(data: &[u8]) -> Vec<u8> {
    let mut decoder = minicbor::Decoder::new(data);
    decoder.array().unwrap();
    let start = decoder.position();
    decoder.skip().unwrap();
    let end = decoder.position();
    Hasher::<256>::hash(&data[start..end]).to_vec()
}

fn convert_plutus_data(data: conway::PlutusData) -> cardano::PlutusData {
    let inner = match data {
        conway::PlutusData::Constr(con) => {
            let mut constr = cardano::Constr {
                tag: con.tag as u32,
                any_constructor: con.any_constructor.unwrap_or_default(),
                ..cardano::Constr::default()
            };
            for field in con.fields.to_vec() {
                constr.fields.push(convert_plutus_data(field));
            }
            cardano::plutus_data::PlutusData::Constr(constr)
        }
        conway::PlutusData::Map(map) => {
            let mut new_map = cardano::PlutusDataMap::default();
            for (key, value) in map.to_vec() {
                new_map.pairs.push(cardano::PlutusDataPair {
                    key: Some(convert_plutus_data(key)),
                    value: Some(convert_plutus_data(value)),
                });
            }
            cardano::plutus_data::PlutusData::Map(new_map)
        }
        conway::PlutusData::BigInt(int) => {
            let inner = match int {
                conway::BigInt::Int(i) => {
                    let value: i128 = i.into();
                    cardano::big_int::BigInt::Int(value as i64)
                }
                conway::BigInt::BigUInt(i) => cardano::big_int::BigInt::BigUInt(i.to_vec().into()),
                conway::BigInt::BigNInt(i) => cardano::big_int::BigInt::BigNInt(i.to_vec().into()),
            };
            cardano::plutus_data::PlutusData::BigInt(cardano::BigInt {
                big_int: Some(inner),
            })
        }
        conway::PlutusData::BoundedBytes(bytes) => {
            cardano::plutus_data::PlutusData::BoundedBytes(bytes.to_vec().into())
        }
        conway::PlutusData::Array(array) => {
            let mut new_array = cardano::PlutusDataArray::default();
            for item in array.to_vec() {
                new_array.items.push(convert_plutus_data(item));
            }
            cardano::plutus_data::PlutusData::Array(new_array)
        }
    };
    cardano::PlutusData {
        plutus_data: Some(inner),
    }
}

fn convert_native_script(script: &conway::NativeScript) -> cardano::NativeScript {
    fn convert_native_script_list(scripts: &[conway::NativeScript]) -> cardano::NativeScriptList {
        let mut new_list = cardano::NativeScriptList::default();
        for script in scripts {
            new_list.items.push(convert_native_script(script));
        }
        new_list
    }
    use cardano::native_script::NativeScript as InnerNativeScript;
    let inner = match script {
        conway::NativeScript::ScriptPubkey(hash) => {
            InnerNativeScript::ScriptPubkey(hash.to_vec().into())
        }
        conway::NativeScript::ScriptAll(scripts) => {
            InnerNativeScript::ScriptAll(convert_native_script_list(scripts))
        }
        conway::NativeScript::ScriptAny(scripts) => {
            InnerNativeScript::ScriptAny(convert_native_script_list(scripts))
        }
        conway::NativeScript::ScriptNOfK(k, scripts) => {
            InnerNativeScript::ScriptNOfK(cardano::ScriptNOfK {
                k: *k,
                scripts: scripts.iter().map(convert_native_script).collect(),
            })
        }
        conway::NativeScript::InvalidBefore(t) => InnerNativeScript::InvalidBefore(*t),
        conway::NativeScript::InvalidHereafter(t) => InnerNativeScript::InvalidHereafter(*t),
    };
    cardano::NativeScript {
        native_script: Some(inner),
    }
}

fn convert_txi(real_input: &conway::TransactionInput) -> cardano::TxInput {
    cardano::TxInput {
        tx_hash: real_input.transaction_id.to_vec().into(),
        output_index: real_input.index as u32,
        as_output: None,
        redeemer: None,
    }
}

fn convert_txo(real_output: &conway::TransactionOutput) -> cardano::TxOutput {
    use pallas_primitives::{alonzo, conway};
    let mut output = cardano::TxOutput::default();
    match real_output {
        conway::TransactionOutput::Legacy(txo) => {
            output.address = txo.address.to_vec().into();
            if let Some(hash) = txo.datum_hash {
                output.datum = Some(cardano::Datum {
                    hash: hash.to_vec().into(),
                    ..cardano::Datum::default()
                });
            }
            match &txo.amount {
                alonzo::Value::Coin(c) => {
                    output.coin = *c;
                }
                alonzo::Value::Multiasset(c, assets) => {
                    output.coin = *c;
                    for (policy_id, policy_assets) in assets.iter() {
                        let assets = policy_assets
                            .iter()
                            .map(|(name, amount)| cardano::Asset {
                                name: name.to_vec().into(),
                                output_coin: *amount,
                                ..cardano::Asset::default()
                            })
                            .collect();
                        output.assets.push(cardano::Multiasset {
                            policy_id: policy_id.to_vec().into(),
                            assets,
                            ..cardano::Multiasset::default()
                        });
                    }
                }
            }
        }
        conway::TransactionOutput::PostAlonzo(txo) => {
            output.address = txo.address.to_vec().into();
            if let Some(datum_option) = txo.datum_option.as_deref() {
                let mut datum = cardano::Datum::default();
                match datum_option {
                    conway::DatumOption::Hash(hash) => {
                        datum.hash = hash.to_vec().into();
                    }
                    conway::DatumOption::Data(data) => {
                        let mut cbor = vec![];
                        minicbor::encode(data, &mut cbor).expect("infallible");
                        datum.hash = data.0.compute_hash().to_vec().into();
                        datum.payload = Some(convert_plutus_data((*data.0).clone()));
                        datum.original_cbor = cbor.into();
                    }
                }
            }
            match &txo.value {
                conway::Value::Coin(c) => {
                    output.coin = *c;
                }
                conway::Value::Multiasset(c, assets) => {
                    output.coin = *c;
                    for (policy_id, policy_assets) in assets.iter() {
                        let assets = policy_assets
                            .iter()
                            .map(|(name, amount)| cardano::Asset {
                                name: name.to_vec().into(),
                                output_coin: amount.into(),
                                ..cardano::Asset::default()
                            })
                            .collect();
                        output.assets.push(cardano::Multiasset {
                            policy_id: policy_id.to_vec().into(),
                            assets,
                            ..cardano::Multiasset::default()
                        });
                    }
                }
            }
            if let Some(script) = txo.script_ref.as_deref() {
                let inner = match script {
                    conway::ScriptRef::NativeScript(script) => {
                        cardano::script::Script::Native(convert_native_script(script))
                    }
                    conway::ScriptRef::PlutusV1Script(script) => {
                        cardano::script::Script::PlutusV1(script.0.to_vec().into())
                    }
                    conway::ScriptRef::PlutusV2Script(script) => {
                        cardano::script::Script::PlutusV2(script.0.to_vec().into())
                    }
                    conway::ScriptRef::PlutusV3Script(script) => {
                        cardano::script::Script::PlutusV3(script.0.to_vec().into())
                    }
                };
                output.script = Some(cardano::Script {
                    script: Some(inner),
                });
            }
        }
    }
    output
}
fn convert_tx(bytes: &[u8]) -> cardano::Tx {
    let mut tx = cardano::Tx {
        hash: hash_tx(bytes).into(),
        ..cardano::Tx::default()
    };
    let real_tx: conway::Tx = minicbor::decode(bytes).unwrap();
    for real_input in &real_tx.transaction_body.inputs {
        tx.inputs.push(convert_txi(real_input));
    }
    for real_output in &real_tx.transaction_body.outputs {
        tx.outputs.push(convert_txo(real_output));
    }
    tx
}
