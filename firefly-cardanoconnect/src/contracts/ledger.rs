use std::collections::{hash_map::Entry, HashMap};

use async_trait::async_trait;
use balius_runtime::ledgers::{CustomLedger, LedgerError, TxoRef, Utxo, UtxoPage, UtxoPattern};
use blockfrost::{BlockFrostSettings, BlockfrostAPI, Pagination};
use pallas_traverse::MultiEraTx;

pub struct BlockfrostLedger {
    client: BlockfrostAPI,
}

impl BlockfrostLedger {
    pub fn new(key: &str) -> Self {
        Self {
            client: BlockfrostAPI::new(key, BlockFrostSettings::new()),
        }
    }
}

#[async_trait]
impl CustomLedger for BlockfrostLedger {
    async fn read_utxos(&mut self, refs: Vec<TxoRef>) -> Result<Vec<Utxo>, LedgerError> {
        let mut txs = TxDict::new(&mut self.client);
        let mut result = vec![];
        for ref_ in &refs {
            let tx_bytes = txs.get_bytes(hex::encode(&ref_.tx_hash)).await?;
            let tx = TxDict::decode_tx(tx_bytes)?;
            let Some(txo) = tx.output_at(ref_.tx_index as usize) else {
                return Err(LedgerError::NotFound(ref_.clone()));
            };
            result.push(Utxo {
                ref_: ref_.clone(),
                body: txo.encode(),
            });
        }
        Ok(result)
    }

    async fn search_utxos(
        &mut self,
        pattern: UtxoPattern,
        start: Option<String>,
        max_items: u32,
    ) -> Result<UtxoPage, LedgerError> {
        if pattern.asset.is_some() {
            return Err(LedgerError::Internal(
                "querying by asset is not implemented".into(),
            ));
        }
        let Some(address) = pattern.address else {
            return Err(LedgerError::Internal("address is required".into()));
        };
        let address = pallas_addresses::Address::from_bytes(&address.exact_address)
            .and_then(|a| a.to_bech32())
            .map_err(|err| LedgerError::Internal(err.to_string()))?;
        let page = match start {
            Some(s) => s
                .parse::<usize>()
                .map_err(|e| LedgerError::Internal(e.to_string()))?,
            None => 1,
        };

        let pagination = Pagination::new(blockfrost::Order::Asc, page, max_items as usize);
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
            let Some(txo) = tx.output_at(utxo.tx_index as usize) else {
                return Err(LedgerError::NotFound(ref_));
            };

            utxos.push(Utxo {
                ref_,
                body: txo.encode(),
            });
        }

        let next_token = if utxos.len() == max_items as usize {
            Some((page + 1).to_string())
        } else {
            None
        };

        Ok(UtxoPage { utxos, next_token })
    }
}

struct TxDict<'a> {
    client: &'a mut BlockfrostAPI,
    txs: HashMap<String, Vec<u8>>,
}
impl<'a> TxDict<'a> {
    fn new(client: &'a mut BlockfrostAPI) -> Self {
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
