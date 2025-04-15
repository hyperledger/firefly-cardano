use anyhow::Result;
use chainsync::BlockfrostChainSync;
use client::BlockfrostClient;
use ledger::BlockfrostLedger;
use pallas_primitives::conway::Tx;

mod chainsync;
mod client;
mod ledger;

pub struct Blockfrost {
    client: BlockfrostClient,
    genesis_hash: String,
}

impl Blockfrost {
    pub fn new(key: &str, genesis_hash: &str) -> Self {
        Self {
            client: BlockfrostClient::new(key),
            genesis_hash: genesis_hash.to_string(),
        }
    }

    pub async fn submit(&self, transaction: Tx<'_>) -> Result<String> {
        let transaction_data = {
            let mut bytes = vec![];
            minicbor::encode(transaction, &mut bytes).expect("infallible");
            bytes
        };
        self.client.transactions_submit(transaction_data).await
    }

    pub async fn open_chainsync(&self) -> Result<BlockfrostChainSync> {
        BlockfrostChainSync::new(self.client.clone(), self.genesis_hash.clone()).await
    }

    pub fn ledger(&self) -> BlockfrostLedger {
        BlockfrostLedger::new(self.client.clone())
    }
}
