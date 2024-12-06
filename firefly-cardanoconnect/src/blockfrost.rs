use anyhow::Result;
pub use blockfrost::Pagination;
use blockfrost::{BlockFrostSettings, BlockfrostAPI, BlockfrostError, BlockfrostResult};
pub use blockfrost_openapi::models::{AddressUtxoContentInner, BlockContent, TxContentCbor};

#[derive(Debug, Clone)]
pub struct BlockfrostClient {
    api: BlockfrostAPI,
}

impl BlockfrostClient {
    pub fn new(key: &str) -> Self {
        use blockfrost::USER_AGENT;
        let mut settings = BlockFrostSettings::new();
        settings
            .headers
            .insert("User-Agent".into(), format!("{USER_AGENT} (firefly)"));
        Self {
            api: BlockfrostAPI::new(key, settings),
        }
    }

    pub async fn addresses_utxos(
        &self,
        address: &str,
        pagination: Pagination,
    ) -> Result<Vec<AddressUtxoContentInner>> {
        Ok(self.api.addresses_utxos(address, pagination).await?)
    }

    pub async fn blocks_latest(&self) -> Result<BlockContent> {
        Ok(self.api.blocks_latest().await?)
    }

    pub async fn blocks_next(
        &self,
        hash: &str,
        pagination: Pagination,
    ) -> Result<Vec<BlockContent>> {
        Ok(self.api.blocks_next(hash, pagination).await?)
    }

    pub async fn blocks_previous(
        &self,
        hash: &str,
        pagination: Pagination,
    ) -> Result<Vec<BlockContent>> {
        Ok(self.api.blocks_previous(hash, pagination).await?)
    }

    pub async fn blocks_txs(&self, hash: &str) -> Result<Vec<String>> {
        let pagination = Pagination::all();
        Ok(self.api.blocks_txs(hash, pagination).await?)
    }

    pub async fn transactions_cbor(&self, hash: &str) -> Result<TxContentCbor> {
        Ok(self.api.transactions_cbor(hash).await?)
    }

    pub async fn transactions_submit(&self, transaction_data: Vec<u8>) -> Result<String> {
        Ok(self.api.transactions_submit(transaction_data).await?)
    }

    pub async fn try_blocks_by_id(&self, hash: &str) -> Result<Option<BlockContent>> {
        self.api.blocks_by_id(hash).await.none_on_404()
    }

    pub async fn try_blocks_next(
        &self,
        hash: &str,
        pagination: Pagination,
    ) -> Result<Option<Vec<BlockContent>>> {
        self.api.blocks_next(hash, pagination).await.none_on_404()
    }
}

trait BlockfrostResultExt {
    type T;
    fn none_on_404(self) -> Result<Option<Self::T>>;
}

impl<T> BlockfrostResultExt for BlockfrostResult<T> {
    type T = T;
    fn none_on_404(self) -> Result<Option<Self::T>> {
        match self {
            Err(BlockfrostError::Response { reason, .. }) if reason.status_code == 404 => Ok(None),
            Err(error) => Err(error.into()),
            Ok(res) => Ok(Some(res)),
        }
    }
}
