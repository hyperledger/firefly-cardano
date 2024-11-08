use std::sync::Arc;

use anyhow::Context;
use firefly_server::apitypes::{ApiError, ApiResult};
use pallas_primitives::conway::Tx;
use serde_json::Value;

use crate::{
    blockchain::BlockchainClient, contracts::ContractManager, persistence::Persistence,
    signer::CardanoSigner,
};

use super::{Operation, OperationId, OperationStatus};

pub struct OperationsManager {
    blockchain: Arc<BlockchainClient>,
    contracts: Arc<ContractManager>,
    persistence: Arc<dyn Persistence>,
    signer: Arc<CardanoSigner>,
}

impl OperationsManager {
    pub fn new(
        blockchain: Arc<BlockchainClient>,
        contracts: Arc<ContractManager>,
        persistence: Arc<dyn Persistence>,
        signer: Arc<CardanoSigner>,
    ) -> Self {
        Self {
            blockchain,
            contracts,
            persistence,
            signer,
        }
    }

    pub async fn invoke(
        &self,
        id: OperationId,
        from: &str,
        contract: &str,
        method: &str,
        params: Value,
    ) -> ApiResult<()> {
        let mut op = Operation {
            id,
            status: OperationStatus::Pending,
            tx_id: None,
        };
        self.persistence.write_operation(&op).await?;
        let result = self.contracts.invoke(contract, method, params).await;
        let value = match result {
            Ok(v) => v,
            Err(err) => {
                op.status = OperationStatus::Failed(err.to_string());
                self.persistence.write_operation(&op).await?;
                return Err(err.into());
            }
        };
        if let Some(tx) = value {
            op.tx_id = Some(self.submit_transaction(from, tx).await?);
        }

        op.status = OperationStatus::Succeeded;
        self.persistence.write_operation(&op).await?;

        Ok(())
    }

    pub async fn get_operation(&self, id: &OperationId) -> ApiResult<Operation> {
        let Some(op) = self.persistence.read_operation(id).await? else {
            return Err(ApiError::not_found("No operation found with that id"));
        };
        Ok(op)
    }

    async fn submit_transaction(&self, address: &str, tx: Vec<u8>) -> ApiResult<String> {
        let mut transaction: Tx = minicbor::decode(&tx)?;
        self.signer
            .sign(address.to_string(), &mut transaction)
            .await?;
        let tx_id = self
            .blockchain
            .submit(transaction)
            .await
            .context("could not submit transaction")?;
        Ok(tx_id)
    }
}
