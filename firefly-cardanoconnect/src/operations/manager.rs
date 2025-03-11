use std::{sync::Arc, time::SystemTime};

use anyhow::Context;
use chrono::{DateTime, Utc};
use firefly_server::apitypes::{ApiError, ApiResult};
use pallas_primitives::conway::Tx;
use serde_json::Value;
use tokio::sync::watch;

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
    operation_update_sink: watch::Sender<DateTime<Utc>>,
}

impl OperationsManager {
    pub fn new(
        blockchain: Arc<BlockchainClient>,
        contracts: Arc<ContractManager>,
        persistence: Arc<dyn Persistence>,
        signer: Arc<CardanoSigner>,
        operation_update_sink: watch::Sender<DateTime<Utc>>,
    ) -> Self {
        Self {
            blockchain,
            contracts,
            persistence,
            signer,
            operation_update_sink,
        }
    }

    pub async fn deploy(&self, id: OperationId, name: &str, contract: &[u8]) -> ApiResult<()> {
        let mut op = Operation {
            id,
            status: OperationStatus::Pending,
            tx_id: None,
            contract_address: Some(name.to_string()),
        };
        self.update_operation(&op).await?;
        match self.contracts.deploy(name, contract).await {
            Ok(()) => {
                op.status = OperationStatus::Succeeded;
                self.update_operation(&op).await?;
                Ok(())
            }
            Err(err) => {
                op.status = OperationStatus::Failed(err.to_string());
                self.update_operation(&op).await?;
                Err(err.into())
            }
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
            contract_address: None,
        };
        self.update_operation(&op).await?;
        let result = self.contracts.invoke(contract, method, params).await;
        let value = match result {
            Ok(v) => v,
            Err(err) => {
                op.status = OperationStatus::Failed(err.to_string());
                self.update_operation(&op).await?;
                return Err(err.into());
            }
        };
        if let Some(tx) = value {
            let tx_id = self.submit_transaction(from, tx).await?;
            op.tx_id = Some(tx_id.clone());
            self.contracts.handle_submit(contract, method, &tx_id).await;
        }

        op.status = OperationStatus::Succeeded;
        self.update_operation(&op).await?;

        Ok(())
    }

    pub async fn get_operation(&self, id: &OperationId) -> ApiResult<Operation> {
        let Some(op) = self.persistence.read_operation(id).await? else {
            return Err(ApiError::not_found("No operation found with that id"));
        };
        Ok(op)
    }

    async fn update_operation(&self, op: &Operation) -> ApiResult<()> {
        // Notify consumers about this status update.
        let now = SystemTime::now();
        self.persistence.write_operation(op).await?;
        self.operation_update_sink.send_replace(now.into());
        Ok(())
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
