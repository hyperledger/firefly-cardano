use serde::{Deserialize, Serialize};

use crate::strong_id;

strong_id!(OperationId, String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub status: OperationStatus,
    pub tx_id: Option<String>,
    pub contract_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperationStatus {
    Succeeded,
    Pending,
    Failed(String),
}
impl OperationStatus {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Succeeded => "Succeeded",
            Self::Pending => "Pending",
            Self::Failed(_) => "Failed",
        }
    }
    pub fn error_message(&self) -> Option<&str> {
        if let Self::Failed(msg) = self {
            Some(msg)
        } else {
            None
        }
    }
}
