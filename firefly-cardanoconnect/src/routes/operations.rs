use axum::{
    Json,
    extract::{Path, State},
};
use firefly_server::apitypes::{ApiResult, NoContent};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, operations::Operation};

#[derive(Deserialize, JsonSchema)]
pub struct QueryRequest {
    /// The name of the contract getting invoked.
    pub address: String,
    /// A description of the method getting invoked.
    pub method: ABIMethod,
    /// Any parameters needed to invoke the method.
    pub params: Vec<Value>,
}

#[derive(Deserialize, JsonSchema)]
pub struct InvokeRequest {
    /// The FireFly operation ID of this request.
    pub id: String,
    /// The address which should be signing any transactions.
    pub from: String,
    /// The name of the contract getting invoked.
    pub address: String,
    /// A description of the method getting invoked.
    pub method: ABIMethod,
    /// Any parameters needed to invoke the method.
    pub params: Vec<Value>,
}

#[derive(Deserialize, JsonSchema)]
pub struct DeployRequest {
    /// The FireFly operation ID of this request.
    pub id: String,
    /// A hex-encoded WASM component.
    pub contract: String,
    /// A description of the schema for this contract.
    pub definition: ABIContract,
}

#[derive(Deserialize, JsonSchema)]
pub struct ABIContract {
    pub name: String,
    pub version: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ABIMethod {
    pub name: String,
    pub params: Vec<ABIParameter>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ABIParameter {
    pub name: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationPathParameters {
    pub id: String,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetOperationStatusResponse {
    pub id: String,
    pub status: String,
    pub transaction_hash: Option<String>,
    pub error_message: Option<String>,
    pub receipt: OperationReceipt,
}

impl From<Operation> for GetOperationStatusResponse {
    fn from(value: Operation) -> Self {
        Self {
            id: value.id.into(),
            status: value.status.name().into(),
            transaction_hash: value.tx_id,
            error_message: value.status.error_message().map(|s| s.to_string()),
            receipt: OperationReceipt { protocol_id: None },
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationReceipt {
    pub protocol_id: Option<String>,
}

pub async fn deploy_contract(
    State(AppState { operations, .. }): State<AppState>,
    Json(req): Json<DeployRequest>,
) -> ApiResult<NoContent> {
    let id = req.id.into();
    let address = format!("{}@{}", req.definition.name, req.definition.version);
    let contract = hex::decode(req.contract)?;
    match operations.deploy(id, &address, &contract).await {
        Ok(()) => Ok(NoContent),
        Err(error) => Err(error.with_field("submissionRejected", true)),
    }
}

pub async fn invoke_contract(
    State(AppState { operations, .. }): State<AppState>,
    Json(req): Json<InvokeRequest>,
) -> ApiResult<NoContent> {
    let id = req.id.into();
    let from = &req.from;
    let contract = &req.address;
    let method = &req.method.name;
    let params = format_params(&req.method, req.params);
    match operations.invoke(id, from, contract, method, params).await {
        Ok(()) => Ok(NoContent),
        Err(error) => Err(error.with_field("submissionRejected", true)),
    }
}

pub async fn query_contract(
    State(AppState { operations, .. }): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> ApiResult<Json<Value>> {
    let contract = &req.address;
    let method = &req.method.name;
    let params = format_params(&req.method, req.params);
    let result = operations.query(contract, method, params).await?;
    Ok(Json(result))
}

pub async fn get_operation_status(
    State(AppState { operations, .. }): State<AppState>,
    Path(OperationPathParameters { id }): Path<OperationPathParameters>,
) -> ApiResult<Json<GetOperationStatusResponse>> {
    let id = id.into();
    let op = operations.get_operation(&id).await?;
    Ok(Json(op.into()))
}

fn format_params(method: &ABIMethod, values: Vec<Value>) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    let mut params = serde_json::Map::new();
    for (schema, value) in method.params.iter().zip(values) {
        params.insert(schema.name.clone(), value);
    }
    params.into()
}
