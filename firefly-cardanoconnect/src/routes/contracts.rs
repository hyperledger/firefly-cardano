use axum::{extract::State, Json};
use firefly_server::apitypes::{ApiError, ApiResult, NoContent};
use reqwest::StatusCode;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use crate::AppState;

#[derive(Deserialize, JsonSchema)]
pub struct InvokeRequest {
    #[expect(unused)]
    /// The FireFly operation ID of this request.
    pub id: String,
    /// The name of the contract getting invoked
    pub address: String,
    /// A description of the method getting invoked.
    pub method: ABIMethod,
    /// Any parameters needed to invoke the method.
    pub params: Vec<Value>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ABIMethod {
    pub name: String,
    pub params: Vec<ABIParameter>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ABIParameter {
    pub name: String,
}

pub async fn invoke_contract(
    State(AppState { contracts, .. }): State<AppState>,
    Json(req): Json<InvokeRequest>,
) -> ApiResult<NoContent> {
    let mut params = serde_json::Map::new();
    for (schema, value) in req.method.params.iter().zip(req.params.into_iter()) {
        params.insert(schema.name.to_string(), value);
    }
    match contracts
        .invoke(&req.address, &req.method.name, params.into())
        .await
    {
        Ok(_res) => {
            // TODO: send res to the websocket
            Ok(NoContent)
        }
        Err(error) => {
            let err = ApiError::new(StatusCode::BAD_REQUEST, error.to_string())
                .with_field("submissionRejected", true);
            Err(err)
        }
    }
}
