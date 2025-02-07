use std::time::Duration;

use anyhow::{bail, Result};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};

pub struct FireflyClient {
    client: Client,
    base_url: String,
}

impl FireflyClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: format!("{base_url}/api/v1"),
        }
    }

    pub async fn deploy_contract(&self, req: &DeployContractRequest) -> Result<ContractLocation> {
        let url = format!("{}/contracts/deploy", self.base_url);
        let res = self.client.post(url).json(req).send().await?;
        let res = Self::extract_error(res).await?;
        let body: DeployContractResponse = res.json().await?;
        self.poll_deploy_contract_location(&body.id).await
    }

    pub async fn deploy_interface(&self, req: &ContractDefinition) -> Result<Interface> {
        let url = format!("{}/contracts/interfaces", self.base_url);
        let res = self.client.post(url).json(req).send().await?;
        let res = Self::extract_error(res).await?;
        let body: Interface = res.json().await?;
        Ok(body)
    }

    pub async fn create_api(&self, req: &CreateApiRequest) -> Result<String> {
        let url = format!("{}/apis", self.base_url);
        let res = self.client.post(url).json(req).send().await?;
        let res = Self::extract_error(res).await?;
        let body: CreateApiResponse = res.json().await?;
        Ok(body.urls.ui)
    }

    async fn poll_deploy_contract_location(&self, operation: &str) -> Result<ContractLocation> {
        let url = format!("{}/operations/{operation}", self.base_url);
        loop {
            let res = self.client.get(&url).send().await?;
            let res = Self::extract_error(res).await?;
            let body: OperationStatusResponse = res.json().await?;
            if body.status == "Failed" {
                bail!("Contract deployment failed");
            }
            if body.status == "Succeeded" {
                let Some(location) = body.output.and_then(|r| r.contract_location) else {
                    bail!("No contract location attached to response");
                };
                return Ok(location);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn extract_error(res: Response) -> Result<Response> {
        if !res.status().is_success() {
            let default_msg = res.status().to_string();
            let message = res.text().await.unwrap_or(default_msg);
            bail!("request failed: {}", message);
        }
        Ok(res)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployContractRequest {
    pub contract: String,
    pub definition: ContractDefinition,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractDefinition {
    pub name: String,
    version: String,
    methods: Vec<serde_json::Value>,
    events: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeployContractResponse {
    id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationStatusResponse {
    status: String,
    output: Option<OperationReceipt>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationReceipt {
    contract_location: Option<ContractLocation>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractLocation {
    address: String,
}

#[derive(Serialize, Deserialize)]
pub struct Interface {
    id: String,
}

#[derive(Serialize)]
pub struct CreateApiRequest {
    pub name: String,
    pub location: ContractLocation,
    pub interface: Interface,
}

#[derive(Deserialize)]
struct CreateApiResponse {
    urls: Urls,
}

#[derive(Deserialize)]
struct Urls {
    ui: String,
}
