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
        if let Some(interface) = self.try_get_interface(&req.name, &req.version).await? {
            self.delete_interface(&interface.id).await?;
        }
        self.create_interface(req).await
    }

    async fn try_get_interface(&self, name: &str, version: &str) -> Result<Option<Interface>> {
        let url = format!("{}/contracts/interfaces/{name}/{version}", self.base_url);
        let res = self.client.get(url).send().await?;
        if res.status().as_u16() == 404 {
            return Ok(None);
        }
        let res = Self::extract_error(res).await?;
        let body: Interface = res.json().await?;
        Ok(Some(body))
    }

    async fn create_interface(&self, req: &ContractDefinition) -> Result<Interface> {
        let url = format!("{}/contracts/interfaces", self.base_url);
        let res = self.client.post(url).json(req).send().await?;
        let res = Self::extract_error(res).await?;
        let body: Interface = res.json().await?;
        Ok(body)
    }

    async fn delete_interface(&self, id: &str) -> Result<()> {
        let url = format!("{}/contracts/interfaces/{id}", self.base_url);
        let res = self.client.delete(url).send().await?;
        Self::extract_error(res).await?;
        Ok(())
    }

    pub async fn deploy_api(&self, req: &CreateApiRequest) -> Result<String> {
        let res = match self.try_get_api(&req.name).await? {
            Some(api) => self.update_api(req, &api.id).await?,
            None => self.create_api(req).await?,
        };
        Ok(res.urls.ui)
    }

    async fn try_get_api(&self, name: &str) -> Result<Option<ApiResponse>> {
        let url = format!("{}/apis/{name}", self.base_url);
        let res = self.client.get(url).send().await?;
        if res.status().as_u16() == 404 {
            return Ok(None);
        }
        let res = Self::extract_error(res).await?;
        let body: ApiResponse = res.json().await?;
        Ok(Some(body))
    }

    async fn create_api(&self, req: &CreateApiRequest) -> Result<ApiResponse> {
        let url = format!("{}/apis", self.base_url);
        let res = self.client.post(url).json(req).send().await?;
        let res = Self::extract_error(res).await?;
        let body: ApiResponse = res.json().await?;
        Ok(body)
    }

    async fn update_api(&self, req: &CreateApiRequest, id: &str) -> Result<ApiResponse> {
        let url = format!("{}/apis/{id}", self.base_url);
        let res = self.client.put(url).json(req).send().await?;
        let res = Self::extract_error(res).await?;
        let body: ApiResponse = res.json().await?;
        Ok(body)
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
struct ApiResponse {
    id: String,
    urls: Urls,
}

#[derive(Deserialize)]
struct Urls {
    ui: String,
}
