use anyhow::{Result, bail};
use reqwest::{Client, Response};
use serde::Serialize;
use uuid::Uuid;

pub struct FireflyCardanoClient {
    client: Client,
    v1_base_url: String,
}

impl FireflyCardanoClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            v1_base_url: format!("{base_url}/api/v1"),
        }
    }

    pub async fn deploy_contract(&self, name: &str, version: &str, contract: &str) -> Result<()> {
        let url = format!("{}/contracts/deploy", self.v1_base_url);
        let req = DeployContractRequest {
            id: Uuid::new_v4().to_string(),
            contract: contract.to_string(),
            definition: ABIContract {
                name: name.to_string(),
                version: version.to_string(),
            },
        };
        let res = self.client.post(url).json(&req).send().await?;
        Self::extract_error(res).await?;
        Ok(())
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
struct DeployContractRequest {
    pub id: String,
    pub contract: String,
    pub definition: ABIContract,
}

#[derive(Serialize)]
struct ABIContract {
    pub name: String,
    pub version: String,
}
