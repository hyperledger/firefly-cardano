use anyhow::{Result, bail};
use reqwest::{Client, Response};
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub struct FireflyCardanoClient {
    client: Client,
    base_url: String,
}

impl FireflyCardanoClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.to_string() + "/api/v1",
        }
    }

    pub async fn get_chain_tip(&self) -> Result<Tip> {
        let url = format!("{}/chain/tip", self.base_url);
        let res = self.client.get(url).send().await?;
        let res = Self::extract_error(res).await?;
        let body: Tip = res.json().await?;
        Ok(body)
    }

    pub async fn invoke_contract(
        &self,
        from: &str,
        address: &str,
        method: &str,
        params: impl IntoIterator<Item = (&str, Value)>,
    ) -> Result<Option<String>> {
        let url = format!("{}/contracts/invoke", self.base_url);
        let op_id = uuid::Uuid::new_v4().to_string();
        let mut req = InvokeContractRequest {
            address: address.to_string(),
            from: from.to_string(),
            id: op_id.clone(),
            method: ABIMethod {
                name: method.to_string(),
                params: vec![],
            },
            params: vec![],
        };
        for (name, value) in params.into_iter() {
            req.method.params.push(ABIParameter { name: name.into() });
            req.params.push(value);
        }
        let res = self.client.post(url).json(&req).send().await?;
        Self::extract_error(res).await?;

        // contracts are synchronous, so the result is already available
        let url = format!("{}/transactions/{op_id}", self.base_url);
        let res = self.client.get(url).send().await?;
        let res = Self::extract_error(res).await?;
        let body: OperationStatus = res.json().await?;
        match body.status.as_str() {
            "Succeeded" => Ok(body.transaction_hash),
            other => bail!(
                "Unexpected status {other}: {}",
                body.error_message.unwrap_or_default()
            ),
        }
    }

    pub async fn create_stream(&self, settings: &StreamSettings) -> Result<String> {
        let url = format!("{}/eventstreams", self.base_url);
        let res = self.client.post(url).json(settings).send().await?;
        let res = Self::extract_error(res).await?;
        let body: EventStream = res.json().await?;
        Ok(body.id)
    }

    pub async fn list_streams(&self) -> Result<Vec<EventStream>> {
        let url = format!("{}/eventstreams", self.base_url);
        let res = self.client.get(url).send().await?;
        let res = Self::extract_error(res).await?;
        let body: Vec<EventStream> = res.json().await?;
        Ok(body)
    }

    pub async fn create_listener(
        &self,
        stream_id: &str,
        settings: &ListenerSettings,
    ) -> Result<String> {
        let url = format!("{}/eventstreams/{}/listeners", self.base_url, stream_id);
        let res = self.client.post(url).json(settings).send().await?;
        let res = Self::extract_error(res).await?;
        let body: Listener = res.json().await?;
        Ok(body.id)
    }

    pub async fn list_listeners(&self, stream_id: &str) -> Result<Vec<Listener>> {
        let url = format!("{}/eventstreams/{}/listeners", self.base_url, stream_id);
        let res = self.client.get(url).send().await?;
        let res = Self::extract_error(res).await?;
        let body: Vec<Listener> = res.json().await?;
        Ok(body)
    }

    pub async fn delete_listener(&self, stream_id: &str, listener_id: &str) -> Result<()> {
        let url = format!(
            "{}/eventstreams/{}/listeners/{}",
            self.base_url, stream_id, listener_id
        );
        let res = self.client.delete(url).send().await?;
        Self::extract_error(res).await?;
        Ok(())
    }

    pub async fn connect_ws(&self) -> Result<WebSocket> {
        let url = format!("{}/ws", self.base_url);
        let res = self.client.get(url).upgrade().send().await?;
        Ok(res.into_websocket().await?)
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
#[serde(tag = "type", rename_all = "camelCase")]
pub enum FireflyWebSocketRequest {
    #[serde(rename_all = "camelCase")]
    Listen { topic: String },
    #[serde(rename_all = "camelCase")]
    Ack { topic: String, batch_number: u64 },
}
impl From<FireflyWebSocketRequest> for Message {
    fn from(value: FireflyWebSocketRequest) -> Self {
        Message::Text(serde_json::to_string(&value).unwrap())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireflyWebSocketEventBatch {
    pub batch_number: u64,
    pub events: Vec<FireflyWebSocketEvent>,
}
impl TryFrom<Message> for FireflyWebSocketEventBatch {
    type Error = anyhow::Error;

    fn try_from(value: Message) -> std::result::Result<Self, Self::Error> {
        match value {
            Message::Text(str) => Ok(serde_json::from_str(&str)?),
            Message::Binary(bytes) => Ok(serde_json::from_slice(&bytes)?),
            _ => bail!("unexpected response"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum FireflyWebSocketEvent {
    ContractEvent(FireflyWebSocketContractEvent),
    Receipt,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireflyWebSocketContractEvent {
    pub listener_id: String,
    pub signature: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub data: Value,
}

#[derive(Serialize)]
struct InvokeContractRequest {
    address: String,
    from: String,
    id: String,
    method: ABIMethod,
    params: Vec<Value>,
}

#[derive(Serialize)]
struct ABIMethod {
    name: String,
    params: Vec<ABIParameter>,
}

#[derive(Serialize)]
struct ABIParameter {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationStatus {
    status: String,
    transaction_hash: Option<String>,
    error_message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tip {
    pub block_slot: u64,
    pub block_hash: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamSettings {
    pub name: String,
    pub batch_size: usize,
    #[serde(rename = "batchTimeoutMS")]
    pub batch_timeout_ms: u64,
}

#[derive(Deserialize)]
pub struct EventStream {
    pub id: String,
    pub name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListenerSettings {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ListenerType,
    pub from_block: String,
    pub filters: Vec<ListenerFilter>,
}

#[derive(Deserialize)]
pub struct Listener {
    pub id: String,
    pub name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ListenerType {
    Events,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ListenerFilter {
    #[serde(rename_all = "camelCase")]
    Event {
        contract: String,
        event_path: String,
    },
}
