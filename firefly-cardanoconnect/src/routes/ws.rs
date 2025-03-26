use std::time::SystemTime;

use anyhow::{Context, Result, bail};
use axum::{
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{Level, error, instrument, warn};

use crate::{
    AppState,
    operations::{Operation, OperationStatus},
    streams::{Batch, BatchEvent, ContractEvent},
};

async fn handle_socket(
    AppState { stream_manager, .. }: AppState,
    mut socket: WebSocket,
) -> Result<()> {
    let Some(message) = read_message(&mut socket).await? else {
        bail!("socket closed before we could read a message");
    };

    let topic = match message {
        IncomingMessage::Listen(ListenMessage { topic }) => topic,
        other => bail!("unexpected first message: {:?}", other),
    };
    let mut subscription = stream_manager.subscribe(&topic).await?;

    while let Some(batch) = subscription.recv().await {
        send_batch(&mut socket, &topic, batch).await?;
    }
    Ok(())
}

async fn send_batch(socket: &mut WebSocket, topic: &str, batch: Batch) -> Result<()> {
    let outgoing_batch = OutgoingBatch {
        batch_number: batch.batch_number,
        events: batch.events.iter().map(|e| e.into()).collect(),
    };
    let outgoing_json = serde_json::to_string(&outgoing_batch)?;
    socket.send(Message::Text(outgoing_json.into())).await?;

    let Some(response) = read_message(socket).await? else {
        bail!("socket was closed after sending a batch");
    };
    match response {
        IncomingMessage::Ack(ack) => {
            if ack.topic != topic {
                bail!("client acked messages from the wrong topic");
            }
            if ack.batch_number != batch.batch_number {
                bail!("client acked the wrong batch");
            }
            batch.ack();
        }
        IncomingMessage::Error(err) => {
            if err.topic != topic {
                bail!("client acked messages from the wrong topic");
            }
            error!("client couldn't process batch: {}", err.message);
        }
        IncomingMessage::ListenReplies => {
            // do nothing, we already send replies whether they ask or not
        }
        other => {
            bail!("unexpected response to batch! {:?}", other);
        }
    }
    Ok(())
}

fn systemtime_to_rfc3339(value: SystemTime) -> String {
    let date: DateTime<Utc> = value.into();
    date.to_rfc3339()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListenMessage {
    topic: String,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AckMessage {
    topic: String,
    batch_number: u64,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ErrorMessage {
    topic: String,
    // batch_number: u64,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum IncomingMessage {
    Listen(ListenMessage),
    ListenReplies,
    Ack(AckMessage),
    Error(ErrorMessage),
}

#[instrument(err(Debug, level = Level::WARN), skip(socket))]
async fn read_message(socket: &mut WebSocket) -> Result<Option<IncomingMessage>> {
    loop {
        let Some(message) = socket.recv().await else {
            warn!("socket has been closed");
            return Ok(None);
        };
        let message = message.context("could not read websocket message")?;
        let data: &[u8] = match &message {
            Message::Ping(_) | Message::Pong(_) => {
                continue;
            }
            Message::Close(frame) => {
                let reason = frame
                    .as_ref()
                    .map(|f| f.reason.to_owned())
                    .unwrap_or("socket was closed".into());
                warn!("socket was closed: {}", reason);
                return Ok(None);
            }
            Message::Binary(bytes) => bytes,
            Message::Text(string) => string.as_bytes(),
        };
        let incoming = serde_json::from_slice(data).context("could not parse websocket message")?;
        return Ok(Some(incoming));
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum OutgoingEvent {
    ContractEvent(OutgoingContractEvent),
    Receipt(OutgoingOperation),
}

impl From<&BatchEvent> for OutgoingEvent {
    fn from(value: &BatchEvent) -> Self {
        match value {
            BatchEvent::ContractEvent(event) => Self::ContractEvent(event.into()),
            BatchEvent::Receipt(receipt) => Self::Receipt(receipt.into()),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutgoingContractEvent {
    listener_id: Option<String>,
    address: Option<String>,
    signature: String,
    block_hash: String,
    block_number: Option<u64>,
    transaction_hash: String,
    transaction_index: u64,
    log_index: u64,
    timestamp: Option<String>,
    data: serde_json::Value,
}

impl From<&ContractEvent> for OutgoingContractEvent {
    fn from(e: &ContractEvent) -> Self {
        Self {
            listener_id: Some(e.id.listener_id.clone().into()),
            address: e.id.address.clone(),
            signature: e.id.signature.clone(),
            block_number: e.id.block_number,
            block_hash: e.id.block_hash.clone(),
            transaction_hash: e.id.transaction_hash.clone(),
            transaction_index: e.id.transaction_index,
            log_index: e.id.log_index,
            timestamp: e.id.timestamp.map(systemtime_to_rfc3339),
            data: e.data.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutgoingOperation {
    headers: OperationHeaders,
    transaction_hash: Option<String>,
    error_message: Option<String>,
    contract_location: Option<ContractLocation>,
}

impl From<&Operation> for OutgoingOperation {
    fn from(op: &Operation) -> Self {
        Self {
            headers: OperationHeaders {
                request_id: op.id.to_string(),
                type_: match op.status {
                    OperationStatus::Succeeded => "TransactionSuccess".into(),
                    OperationStatus::Pending => "TransactionUpdate".into(),
                    OperationStatus::Failed(_) => "TransactionFailed".into(),
                },
            },
            transaction_hash: op.tx_id.clone(),
            error_message: op.status.error_message().map(|m| m.to_string()),
            contract_location: op
                .contract_address
                .clone()
                .map(|address| ContractLocation { address }),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutgoingBatch {
    batch_number: u64,
    events: Vec<OutgoingEvent>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationHeaders {
    request_id: String,
    #[serde(rename = "type")]
    type_: String,
}

#[derive(Debug, Serialize)]
struct ContractLocation {
    address: String,
}

pub async fn handle_socket_upgrade(
    State(app_state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(|socket| async move {
        if let Err(error) = handle_socket(app_state, socket).await {
            warn!("socket error: {:?}", error);
        }
    })
}
