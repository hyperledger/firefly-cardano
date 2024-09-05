use anyhow::{bail, Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::Response,
};
use serde::{Deserialize, Serialize};
use tracing::{error, instrument, warn};

use crate::AppState;

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
        let outgoing_batch = OutgoingBatch {
            batch_number: batch.batch_number,
            events: batch
                .events
                .iter()
                .map(|e| BlockEvent {
                    listener_id: e.listener_id.clone().into(),
                    block_number: e.block_info.block_number,
                    block_hash: e.block_info.block_hash.clone(),
                    parent_hash: e.block_info.parent_hash.clone(),
                    transaction_hashes: e.block_info.transaction_hashes.clone(),
                })
                .collect(),
        };
        let outgoing_json = serde_json::to_string(&outgoing_batch)?;
        socket.send(Message::Text(outgoing_json)).await?;

        let Some(response) = read_message(&mut socket).await? else {
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
                continue;
            }
            other => {
                bail!("unexpected response to batch! {:?}", other);
            }
        }
    }
    Ok(())
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
#[serde(tag = "type", rename_all = "camelCase")]
enum IncomingMessage {
    Listen(ListenMessage),
    Ack(AckMessage),
    Error(ErrorMessage),
}

#[instrument(err(Debug), skip(socket))]
async fn read_message(socket: &mut WebSocket) -> Result<Option<IncomingMessage>> {
    loop {
        let Some(message) = socket.recv().await else {
            warn!("socket has been closed");
            return Ok(None);
        };
        let data = match message.context("could not read websocket message")? {
            Message::Ping(_) | Message::Pong(_) => {
                continue;
            }
            Message::Close(frame) => {
                let reason = frame
                    .map(|f| f.reason.into_owned())
                    .unwrap_or("socket was closed".into());
                warn!("socket was closed: {}", reason);
                return Ok(None);
            }
            Message::Binary(bytes) => bytes,
            Message::Text(string) => string.into_bytes(),
        };
        let incoming =
            serde_json::from_slice(&data).context("could not parse websocket message")?;
        return Ok(Some(incoming));
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockEvent {
    listener_id: String,
    block_number: u64,
    block_hash: String,
    parent_hash: String,
    transaction_hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutgoingBatch {
    batch_number: u64,
    events: Vec<BlockEvent>,
}

pub async fn handle_socket_upgrade(
    State(app_state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(|socket| async move {
        if let Err(error) = handle_socket(app_state, socket).await {
            error!("socket error: {:?}", error);
        }
    })
}
