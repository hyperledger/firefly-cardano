use std::path::PathBuf;

use aide::axum::{routing::get, ApiRouter, IntoApiResponse};
use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        WebSocketUpgrade,
    },
    response::Response,
    Json,
};
use clap::Parser;
use config::load_config;

mod config;

async fn health() -> impl IntoApiResponse {
    Json("Hello, world!")
}

async fn handle_socket(mut socket: WebSocket) {
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(msg) => {
                if let Some(response) = process_message(msg) {
                    if let Err(error) = socket.send(response).await {
                        println!("client disconnected: {}", error);
                        return;
                    }
                }
            }
            Err(error) => {
                println!("client disconnected: {}", error);
                return;
            }
        }
    }
}

fn process_message(msg: Message) -> Option<Message> {
    match msg {
        Message::Text(text) => {
            println!("WS received message {}", text);
            Some(Message::Text(text))
        }
        Message::Binary(bytes) => {
            println!("WS received message {:?}", bytes);
            Some(Message::Binary(bytes))
        }
        _ => None,
    }
}

async fn handle_socket_upgrade(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(short = 'f', long)]
    pub config_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config_file = args.config_file.as_deref();
    let config = load_config(config_file)?;

    let router = ApiRouter::new()
        .api_route("/api/health", get(health))
        .route("/api/ws", axum::routing::get(handle_socket_upgrade));

    firefly_server::server::serve(&config.api, router).await
}
