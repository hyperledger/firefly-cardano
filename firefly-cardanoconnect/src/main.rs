use std::path::PathBuf;

use aide::{
    axum::{routing::get, ApiRouter, IntoApiResponse},
    openapi::{Info, OpenApi},
};
use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        WebSocketUpgrade,
    },
    response::{Html, Response},
    Extension, Json,
};
use axum_swagger_ui::swagger_ui;
use clap::Parser;
use config::load_config;
use tokio::net::TcpListener;

mod config;

async fn health() -> impl IntoApiResponse {
    Json("Hello, world!")
}

async fn serve_api(Extension(api): Extension<OpenApi>) -> impl IntoApiResponse {
    Json(api)
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

    let app = ApiRouter::new()
        .api_route("/api/health", get(health))
        .route("/api/ws", axum::routing::get(handle_socket_upgrade))
        .route("/api/openapi.json", get(serve_api))
        .route(
            "/api",
            get(|| async { Html(swagger_ui("/api/openapi.json")) }),
        );

    let mut api = OpenApi {
        info: Info {
            description: Some("Firefly Cardano Connector".to_string()),
            ..Info::default()
        },
        ..OpenApi::default()
    };

    let listener = TcpListener::bind(format!("{}:{}", config.api.address, config.api.port)).await?;

    axum::serve(
        listener,
        app
            // Generate the documentation.
            .finish_api(&mut api)
            // Expose the documentation to the handlers.
            .layer(Extension(api))
            .into_make_service(),
    )
    .await?;

    Ok(())
}
