use std::{path::PathBuf, sync::Arc};

use aide::axum::{
    routing::{get, post},
    ApiRouter, IntoApiResponse,
};
use anyhow::Result;
use axum::Json;
use clap::Parser;
use config::load_config;
use keys::KeyStore;
use routes::sign_transaction;

mod config;
mod keys;
mod routes;

async fn health() -> impl IntoApiResponse {
    Json("Hello, world!")
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(short = 'f', long)]
    pub config_file: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    key_store: Arc<KeyStore>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config_file = args.config_file.as_deref();
    let config = load_config(config_file)?;

    let key_store = KeyStore::default();
    let state = AppState {
        key_store: Arc::new(key_store),
    };

    let router = ApiRouter::new()
        .api_route("/api/health", get(health))
        .api_route("/api/sign", post(sign_transaction))
        .with_state(state);
    firefly_server::server::serve(&config.api, router).await
}
