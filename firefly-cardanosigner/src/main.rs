use std::{path::PathBuf, sync::Arc};

use aide::axum::{
    ApiRouter, IntoApiResponse,
    routing::{get, post},
};
use anyhow::Result;
use axum::Json;
use clap::Parser;
use config::{FireflySignerConfig, load_config};
use firefly_server::instrumentation;
use keys::KeyStore;
use routes::sign_transaction;
use tracing::instrument;

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
    pub config_file: Vec<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    key_store: Arc<KeyStore>,
}

#[instrument(err(Debug))]
async fn init_state(config: &FireflySignerConfig) -> Result<AppState> {
    let key_store = match &config.file_wallet {
        Some(file_wallet_config) => KeyStore::from_fs(file_wallet_config)?,
        None => KeyStore::default(),
    };
    let state = AppState {
        key_store: Arc::new(key_store),
    };

    Ok(state)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = load_config(args.config_file)?;

    instrumentation::init(&config.log)?;

    let state = init_state(&config).await?;

    let router = ApiRouter::new()
        .api_route("/health", get(health))
        .api_route("/sign", post(sign_transaction))
        .with_state(state);
    firefly_server::server::serve(&config.api, router).await
}
