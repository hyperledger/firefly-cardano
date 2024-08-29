use std::{path::PathBuf, sync::Arc};

use aide::axum::{
    routing::{get, post},
    ApiRouter,
};
use anyhow::Result;
use blockchain::BlockchainClient;
use clap::Parser;
use config::load_config;
use routes::{health::health, transaction::submit_transaction, ws::handle_socket_upgrade};
use signer::CardanoSigner;

mod blockchain;
mod config;
mod routes;
mod signer;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(short = 'f', long)]
    pub config_file: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    pub blockchain: Arc<BlockchainClient>,
    pub signer: Arc<CardanoSigner>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config_file = args.config_file.as_deref();
    let config = load_config(config_file)?;

    let state = AppState {
        blockchain: Arc::new(BlockchainClient::new(&config).await?),
        signer: Arc::new(CardanoSigner::new(&config)?),
    };

    let router = ApiRouter::new()
        .api_route("/api/health", get(health))
        .api_route("/api/transactions", post(submit_transaction))
        .route("/api/ws", axum::routing::get(handle_socket_upgrade))
        .with_state(state);

    firefly_server::server::serve(&config.api, router).await
}
