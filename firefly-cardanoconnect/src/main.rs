use std::{path::PathBuf, sync::Arc};

use aide::axum::{
    routing::{get, post},
    ApiRouter,
};
use anyhow::Result;
use blockchain::BlockchainClient;
use clap::Parser;
use config::load_config;
use persistence::Persistence;
use routes::{
    health::health,
    streams::{create_stream, delete_stream, get_stream, list_streams, update_stream},
    transaction::submit_transaction,
    ws::handle_socket_upgrade,
};
use signer::CardanoSigner;
use streams::StreamManager;

mod blockchain;
mod config;
mod persistence;
mod routes;
mod signer;
mod streams;
mod utils;

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
    pub stream_manager: Arc<StreamManager>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config_file = args.config_file.as_deref();
    let config = load_config(config_file)?;

    let persistence = Arc::new(Persistence::default());

    let state = AppState {
        blockchain: Arc::new(BlockchainClient::new(&config).await?),
        signer: Arc::new(CardanoSigner::new(&config)?),
        stream_manager: Arc::new(StreamManager::new(persistence)),
    };

    let router = ApiRouter::new()
        .api_route("/api/health", get(health))
        .api_route("/api/transactions", post(submit_transaction))
        .api_route("/api/eventstreams", post(create_stream).get(list_streams))
        .api_route(
            "/api/eventstreams/:streamId",
            get(get_stream).patch(update_stream).delete(delete_stream),
        )
        .route("/api/ws", axum::routing::get(handle_socket_upgrade))
        .with_state(state);

    firefly_server::server::serve(&config.api, router).await
}
