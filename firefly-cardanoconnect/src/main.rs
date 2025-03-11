use std::{path::PathBuf, sync::Arc, time::SystemTime};

use aide::axum::{
    routing::{get, post},
    ApiRouter,
};
use anyhow::Result;
use blockchain::BlockchainClient;
use clap::Parser;
use config::{load_config, CardanoConnectConfig};
use contracts::ContractManager;
use firefly_server::instrumentation;
use operations::OperationsManager;
use routes::{
    chain::get_chain_tip,
    health::health,
    operations::{deploy_contract, get_operation_status, invoke_contract},
    streams::{
        create_listener, create_stream, delete_listener, delete_stream, get_listener, get_stream,
        list_listeners, list_streams, update_stream,
    },
    transaction::submit_transaction,
    ws::handle_socket_upgrade,
};
use signer::CardanoSigner;
use streams::StreamManager;
use tokio::sync::watch;
use tracing::instrument;

mod blockchain;
mod config;
mod contracts;
mod operations;
mod persistence;
mod routes;
mod signer;
mod streams;
mod utils;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(short = 'f', long)]
    pub config_file: Vec<PathBuf>,
    #[clap(long)]
    pub mock_data: bool,
}

#[derive(Clone)]
struct AppState {
    pub blockchain: Arc<BlockchainClient>,
    pub operations: Arc<OperationsManager>,
    pub signer: Arc<CardanoSigner>,
    pub stream_manager: Arc<StreamManager>,
}

#[instrument(err(Debug))]
async fn init_state(config: &CardanoConnectConfig, mock_data: bool) -> Result<AppState> {
    let persistence = persistence::init(&config.persistence).await?;
    let signer = Arc::new(CardanoSigner::new(config)?);
    let blockchain = if mock_data {
        Arc::new(BlockchainClient::mock().await)
    } else {
        Arc::new(BlockchainClient::new(config).await?)
    };
    let contracts = if let Some(contracts) = &config.contracts {
        Arc::new(ContractManager::new(contracts, &blockchain).await?)
    } else {
        Arc::new(ContractManager::none())
    };

    let operation_update_sink = watch::Sender::new(SystemTime::now().into());
    let operations = Arc::new(OperationsManager::new(
        blockchain.clone(),
        contracts.clone(),
        persistence.clone(),
        signer.clone(),
        operation_update_sink.clone(),
    ));

    let state = AppState {
        blockchain: blockchain.clone(),
        operations,
        signer,
        stream_manager: Arc::new(
            StreamManager::new(blockchain, contracts, persistence, operation_update_sink).await?,
        ),
    };

    Ok(state)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = load_config(args.config_file)?;

    instrumentation::init(&config.log)?;

    let state = init_state(&config, args.mock_data).await?;

    let router = ApiRouter::new()
        .api_route("/health", get(health))
        .api_route("/contracts/deploy", post(deploy_contract))
        .api_route("/contracts/invoke", post(invoke_contract))
        .api_route("/transactions", post(submit_transaction))
        .api_route("/transactions/{id}", get(get_operation_status))
        .api_route("/eventstreams", post(create_stream).get(list_streams))
        .api_route(
            "/eventstreams/{streamId}",
            get(get_stream).patch(update_stream).delete(delete_stream),
        )
        .api_route(
            "/eventstreams/{streamId}/listeners",
            post(create_listener).get(list_listeners),
        )
        .api_route(
            "/eventstreams/{streamId}/listeners/{listenerId}",
            get(get_listener).delete(delete_listener),
        )
        .api_route("/chain/tip", get(get_chain_tip))
        .route("/ws", axum::routing::get(handle_socket_upgrade))
        .with_state(state);

    firefly_server::server::serve(&config.api, router).await
}
