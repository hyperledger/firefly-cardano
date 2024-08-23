use std::path::PathBuf;

use aide::axum::{routing::get, ApiRouter, IntoApiResponse};
use anyhow::Result;
use axum::Json;
use clap::Parser;
use config::load_config;

mod config;

async fn health() -> impl IntoApiResponse {
    Json("Hello, world!")
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

    let router = ApiRouter::new().api_route("/api/health", get(health));
    firefly_server::server::serve(&config.api, router).await
}
