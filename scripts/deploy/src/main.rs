use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use clients::{CreateApiRequest, DeployContractRequest, FireflyClient};

mod clients;
mod contracts;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    contract_path: PathBuf,
    #[arg(long, default_value = "http://localhost:5000")]
    firefly_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;

    let contract = contracts::compile(&args.contract_path)?;

    let client = FireflyClient::new(&args.firefly_url);

    let definition_path = args.contract_path.join("contract.json");
    let definition_str =
        std::fs::read_to_string(definition_path).context("Could not find contract.json")?;
    let request = DeployContractRequest {
        contract,
        definition: serde_json::from_str(&definition_str)?,
    };

    println!("Deploying contract to {}", args.firefly_url);
    let location = client.deploy_contract(&request).await?;
    println!("location: {}", serde_json::to_string(&location)?);
    let interface = client.deploy_interface(&request.definition).await?;
    println!("interface: {}", serde_json::to_string(&interface)?);
    let urls = client
        .deploy_api(&CreateApiRequest {
            name: format!("{}-{}", request.definition.name, request.definition.version),
            location,
            interface,
        })
        .await?;
    println!("API available at {}", urls.api);
    println!("Swagger UI at {}", urls.ui);

    Ok(())
}
