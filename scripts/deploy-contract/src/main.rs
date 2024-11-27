use std::{path::PathBuf, process::Command};

use anyhow::{bail, Result};
use clap::Parser;
use firefly::FireflyCardanoClient;
use wit_component::ComponentEncoder;

mod firefly;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    contract_path: PathBuf,
    #[arg(long, default_value = "http://localhost:5018")]
    firefly_cardano_url: String,
    #[arg(long)]
    firefly_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let Some(name) = args.contract_path.file_name() else {
        bail!("couldn't find contract name");
    };
    let Some(name) = name.to_str() else {
        bail!("invalid contract name");
    };

    println!("Compiling {name}...");

    Command::new("cargo")
        .arg("build")
        .arg("--target")
        .arg("wasm32-unknown-unknown")
        .arg("--release")
        .current_dir(&args.contract_path)
        .exec()?;

    let filename = format!("{}.wasm", name.replace("-", "_"));
    let path = args
        .contract_path
        .join("target")
        .join("wasm32-unknown-unknown")
        .join("release")
        .join(filename);

    println!("Bundling {name} as WASM component...");
    let module = wat::Parser::new().parse_file(path)?;
    let component = ComponentEncoder::default()
        .validate(true)
        .module(&module)?
        .encode()?;
    let contract = hex::encode(&component);

    println!("Deploying {name} to FireFly...");
    let base_url = args
        .firefly_url
        .map(|u| format!("{u}/api/v1"))
        .unwrap_or(args.firefly_cardano_url);
    let firefly = FireflyCardanoClient::new(&base_url);
    firefly.deploy_contract(name, &contract).await?;

    Ok(())
}

trait CommandExt {
    fn exec(&mut self) -> Result<()>;
}

impl CommandExt for Command {
    fn exec(&mut self) -> Result<()> {
        let output = self.output()?;
        if !output.stderr.is_empty() {
            eprintln!("{}", String::from_utf8(output.stderr)?);
        }
        if !output.status.success() {
            bail!("command failed: {}", output.status);
        }
        Ok(())
    }
}
