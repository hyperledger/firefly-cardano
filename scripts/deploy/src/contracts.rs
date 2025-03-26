use std::{path::Path, process::Command};

use anyhow::{Result, bail};
use wit_component::ComponentEncoder;

pub fn compile(contract_path: &Path) -> Result<String> {
    let Some(name) = contract_path.file_name() else {
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
        .current_dir(contract_path)
        .exec()?;

    let filename = format!("{}.wasm", name.replace("-", "_"));
    let path = contract_path
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
    Ok(hex::encode(&component))
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
