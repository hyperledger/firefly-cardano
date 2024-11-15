use std::{ffi::OsString, fs, process::Command};

use anyhow::{bail, Result};
use serde::Deserialize;

use crate::Args;

pub fn build_transaction(args: &Args) -> Result<String> {
    let utxos = list_utxos(args)?;
    println!(
        "balance: {} lovelace",
        utxos.iter().map(|u| u.amount_lovelace).sum::<u64>()
    );

    let utxos_to_spend = choose_utxos_to_spend(utxos, args.amount)?;
    build_tx(utxos_to_spend, args)
}

fn exec_cardano_cli<'a, IC, IA>(program: &String, command: IC, args: IA) -> Result<String>
where
    IC: IntoIterator<Item = &'a str>,
    IA: IntoIterator<Item = (&'a str, OsString)>,
{
    let mut base = Command::new(program);
    let mut command = base.args(command);
    for (key, value) in args {
        command = command.arg(key).arg(value);
    }
    let output = command.output()?;
    if !output.stderr.is_empty() {
        eprintln!("{}", String::from_utf8(output.stderr)?);
    }
    if !output.status.success() {
        bail!("request failed")
    }

    Ok(String::from_utf8(output.stdout)?)
}

#[derive(Debug)]
struct Utxo {
    tx_hash: String,
    tx_ix: usize,
    amount_lovelace: u64,
}

fn parse_utxo(line: &str) -> Result<Utxo> {
    let mut iter = line.split_whitespace();
    let Some(hash) = iter.next() else {
        bail!("missing hash: {line}");
    };
    let Some(ix) = iter.next() else {
        bail!("missing ix: {line}");
    };
    let Some(amount) = iter.next() else {
        bail!("missing lovelace: {line}")
    };
    if !matches!(iter.next(), Some("lovelace")) {
        bail!("unexpected format: {line}");
    }
    Ok(Utxo {
        tx_hash: hash.to_string(),
        tx_ix: ix.parse()?,
        amount_lovelace: amount.parse()?,
    })
}

fn list_utxos(args: &Args) -> Result<Vec<Utxo>> {
    let text = exec_cardano_cli(
        &args.cardano_cli,
        ["query", "utxo"],
        [
            ("--address", (&args.addr_from).into()),
            ("--testnet-magic", args.testnet_magic.to_string().into()),
            ("--socket-path", (&args.socket_path).into()),
        ],
    )?;
    let mut utxos = vec![];
    for line in text.lines().skip(2) {
        match parse_utxo(line) {
            Ok(utxo) => utxos.push(utxo),
            Err(err) => println!("{err}"),
        }
    }
    Ok(utxos)
}

fn choose_utxos_to_spend(utxos: Vec<Utxo>, amount: u64) -> Result<Vec<Utxo>> {
    if amount == 0 {
        bail!("spend somethin, will ya?");
    }
    let mut seen_so_far = 0;
    let mut to_spend = vec![];
    for utxo in utxos {
        seen_so_far += utxo.amount_lovelace;
        to_spend.push(utxo);
        if seen_so_far >= amount {
            return Ok(to_spend);
        }
    }
    bail!("Not enough funds (have {seen_so_far}, need {amount})");
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CborHexWrapper {
    cbor_hex: String,
}
fn build_tx(utxos: Vec<Utxo>, args: &Args) -> Result<String> {
    let mut cmd_args = vec![
        ("--testnet-magic", args.testnet_magic.to_string().into()),
        ("--socket-path", (&args.socket_path).into()),
    ];
    for utxo in utxos {
        cmd_args.push(("--tx-in", format!("{}#{}", utxo.tx_hash, utxo.tx_ix).into()));
    }
    cmd_args.push((
        "--tx-out",
        format!("{}+{}", args.addr_to, args.amount).into(),
    ));
    cmd_args.push(("--change-address", (&args.addr_from).into()));
    cmd_args.push(("--out-file", "temp.json".into()));

    exec_cardano_cli(
        &args.cardano_cli,
        ["conway", "transaction", "build"],
        cmd_args,
    )?;
    let text = fs::read_to_string("temp.json")?;
    fs::remove_file("temp.json")?;
    let wrapper: CborHexWrapper = serde_json::de::from_str(&text)?;
    Ok(wrapper.cbor_hex)
}
