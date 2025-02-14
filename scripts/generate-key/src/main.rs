use std::path::PathBuf;

use anyhow::Result;
use bech32::{Bech32, Hrp};
use clap::Parser;
use pallas_crypto::{hash::Hasher, key::ed25519::SecretKey};
use pallas_primitives::BoundedBytes;
use rand::thread_rng;
use serde::Serialize;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "infra/wallet")]
    wallet_dir: PathBuf,
    #[arg(short, long)]
    testnet: bool
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SigningKeyContents {
    #[serde(rename = "type")]
    type_: String,
    description: String,
    cbor_hex: String,
}

fn encode_address(public_key: &[u8], header: u8, hrp: &Hrp) -> Result<String> {
    let mut bytes = Vec::with_capacity(29);
    bytes.push(header);
    bytes.extend_from_slice(Hasher::<224>::hash(public_key).as_slice());
    Ok(bech32::encode::<Bech32>(*hrp, &bytes)?)
}

fn generate_signing_key(testnet: bool) -> Result<(String, SigningKeyContents)> {
    let mut rand = thread_rng();
    let private = SecretKey::new(&mut rand);

    let (header, hrp) = if testnet {
        (0x60, Hrp::parse_unchecked("addr_test"))
    } else {
        (0x61, Hrp::parse_unchecked("addr"))
    };

    let address = encode_address(private.public_key().as_ref(), header, &hrp)?;
    let key_bytes: Vec<u8> = unsafe { SecretKey::leak_into_bytes(private) }.into();
    let cbor_hex = {
        let mut bytes = vec![];
        minicbor::encode(BoundedBytes::from(key_bytes), &mut bytes).expect("infallible");
        hex::encode(bytes)
    };

    let contents =  SigningKeyContents {
        type_: "PaymentSigningKeyShelley_ed25519".into(),
        description: "Payment Signing Key".into(),
        cbor_hex,
    };
    Ok((address, contents))
}

fn main() -> Result<()> {
    let args = Args::try_parse()?;

    let (address, key) = generate_signing_key(args.testnet)?;

    let mut path = args.wallet_dir.join(&address);
    path.set_extension("skey");
    std::fs::write(&path, serde_json::to_string_pretty(&key)?)?;

    println!("New address: {address}");
    println!("Signing key has been saved to {}", path.display());

    Ok(())
}