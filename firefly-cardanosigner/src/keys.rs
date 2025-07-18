use std::{collections::HashMap, fs};

use anyhow::{Context, Result, anyhow, bail};
use pallas_addresses::{Address, PaymentKeyHash, ShelleyPaymentPart};
use pallas_crypto::{
    hash::Hasher,
    key::ed25519::{SecretKey, SecretKeyExtended},
};
use pallas_primitives::conway::PlutusData::BoundedBytes;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::{config::FileWalletConfig, private_key::PrivateKey};

#[derive(Default)]
pub struct KeyStore {
    keys: HashMap<PaymentKeyHash, PrivateKey>,
}

#[derive(Deserialize)]
struct SigningKeyContents {
    #[serde(rename = "type")]
    type_: String,
    #[serde(rename = "cborHex")]
    cbor_hex: String,
}

impl KeyStore {
    pub fn from_fs(config: &FileWalletConfig) -> Result<Self> {
        let mut keys = HashMap::new();

        let dir_entries = fs::read_dir(&config.path).context("could not read fileWallet.path")?;
        for dir_entry_res in dir_entries {
            let dir_entry = dir_entry_res.context("could not read directory entry")?;
            debug!("Loading key \"{}\"", dir_entry.file_name().display());
            let raw_contents = std::fs::read_to_string(dir_entry.path())?;
            let contents: SigningKeyContents = serde_json::from_str(&raw_contents)?;
            let cbor =
                hex::decode(&contents.cbor_hex).context("could not decode signing key hex")?;
            let key = match contents.type_.as_str() {
                "PaymentSigningKeyShelley_ed25519" => read_normal_key(cbor),
                "PaymentExtendedSigningKeyShelley_ed25519_bip32" => read_extended_key(cbor),
                type_ => Err(anyhow!("Unrecognized key type: {type_}")),
            }
            .context("could not read signing key")?;

            let hash = Hasher::<224>::hash(key.public_key().as_ref());
            keys.insert(hash, key);
        }
        if keys.is_empty() {
            warn!("No keys found in the wallet.");
        }
        Ok(Self { keys })
    }

    pub fn find_signing_key(&self, address: &str) -> Result<Option<&PrivateKey>> {
        debug!("finding signing key for {address}");
        let Address::Shelley(addr) = Address::from_bech32(address)? else {
            warn!("could not parse address {address} as shelley");
            return Ok(None);
        };
        let ShelleyPaymentPart::Key(hash) = addr.payment() else {
            warn!("address {address} does not have a key for payment");
            return Ok(None);
        };
        Ok(self.keys.get(hash))
    }
}

fn read_normal_key(cbor: Vec<u8>) -> Result<PrivateKey> {
    let bytes: Vec<u8> = match minicbor::decode(&cbor)? {
        BoundedBytes(bytes) => bytes.into(),
        _ => bail!("Invalid CBOR"),
    };
    if bytes.len() != SecretKey::SIZE {
        bail!(
            "secret keys must have {} bytes, this key has {}",
            SecretKey::SIZE,
            bytes.len()
        );
    }
    let mut key_bytes = [0; 32];
    key_bytes.copy_from_slice(&bytes);
    let secret_key: SecretKey = key_bytes.into();
    Ok(PrivateKey::Normal(secret_key))
}

fn read_extended_key(cbor: Vec<u8>) -> Result<PrivateKey> {
    let bytes: Vec<u8> = match minicbor::decode(&cbor)? {
        BoundedBytes(bytes) => bytes.into(),
        _ => bail!("Invalid CBOR"),
    };
    // The first 64 bytes are the signing key.
    // The next 32 bytes are the verification key, and the last 32 are the chain code.
    if bytes.len() != 128 {
        bail!(
            "extended secret keys must have 128 bytes, this key has {}",
            bytes.len()
        );
    }
    let mut key_bytes = [0; SecretKeyExtended::SIZE];
    key_bytes.copy_from_slice(&bytes[0..SecretKeyExtended::SIZE]);
    let secret_key = SecretKeyExtended::from_bytes(key_bytes)?;
    Ok(PrivateKey::Extended(secret_key))
}
