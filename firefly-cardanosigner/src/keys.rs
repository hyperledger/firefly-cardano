use std::collections::HashMap;

use anyhow::Result;
use pallas_crypto::key::ed25519::SecretKey;
use pallas_wallet::PrivateKey;
use rand::thread_rng;

pub struct KeyStore {
    keys: HashMap<String, PrivateKey>,
}

const FAKE_ADDR: &str = "addr1_fake";

impl Default for KeyStore {
    fn default() -> Self {
        let key = SecretKey::new(thread_rng());
        let mut keys = HashMap::new();
        keys.insert(FAKE_ADDR.to_string(), PrivateKey::Normal(key));
        Self { keys }
    }
}

impl KeyStore {
    pub fn find_signing_key(&self, address: &str) -> Result<Option<&PrivateKey>> {
        Ok(self.keys.get(address))
    }
}
