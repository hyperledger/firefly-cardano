use balius_sdk::{WorkerResult, wit::balius::app::kv};
use serde::{Deserialize, Serialize};

/// Retrieve a value from the KV store. Returns None if the value does not already exist.
pub fn get<D: for<'a> Deserialize<'a>>(key: &str) -> WorkerResult<Option<D>> {
    match kv::get_value(key) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(kv::KvError::NotFound(_)) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Store a value in the KV store.
pub fn set<S: Serialize>(key: &str, value: &S) -> WorkerResult<()> {
    kv::set_value(key, &serde_json::to_vec(value)?)?;
    Ok(())
}
