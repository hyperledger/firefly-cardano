use balius_sdk::{wit::balius::app::kv, WorkerResult};
use serde::{Deserialize, Serialize};

pub fn get<D: for<'a> Deserialize<'a>>(key: &str) -> WorkerResult<Option<D>> {
    match kv::get_value(key) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(kv::KvError::NotFound(_)) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn set<S: Serialize>(key: &str, value: &S) -> WorkerResult<()> {
    kv::set_value(key, &serde_json::to_vec(value)?)?;
    Ok(())
}
