use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;
use balius_runtime::kv::{KvError, KvProvider, Payload};
use tokio_rusqlite::Connection;

pub struct SqliteKv {
    conn: Connection,
}

impl SqliteKv {
    pub async fn new(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).await?;
        Ok(Self { conn })
    }

    pub async fn init(&self, contract: &str) -> Result<()> {
        let table_name = self.table_name(contract);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\" (
                \"key\" TEXT NOT NULL PRIMARY KEY,
                \"value\" BLOB NOT NULL
            )"
        );
        self.conn.call_unwrap(move |c| c.execute(&sql, [])).await?;
        Ok(())
    }

    fn table_name(&self, contract: &str) -> String {
        format!("kv_{}", hex::encode(contract))
    }
}

#[async_trait]
impl KvProvider for SqliteKv {
    async fn get_value(&mut self, worker_id: &str, key: String) -> Result<Payload, KvError> {
        let table = self.table_name(worker_id);
        let sql = format!(
            "SELECT \"value\"
            FROM \"{table}\"
            WHERE \"key\" = ?1"
        );
        let k = key.clone();
        let result: Option<Payload> = self
            .conn
            .call_unwrap(move |c| match c.prepare_cached(&sql)?.query([k])?.next()? {
                Some(x) => Ok(Some(x.get("value")?)),
                None => Ok(None),
            })
            .await
            .map_err(|err: rusqlite::Error| KvError::Upstream(err.to_string()))?;
        match result {
            Some(value) => Ok(value),
            None => Err(KvError::NotFound(key)),
        }
    }

    async fn set_value(
        &mut self,
        worker_id: &str,
        key: String,
        value: Payload,
    ) -> Result<(), KvError> {
        let table = self.table_name(worker_id);
        let sql = format!(
            "INSERT INTO \"{table}\" (\"key\", \"value\")
            VALUES (?1, ?2)
            ON CONFLICT(\"key\") DO UPDATE SET \"value\" = excluded.\"value\""
        );
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached(&sql)?
                    .execute(rusqlite::params![key, value])?;
                Ok(())
            })
            .await
            .map_err(|err: rusqlite::Error| KvError::Upstream(err.to_string()))
    }

    async fn list_values(
        &mut self,
        worker_id: &str,
        prefix: String,
    ) -> Result<Vec<String>, KvError> {
        let table = self.table_name(worker_id);
        let sql = format!(
            "SELECT \"key\"
            FROM \"{table}\"
            WHERE \"key\" LIKE ?1
            ORDER BY \"key\""
        );
        let result: rusqlite::Result<Vec<String>> = self
            .conn
            .call_unwrap(move |c| {
                c.prepare_cached(&sql)?
                    .query_and_then([format!("{prefix}%")], |row| row.get::<&str, String>("key"))?
                    .collect()
            })
            .await;
        result.map_err(|err| KvError::Upstream(err.to_string()))
    }
}
