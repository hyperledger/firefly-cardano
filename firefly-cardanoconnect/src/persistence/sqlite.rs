use std::{path::PathBuf, time::Duration};

use anyhow::{bail, Result};
use async_trait::async_trait;
use firefly_server::apitypes::{ApiError, ApiResult};
use rusqlite::{params, types::FromSql, Row, ToSql};
use serde::Deserialize;
use tokio_rusqlite::Connection;

use crate::{
    operations::{Operation, OperationId, OperationStatus},
    streams::{BlockInfo, BlockRecord, Listener, ListenerId, Stream, StreamCheckpoint, StreamId},
};

use super::Persistence;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("db/migrations/sqlite");
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqliteConfig {
    path: PathBuf,
}

pub struct SqlitePersistence {
    conn: Connection,
}

impl SqlitePersistence {
    pub async fn new(config: &SqliteConfig) -> Result<Self> {
        let conn = Connection::open(&config.path).await?;
        conn.call_unwrap(|c| embedded::migrations::runner().run(c))
            .await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl Persistence for SqlitePersistence {
    async fn write_stream(&self, stream: &Stream) -> ApiResult<()> {
        let stream = stream.clone();
        self.conn
            .call_unwrap(move |c| {
                let existing = c
                    .prepare_cached("SELECT id FROM streams WHERE name = ?1")?
                    .query_map([&stream.name], |res| {
                        let id: String = res.get("id")?;
                        let id: StreamId = id.into();
                        Ok(id)
                    })?
                    .next()
                    .transpose()?;
                if existing.clone().is_some_and(|id| id != stream.id) {
                    return ApiResult::Err(ApiError::conflict(
                        "stream with this name already exists",
                    ));
                }
                c.prepare_cached(
                    "INSERT INTO streams (id, name, batch_size, batch_timeout)
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name,
                        batch_size=excluded.batch_size,
                        batch_timeout=excluded.batch_timeout",
                )?
                .execute(params![
                    stream.id.to_string(),
                    stream.name,
                    stream.batch_size,
                    SqliteDuration(stream.batch_timeout)
                ])?;
                Ok(())
            })
            .await
    }
    async fn read_stream(&self, id: &StreamId) -> ApiResult<Option<Stream>> {
        let id = id.clone();
        self.conn
            .call_unwrap(move |c| {
                let Some(stream) = c
                    .prepare_cached(
                        "SELECT id, name, batch_size, batch_timeout
                        FROM streams
                        WHERE id = ?1",
                    )?
                    .query_and_then([id.to_string()], parse_stream)?
                    .next()
                else {
                    return Ok(None);
                };
                Ok(Some(stream?))
            })
            .await
    }
    async fn delete_stream(&self, id: &StreamId) -> Result<()> {
        let id = id.clone();
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached("DELETE FROM streams WHERE id = ?1")?
                    .execute([id.to_string()])?;
                Ok(())
            })
            .await
    }
    async fn list_streams(
        &self,
        after: Option<StreamId>,
        limit: Option<usize>,
    ) -> Result<Vec<Stream>> {
        let after = after.map(|id| id.to_string()).unwrap_or_default();
        let limit = limit.map(|l| l as u32).unwrap_or(u32::MAX);
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached(
                    "SELECT id, name, batch_size, batch_timeout
                    FROM streams
                    WHERE id > ?1
                    ORDER BY id
                    LIMIT ?2",
                )?
                .query_and_then(params![after, limit], parse_stream)?
                .collect()
            })
            .await
    }

    async fn write_listener(&self, listener: &Listener) -> ApiResult<()> {
        if self.read_stream(&listener.stream_id).await?.is_none() {
            return Err(ApiError::not_found("No stream found with that ID"));
        }
        let listener = listener.clone();
        let listener_type = serde_json::to_string(&listener.listener_type)?;
        let stream_id = listener.stream_id.to_string();
        let filters = serde_json::to_string(&listener.filters)?;
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached(
                    "INSERT INTO listeners (id, name, type, stream_id, filters)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name,
                        type=excluded.type,
                        stream_id=excluded.stream_id,
                        filters=excluded.filters",
                )?
                .execute(params![
                    listener.id.to_string(),
                    listener.name,
                    listener_type,
                    stream_id,
                    filters
                ])?;
                Ok(())
            })
            .await
    }
    async fn read_listener(
        &self,
        stream_id: &StreamId,
        listener_id: &ListenerId,
    ) -> ApiResult<Option<Listener>> {
        if self.read_stream(stream_id).await?.is_none() {
            return Err(ApiError::not_found("No stream found with that ID"));
        }
        let listener_id = listener_id.clone();
        self.conn
            .call_unwrap(move |c| {
                let Some(listener) = c
                    .prepare_cached(
                        "SELECT id, name, type, stream_id, filters
                        FROM listeners
                        WHERE id = ?1",
                    )?
                    .query_and_then([listener_id.to_string()], parse_listener)?
                    .next()
                else {
                    return Ok(None);
                };
                Ok(Some(listener?))
            })
            .await
    }
    async fn delete_listener(&self, stream_id: &StreamId, listener_id: &ListenerId) -> Result<()> {
        let stream_id = stream_id.clone();
        let listener_id = listener_id.clone();
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached("DELETE FROM listeners WHERE id = ?1 AND stream_id = ?2")?
                    .execute([listener_id.to_string(), stream_id.to_string()])?;
                c.prepare_cached("DELETE FROM block_records WHERE listener_id = ?1")?
                    .execute([listener_id.to_string()])?;
                Ok(())
            })
            .await
    }

    async fn list_listeners(
        &self,
        stream_id: &StreamId,
        after: Option<ListenerId>,
        limit: Option<usize>,
    ) -> ApiResult<Vec<Listener>> {
        if self.read_stream(stream_id).await?.is_none() {
            return Err(ApiError::not_found("No stream found with that ID"));
        }
        let stream_id = stream_id.clone();
        let after = after.map(|id| id.to_string()).unwrap_or_default();
        let limit = limit.map(|l| l as u32).unwrap_or(u32::MAX);
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached(
                    "SELECT id, name, type, stream_id, filters
                    FROM listeners
                    WHERE stream_id = ?1
                    AND id > ?2
                    ORDER BY id
                    LIMIT ?3",
                )?
                .query_and_then(params![stream_id.to_string(), after, limit], parse_listener)?
                .collect()
            })
            .await
    }
    async fn write_checkpoint(&self, checkpoint: &StreamCheckpoint) -> ApiResult<()> {
        let stream_id = checkpoint.stream_id.to_string();
        let listeners = serde_json::to_string(&checkpoint.listeners)?;
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached(
                    "INSERT INTO stream_checkpoints (stream_id, listeners)
                    VALUES (?1, ?2)
                    ON CONFLICT (stream_id) DO UPDATE SET
                        listeners=excluded.listeners",
                )?
                .execute([stream_id, listeners])?;
                Ok(())
            })
            .await
    }
    async fn read_checkpoint(&self, stream_id: &StreamId) -> ApiResult<Option<StreamCheckpoint>> {
        let stream_id = stream_id.clone();
        self.conn
            .call_unwrap(move |c| {
                let Some(checkpoint) = c
                    .prepare_cached(
                        "SELECT stream_id, listeners
                        FROM stream_checkpoints
                        WHERE stream_id = ?1",
                    )?
                    .query_and_then([stream_id.to_string()], parse_checkpoint)?
                    .next()
                else {
                    return Ok(None);
                };
                Ok(Some(checkpoint?))
            })
            .await
    }

    async fn load_history(&self, listener: &ListenerId) -> Result<Vec<BlockRecord>> {
        let listener = listener.clone();
        self.conn
            .call_unwrap(move |c| {
                c.prepare_cached(
                    "SELECT block_height, block_slot, block_hash, parent_hash, transaction_hashes, transactions, rolled_back
                    FROM block_records
                    WHERE listener_id = ?1
                    ORDER BY id",
                )?
                .query_and_then(params![listener.to_string()], parse_block_record)?
                .collect()
            })
            .await
    }
    async fn save_block_records(
        &self,
        listener: &ListenerId,
        new_records: Vec<BlockRecord>,
    ) -> Result<()> {
        if new_records.is_empty() {
            return Ok(());
        }

        let listener_id = listener.to_string();
        self.conn.call_unwrap(move |c| {
            let tx = c.transaction()?;
            let mut insert = tx.prepare_cached(
                "INSERT INTO block_records (listener_id, block_height, block_slot, block_hash, parent_hash, transaction_hashes, transactions, rolled_back)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;

            for record in new_records {
                let block_height = record.block.block_height;
                let block_slot = record.block.block_slot;
                let block_hash = record.block.block_hash.clone();
                let parent_hash = record.block.parent_hash.clone();
                let transaction_hashes = serde_json::to_string(&record.block.transaction_hashes)?;
                let transactions = {
                    let mut bytes = vec![];
                    minicbor::encode(&record.block.transactions, &mut bytes).expect("infallible");
                    bytes
                };
                let rolled_back = record.rolled_back;

                insert.execute(params![listener_id, block_height, block_slot, block_hash, parent_hash, transaction_hashes, transactions, rolled_back])?;
            }

            drop(insert);
            tx.commit()?;
            Ok(())
        }).await
    }

    async fn write_operation(&self, op: &Operation) -> ApiResult<()> {
        let op = op.clone();
        self.conn
            .call_unwrap(move |c| {
                let status = op.status.name();
                let error_message = op.status.error_message();
                c.prepare_cached(
                    "INSERT INTO operations (id, status, error_message, tx_id)
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT(id) DO UPDATE SET
                        status=excluded.status,
                        error_message=excluded.error_message,
                        tx_id=excluded.tx_id",
                )?
                .execute(params![
                    op.id.to_string(),
                    status,
                    error_message,
                    op.tx_id,
                ])?;
                Ok(())
            })
            .await
    }

    async fn read_operation(&self, id: &OperationId) -> ApiResult<Option<Operation>> {
        let id = id.clone();
        self.conn
            .call_unwrap(move |c| {
                let Some(op) = c
                    .prepare_cached(
                        "SELECT id, status, error_message, tx_id
                        FROM operations
                        WHERE id = ?1",
                    )?
                    .query_and_then([id.to_string()], parse_operation)?
                    .next()
                else {
                    return Ok(None);
                };
                Ok(Some(op?))
            })
            .await
    }
}

fn parse_stream(row: &Row) -> Result<Stream> {
    let id: String = row.get("id")?;
    let name: String = row.get("name")?;
    let batch_size: usize = row.get("batch_size")?;
    let batch_timeout: SqliteDuration = row.get("batch_timeout")?;
    Ok(Stream {
        id: id.into(),
        name,
        batch_size,
        batch_timeout: batch_timeout.0,
    })
}

fn parse_listener(row: &Row) -> ApiResult<Listener> {
    let id: String = row.get("id")?;
    let name: String = row.get("name")?;
    let listener_type: String = row.get("type")?;
    let stream_id: String = row.get("stream_id")?;
    let filters: String = row.get("filters")?;
    Ok(Listener {
        id: id.into(),
        name,
        listener_type: serde_json::from_str(&listener_type)?,
        stream_id: stream_id.into(),
        filters: serde_json::from_str(&filters)?,
    })
}

fn parse_checkpoint(row: &Row) -> Result<StreamCheckpoint> {
    let stream_id: String = row.get("stream_id")?;
    let listeners: String = row.get("listeners")?;
    Ok(StreamCheckpoint {
        stream_id: stream_id.into(),
        listeners: serde_json::from_str(&listeners)?,
    })
}

fn parse_block_record(row: &Row) -> Result<BlockRecord> {
    let block_height: Option<u64> = row.get("block_height")?;
    let block_slot: Option<u64> = row.get("block_slot")?;
    let block_hash: String = row.get("block_hash")?;
    let parent_hash: Option<String> = row.get("parent_hash")?;
    let transaction_hashes = {
        let raw: String = row.get("transaction_hashes")?;
        serde_json::from_str(&raw)?
    };
    let transactions = {
        let raw: Vec<u8> = row.get("transactions")?;
        minicbor::decode(&raw)?
    };
    let rolled_back: bool = row.get("rolled_back")?;
    Ok(BlockRecord {
        block: BlockInfo {
            block_height,
            block_slot,
            block_hash,
            parent_hash,
            transaction_hashes,
            transactions,
        },
        rolled_back,
    })
}

fn parse_operation(row: &Row) -> Result<Operation> {
    let id: String = row.get("id")?;
    let error_message: Option<String> = row.get("error_message")?;
    let status = match error_message {
        Some(error) => OperationStatus::Failed(error),
        None => match row.get::<&str, String>("status")?.as_str() {
            "Succeeded" => OperationStatus::Succeeded,
            "Pending" => OperationStatus::Pending,
            "Failed" => OperationStatus::Failed("".into()),
            other => bail!("unrecognized status {other}"),
        },
    };
    let tx_id: Option<String> = row.get("tx_id")?;
    Ok(Operation {
        id: id.into(),
        status,
        tx_id,
    })
}

struct SqliteDuration(Duration);

impl ToSql for SqliteDuration {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let str = self.0.as_millis().to_string();
        Ok(str.into())
    }
}

impl FromSql for SqliteDuration {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let str = value.as_str()?;
        let millis: u128 = match str.parse() {
            Ok(n) => n,
            Err(err) => return Err(rusqlite::types::FromSqlError::Other(Box::new(err))),
        };
        let secs = (millis / 1_000) as u64;
        let nanos = (millis % 1_000) as u32 * 1_000_000;
        let duration = Duration::new(secs, nanos);
        Ok(SqliteDuration(duration))
    }
}
