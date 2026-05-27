use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgPoolOptions};

const UPSERT_HANDLE_SQL: &str = include_str!("../../sql/upsert_handle.sql");
const UPSERT_HANDLE_PARENT_SQL: &str = include_str!("../../sql/upsert_handle_parent.sql");

pub struct NewHandle {
    pub handle_id: String,
    pub chain_id: i32,
    pub operator: String,
    pub caller: Option<String>,
    // TODO: make non-Option once subgraph indexes ValidateInputProof.
    pub tx_hash: Option<String>,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub block_number: Option<i64>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub processed_by_subgraph: bool,
    pub processed_by_s3: bool,
    pub processed_by_nats: bool,
}

pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn upsert_handle(&self, handle: &NewHandle) -> Result<()> {
        sqlx::query(UPSERT_HANDLE_SQL)
            .bind(&handle.handle_id)
            .bind(handle.chain_id)
            .bind(&handle.operator)
            .bind(&handle.caller)
            .bind(&handle.tx_hash)
            .bind(handle.block_timestamp)
            .bind(handle.block_number)
            .bind(handle.resolved_at)
            .bind(handle.processed_by_subgraph)
            .bind(handle.processed_by_s3)
            .bind(handle.processed_by_nats)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_handle_parent(&self, child_id: &str, parent_id: &str) -> Result<()> {
        sqlx::query(UPSERT_HANDLE_PARENT_SQL)
            .bind(child_id)
            .bind(parent_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
