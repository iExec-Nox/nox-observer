use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgPoolOptions};

pub struct NewHandle {
    pub handle_id: String,
    pub chain_id: i32,
    pub operator: String,
    pub caller: String,
    pub tx_hash: String,
    pub block_timestamp: DateTime<Utc>,
    pub block_number: i64,
    pub resolved_at: Option<DateTime<Utc>>,
    pub processed_by_subgraph: bool,
    pub processed_by_s3: bool,
    pub processed_by_nats: bool,
}

pub struct Repository {
    pool: PgPool,
}

impl Repository {
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn upsert_handle(&self, handle: &NewHandle) -> Result<()> {
        sqlx::query_file!(
            "sql/upsert_handle.sql",
            handle.handle_id,
            handle.chain_id,
            handle.operator,
            handle.caller,
            handle.tx_hash,
            handle.block_timestamp,
            handle.block_number,
            handle.resolved_at,
            handle.processed_by_subgraph,
            handle.processed_by_s3,
            handle.processed_by_nats,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_handle_parent(&self, child_id: &str, parent_id: &str) -> Result<()> {
        sqlx::query_file!("sql/upsert_handle_parent.sql", child_id, parent_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
