use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgPoolOptions};

const UPSERT_HANDLE_SQL: &str = include_str!("../sql/upsert_handle.sql");
const UPSERT_HANDLE_PARENT_SQL: &str = include_str!("../sql/upsert_handle_parent.sql");

pub struct NewHandle {
    pub handle_id: String,
    pub chain_id: i32,
    pub operator: String,
    pub caller: Option<String>,
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
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn upsert_handle(&self, handle: &NewHandle) -> Result<(), sqlx::Error> {
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

    pub async fn upsert_handle_parent(&self, child_id: &str, parent_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(UPSERT_HANDLE_PARENT_SQL)
            .bind(child_id)
            .bind(parent_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Returns the persisted subgraph poller skip, or 0 if absent.
    pub async fn load_skip(&self) -> Result<i64, sqlx::Error> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT skip FROM subgraph_poller_state")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|(s,)| s).unwrap_or(0))
    }

    pub async fn save_skip(&self, skip: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO subgraph_poller_state (skip) VALUES ($1)
             ON CONFLICT (id) DO UPDATE
                SET skip = EXCLUDED.skip, updated_at = now()",
        )
        .bind(skip)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
