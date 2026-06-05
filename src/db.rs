use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgPoolOptions};

const UPSERT_HANDLE_SQL: &str = include_str!("../sql/upsert_handle.sql");
const UPSERT_HANDLE_PARENT_SQL: &str = include_str!("../sql/upsert_handle_parent.sql");
const MARK_HANDLE_RESOLVED_SQL: &str = include_str!("../sql/mark_handle_resolved.sql");

#[derive(Debug)]
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

#[derive(Clone)]
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
        bind_upsert_handle(handle).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn upsert_handles_in_tx(&self, handles: &[NewHandle]) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        for h in handles {
            bind_upsert_handle(h).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_handle_parent(
        &self,
        child_id: &str,
        parent_id: &str,
    ) -> Result<(), sqlx::Error> {
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

    pub async fn fetch_unresolved_handles(
        &self,
        limit: i64,
    ) -> Result<Vec<(String, i32)>, sqlx::Error> {
        let rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT handle_id, chain_id
             FROM handles
             WHERE NOT processed_by_s3
             ORDER BY block_timestamp DESC NULLS FIRST
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn mark_resolved_by_s3(
        &self,
        resolved: &[(String, DateTime<Utc>)],
    ) -> Result<u64, sqlx::Error> {
        if resolved.is_empty() {
            return Ok(0);
        }
        let handle_ids: Vec<&str> = resolved.iter().map(|(id, _)| id.as_str()).collect();
        let resolved_ats: Vec<DateTime<Utc>> = resolved.iter().map(|(_, ts)| *ts).collect();
        let result = sqlx::query(MARK_HANDLE_RESOLVED_SQL)
            .bind(&handle_ids)
            .bind(&resolved_ats)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

/// ------------- Helpers -------------
fn bind_upsert_handle(
    handle: &NewHandle,
) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments> {
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
}
