use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgPoolOptions};

const UPSERT_HANDLE_SQL: &str = include_str!("../sql/upsert_handle.sql");
const UPSERT_HANDLE_PARENT_SQL: &str = include_str!("../sql/upsert_handle_parent.sql");

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

    /// Upsert a batch of handles in a single Postgres transaction.
    ///
    /// Used by the NATS consumer: one PG tx per NATS message — commit precedes
    /// ack. Idempotency is guaranteed by `ON CONFLICT (handle_id) DO UPDATE` in
    /// `sql/upsert_handle.sql` (COALESCE preserves columns written by sibling writers).
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
}

/// Bind a [`NewHandle`] to the canonical upsert statement.
///
/// Single source of truth for the bind list so the pool path ([`Db::upsert_handle`])
/// and the transaction path ([`Db::upsert_handles_in_tx`]) can't drift — the
/// returned [`sqlx::query::Query`] executes against any `sqlx::Executor`
/// (`&PgPool` or `&mut` transaction connection).
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
