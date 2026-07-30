use chrono::{DateTime, Utc};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};

use crate::config::DatabaseConfig;

const UPSERT_HANDLE_SQL: &str = include_str!("../sql/upsert_handle.sql");
const UPSERT_HANDLE_PARENT_SQL: &str = include_str!("../sql/upsert_handle_parent.sql");
const MARK_HANDLE_RESOLVED_SQL: &str = include_str!("../sql/mark_handle_resolved.sql");
const HANDLE_STATS_SQL: &str = include_str!("../sql/handle_stats.sql");

/// Result of `sql/handle_stats.sql`: not-yet-resolved handles for a chain,
/// partitioned by the monitoring grace period into `unresolved` (past grace) and
/// `resolving` (within grace, or not yet timestamped by the subgraph), plus two
/// reference figures. All `COUNT`/`COUNT... FILTER` columns are never `NULL`;
/// `MIN`/`MAX` columns are `None` when the corresponding bucket has zero rows
/// or when every matching row has a NULL `block_number`, since `MIN`/`MAX` return
/// `NULL` in that case too.
#[derive(Debug, sqlx::FromRow)]
pub struct HandleStats {
    pub unresolved_count: i64,
    pub unresolved_oldest_block: Option<i64>,
    pub unresolved_newest_block: Option<i64>,
    pub resolving_count: i64,
    pub resolving_oldest_block: Option<i64>,
    pub resolving_newest_block: Option<i64>,
    pub resolved_but_not_seen_by_subgraph: i64,
    pub ignored_count: i64,
    pub latest_seen_block: Option<i64>,
}

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
    /// Builds the pooled Postgres connection.
    ///
    /// When TLS is enabled the connection uses `sslmode=require`: traffic is
    /// encrypted but the server certificate is not verified. When disabled the
    /// connection is plaintext for local development.
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, sqlx::Error> {
        let mut opts = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .username(&config.user)
            .password(&config.password)
            .database(&config.dbname);
        if config.tls_enabled {
            opts = opts.ssl_mode(PgSslMode::Require);
        } else {
            opts = opts.ssl_mode(PgSslMode::Disable);
        }
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }

    /// Borrows the underlying pool. Used by the `sqlx::migrate!` macro, which
    /// needs to acquire its own dedicated connection to hold an advisory lock
    /// across the whole migration run.
    pub fn pool(&self) -> &PgPool {
        &self.pool
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

    /// Returns the cursor block persisted for `chain_id`'s subgraph poller, or
    /// `None` if no cursor has been saved yet.
    pub async fn load_cursor_block(&self, chain_id: i32) -> Result<Option<i64>, sqlx::Error> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT cursor_block FROM subgraph_poller_state WHERE chain_id = $1")
                .bind(chain_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(s,)| s))
    }

    pub async fn save_cursor_block(
        &self,
        chain_id: i32,
        cursor_block: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO subgraph_poller_state (chain_id, cursor_block) VALUES ($1, $2)
             ON CONFLICT (chain_id) DO UPDATE
                SET cursor_block = EXCLUDED.cursor_block, updated_at = now()",
        )
        .bind(chain_id)
        .bind(cursor_block)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_unresolved_handles(
        &self,
        chain_ids: &[i32],
        limit: i64,
    ) -> Result<Vec<(String, i32, Option<DateTime<Utc>>)>, sqlx::Error> {
        if chain_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<(String, i32, Option<DateTime<Utc>>)> = sqlx::query_as(
            "SELECT handle_id, chain_id, block_timestamp
             FROM handles
             WHERE NOT processed_by_s3
               AND NOT ignored
               AND chain_id = ANY($1)
             ORDER BY block_timestamp DESC NULLS FIRST
             LIMIT $2",
        )
        .bind(chain_ids)
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

    /// Counts not-yet-resolved handles for `chain_id`, partitioned by
    /// `grace_deadline` into `unresolved` (past grace) and `resolving` (within
    /// grace, or not yet timestamped), plus the resolved-but-unseen-by-subgraph
    /// and latest-seen-block reference figures. Pass `Utc::now() - grace_period`
    /// as `grace_deadline`.
    pub async fn fetch_handle_stats(
        &self,
        chain_id: i32,
        grace_deadline: DateTime<Utc>,
    ) -> Result<HandleStats, sqlx::Error> {
        sqlx::query_as(HANDLE_STATS_SQL)
            .bind(chain_id)
            .bind(grace_deadline)
            .fetch_one(&self.pool)
            .await
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
