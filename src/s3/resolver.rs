use std::time::Duration;

use tokio::time::{MissedTickBehavior, interval};
use tracing::{error, info, warn};

use super::client::S3Client;
use crate::db::Db;
use crate::errors::S3ResolverError;

/// Periodically resolves handles whose ciphertext has appeared in S3, setting
/// `resolved_at` + `processed_by_s3`
pub struct S3Resolver {
    s3: S3Client,
    db: Db,
    poll_interval: Duration,
    batch_size: i64,
}

impl S3Resolver {
    pub fn new(s3: S3Client, db: Db, poll_interval: Duration, batch_size: i64) -> Self {
        Self {
            s3,
            db,
            poll_interval,
            batch_size,
        }
    }

    pub async fn run(self) -> Result<(), S3ResolverError> {
        info!(
            poll_interval = ?self.poll_interval,
            batch_size = self.batch_size,
            chains = ?self.s3.configured_chains(),
            "s3 resolver starting"
        );
        let mut ticker = interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            // Errors never break the loop: the next tick is the natural retry.
            // Transient failures (network, 5xx) log at warn, permanent ones at
            // error so a misconfiguration stays visible without halting the loop.
            if let Err(e) = self.resolve_once().await {
                if e.is_transient() {
                    warn!("s3 resolve tick failed (transient): {e}");
                } else {
                    error!("s3 resolve tick failed: {e}");
                }
            }
        }
    }

    /// Fetch one batch of unresolved handles, keep those whose ciphertext is
    /// already present in S3, and mark them resolved.
    ///
    /// `resolved_at` is set from the S3 upload time, which may predate the
    /// on-chain `block_timestamp`; the DB clamps it with
    /// `GREATEST(resolved_at, block_timestamp)` to keep the resolution time
    /// monotonic relative to emission.
    async fn resolve_once(&self) -> Result<(), S3ResolverError> {
        let chains = self.s3.configured_chains();
        let candidates = self
            .db
            .fetch_unresolved_handles(&chains, self.batch_size)
            .await?;
        if candidates.is_empty() {
            return Ok(());
        }
        let present = self.s3.filter_present(&candidates).await?;
        if present.is_empty() {
            return Ok(());
        }
        let resolved: Vec<_> = present
            .into_iter()
            .map(|(handle_id, s3_last_modified, _)| (handle_id, s3_last_modified))
            .collect();
        let n = self.db.mark_resolved_by_s3(&resolved).await?;
        info!(
            resolved = n,
            fetched = candidates.len(),
            "s3 resolver marked handles resolved"
        );
        Ok(())
    }
}
