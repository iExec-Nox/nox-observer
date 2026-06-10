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

    /// Drain unresolved handles in a loop, adapting cadence to the backlog:
    /// sleep `poll_interval` only when the previous batch was incomplete (caught
    /// up with the writers); otherwise loop immediately to clear backlog at full
    /// speed. The semaphore in `S3Client` still caps S3 concurrency regardless.
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
            match self.resolve_once().await {
                Ok(saturated) => {
                    if !saturated {
                        ticker.tick().await;
                    }
                }
                Err(e) => {
                    if e.is_transient() {
                        warn!("s3 resolve tick failed (transient): {e}");
                    } else {
                        error!("s3 resolve tick failed: {e}");
                    }
                    ticker.tick().await;
                }
            }
        }
    }

    /// Fetch one batch of unresolved handles, keep those whose ciphertext is
    /// already present in S3, and mark them resolved.
    ///
    /// Returns `Ok(true)` when the DB fetch returned a full `batch_size` page,
    /// signalling there is likely more backlog to drain immediately. Returns
    /// `Ok(false)` once the batch is incomplete (caught up with the writers).
    ///
    /// `resolved_at` is set from the S3 upload time, which may predate the
    /// on-chain `block_timestamp`; the DB clamps it with
    /// `GREATEST(resolved_at, block_timestamp)` to keep the resolution time
    /// monotonic relative to emission.
    async fn resolve_once(&self) -> Result<bool, S3ResolverError> {
        let chains = self.s3.configured_chains();
        let candidates = self
            .db
            .fetch_unresolved_handles(&chains, self.batch_size)
            .await?;
        if candidates.is_empty() {
            return Ok(false);
        }
        let fetched = candidates.len();
        let page_full = (fetched as i64) >= self.batch_size;
        let present = self.s3.filter_present(&candidates).await?;
        if present.is_empty() {
            return Ok(false);
        }
        let resolved: Vec<_> = present
            .into_iter()
            .map(|(handle_id, s3_last_modified, _)| (handle_id, s3_last_modified))
            .collect();
        let n = self.db.mark_resolved_by_s3(&resolved).await?;
        // "Saturated" means: DB page was full AND we actually made progress.
        // Both conditions must hold to justify looping immediately:
        // - page_full alone is not enough: a backlog of not-yet-uploaded handles
        //   would hot-loop without progress (re-HEAD the same missing keys).
        // - progress alone (page_full = false) means we've caught up with writers.
        let saturated = page_full && n > 0;
        info!(
            resolved = n,
            fetched, saturated, "s3 resolver marked handles resolved"
        );
        Ok(saturated)
    }
}
