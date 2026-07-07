use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::{error, info, warn};

use super::client::SubgraphClient;
use crate::db::{Db, NewHandle};
use crate::errors::SubgraphPollerError;

const MAX_CONSECUTIVE_RETRIES: u32 = 5;
const MAX_BACKOFF_EXPONENT: u32 = 5; // 2^5 = 32s cap per attempt
const CURSOR_ID_START: &str = "0x";

/// Composite pagination cursor over the subgraph's `(blockNumber, id)` ordering.
/// Only `block` is persisted; `id` is in-memory and resets to `CURSOR_ID_START`
/// on restart, which harmlessly re-scans the resume block (upserts are idempotent).
struct Cursor {
    block: i64,
    id: String,
}

pub struct SubgraphPoller {
    subgraph: SubgraphClient,
    db: Db,
    chain_id: i32,
    poll_interval: Duration,
    batch_size: i64,
    cursor: Cursor,
}

impl SubgraphPoller {
    pub async fn new(
        subgraph: SubgraphClient,
        db: Db,
        chain_id: i32,
        poll_interval: Duration,
        batch_size: i64,
        start_block: i64,
    ) -> Result<Self, sqlx::Error> {
        let cursor_block = db.load_cursor_block(chain_id).await?.unwrap_or(start_block);
        Ok(Self {
            subgraph,
            db,
            chain_id,
            poll_interval,
            batch_size,
            cursor: Cursor {
                block: cursor_block,
                id: CURSOR_ID_START.to_string(),
            },
        })
    }

    pub async fn run(mut self) -> Result<(), SubgraphPollerError> {
        info!(
            chain_id = self.chain_id,
            "poller starting; resuming from block={}", self.cursor.block
        );
        self.catch_up().await?;
        info!(
            chain_id = self.chain_id,
            "caught up at block={}; entering live mode", self.cursor.block
        );

        let mut ticker = interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            if let Err(e) = self.fetch_and_process_page().await {
                // Live mode swallows all errors: the next tick is our natural retry.
                error!("live poll failed: {e:#}");
            }
        }
    }

    /// Walk the whole history at full speed, retrying on transient errors with
    /// exponential backoff. Stops when a page is non-full (history caught up).
    async fn catch_up(&mut self) -> Result<(), SubgraphPollerError> {
        let mut consecutive_failures: u32 = 0;

        loop {
            match self.fetch_and_process_page().await {
                Ok(n) => {
                    consecutive_failures = 0;
                    if (n as i64) < self.batch_size {
                        return Ok(());
                    }
                }
                Err(e) if e.is_transient() => {
                    consecutive_failures += 1;
                    if consecutive_failures > MAX_CONSECUTIVE_RETRIES {
                        error!(
                            "aborting catch-up after {MAX_CONSECUTIVE_RETRIES} consecutive \
                             transient errors"
                        );
                        return Err(e);
                    }
                    let backoff =
                        Duration::from_secs(1u64 << consecutive_failures.min(MAX_BACKOFF_EXPONENT));
                    warn!(
                        "transient error during catch-up \
                         (attempt {consecutive_failures}/{MAX_CONSECUTIVE_RETRIES}, \
                         retrying in {backoff:?}): {e:#}"
                    );
                    sleep(backoff).await;
                }
                Err(e) => {
                    // Permanent error: no point retrying.
                    return Err(e);
                }
            }
        }
    }

    /// Fetch the next page from the subgraph cursor, upsert its handles, and
    /// advance the cursor.
    ///
    /// Handles arrive ordered by `(blockNumber asc, id asc)`, so the last one in
    /// the page is the greatest `(block, id)`. Advancing the composite cursor to
    /// it makes the next fetch resume strictly after this handle, which
    /// guarantees forward progress even when a single block holds more than
    /// `batch_size` handles (a block-only cursor would re-fetch that block
    /// forever). Only the cursor's block is persisted.
    async fn fetch_and_process_page(&mut self) -> Result<usize, SubgraphPollerError> {
        let data = self
            .subgraph
            .fetch_handles(self.cursor.block, &self.cursor.id, self.batch_size)
            .await?;
        let n = data.handles.len();
        if n == 0 {
            return Ok(0);
        }

        for h in &data.handles {
            let block_number = h.block_number.as_deref().map(parse_block_number);
            let block_timestamp = h.block_timestamp.as_deref().map(parse_timestamp);

            let new = NewHandle {
                handle_id: h.id.clone(),
                chain_id: self.chain_id,
                operator: h.operator.clone(),
                caller: None, // filled later by nats_consumer
                tx_hash: h.transaction_hash.clone(),
                block_timestamp,
                block_number,
                resolved_at: None,
                processed_by_subgraph: true,
                processed_by_s3: false,
                processed_by_nats: false,
            };
            self.db.upsert_handle(&new).await?;

            for p in &h.parent_handles {
                self.db.upsert_handle_parent(&h.id, &p.id).await?;
            }
        }

        if let Some(last) = data.handles.last()
            && let Some(bn) = last.block_number.as_deref().map(parse_block_number)
        {
            self.cursor.block = bn;
            self.cursor.id = last.id.clone();
        }

        self.db
            .save_cursor_block(self.chain_id, self.cursor.block)
            .await?;

        info!(
            chain_id = self.chain_id,
            "polled {n} handles (block={}, id={})", self.cursor.block, self.cursor.id
        );
        Ok(n)
    }
}

/// The subgraph schema guarantees `BigInt` is a valid integer string;
/// a parse failure here means the upstream contract is broken.
fn parse_block_number(s: &str) -> i64 {
    s.parse::<i64>()
        .expect("subgraph contract: block_number must be a valid i64")
}

fn parse_timestamp(s: &str) -> DateTime<Utc> {
    let secs: i64 = s
        .parse()
        .expect("subgraph contract: block_timestamp must be a valid i64");
    DateTime::from_timestamp(secs, 0)
        .expect("subgraph contract: block_timestamp must fit in chrono's range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_block_number_ok() {
        assert_eq!(parse_block_number("12345"), 12345);
    }

    #[test]
    fn parse_timestamp_ok() {
        let dt = parse_timestamp("1700000000");
        assert_eq!(dt.timestamp(), 1700000000);
    }
}
