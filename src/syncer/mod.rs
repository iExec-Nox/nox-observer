use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{error, info};

use crate::db::Repository;
use crate::subgraph::SubgraphClient;

pub struct Syncer {
    subgraph: SubgraphClient,
    repo: Repository,
    chain_id: i32,
    poll_interval: Duration,
    batch_size: i64,
    last_block: i64,
}

impl Syncer {
    pub fn new(
        subgraph: SubgraphClient,
        repo: Repository,
        chain_id: i32,
        poll_interval: Duration,
        batch_size: i64,
    ) -> Self {
        Self {
            subgraph,
            repo,
            chain_id,
            poll_interval,
            batch_size,
            last_block: 0,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut ticker = interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!("syncer started; poll_interval={:?}", self.poll_interval);

        loop {
            ticker.tick().await;
            match self.sync_once().await {
                Ok(n) if n > 0 => info!("synced {n} handles (last_block={})", self.last_block),
                Ok(_) => {}
                Err(e) => error!("sync iteration failed: {e:#}"),
            }
        }
    }

    async fn sync_once(&mut self) -> Result<usize> {
        // TODO 1 — fetch handles past the cursor:
        //   let data = self.subgraph.fetch_handles(self.last_block.to_string(), self.batch_size).await?;
        //   If data.handles is empty, return Ok(0).

        // TODO 2 — process each handle:
        //   For each h in &data.handles:
        //     - parse block_number / block_timestamp.
        //     - build NewHandle with:
        //         chain_id = self.chain_id,
        //         caller   = None,                    // filled later by nats_consumer
        //         resolved_at = None,
        //         processed_by_subgraph = true,
        //         processed_by_s3 = false,
        //         processed_by_nats = false.
        //     - self.repo.upsert_handle(&new).await?
        //     - for each p in &h.parent_handles: self.repo.upsert_handle_parent(&h.id, &p.id).await?
        //     - increment a `processed` counter.

        // TODO 3 — advance the cursor:
        //   self.last_block = data.handles.iter()
        //       .map(|h| parse_block_number(&h.block_number).unwrap_or(self.last_block))
        //       .max()
        //       .unwrap_or(self.last_block);

        // TODO 4 — return Ok(processed_count).
        todo!()
    }
}

fn parse_block_number(s: &str) -> Result<i64> {
    s.parse::<i64>().context("invalid block_number")
}

fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
    let secs: i64 = s.parse().context("invalid block_timestamp")?;
    DateTime::from_timestamp(secs, 0).ok_or_else(|| anyhow!("out-of-range block_timestamp: {secs}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_block_number_ok() {
        assert_eq!(parse_block_number("12345").unwrap(), 12345);
    }

    #[test]
    fn parse_block_number_invalid() {
        assert!(parse_block_number("not a number").is_err());
    }

    #[test]
    fn parse_timestamp_ok() {
        let dt = parse_timestamp("1700000000").unwrap();
        assert_eq!(dt.timestamp(), 1700000000);
    }
}
