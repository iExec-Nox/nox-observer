use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{error, info};

use super::client::SubgraphClient;
use crate::db::{Db, NewHandle};

pub struct Poller {
    subgraph: SubgraphClient,
    db: Db,
    chain_id: i32,
    poll_interval: Duration,
    batch_size: i64,
    skip: i64,
}

impl Poller {
    pub async fn new(
        subgraph: SubgraphClient,
        db: Db,
        chain_id: i32,
        poll_interval: Duration,
        batch_size: i64,
    ) -> Result<Self> {
        let skip = db.load_skip().await?;
        Ok(Self {
            subgraph,
            db,
            chain_id,
            poll_interval,
            batch_size,
            skip,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        info!("poller starting; resuming from skip={}", self.skip);
        self.catch_up().await?;
        info!("caught up at skip={}; entering live mode", self.skip);

        let mut ticker = interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            if let Err(e) = self.fetch_and_process_page().await {
                error!("live poll failed: {e:#}");
            }
        }
    }

    /// Walk the whole history at full speed. Stops when a page is non-full.
    async fn catch_up(&mut self) -> Result<()> {
        loop {
            let n = self.fetch_and_process_page().await?;
            if (n as i64) < self.batch_size {
                break;
            }
        }
        Ok(())
    }

    async fn fetch_and_process_page(&mut self) -> Result<usize> {
        let data = self
            .subgraph
            .fetch_handles(self.skip, self.batch_size)
            .await?;
        let n = data.handles.len();
        if n == 0 {
            return Ok(0);
        }

        for h in &data.handles {
            let block_number = h
                .block_number
                .as_ref()
                .map(|s| parse_block_number(s))
                .transpose()?;
            let block_timestamp = h
                .block_timestamp
                .as_ref()
                .map(|s| parse_timestamp(s))
                .transpose()?;

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

        self.skip += n as i64;
        self.db.save_skip(self.skip).await?;
        info!("polled {n} handles (skip={})", self.skip);
        Ok(n)
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
