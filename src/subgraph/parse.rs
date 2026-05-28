//! Conversions from subgraph string scalars (BigInt) to strict Rust types.

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};

pub fn parse_block_number(s: &str) -> Result<i64> {
    s.parse::<i64>().context("invalid block_number")
}

pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
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
