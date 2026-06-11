use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::{
    Client,
    config::{Builder, Credentials, timeout::TimeoutConfig},
    error::SdkError,
};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::config::{S3ChainConfig, S3Config};
use crate::errors::S3ResolverError;

pub struct ChainBucket {
    client: Client,
    bucket: String,
}

pub struct S3Client {
    chains: HashMap<i32, ChainBucket>,
    /// Shared across all chains, caps in-flight S3 operations globally.
    semaphore: Arc<Semaphore>,
}

impl S3Client {
    pub async fn new(config: &S3Config) -> Result<Self, S3ResolverError> {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_requests));
        let mut chains = HashMap::with_capacity(config.chains.len());

        for (key, chain_cfg) in &config.chains {
            let chain_id = key.parse::<i32>().map_err(|_| {
                S3ResolverError::S3(format!(
                    "invalid chain_id key in s3.chains: '{key}' is not a valid i32"
                ))
            })?;

            let chain_bucket = build_chain_bucket(chain_cfg).await?;
            validate_bucket(&chain_bucket, key).await?;
            chains.insert(chain_id, chain_bucket);
        }

        Ok(Self { chains, semaphore })
    }

    /// Chain IDs that have a configured bucket, for startup diagnostics.
    pub fn configured_chains(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self.chains.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Returns the object's `LastModified` (when the ciphertext landed in S3)
    /// when present, `None` on a clean 404.
    pub async fn handle_exists(
        &self,
        chain_id: i32,
        key: &str,
    ) -> Result<Option<DateTime<Utc>>, S3ResolverError> {
        let chain_bucket = self.chains.get(&chain_id).ok_or_else(|| {
            S3ResolverError::S3(format!("no S3 bucket configured for chain_id {chain_id}"))
        })?;

        match chain_bucket
            .client
            .head_object()
            .bucket(&chain_bucket.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => {
                // S3 always returns LastModified for an existing object; if it is
                // absent or out of range, the object is still present, so resolve
                // it with a now() fallback rather than dropping it.
                let resolved_at = out
                    .last_modified()
                    .and_then(smithy_to_chrono)
                    .unwrap_or_else(|| {
                        warn!(
                            chain_id,
                            key,
                            "head_object returned no usable LastModified; falling back to now()"
                        );
                        Utc::now()
                    });
                Ok(Some(resolved_at))
            }
            Err(e) => {
                // Classify from the typed SDK error: a missing object is a clean
                // "not present", 5xx and network/timeout failures are transient
                // (worth retrying next tick), everything else (4xx auth, malformed
                // request) is permanent.
                let transient = match &e {
                    SdkError::ServiceError(se) => {
                        let status = se.raw().status().as_u16();
                        if status == 404 {
                            return Ok(None);
                        }
                        status >= 500
                    }
                    SdkError::TimeoutError(_)
                    | SdkError::DispatchFailure(_)
                    | SdkError::ResponseError(_) => true,
                    _ => false,
                };
                let msg = format!("head_object failed for chain {chain_id} key '{key}': {e}");
                Err(if transient {
                    S3ResolverError::S3Transient(msg)
                } else {
                    S3ResolverError::S3(msg)
                })
            }
        }
    }

    /// Dispatch a HEAD per candidate concurrently (throttled by the shared
    /// semaphore) and keep only those whose ciphertext is present in S3.
    pub async fn filter_present(
        &self,
        candidates: &[(String, i32, Option<DateTime<Utc>>)],
    ) -> Result<Vec<(String, DateTime<Utc>, Option<DateTime<Utc>>)>, S3ResolverError> {
        let client = self;
        let results = join_all(candidates.iter().map(
            |(handle_id, chain_id, block_timestamp)| async move {
                let _permit = client
                    .semaphore
                    .acquire()
                    .await
                    .map_err(|e| S3ResolverError::S3(format!("semaphore error: {e}")))?;
                client.handle_exists(*chain_id, handle_id).await.map(|ts| {
                    ts.map(|resolved_at| (handle_id.clone(), resolved_at, *block_timestamp))
                })
            },
        ))
        .await;

        let mut present = Vec::new();
        for r in results {
            if let Some(entry) = r? {
                present.push(entry);
            }
        }
        Ok(present)
    }
}

/// Convert the AWS SDK timestamp returned by `head_object` into a `chrono`
/// UTC datetime. Returns `None` only for out-of-range values, which a real S3
/// `LastModified` never produces.
fn smithy_to_chrono(dt: &aws_sdk_s3::primitives::DateTime) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
}

async fn build_chain_bucket(config: &S3ChainConfig) -> Result<ChainBucket, S3ResolverError> {
    let credentials =
        Credentials::new(&config.access_key, &config.secret_key, None, None, "static");

    let mut builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(credentials)
        .region(aws_config::Region::new(config.region.clone()))
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(config.timeout))
                .build(),
        );

    if let Some(ref url) = config.endpoint_url {
        builder = builder.endpoint_url(url);
    }

    let aws_config = builder.load().await;

    let path_style = config.endpoint_url.is_some();
    let s3_config = Builder::from(&aws_config)
        .force_path_style(path_style)
        .build();

    Ok(ChainBucket {
        client: Client::from_conf(s3_config),
        bucket: config.bucket.clone(),
    })
}

async fn validate_bucket(
    chain_bucket: &ChainBucket,
    chain_key: &str,
) -> Result<(), S3ResolverError> {
    chain_bucket
        .client
        .head_bucket()
        .bucket(&chain_bucket.bucket)
        .send()
        .await
        .map_err(|e| {
            S3ResolverError::S3(format!(
                "S3 bucket '{}' for chain '{}' is not accessible: {}",
                chain_bucket.bucket,
                chain_key,
                e.into_service_error()
            ))
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smithy_to_chrono_maps_secs_and_subsec_nanos() {
        // 2021-01-01T00:00:00Z plus 500ms.
        let smithy =
            aws_sdk_s3::primitives::DateTime::from_secs_and_nanos(1_609_459_200, 500_000_000);
        let expected = DateTime::from_timestamp(1_609_459_200, 500_000_000).unwrap();
        assert_eq!(smithy_to_chrono(&smithy), Some(expected));
    }

    #[test]
    fn new_returns_err_when_chain_key_is_non_numeric() {
        let config = crate::config::S3Config {
            chains: {
                let mut m = HashMap::new();
                m.insert(
                    "not-a-number".to_string(),
                    crate::config::S3ChainConfig {
                        endpoint_url: None,
                        bucket: "b".to_string(),
                        access_key: "a".to_string(),
                        secret_key: "s".to_string(),
                        region: "us-east-1".to_string(),
                        timeout: 30,
                    },
                );
                m
            },
            poll_interval_seconds: 10,
            batch_size: 500,
            max_concurrent_requests: 4,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(S3Client::new(&config));
        assert!(result.is_err());
        match result {
            Err(S3ResolverError::S3(ref msg)) => {
                assert!(msg.contains("not-a-number"), "unexpected message: {msg}");
            }
            _ => panic!("expected S3ResolverError::S3"),
        }
    }
}
