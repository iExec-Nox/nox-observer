use std::collections::HashMap;

use config::{Config as ConfigBuilder, ConfigError, Environment};
use config_secret::EnvironmentSecretFile;
use serde::{Deserialize, Serialize};
use tracing::debug;
use validator::{Validate, ValidationError};

#[derive(Debug, Deserialize, Validate)]
pub struct Config {
    #[validate(nested)]
    pub server: ServerConfig,
    #[validate(nested)]
    pub subgraph: SubgraphConfig,
    #[validate(nested)]
    pub database: DatabaseConfig,
    #[validate(nested)]
    pub nats: NatsConfig,
    #[validate(nested)]
    pub s3: S3Config,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

/// Subgraph poller configuration.
///
/// `chains` maps chain IDs (as strings, because `config` deserializes map keys
/// as strings) to per-chain subgraph endpoint URLs. The custom validator below
/// enforces: at least one chain, every key parses as `i32` (matches the `INT`
/// `chain_id` column), every URL is well-formed.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SubgraphConfig {
    #[validate(custom(function = "validate_subgraph_chains"))]
    pub chains: HashMap<String, String>,
    #[validate(range(min = 1, max = 3600))]
    pub poll_interval_seconds: u64,
    #[validate(range(min = 1, max = 1000))]
    pub batch_size: u32,
}

fn validate_subgraph_chains(chains: &HashMap<String, String>) -> Result<(), ValidationError> {
    if chains.is_empty() {
        return Err(ValidationError::new(
            "subgraph.chains must contain at least one chain",
        ));
    }
    for (chain_id, url) in chains {
        if chain_id.parse::<i32>().is_err() {
            return Err(ValidationError::new("invalid_chain_id").with_message(
                format!("subgraph.chains key '{chain_id}' must be a valid i32").into(),
            ));
        }
        if reqwest::Url::parse(url).is_err() {
            return Err(ValidationError::new("invalid_chain_url").with_message(
                format!("subgraph.chains[{chain_id}] is not a valid URL: {url}").into(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct DatabaseConfig {
    #[validate(url)]
    pub url: String,
    #[validate(range(min = 1, max = 100))]
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct TlsConfig {
    pub enabled: bool,
    #[serde(default)]
    pub ca: String,
    #[serde(default)]
    pub cert: String,
    #[serde(default)]
    pub key: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct NatsConfig {
    #[validate(custom(function = "validate_nats_urls"))]
    pub urls: Vec<String>,
    #[validate(nested)]
    pub tls: TlsConfig,
    pub stream_name: String,
    pub consumer_name: String,
    #[validate(range(min = 10))]
    pub consumer_max_deliver: i64,
    #[validate(range(min = 10, max = 200))]
    pub max_ack_pending: i64,
    #[validate(range(min = 10, max = 200))]
    pub max_batch: i64,
}

fn validate_nats_urls(urls: &Vec<String>) -> Result<(), ValidationError> {
    if urls.is_empty() {
        return Err(ValidationError::new(
            "nats.urls must contain at least one URL",
        ));
    }
    for u in urls {
        if !u.starts_with("nats://") && !u.starts_with("tls://") {
            return Err(ValidationError::new(
                "each nats url must start with nats:// or tls://",
            ));
        }
    }
    Ok(())
}

/// S3 resolver configuration covering all chains and tuning knobs.
///
/// `chains` maps chain IDs (as strings, because the `config` crate lowercases
/// env keys and deserializes map keys as strings) to per-chain S3 settings.
/// The downstream consumer parses them into `i32` chain IDs, matching the
/// `INT` `chain_id` column on the `handles` table. Chosen over `HashMap<i32, _>`
/// because the config crate produces string-typed map keys from env, causing an
/// integer deserialization failure.
///
/// `Serialize` is required (unlike sibling config structs): `validator` 0.20's
/// derive on the `#[validate(nested)]` `HashMap` field emits a bound requiring
/// `S3ChainConfig: Serialize`. Do not remove it without dropping nested
/// validation. It is never actually serialized at runtime.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct S3Config {
    #[validate(custom(function = "validate_s3_chains_non_empty"))]
    #[validate(nested)]
    pub chains: HashMap<String, S3ChainConfig>,
    #[validate(range(min = 1, max = 3600))]
    pub poll_interval_seconds: u64,
    #[validate(range(min = 1, max = 1000))]
    pub batch_size: u32,
    #[validate(range(min = 1, max = 256))]
    pub max_concurrent_requests: usize,
}

/// Per-chain S3 connection configuration.
///
/// `access_key`, `secret_key`, `bucket`, and `region` are required (no defaults).
/// `timeout` defaults to 30 seconds.
///
/// `Debug` is implemented manually to keep `access_key` and `secret_key` out of
/// log and panic output.
#[derive(Clone, Serialize, Deserialize, Validate)]
pub struct S3ChainConfig {
    /// Optional custom endpoint for S3-compatible backends (e.g. MinIO). When unset, the AWS SDK uses standard regional endpoints.
    #[validate(url)]
    pub endpoint_url: Option<String>,
    #[validate(length(min = 1))]
    pub bucket: String,
    #[validate(length(min = 1))]
    pub access_key: String,
    #[validate(length(min = 1))]
    pub secret_key: String,
    #[validate(length(min = 1))]
    pub region: String,
    #[serde(default = "default_s3_timeout")]
    #[validate(range(min = 1, max = 300))]
    pub timeout: u64,
}

impl std::fmt::Debug for S3ChainConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ChainConfig")
            .field("endpoint_url", &self.endpoint_url)
            .field("bucket", &self.bucket)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("region", &self.region)
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn default_s3_timeout() -> u64 {
    30
}

fn validate_s3_chains_non_empty(
    chains: &HashMap<String, S3ChainConfig>,
) -> Result<(), ValidationError> {
    if chains.is_empty() {
        return Err(ValidationError::new(
            "s3.chains must contain at least one chain",
        ));
    }
    for chain_id in chains.keys() {
        if chain_id.parse::<i32>().is_err() {
            return Err(ValidationError::new("invalid_chain_id")
                .with_message(format!("s3.chains key '{chain_id}' must be a valid i32").into()));
        }
    }
    Ok(())
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config = ConfigBuilder::builder()
            .set_default("server.host", "127.0.0.1")?
            .set_default("server.port", 9000)?
            .set_default("subgraph.poll_interval_seconds", 10)?
            .set_default("subgraph.batch_size", 1000)?
            .set_default("database.max_connections", 5)?
            .set_default("nats.urls", Vec::<String>::new())?
            .set_default("nats.tls.enabled", true)?
            .set_default("nats.tls.ca", "")?
            .set_default("nats.tls.cert", "")?
            .set_default("nats.tls.key", "")?
            .set_default("nats.stream_name", "nox_ingestor")?
            .set_default("nats.consumer_name", "nox_observer_consumer")?
            .set_default("nats.consumer_max_deliver", 10)?
            .set_default("nats.max_ack_pending", 10)?
            .set_default("nats.max_batch", 10)?
            .set_default("s3.poll_interval_seconds", 10)?
            .set_default("s3.batch_size", 500)?
            .set_default("s3.max_concurrent_requests", 16)?
            .add_source(
                Environment::with_prefix("NOX_OBSERVER")
                    .prefix_separator("_")
                    .separator("__")
                    .list_separator(",")
                    .with_list_parse_key("nats.urls")
                    .try_parsing(true),
            )
            // Load structured config sections from files referenced by
            // NOX_OBSERVER_<section>_FILE env vars (e.g. NOX_OBSERVER_DATABASE_FILE=/run/secrets/db.toml).
            // The file is parsed as TOML/JSON/YAML by extension and its keys merge under the matching section.
            .add_source(EnvironmentSecretFile::with_prefix("NOX_OBSERVER").separator("_"))
            .build()?;

        config.try_deserialize()
    }

    pub fn bind_addr(&self) -> String {
        let addr = format!("{}:{}", self.server.host, self.server.port);
        debug!("Starting nox-observer server on {}", addr);
        addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Shared test constants — required-env values repeated across tests ───
    const TEST_SUBGRAPH_URL: &str = "https://example.com/sg";
    const TEST_DATABASE_URL: &str = "postgres://x:y@h/d";

    /// Subgraph + database env entries required by every load-then-validate test.
    fn required_non_nats_env() -> [(&'static str, Option<&'static str>); 2] {
        [
            (
                "NOX_OBSERVER_SUBGRAPH__CHAINS__421614",
                Some(TEST_SUBGRAPH_URL),
            ),
            ("NOX_OBSERVER_DATABASE__URL", Some(TEST_DATABASE_URL)),
        ]
    }

    /// Required NATS env block for tests where load+validate must succeed.
    fn nats_required_env() -> [(&'static str, Option<&'static str>); 5] {
        [
            (
                "NOX_OBSERVER_NATS__URLS",
                Some("nats://localhost:4222,nats://localhost:4223"),
            ),
            ("NOX_OBSERVER_NATS__TLS__ENABLED", Some("true")),
            ("NOX_OBSERVER_NATS__TLS__CA", Some("ca-pem")),
            ("NOX_OBSERVER_NATS__TLS__CERT", Some("cert-pem")),
            ("NOX_OBSERVER_NATS__TLS__KEY", Some("key-pem")),
        ]
    }

    /// Required S3 env block (single chain) for tests where load+validate must succeed.
    fn s3_required_env() -> [(&'static str, Option<&'static str>); 5] {
        [
            (
                "NOX_OBSERVER_S3__CHAINS__421614__BUCKET",
                Some("test-bucket"),
            ),
            (
                "NOX_OBSERVER_S3__CHAINS__421614__ACCESS_KEY",
                Some("test-access-key"),
            ),
            (
                "NOX_OBSERVER_S3__CHAINS__421614__SECRET_KEY",
                Some("test-secret-key"),
            ),
            ("NOX_OBSERVER_S3__CHAINS__421614__REGION", Some("us-east-1")),
            ("NOX_OBSERVER_S3__CHAINS__421614__TIMEOUT", Some("30")),
        ]
    }

    #[test]
    fn load_returns_defaults_when_only_required_env_vars_set() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = required_non_nats_env().to_vec();
        vars.extend(nats_required_env());
        vars.extend(s3_required_env());
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!("127.0.0.1", config.server.host);
            assert_eq!(9000, config.server.port);
            assert_eq!(10, config.subgraph.poll_interval_seconds);
            assert_eq!(1000, config.subgraph.batch_size);
            assert_eq!(1, config.subgraph.chains.len());
            assert_eq!(
                TEST_SUBGRAPH_URL,
                config.subgraph.chains.get("421614").unwrap().as_str()
            );
            assert_eq!(5, config.database.max_connections);
            assert_eq!(2, config.nats.urls.len());
            assert!(config.nats.tls.enabled);
            assert_eq!("nox_ingestor", config.nats.stream_name);
            assert_eq!("nox_observer_consumer", config.nats.consumer_name);
            assert_eq!(10, config.nats.consumer_max_deliver);
            assert_eq!(10, config.nats.max_ack_pending);
            assert_eq!(10, config.nats.max_batch);
            assert_eq!(10, config.s3.poll_interval_seconds);
            assert_eq!(500, config.s3.batch_size);
            assert_eq!(16, config.s3.max_concurrent_requests);
            assert_eq!(1, config.s3.chains.len());
        });
    }

    #[test]
    fn load_returns_env_values_when_env_vars_set() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = vec![
            ("NOX_OBSERVER_SERVER__HOST", Some("0.0.0.0")),
            ("NOX_OBSERVER_SERVER__PORT", Some("8080")),
        ];
        vars.extend(required_non_nats_env());
        vars.extend(nats_required_env());
        vars.extend(s3_required_env());
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!("0.0.0.0", config.server.host);
            assert_eq!(8080, config.server.port);
        });
    }

    #[test]
    fn load_returns_file_values_when_secret_file_env_var_set() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let tmp = std::env::temp_dir().join(format!("nox_observer_secret_{unique}.toml"));
        std::fs::write(&tmp, "host = \"10.0.0.5\"\nport = 9090\n").expect("write tempfile");
        let tmp_str = tmp.to_str().unwrap().to_string();

        let mut vars = vec![("NOX_OBSERVER_SERVER_FILE", Some(tmp_str.as_str()))];
        vars.extend(required_non_nats_env());
        vars.extend(nats_required_env());
        vars.extend(s3_required_env());
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!("10.0.0.5", config.server.host);
            assert_eq!(9090, config.server.port);
        });

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn load_succeeds_validate_fails_when_nats_urls_empty() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = required_non_nats_env().to_vec();
        vars.extend(s3_required_env());
        vars.extend([
            ("NOX_OBSERVER_NATS__TLS__ENABLED", Some("false")),
            ("NOX_OBSERVER_NATS__URLS", None::<&str>),
        ]);
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load (empty urls default)");
            let result = config.validate();
            assert!(result.is_err(), "validate should fail on empty nats.urls");
        });
    }

    /// Build a `NatsConfig` with the production-default stream/consumer names,
    /// disabled TLS, and the canonical pull-config tuning. Tests override only
    /// the field they're asserting on.
    fn nats_config_with_urls(urls: Vec<String>) -> NatsConfig {
        NatsConfig {
            urls,
            tls: TlsConfig {
                enabled: false,
                ca: String::new(),
                cert: String::new(),
                key: String::new(),
            },
            stream_name: "nox_ingestor".to_string(),
            consumer_name: "nox_observer_consumer".to_string(),
            consumer_max_deliver: 10,
            max_ack_pending: 10,
            max_batch: 10,
        }
    }

    #[test]
    fn validate_returns_err_when_url_scheme_is_neither_nats_nor_tls() {
        let cfg = nats_config_with_urls(vec!["http://not-nats:4222".to_string()]);
        let result = cfg.validate();
        assert!(result.is_err(), "validate should reject http:// url scheme");
    }

    #[test]
    fn validate_returns_ok_when_url_schemes_are_nats_and_tls() {
        let cfg = nats_config_with_urls(vec![
            "nats://h1:4222".to_string(),
            "tls://h2:4222".to_string(),
        ]);
        cfg.validate()
            .expect("nats:// + tls:// schemes are allowed");
    }

    #[test]
    fn load_returns_err_when_required_env_vars_missing() {
        temp_env::with_vars(
            [
                ("NOX_OBSERVER_SUBGRAPH__CHAINS__421614", None::<&str>),
                ("NOX_OBSERVER_DATABASE__URL", None::<&str>),
            ],
            || {
                let result = Config::load();
                assert!(
                    result.is_err(),
                    "load() should fail when required env vars are unset"
                );
            },
        );
    }

    #[test]
    fn s3_load_returns_defaults_when_only_required_chain_env_set() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = required_non_nats_env().to_vec();
        vars.extend(nats_required_env());
        vars.extend(s3_required_env());
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!(10, config.s3.poll_interval_seconds);
            assert_eq!(500, config.s3.batch_size);
            assert_eq!(16, config.s3.max_concurrent_requests);
            let chain = config
                .s3
                .chains
                .get("421614")
                .expect("chain 421614 present");
            assert_eq!("test-bucket", chain.bucket);
            assert_eq!("us-east-1", chain.region);
            assert_eq!(30, chain.timeout);
            assert!(chain.endpoint_url.is_none());
        });
    }

    #[test]
    fn s3_chain_timeout_defaults_to_30_when_timeout_env_omitted() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = required_non_nats_env().to_vec();
        vars.extend(nats_required_env());
        vars.extend([
            (
                "NOX_OBSERVER_S3__CHAINS__421614__BUCKET",
                Some("test-bucket"),
            ),
            (
                "NOX_OBSERVER_S3__CHAINS__421614__ACCESS_KEY",
                Some("test-access-key"),
            ),
            (
                "NOX_OBSERVER_S3__CHAINS__421614__SECRET_KEY",
                Some("test-secret-key"),
            ),
            ("NOX_OBSERVER_S3__CHAINS__421614__REGION", Some("us-east-1")),
        ]);
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load without TIMEOUT env");
            config.validate().expect("should validate");
            let chain = config
                .s3
                .chains
                .get("421614")
                .expect("chain 421614 present");
            assert_eq!(30, chain.timeout);
        });
    }

    #[test]
    fn s3_parses_two_map_entries_when_two_chains_configured() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = required_non_nats_env().to_vec();
        vars.extend(nats_required_env());
        vars.extend([
            ("NOX_OBSERVER_S3__CHAINS__1__BUCKET", Some("bucket-chain-1")),
            ("NOX_OBSERVER_S3__CHAINS__1__ACCESS_KEY", Some("ak1")),
            ("NOX_OBSERVER_S3__CHAINS__1__SECRET_KEY", Some("sk1")),
            ("NOX_OBSERVER_S3__CHAINS__1__REGION", Some("eu-west-1")),
            ("NOX_OBSERVER_S3__CHAINS__1__TIMEOUT", Some("60")),
            ("NOX_OBSERVER_S3__CHAINS__2__BUCKET", Some("bucket-chain-2")),
            ("NOX_OBSERVER_S3__CHAINS__2__ACCESS_KEY", Some("ak2")),
            ("NOX_OBSERVER_S3__CHAINS__2__SECRET_KEY", Some("sk2")),
            ("NOX_OBSERVER_S3__CHAINS__2__REGION", Some("us-west-2")),
            ("NOX_OBSERVER_S3__CHAINS__2__TIMEOUT", Some("45")),
        ]);
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!(2, config.s3.chains.len());
            let c1 = config.s3.chains.get("1").expect("chain 1 present");
            assert_eq!("bucket-chain-1", c1.bucket);
            assert_eq!(60, c1.timeout);
            let c2 = config.s3.chains.get("2").expect("chain 2 present");
            assert_eq!("bucket-chain-2", c2.bucket);
            assert_eq!(45, c2.timeout);
        });
    }

    #[test]
    fn subgraph_parses_two_map_entries_when_two_chains_configured() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = vec![
            ("NOX_OBSERVER_DATABASE__URL", Some(TEST_DATABASE_URL)),
            (
                "NOX_OBSERVER_SUBGRAPH__CHAINS__1",
                Some("https://example.com/sg-mainnet"),
            ),
            (
                "NOX_OBSERVER_SUBGRAPH__CHAINS__421614",
                Some("https://example.com/sg-arbitrum-sepolia"),
            ),
        ];
        vars.extend(nats_required_env());
        vars.extend(s3_required_env());
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!(2, config.subgraph.chains.len());
            assert_eq!(
                "https://example.com/sg-mainnet",
                config.subgraph.chains.get("1").unwrap().as_str()
            );
            assert_eq!(
                "https://example.com/sg-arbitrum-sepolia",
                config.subgraph.chains.get("421614").unwrap().as_str()
            );
        });
    }

    #[test]
    fn subgraph_validate_returns_err_when_chains_empty() {
        let cfg = SubgraphConfig {
            chains: HashMap::new(),
            poll_interval_seconds: 10,
            batch_size: 1000,
        };
        let result = cfg.validate();
        assert!(
            result.is_err(),
            "validate should reject empty subgraph.chains"
        );
    }

    #[test]
    fn s3_validate_returns_err_when_chains_empty() {
        let s3 = S3Config {
            chains: HashMap::new(),
            poll_interval_seconds: 10,
            batch_size: 500,
            max_concurrent_requests: 16,
        };
        let result = s3.validate();
        assert!(result.is_err(), "validate should reject empty s3.chains");
    }

    fn s3_chain_config_with_bucket(bucket: &str) -> S3ChainConfig {
        S3ChainConfig {
            endpoint_url: None,
            bucket: bucket.to_string(),
            access_key: "test-access-key".to_string(),
            secret_key: "test-secret-key".to_string(),
            region: "us-east-1".to_string(),
            timeout: 30,
        }
    }

    #[test]
    fn s3_validate_returns_err_when_bucket_empty() {
        let cfg = s3_chain_config_with_bucket("");
        let result = cfg.validate();
        assert!(result.is_err(), "validate should reject empty bucket");
    }
}
