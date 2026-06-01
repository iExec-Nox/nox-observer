use config::{Config as ConfigBuilder, ConfigError, Environment};
use config_secret::EnvironmentSecretFile;
use serde::Deserialize;
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
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SubgraphConfig {
    #[validate(url)]
    pub url: String,
    #[validate(range(min = 1))]
    pub chain_id: u64,
    #[validate(range(min = 1, max = 3600))]
    pub poll_interval_seconds: u64,
    #[validate(range(min = 1, max = 1000))]
    pub batch_size: u32,
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

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config = ConfigBuilder::builder()
            .set_default("server.host", "127.0.0.1")?
            .set_default("server.port", 9000)?
            .set_default("subgraph.poll_interval_seconds", 10)?
            .set_default("subgraph.batch_size", 1000)?
            .set_default("database.max_connections", 5)?
            // NATS consumer defaults — match nox-runner's NatsConfig defaults.
            // `nats.urls` has no production default; an empty default lets
            // Config::load() succeed and surfaces the missing value as a
            // validation error in `validate()` (matches subgraph.url / database.url
            // pattern).
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
            // Load environment variables (NOX_OBSERVER_*).
            // `nats.urls` is comma-list-parsed so deployments can pass a single
            // env var (e.g. NOX_OBSERVER_NATS__URLS=nats://h1,nats://h2,nats://h3).
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
    const TEST_SUBGRAPH_CHAIN_ID: &str = "421614";
    const TEST_DATABASE_URL: &str = "postgres://x:y@h/d";

    /// Subgraph + database env entries required by every load-then-validate test.
    fn required_non_nats_env() -> [(&'static str, Option<&'static str>); 3] {
        [
            ("NOX_OBSERVER_SUBGRAPH__URL", Some(TEST_SUBGRAPH_URL)),
            (
                "NOX_OBSERVER_SUBGRAPH__CHAIN_ID",
                Some(TEST_SUBGRAPH_CHAIN_ID),
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

    #[test]
    fn load_returns_defaults_when_only_required_env_vars_set() {
        let mut vars: Vec<(&'static str, Option<&'static str>)> = required_non_nats_env().to_vec();
        vars.extend(nats_required_env());
        temp_env::with_vars(vars, || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!("127.0.0.1", config.server.host);
            assert_eq!(9000, config.server.port);
            assert_eq!(10, config.subgraph.poll_interval_seconds);
            assert_eq!(1000, config.subgraph.batch_size);
            assert_eq!(5, config.database.max_connections);
            // NATS defaults
            assert_eq!(2, config.nats.urls.len());
            assert!(config.nats.tls.enabled);
            assert_eq!("nox_ingestor", config.nats.stream_name);
            assert_eq!("nox_observer_consumer", config.nats.consumer_name);
            assert_eq!(10, config.nats.consumer_max_deliver);
            assert_eq!(10, config.nats.max_ack_pending);
            assert_eq!(10, config.nats.max_batch);
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
        // urls default is empty; with no env override, load() succeeds but
        // validate() rejects via validate_nats_urls.
        let mut vars: Vec<(&'static str, Option<&'static str>)> = required_non_nats_env().to_vec();
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
                ("NOX_OBSERVER_SUBGRAPH__URL", None::<&str>),
                ("NOX_OBSERVER_SUBGRAPH__CHAIN_ID", None::<&str>),
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
}
