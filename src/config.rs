use config::{Config as ConfigBuilder, ConfigError, Environment};
use config_secret::EnvironmentSecretFile;
use serde::Deserialize;
use tracing::debug;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct Config {
    #[validate(nested)]
    pub server: ServerConfig,
    pub subgraph: SubgraphConfig,
    #[validate(nested)]
    pub database: DatabaseConfig,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SubgraphConfig {
    #[validate(length(min = 1))]
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
    #[validate(length(min = 1))]
    pub url: String,
    #[validate(range(min = 1, max = 100))]
    pub max_connections: u32,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config = ConfigBuilder::builder()
            .set_default("server.host", "127.0.0.1")?
            .set_default("server.port", 9000)?
            // Load environment variables (NOX_OBSERVER_*)
            .add_source(
                Environment::with_prefix("NOX_OBSERVER")
                    .prefix_separator("_")
                    .separator("__"),
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

    #[test]
    fn load_returns_defaults_when_no_env_vars_set() {
        temp_env::with_vars::<&str, &str, _, _>([], || {
            let config = Config::load().expect("should load");
            config.validate().expect("should validate");
            assert_eq!("127.0.0.1", config.server.host);
            assert_eq!(9000, config.server.port);
        });
    }

    #[test]
    fn load_returns_env_values_when_env_vars_set() {
        temp_env::with_vars(
            [
                ("NOX_OBSERVER_SERVER__HOST", Some("0.0.0.0")),
                ("NOX_OBSERVER_SERVER__PORT", Some("8080")),
            ],
            || {
                let config = Config::load().expect("should load");
                config.validate().expect("should validate");
                assert_eq!("0.0.0.0", config.server.host);
                assert_eq!(8080, config.server.port);
            },
        );
    }

    #[test]
    fn load_returns_file_values_when_secret_file_env_var_set() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let tmp = std::env::temp_dir().join(format!("nox_observer_secret_{unique}.toml"));
        std::fs::write(&tmp, "host = \"10.0.0.5\"\nport = 9090\n").expect("write tempfile");

        temp_env::with_vars(
            [("NOX_OBSERVER_SERVER_FILE", Some(tmp.to_str().unwrap()))],
            || {
                let config = Config::load().expect("should load");
                config.validate().expect("should validate");
                assert_eq!("10.0.0.5", config.server.host);
                assert_eq!(9090, config.server.port);
            },
        );

        std::fs::remove_file(&tmp).ok();
    }
}
