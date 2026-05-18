use config::{Config as ConfigBuilder, ConfigError, Environment};
use config_secret::EnvironmentSecretFile;
use serde::Deserialize;
use tracing::debug;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct Config {
    #[validate(nested)]
    pub server: ServerConfig,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
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
            // Load secrets from files (NOX_OBSERVER_*_FILE -> reads file content)
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
}
