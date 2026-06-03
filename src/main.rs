pub mod application;
pub mod config;
pub mod db;
pub mod errors;
pub mod events;
pub mod handlers;
pub mod nats;
pub mod subgraph;

use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use validator::Validate;

use crate::application::Application;
use crate::config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    async_nats::rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap_or_else(|_| error!("Failed to install rustls ring crypto provider"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::load().inspect_err(|e| error!("Failed to load configuration: {e}"))?;
    config
        .validate()
        .inspect_err(|e| error!("Invalid configuration: {e}"))?;

    info!("Starting nox-observer on {}", config.bind_addr());
    let app = Application::new(config).await?;
    app.run().await?;

    Ok(())
}
