use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Router, extract::FromRef, routing::get};
use axum_prometheus::{
    Handle, MakeDefaultHandle, PrometheusMetricLayer, PrometheusMetricLayerBuilder,
};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::db::Db;
use crate::handlers;
use crate::nats::{NatsClient, NatsConsumer};
use crate::s3::{S3Client, S3Resolver};
use crate::subgraph::{Poller, SubgraphClient};

#[derive(Clone)]
pub struct AppState {
    pub metrics_handle: PrometheusHandle,
}

impl FromRef<AppState> for PrometheusHandle {
    fn from_ref(state: &AppState) -> Self {
        state.metrics_handle.clone()
    }
}

pub struct Application {
    config: Config,
    state: AppState,
    prometheus_layer: PrometheusMetricLayer<'static>,
    poller: Poller,
    nats_consumer: NatsConsumer,
    s3_resolver: S3Resolver,
}

impl Application {
    pub async fn new(config: Config) -> Result<Self> {
        let prometheus_layer = PrometheusMetricLayerBuilder::new()
            .with_allow_patterns(&["/", "/health", "/metrics"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        let db = Db::connect(&config.database.url, config.database.max_connections)
            .await
            .context("Failed to connect to the database")?;

        let subgraph = SubgraphClient::new(config.subgraph.url.clone())
            .context("Failed to build the subgraph client")?;

        let chain_id: i32 = config
            .subgraph
            .chain_id
            .try_into()
            .context("subgraph.chain_id does not fit in i32")?;

        let poller = Poller::new(
            subgraph,
            db.clone(),
            chain_id,
            Duration::from_secs(config.subgraph.poll_interval_seconds),
            i64::from(config.subgraph.batch_size),
        )
        .await
        .context("Failed to initialize the subgraph poller")?;

        let nats_client = NatsClient::connect(&config.nats)
            .await
            .context("initializing NATS client")?;
        let nats_consumer = NatsConsumer::new(nats_client, db.clone(), config.nats.clone());

        let s3_client = S3Client::new(&config.s3)
            .await
            .context("Failed to initialize the S3 client")?;
        let s3_resolver = S3Resolver::new(
            s3_client,
            db,
            Duration::from_secs(config.s3.poll_interval_seconds),
            i64::from(config.s3.batch_size),
        );

        Ok(Self {
            config,
            state: AppState { metrics_handle },
            prometheus_layer,
            poller,
            nats_consumer,
            s3_resolver,
        })
    }

    fn build_router(&self) -> Router {
        debug!("Building application router");

        Router::new()
            .route("/", get(handlers::root))
            .route("/health", get(handlers::health_check))
            .route("/metrics", get(handlers::metrics))
            .fallback(handlers::not_found)
            .with_state(self.state.clone())
            .layer(TraceLayer::new_for_http())
            .layer(self.prometheus_layer.clone())
    }

    pub async fn run(self) -> Result<()> {
        let addr = self.config.bind_addr();
        let app = self.build_router();
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("Failed to bind server to address {}", addr))?;

        info!("Server bound to {}", addr);

        let poller = self.poller;
        let nats_consumer = self.nats_consumer;
        let s3_resolver = self.s3_resolver;
        let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

        tokio::select! {
            res = poller.run() => {
                error!("subgraph poller exited; bringing observer down");
                res.context("subgraph poller failed")?;
            }
            res = nats_consumer.run() => {
                error!("nats consumer exited; bringing observer down");
                res.context("nats consumer failed")?;
            }
            res = s3_resolver.run() => {
                error!("s3 resolver exited; bringing observer down");
                res.context("s3 resolver failed")?;
            }
            res = server => {
                res.context("Server encountered an error during execution")?;
            }
        }
        info!("Server shutdown complete");
        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, shutting down gracefully...");
        },
        _ = terminate => {
            info!("Received SIGTERM, shutting down gracefully...");
        },
    }

    warn!("Shutdown signal received, cleaning up...");
}
