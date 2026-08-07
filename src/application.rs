use std::collections::HashSet;
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
use crate::subgraph::{SubgraphClient, SubgraphPoller, SubgraphPollerSupervisor};

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
    subgraph_pollers: Vec<(i32, SubgraphPoller)>,
    nats_consumer: NatsConsumer,
    s3_resolver: S3Resolver,
}

impl Application {
    pub async fn new(config: Config) -> Result<Self> {
        let prometheus_layer = PrometheusMetricLayerBuilder::new()
            .with_allow_patterns(&["/", "/health", "/metrics"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        let db = Db::connect(&config.database)
            .await
            .context("Failed to connect to the database")?;

        // Apply any pending migrations before serving.
        // To revert a migration use sqlx-cli.
        sqlx::migrate!("./migrations")
            .run(db.pool())
            .await
            .context("Failed to run database migrations")?;

        let poll_interval = Duration::from_secs(config.subgraph.poll_interval_seconds);
        let batch_size = i64::from(config.subgraph.batch_size);
        let mut subgraph_pollers = Vec::with_capacity(config.subgraph.chains.len());
        for (chain_id_str, subgraph_chain_config) in &config.subgraph.chains {
            // `validate_subgraph_chains` already enforced this parses as i32, so
            // an unwrap-like context is enough; if it ever fires it's a code bug.
            let chain_id: i32 = chain_id_str.parse().with_context(|| {
                format!("invalid chain_id key '{chain_id_str}' in subgraph.chains (expected i32)")
            })?;
            let subgraph = SubgraphClient::new(subgraph_chain_config.url.clone())
                .with_context(|| format!("Failed to build subgraph client for chain {chain_id}"))?;
            let poller = SubgraphPoller::new(
                subgraph,
                db.clone(),
                chain_id,
                poll_interval,
                batch_size,
                subgraph_chain_config.start_block,
            )
            .await
            .with_context(|| {
                format!("Failed to initialize subgraph poller for chain {chain_id}")
            })?;
            subgraph_pollers.push((chain_id, poller));
        }

        // NATS ingests events from every chain published upstream. To keep the
        // observer self-consistent, restrict the consumer to the chains we can
        // actually serve downstream (i.e. those with a subgraph or S3 config).
        let allowed_chains: HashSet<i32> = config
            .subgraph
            .chains
            .keys()
            .chain(config.s3.chains.keys())
            .filter_map(|s| s.parse::<i32>().ok())
            .collect();

        let nats_client = NatsClient::connect(&config.nats)
            .await
            .context("initializing NATS client")?;
        let nats_consumer =
            NatsConsumer::new(nats_client, db.clone(), config.nats.clone(), allowed_chains);

        let s3_client = S3Client::new(&config.s3)
            .await
            .context("Failed to initialize the S3 client")?;
        let s3_resolver = S3Resolver::new(
            s3_client,
            db.clone(),
            Duration::from_secs(config.s3.poll_interval_seconds),
            i64::from(config.s3.batch_size),
        );

        Ok(Self {
            config,
            state: AppState { metrics_handle },
            prometheus_layer,
            subgraph_pollers,
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

        let nats_consumer = self.nats_consumer;
        let s3_resolver = self.s3_resolver;
        let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

        let mut supervisor = SubgraphPollerSupervisor::spawn(self.subgraph_pollers);

        let exit = tokio::select! {
            err = supervisor.wait_for_exit() => Exit::Poller(err),
            res = nats_consumer.run() => Exit::Nats(res),
            res = s3_resolver.run() => Exit::S3(res),
            res = server => Exit::Server(res),
        };

        supervisor.shutdown().await;

        match exit {
            Exit::Server(res) => {
                info!("Server shutdown complete");
                res.context("Server encountered an error during execution")?;
                Ok(())
            }
            Exit::Nats(res) => {
                error!("nats consumer exited; bringing observer down");
                res.context("nats consumer failed")?;
                Ok(())
            }
            Exit::S3(res) => {
                error!("s3 resolver exited; bringing observer down");
                res.context("s3 resolver failed")?;
                Ok(())
            }
            Exit::Poller(err) => Err(err),
        }
    }
}

enum Exit {
    Poller(anyhow::Error),
    Nats(Result<(), crate::errors::NatsError>),
    S3(Result<(), crate::errors::S3ResolverError>),
    Server(Result<(), std::io::Error>),
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
