use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use axum::{Router, extract::FromRef, routing::get};
use axum_prometheus::{
    Handle, MakeDefaultHandle, PrometheusMetricLayer, PrometheusMetricLayerBuilder,
};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::signal;
use tokio::task::{Id as TaskId, JoinSet};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::db::Db;
use crate::handlers;
use crate::nats::{NatsClient, NatsConsumer};
use crate::s3::{S3Client, S3Resolver};
use crate::subgraph::{
    PollerOutcome, SubgraphClient, SubgraphPoller, drain_poller_set, map_first_poller_exit,
};

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

        ensure_chain_consistency(&config)?;

        let db = Db::connect(&config.database.url, config.database.max_connections)
            .await
            .context("Failed to connect to the database")?;

        let poll_interval = Duration::from_secs(config.subgraph.poll_interval_seconds);
        let batch_size = i64::from(config.subgraph.batch_size);
        let mut subgraph_pollers = Vec::with_capacity(config.subgraph.chains.len());
        for (chain_id_str, url) in &config.subgraph.chains {
            // `validate_subgraph_chains` already enforced this parses as i32, so
            // an unwrap-like context is enough; if it ever fires it's a code bug.
            let chain_id: i32 = chain_id_str.parse().with_context(|| {
                format!("invalid chain_id key '{chain_id_str}' in subgraph.chains (expected i32)")
            })?;
            let subgraph = SubgraphClient::new(url.clone())
                .with_context(|| format!("Failed to build subgraph client for chain {chain_id}"))?;
            let poller =
                SubgraphPoller::new(subgraph, db.clone(), chain_id, poll_interval, batch_size)
                    .await
                    .with_context(|| {
                        format!("Failed to initialize subgraph poller for chain {chain_id}")
                    })?;
            subgraph_pollers.push((chain_id, poller));
        }

        // NATS ingests events from every chain published upstream. To keep the
        // observer self-consistent, restrict the consumer to the chains we can
        // actually serve downstream (i.e. those with a subgraph or S3 config).
        let allowed_chains: HashSet<u32> = config
            .subgraph
            .chains
            .keys()
            .chain(config.s3.chains.keys())
            .filter_map(|s| s.parse::<u32>().ok())
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
            db,
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

        let cancel = CancellationToken::new();
        let mut subgraph_poller_set: JoinSet<PollerOutcome> = JoinSet::new();
        let mut task_to_chain: HashMap<TaskId, i32> = HashMap::new();
        for (chain_id, poller) in self.subgraph_pollers {
            let token = cancel.clone();
            let handle = subgraph_poller_set.spawn(async move {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => PollerOutcome::Cancelled,
                    res = poller.run() => PollerOutcome::Exited(res),
                }
            });
            task_to_chain.insert(handle.id(), chain_id);
        }

        let exit = tokio::select! {
            res = subgraph_poller_set.join_next_with_id() => Exit::Poller(res),
            res = nats_consumer.run() => Exit::Nats(res),
            res = s3_resolver.run() => Exit::S3(res),
            res = server => Exit::Server(res),
        };

        info!("triggering graceful shutdown of remaining subgraph pollers");
        cancel.cancel();
        drain_poller_set(&mut subgraph_poller_set, &task_to_chain).await;

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
            Exit::Poller(maybe) => Err(map_first_poller_exit(maybe, &task_to_chain)),
        }
    }
}

enum Exit {
    Poller(Option<Result<(TaskId, PollerOutcome), tokio::task::JoinError>>),
    Nats(Result<(), crate::errors::ObserverError>),
    S3(Result<(), crate::errors::S3ResolverError>),
    Server(Result<(), std::io::Error>),
}

/// Refuse to start if any subgraph-configured chain lacks a matching S3 bucket:
/// the poller would ingest handles whose ciphertexts could never be resolved,
/// leaving them stuck `processed_by_s3 = false` forever. The reverse direction
/// (S3 without subgraph) is allowed — NATS may still populate those rows.
fn ensure_chain_consistency(config: &Config) -> Result<()> {
    let s3_chains: std::collections::HashSet<&str> =
        config.s3.chains.keys().map(String::as_str).collect();
    let missing: Vec<&str> = config
        .subgraph
        .chains
        .keys()
        .map(String::as_str)
        .filter(|id| !s3_chains.contains(id))
        .collect();
    if !missing.is_empty() {
        return Err(anyhow!(
            "config inconsistency: subgraph poller is configured for chain(s) {missing:?} \
             but no matching S3 bucket is configured. Either add the S3 config for these \
             chain(s) or remove them from subgraph.chains."
        ));
    }
    Ok(())
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
