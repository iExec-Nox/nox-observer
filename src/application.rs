use anyhow::{Context, Result};
use axum::{Router, extract::FromRef, routing::get};
use axum_prometheus::{
    Handle, MakeDefaultHandle, PrometheusMetricLayer, PrometheusMetricLayerBuilder,
};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::handlers;

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
}

impl Application {
    pub async fn new(config: Config) -> Result<Self> {
        let prometheus_layer = PrometheusMetricLayerBuilder::new()
            .with_allow_patterns(&["/", "/health", "/metrics"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        Ok(Self {
            config,
            state: AppState { metrics_handle },
            prometheus_layer,
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

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("Server encountered an error during execution")?;

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
