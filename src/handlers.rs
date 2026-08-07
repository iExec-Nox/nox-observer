use axum::{
    Json,
    extract::State,
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use chrono::Utc;
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::{Value, json};

/// `GET /` — returns service name and current UTC timestamp.
pub async fn root() -> Json<Value> {
    Json(json!({
        "service": "nox-observer",
        "timestamp": Utc::now().to_rfc3339()
    }))
}

/// `GET /health` — liveness probe, always returns `{"status": "ok"}` when the process is up.
pub async fn health_check() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// `GET /metrics` — renders Prometheus metrics in the text exposition format.
pub async fn metrics(State(metrics_handle): State<PrometheusHandle>) -> String {
    metrics_handle.render()
}

/// Fallback handler for unknown routes.
pub async fn not_found(uri: Uri) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("Route not found {}", uri.path()) })),
    )
}
