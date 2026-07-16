use std::collections::HashMap;

use axum::{
    Json,
    extract::{Query, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use chrono::Utc;
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::{Value, json};

use crate::db::Db;
use crate::errors::{ObserverError, ObserverResult};

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

/// Parses and validates the `chain_id` query parameter shared by handle
/// routes. Missing, non-integer, or non-positive values are reported as
/// `BadRequest`.
fn parse_chain_id(params: &HashMap<String, String>) -> ObserverResult<i32> {
    let chain_id: i32 = params
        .get("chain_id")
        .ok_or_else(|| ObserverError::BadRequest("missing query parameter: chain_id".to_string()))?
        .parse()
        .map_err(|_| ObserverError::BadRequest("chain_id must be a valid i32".to_string()))?;

    if chain_id <= 0 {
        return Err(ObserverError::BadRequest(
            "chain_id must be a positive integer".to_string(),
        ));
    }

    Ok(chain_id)
}

/// `GET /v0/handles/unresolved/count?chain_id=<int>` — counts handles that
/// have not yet been resolved for the given chain, along with the block
/// range they span. `oldest_block`/`newest_block` are `null` when there are
/// no unresolved handles.
pub async fn unresolved_count(
    State(db): State<Db>,
    Query(params): Query<HashMap<String, String>>,
) -> ObserverResult<impl IntoResponse> {
    let chain_id = parse_chain_id(&params)?;

    let count = db.fetch_unresolved_count(chain_id).await?;

    Ok(Json(json!({
        "chain_id": chain_id,
        "unresolved": count.unresolved,
        "oldest_block": count.oldest_block,
        "newest_block": count.newest_block,
    })))
}

/// Fallback handler for unknown routes.
pub async fn not_found(uri: Uri) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("Route not found {}", uri.path()) })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_chain_id_returns_ok_when_valid_positive() {
        assert_eq!(
            421614,
            parse_chain_id(&params(&[("chain_id", "421614")])).unwrap()
        );
    }

    #[test]
    fn parse_chain_id_returns_bad_request_when_missing() {
        let result = parse_chain_id(&params(&[]));
        assert!(matches!(result, Err(ObserverError::BadRequest(_))));
    }

    #[test]
    fn parse_chain_id_returns_bad_request_when_empty() {
        let result = parse_chain_id(&params(&[("chain_id", "")]));
        assert!(matches!(result, Err(ObserverError::BadRequest(_))));
    }

    #[test]
    fn parse_chain_id_returns_bad_request_when_zero() {
        let result = parse_chain_id(&params(&[("chain_id", "0")]));
        assert!(matches!(result, Err(ObserverError::BadRequest(_))));
    }

    #[test]
    fn parse_chain_id_returns_bad_request_when_negative() {
        let result = parse_chain_id(&params(&[("chain_id", "-5")]));
        assert!(matches!(result, Err(ObserverError::BadRequest(_))));
    }

    #[test]
    fn parse_chain_id_returns_bad_request_when_non_numeric() {
        let result = parse_chain_id(&params(&[("chain_id", "abc")]));
        assert!(matches!(result, Err(ObserverError::BadRequest(_))));
    }

    #[test]
    fn parse_chain_id_returns_bad_request_when_fractional() {
        let result = parse_chain_id(&params(&[("chain_id", "1.5")]));
        assert!(matches!(result, Err(ObserverError::BadRequest(_))));
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}
