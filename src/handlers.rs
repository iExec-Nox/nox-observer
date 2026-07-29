use std::collections::HashMap;

use axum::{
    Json,
    extract::{Query, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use chrono::{Duration, Utc};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use serde_json::{Value, json};

use crate::config::MonitoringConfig;
use crate::db::{Db, HandleStats};
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
        .map_err(|_| ObserverError::BadRequest("chain_id must be an integer".to_string()))?;

    if chain_id <= 0 {
        return Err(ObserverError::BadRequest(
            "chain_id must be a positive integer".to_string(),
        ));
    }

    Ok(chain_id)
}

/// Block-range bucket within [`HandleStatsResponse`]: how many handles fall
/// in the bucket and the block numbers they span. `oldest_block`/`newest_block`
/// are `null` when `count` is 0.
#[derive(Debug, Serialize, PartialEq)]
pub struct HandleBucket {
    pub count: i64,
    pub oldest_block: Option<i64>,
    pub newest_block: Option<i64>,
}

/// Response body for `GET /v0/handles/stats`.
#[derive(Debug, Serialize, PartialEq)]
pub struct HandleStatsResponse {
    pub chain_id: i32,
    pub latest_seen_block: Option<i64>,
    pub ignored: i64,
    pub resolved_but_not_seen_by_subgraph: i64,
    pub unresolved: HandleBucket,
    pub resolving: HandleBucket,
}

/// `GET /v0/handles/stats?chain_id=<int>` — reports not-yet-resolved
/// handles for the given chain, split into `unresolved` (past the monitoring
/// grace period) and `resolving` (within grace, or too fresh),
/// plus resolved-but-not-seen-by-subgraph and the latest seen block as reference
/// figures. `oldest_block`/`newest_block` are `null` when a bucket's `count` is 0
/// or when every unresolved handle has a NULL `block_number` (e.g. NATS-path handles
/// not yet indexed by the subgraph), since `block_number` is nullable.
pub async fn handle_stats(
    State(db): State<Db>,
    State(monitoring): State<MonitoringConfig>,
    Query(params): Query<HashMap<String, String>>,
) -> ObserverResult<impl IntoResponse> {
    let chain_id = parse_chain_id(&params)?;

    let grace_deadline = Utc::now() - Duration::seconds(monitoring.grace_period_seconds as i64);
    let row = db.fetch_handle_stats(chain_id, grace_deadline).await?;

    Ok(Json(build_response(chain_id, row)))
}

/// Fallback handler for unknown routes.
pub async fn not_found(uri: Uri) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("Route not found {}", uri.path()) })),
    )
}

/// Maps a raw [`HandleStats`] row into the nested response shape. Kept pure
/// (no DB, no clock) so it can be unit-tested directly.
fn build_response(chain_id: i32, row: HandleStats) -> HandleStatsResponse {
    HandleStatsResponse {
        chain_id,
        latest_seen_block: row.latest_seen_block,
        ignored: row.ignored_count,
        resolved_but_not_seen_by_subgraph: row.resolved_but_not_seen_by_subgraph,
        unresolved: HandleBucket {
            count: row.unresolved_count,
            oldest_block: row.unresolved_oldest_block,
            newest_block: row.unresolved_newest_block,
        },
        resolving: HandleBucket {
            count: row.resolving_count,
            oldest_block: row.resolving_oldest_block,
            newest_block: row.resolving_newest_block,
        },
    }
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

    #[test]
    fn build_response_maps_zero_counts_to_null_bounds() {
        let row = HandleStats {
            unresolved_count: 0,
            unresolved_oldest_block: None,
            unresolved_newest_block: None,
            resolving_count: 0,
            resolving_oldest_block: None,
            resolving_newest_block: None,
            resolved_but_not_seen_by_subgraph: 0,
            ignored_count: 0,
            latest_seen_block: None,
        };

        let response = build_response(421614, row);

        assert_eq!(421614, response.chain_id);
        assert_eq!(None, response.latest_seen_block);
        assert_eq!(0, response.ignored);
        assert_eq!(0, response.resolved_but_not_seen_by_subgraph);
        assert_eq!(
            HandleBucket {
                count: 0,
                oldest_block: None,
                newest_block: None
            },
            response.unresolved
        );
        assert_eq!(
            HandleBucket {
                count: 0,
                oldest_block: None,
                newest_block: None
            },
            response.resolving
        );

        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(Value::Null, value["latest_seen_block"]);
        assert_eq!(Value::Null, value["unresolved"]["oldest_block"]);
        assert_eq!(Value::Null, value["unresolved"]["newest_block"]);
        assert_eq!(Value::Null, value["resolving"]["oldest_block"]);
        assert_eq!(Value::Null, value["resolving"]["newest_block"]);
    }

    #[test]
    fn build_response_maps_populated_row_into_nested_buckets() {
        let row = HandleStats {
            unresolved_count: 3,
            unresolved_oldest_block: Some(18_500_210),
            unresolved_newest_block: Some(18_500_412),
            resolving_count: 2,
            resolving_oldest_block: Some(18_500_500),
            resolving_newest_block: Some(18_500_540),
            resolved_but_not_seen_by_subgraph: 5,
            ignored_count: 7,
            latest_seen_block: Some(18_500_600),
        };

        let response = build_response(421614, row);

        assert_eq!(
            HandleStatsResponse {
                chain_id: 421614,
                latest_seen_block: Some(18_500_600),
                ignored: 7,
                resolved_but_not_seen_by_subgraph: 5,
                unresolved: HandleBucket {
                    count: 3,
                    oldest_block: Some(18_500_210),
                    newest_block: Some(18_500_412),
                },
                resolving: HandleBucket {
                    count: 2,
                    oldest_block: Some(18_500_500),
                    newest_block: Some(18_500_540),
                },
            },
            response
        );

        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json!({
                "chain_id": 421614,
                "latest_seen_block": 18_500_600,
                "ignored": 7,
                "resolved_but_not_seen_by_subgraph": 5,
                "unresolved": { "count": 3, "oldest_block": 18_500_210, "newest_block": 18_500_412 },
                "resolving": { "count": 2, "oldest_block": 18_500_500, "newest_block": 18_500_540 },
            }),
            value
        );
    }
}
