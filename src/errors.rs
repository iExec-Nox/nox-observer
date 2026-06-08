use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::json;
use thiserror::Error;
use tracing::warn;

// ==================================
// HTTP layer error (Axum handlers)
// ==================================

#[derive(Error, Debug)]
pub enum ObserverError {
    #[error("NATS consumer error: {0}")]
    Nats(String),

    #[error("Subgraph poller error: {0}")]
    Poller(#[from] PollerError),
}

impl IntoResponse for ObserverError {
    fn into_response(self) -> axum::response::Response {
        warn!("Request failed: {}", self);
        let status = match &self {
            ObserverError::Nats(_) | ObserverError::Poller(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

pub type ObserverResult<T> = Result<T, ObserverError>;

// ==================================
// Subgraph client error
// ==================================

#[derive(Debug, Error)]
pub enum SubgraphError {
    #[error("HTTP request to the subgraph failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("subgraph returned errors: {0:?}")]
    GraphqlErrors(Vec<graphql_client::Error>),

    #[error("subgraph returned no data")]
    EmptyResponse,
}

impl SubgraphError {
    /// Returns true for errors that are likely to resolve on their own
    /// (network blip, momentary server hiccup). Callers may safely retry.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Http(_) | Self::EmptyResponse)
    }
}

pub type SubgraphResult<T> = Result<T, SubgraphError>;

// ==================================
// Subgraph poller error
// ==================================

#[derive(Debug, Error)]
pub enum PollerError {
    #[error(transparent)]
    Subgraph(#[from] SubgraphError),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl PollerError {
    /// Errors that are likely to resolve on their own (network, transient DB).
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Subgraph(e) => e.is_transient(),
            Self::Database(_) => true,
        }
    }
}

// ==================================
// S3 resolver error
// ==================================

#[derive(Debug, Error)]
pub enum S3ResolverError {
    /// A permanent S3 failure (auth, misconfiguration, malformed request).
    #[error("S3 error: {0}")]
    S3(String),

    /// A transient S3 failure (network blip, timeout, 5xx) safe to retry.
    #[error("transient S3 error: {0}")]
    S3Transient(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl S3ResolverError {
    /// Errors that are likely to resolve on their own (network blip, transient DB).
    /// S3 errors are classified at the point of failure from the typed SDK error.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::S3Transient(_) | Self::Database(_))
    }
}

#[cfg(test)]
mod tests {
    use super::S3ResolverError;

    #[test]
    fn is_transient_returns_true_when_variant_is_s3_transient() {
        assert!(S3ResolverError::S3Transient("request timed out".to_string()).is_transient());
    }

    #[test]
    fn is_transient_returns_false_when_variant_is_permanent_s3() {
        assert!(!S3ResolverError::S3("access denied".to_string()).is_transient());
    }
}
