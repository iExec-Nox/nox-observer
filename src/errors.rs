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

    #[error("S3 resolver error: {0}")]
    S3(String),
}

impl IntoResponse for ObserverError {
    fn into_response(self) -> axum::response::Response {
        warn!("Request failed: {}", self);
        let status = match &self {
            ObserverError::Nats(_) | ObserverError::Poller(_) | ObserverError::S3(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
    #[error("S3 error: {0}")]
    S3(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl S3ResolverError {
    /// Errors that are likely to resolve on their own (network blip, transient DB).
    /// S3 errors are transient when they indicate network/timeout/5xx conditions.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::S3(msg) => {
                // Stop-gap substring heuristic over the stringified S3 error;
                // replace with typed aws-sdk error classification
                // (ProvideErrorMetadata / retryability) once the S3 client lands.
                // Deliberately over-matches (e.g. "500" also matches "1500ms"):
                // a false-transient is safe here — the resolver bounded-retries
                // and then skips, it never blocks the pipeline.
                let lower = msg.to_lowercase();
                lower.contains("timeout")
                    || lower.contains("timed out")
                    || lower.contains("connection")
                    || lower.contains("dispatch")
                    || lower.contains("503")
                    || lower.contains("500")
                    || lower.contains("internal")
                    || lower.contains("unavailable")
            }
            Self::Database(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::S3ResolverError;

    #[test]
    fn is_transient_returns_true_when_message_is_timeout() {
        assert!(S3ResolverError::S3("request timed out".to_string()).is_transient());
    }

    #[test]
    fn is_transient_returns_true_when_message_is_503() {
        assert!(S3ResolverError::S3("503 service unavailable".to_string()).is_transient());
    }

    #[test]
    fn is_transient_returns_false_when_message_is_404() {
        assert!(!S3ResolverError::S3("404 not found".to_string()).is_transient());
    }

    #[test]
    fn is_transient_returns_false_when_message_is_access_denied() {
        assert!(!S3ResolverError::S3("access denied".to_string()).is_transient());
    }

    #[test]
    fn is_transient_returns_false_when_message_has_no_known_keyword() {
        // Pins the default-deny path: an unrecognized message is not retried.
        assert!(!S3ResolverError::S3(String::new()).is_transient());
        assert!(!S3ResolverError::S3("bucket not found".to_string()).is_transient());
    }
}
