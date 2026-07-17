use axum::{Json, http::StatusCode, response::IntoResponse, response::Response};
use serde_json::json;
use thiserror::Error;
use tracing::error;

// ==================================
// NATS consumer error
// ==================================

#[derive(Error, Debug)]
pub enum NatsError {
    #[error("NATS connect error: {0}")]
    Connect(String),
    #[error("NATS stream error: {0}")]
    Stream(String),
    #[error("NATS message error: {0}")]
    Message(String),
}

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
pub enum SubgraphPollerError {
    #[error(transparent)]
    Subgraph(#[from] SubgraphError),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl SubgraphPollerError {
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

// ==================================
// HTTP handler error
// ==================================

#[derive(Debug, Error)]
pub enum ObserverError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

pub type ObserverResult<T> = Result<T, ObserverError>;

impl IntoResponse for ObserverError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message.clone()),
            Self::Database(e) => {
                error!("database error handling request: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{ObserverError, S3ResolverError};
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use serde_json::Value;

    #[test]
    fn is_transient_returns_true_when_variant_is_s3_transient() {
        assert!(S3ResolverError::S3Transient("request timed out".to_string()).is_transient());
    }

    #[test]
    fn is_transient_returns_false_when_variant_is_permanent_s3() {
        assert!(!S3ResolverError::S3("access denied".to_string()).is_transient());
    }

    #[tokio::test]
    async fn bad_request_into_response_returns_400_with_message() {
        let response =
            ObserverError::BadRequest("chain_id must be an integer".to_string()).into_response();
        assert_eq!(StatusCode::BAD_REQUEST, response.status());

        let body = body_json(response).await;
        assert_eq!(
            serde_json::json!({ "error": "chain_id must be an integer" }),
            body
        );
    }

    #[tokio::test]
    async fn database_error_into_response_returns_500_and_hides_detail() {
        let response = ObserverError::Database(sqlx::Error::RowNotFound).into_response();
        assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, response.status());

        let body = body_json(response).await;
        assert_eq!(
            serde_json::json!({ "error": "internal server error" }),
            body
        );

        // The underlying sqlx error detail must not leak to the client.
        let error_message = body["error"].as_str().unwrap();
        assert!(!error_message.contains("RowNotFound"));
        assert!(!error_message.contains("no rows returned"));
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
