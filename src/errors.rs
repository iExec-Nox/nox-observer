use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::json;
use thiserror::Error;
use tracing::warn;

// ==================================
// HTTP layer error (Axum handlers)
// ==================================

#[derive(Error, Debug)]
pub enum ObserverError {
    #[error("NATS error: {0}")]
    Nats(String),
}

impl IntoResponse for ObserverError {
    fn into_response(self) -> axum::response::Response {
        warn!("Request failed: {}", self);
        let status = match &self {
            ObserverError::Nats(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
