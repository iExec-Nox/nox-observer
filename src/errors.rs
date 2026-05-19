use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::json;
use thiserror::Error;
use tracing::warn;

#[derive(Error, Debug)]
pub enum ObserverError {
    #[error("Error message example: {0}")]
    ErrorVariantExample(String),
}

impl IntoResponse for ObserverError {
    fn into_response(self) -> axum::response::Response {
        warn!("Request failed: {}", self);
        let status = match &self {
            ObserverError::ErrorVariantExample(_) => StatusCode::BAD_REQUEST,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

pub type ObserverResult<T> = Result<T, ObserverError>;
