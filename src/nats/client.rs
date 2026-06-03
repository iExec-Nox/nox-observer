//! NATS client with JetStream support — consumer-only surface.
//!
//! Pasted from `nox-ingestor/src/nats/client.rs` (verbatim except: dropped
//! publisher-only `setup_stream`/`state`/`is_connected`; swapped error type
//! `NatsError` → `ObserverError::Nats`; renamed env-var labels in error
//! messages from `NOX_INGESTOR_*` to `NOX_OBSERVER_*`).

use async_nats::jetstream::{self, Context as JetStreamContext};
use async_nats::rustls::pki_types::pem::PemObject;
use async_nats::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use async_nats::rustls::{ClientConfig, RootCertStore};
use async_nats::{ConnectOptions, Event};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::{NatsConfig, TlsConfig};
use crate::errors::ObserverError;

/// Connection state for NATS client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Connected => write!(f, "Connected"),
            ConnectionState::Disconnected => write!(f, "Disconnected"),
        }
    }
}

/// NATS client with JetStream support
pub struct NatsClient {
    jetstream: Arc<JetStreamContext>,
    state_rx: watch::Receiver<ConnectionState>,
}

impl NatsClient {
    /// Connect to NATS server
    pub async fn connect(config: &NatsConfig) -> Result<Self, ObserverError> {
        let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);

        let state_tx_clone = state_tx.clone();

        let mut options = ConnectOptions::new()
            .event_callback(move |event| {
                let state_tx = state_tx_clone.clone();
                async move {
                    match event {
                        Event::Connected => {
                            info!("NATS connected");
                            let _ = state_tx.send(ConnectionState::Connected);
                        }
                        Event::Disconnected => {
                            warn!("NATS disconnected");
                            let _ = state_tx.send(ConnectionState::Disconnected);
                        }
                        Event::ServerError(err) => error!(error = %err, "NATS server error"),
                        Event::ClientError(err) => error!(error = %err, "NATS client error"),
                        Event::LameDuckMode => warn!("NATS server in lame duck mode"),
                        Event::SlowConsumer(sid) => {
                            warn!(subscription_id = sid, "NATS slow consumer")
                        }
                        _ => {}
                    }
                }
            })
            .retry_on_initial_connect();

        if config.tls.enabled {
            let tls_config = build_rustls_client_config(&config.tls)?;
            options = options.require_tls(true).tls_client_config(tls_config);
        }

        info!(
            urls = ?config.urls,
            tls = config.tls.enabled,
            "Connecting to NATS"
        );

        let client = options.connect(&config.urls[..]).await.map_err(|e| {
            ObserverError::Nats(format!(
                "Failed to connect to NATS cluster {:?}: {}",
                config.urls, e
            ))
        })?;

        let jetstream = jetstream::new(client.clone());

        info!("NATS client initialized; awaiting connection");

        Ok(Self {
            jetstream: Arc::new(jetstream),
            state_rx,
        })
    }

    /// Get the JetStream context
    pub fn jetstream(&self) -> Arc<JetStreamContext> {
        Arc::clone(&self.jetstream)
    }

    /// Get a receiver for connection state changes
    pub fn state_receiver(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }
}

/// Normalizes a PEM string that may have been collapsed into a single line.
fn normalize_pem(pem: &str) -> String {
    let pem = pem.replace("\\n", "\n");
    let normalized = pem
        .trim_end()
        .replace("----- ", "-----\n")
        .replace(" -----", "\n-----");
    let trimmed = normalized
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    trimmed + "\n"
}

/// Build an in-memory rustls `ClientConfig` from PEM strings supplied via env vars.
fn build_rustls_client_config(tls: &TlsConfig) -> Result<ClientConfig, ObserverError> {
    for (label, value) in [("ca", &tls.ca), ("cert", &tls.cert), ("key", &tls.key)] {
        if value.trim().is_empty() {
            return Err(ObserverError::Nats(format!(
                "TLS enabled but `{label}` PEM content is empty (set NOX_OBSERVER_NATS__TLS__{} env var)",
                label.to_uppercase()
            )));
        }
    }

    let ca = normalize_pem(&tls.ca);
    let cert = normalize_pem(&tls.cert);
    let key = normalize_pem(&tls.key);

    let mut roots = RootCertStore::empty();
    for cert_der in CertificateDer::pem_slice_iter(ca.as_bytes()) {
        let cert_der =
            cert_der.map_err(|e| ObserverError::Nats(format!("Failed to parse CA PEM: {e}")))?;
        roots.add(cert_der).map_err(|e| {
            ObserverError::Nats(format!("Failed to add CA cert to root store: {e}"))
        })?;
    }
    if roots.is_empty() {
        return Err(ObserverError::Nats(
            "No CA certificates found in PEM content".to_string(),
        ));
    }

    let cert_chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ObserverError::Nats(format!("Failed to parse client cert PEM: {e}")))?;
    if cert_chain.is_empty() {
        return Err(ObserverError::Nats(
            "No client certificates found in PEM content".to_string(),
        ));
    }

    let private_key = PrivateKeyDer::from_pem_slice(key.as_bytes())
        .map_err(|e| ObserverError::Nats(format!("Failed to parse client key PEM: {e}")))?;

    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, private_key)
        .map_err(|e| ObserverError::Nats(format!("Failed to build rustls client config: {e}")))
}
