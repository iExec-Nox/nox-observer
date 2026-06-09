//! NATS JetStream consumer, pull loop, 2-tier ack, handle extraction.
//!
//! Pull-loop topology:
//! - one PG transaction per NATS message via `Db::upsert_handles_in_tx`
//! - 2-tier ack: ACK on success / ACK on serde-fail or extract failure
//!   (chain_id or block_number overflow) / no-ack on DB error
//! - extracts handles per operator variant before upserting

use std::collections::HashSet;

use async_nats::jetstream;
use futures_util::StreamExt;
use tracing::{debug, error, info, warn};

use crate::config::NatsConfig;
use crate::db::{Db, NewHandle};
use crate::errors::ObserverError;
use crate::events::TransactionMessage;
use crate::nats::client::{ConnectionState, NatsClient};

pub struct NatsConsumer {
    nats_client: NatsClient,
    db: Db,
    config: NatsConfig,
    /// Chains we want to ingest from NATS. Messages for any other `chain_id`
    /// are silently ACK-discarded so the JetStream consumer doesn't redeliver
    /// them. The set is built at startup from `subgraph.chains ∪ s3.chains`.
    allowed_chains: HashSet<i32>,
}

impl NatsConsumer {
    pub fn new(
        nats_client: NatsClient,
        db: Db,
        config: NatsConfig,
        allowed_chains: HashSet<i32>,
    ) -> Self {
        Self {
            nats_client,
            db,
            config,
            allowed_chains,
        }
    }

    pub async fn run(self) -> Result<(), ObserverError> {
        let jetstream = self.nats_client.jetstream();
        let mut state_rx = self.nats_client.state_receiver();

        let stream = jetstream
            .get_stream(&self.config.stream_name)
            .await
            .map_err(|e| ObserverError::Nats(format!("get_stream failed: {e}")))?;

        let consumer = stream
            .get_or_create_consumer(
                &self.config.consumer_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(self.config.consumer_name.clone()),
                    max_deliver: self.config.consumer_max_deliver,
                    max_ack_pending: self.config.max_ack_pending,
                    max_batch: self.config.max_batch,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| ObserverError::Nats(format!("get_or_create_consumer failed: {e}")))?;

        let mut subscriber = consumer
            .stream()
            .max_messages_per_batch(
                usize::try_from(self.config.max_batch)
                    .map_err(|e| ObserverError::Nats(format!("max_batch overflow: {e}")))?,
            )
            .messages()
            .await
            .map_err(|e| ObserverError::Nats(format!("subscriber init failed: {e}")))?;

        let mut connected = *state_rx.borrow() == ConnectionState::Connected;
        info!(
            connected,
            stream = self.config.stream_name,
            consumer = self.config.consumer_name,
            "NATS consumer entering main loop"
        );

        loop {
            tokio::select! {
                result = state_rx.changed() => {
                    if result.is_err() {
                        warn!("NATS state watch channel closed. Exiting consumer loop");
                        break;
                    }
                    let new_state = *state_rx.borrow();
                    let was_connected = connected;
                    connected = new_state == ConnectionState::Connected;
                    match (was_connected, connected) {
                        (false, true) => info!("NATS reconnected. Resuming pull loop"),
                        (true, false) => warn!("NATS disconnected. Pausing pull loop"),
                        _ => {}
                    }
                }

                maybe_message = subscriber.next(), if connected => {
                    // Exit if `None` because it means the JetStream message stream terminated
                    let Some(message) = maybe_message else {
                        warn!("NATS message stream ended; exiting consumer loop");
                        break;
                    };
                    match message {
                        Ok(msg) => self.process_message(msg).await,
                        Err(e) => {
                            let kind = classify_pull_error(&e);
                            error!(error = %e, kind, "NATS pull error");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Process one delivered message end to end: deserialize, extract handles,
    /// persist in a single transaction, then ack.
    ///
    /// Follows the 2-tier ack policy: poison payloads (deserialize or extract
    /// failures) are ack-discarded, while DB errors are left un-acked so
    /// JetStream redelivers.
    async fn process_message(&self, msg: jetstream::Message) {
        let tx_msg: TransactionMessage = match serde_json::from_slice(&msg.payload) {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "deserialize failed; ACK-discarding poison payload");
                if let Err(ack_err) = msg.ack().await {
                    error!(error = %ack_err, "ACK after deserialize-fail failed");
                }
                return;
            }
        };

        // Skip + ACK messages from chains we don't index (no subgraph/S3 config).
        // The broker keeps delivering everything (no subject-level filter possible
        // until upstream adds chain_id to the subject), so we drop here. The
        // i32::try_from also catches the (theoretical) overflow case: a chain_id
        // that doesn't fit in i32 can't be in the configured set anyway.
        let configured =
            i32::try_from(tx_msg.chain_id).is_ok_and(|id| self.allowed_chains.contains(&id));
        if !configured {
            debug!(
                chain_id = tx_msg.chain_id,
                tx_hash = tx_msg.transaction_hash,
                "ignoring NATS message for non-configured chain; ACK-discarding"
            );
            if let Err(ack_err) = msg.ack().await {
                error!(error = %ack_err, "ACK after chain-filter skip failed");
            }
            return;
        }

        let handles = match extract_handles(&tx_msg) {
            Ok(h) => h,
            Err(e) => {
                error!(
                    error = %e,
                    chain_id = tx_msg.chain_id,
                    tx_hash = tx_msg.transaction_hash,
                    "extract_handles failed; ACK-discarding"
                );
                if let Err(ack_err) = msg.ack().await {
                    error!(error = %ack_err, "ACK after extract-fail failed");
                }
                return;
            }
        };

        // A message carrying no extractable handles (e.g. empty `events`) has
        // nothing to persist, so ACK it directly and skip the empty begin/commit
        // round-trip.
        if handles.is_empty() {
            if let Err(ack_err) = msg.ack().await {
                error!(error = %ack_err, "ACK after empty-extract failed");
            }
            return;
        }

        match self.db.upsert_handles_in_tx(&handles).await {
            Ok(_) => {
                if let Err(ack_err) = msg.ack().await {
                    error!(
                        error = %ack_err,
                        tx_hash = tx_msg.transaction_hash,
                        "ACK failed after successful upsert"
                    );
                } else {
                    debug!(
                        tx_hash = tx_msg.transaction_hash,
                        handles = handles.len(),
                        "ACK after successful upsert"
                    );
                }
            }
            Err(e) => {
                // 2-tier ack: omit ack on DB error → JetStream redelivers after
                // ack_wait, up to max_deliver, then drops. No NAK (NAK would force
                // immediate redelivery; ack_wait is the safer pacing).
                error!(
                    error = %e,
                    tx_hash = tx_msg.transaction_hash,
                    "DB upsert failed; no-ack (JetStream will redeliver)"
                );
            }
        }
    }
}

/// Map a `TransactionMessage` to one [`NewHandle`] per emitted handle. Every row
/// shares the transaction-level metadata `(chain_id, caller, tx_hash,
/// block_number)`; `operator` is the per-event wire tag and `handle_id` is
/// per emitted handle, a single event may emit several rows, and `operator`
/// varies across events.
///
/// Returns `Err(ObserverError::Nats(_))` when `chain_id` overflows i32 or
/// `block_number` overflows i64 (both treated as poison: ACK-discarded by the
/// caller).
fn extract_handles(msg: &TransactionMessage) -> Result<Vec<NewHandle>, ObserverError> {
    let chain_id_i32 = i32::try_from(msg.chain_id).map_err(|_| {
        ObserverError::Nats(format!(
            "chain_id {} does not fit in i32 (handles.chain_id is INT)",
            msg.chain_id
        ))
    })?;
    // Lowercase canonical: alloy's LowerHex impl. Stable for future joins;
    // schema CHECK accepts both cases but we pick one.
    let caller = format!("{:#x}", msg.caller);
    let block_number = i64::try_from(msg.block_number).map_err(|_| {
        ObserverError::Nats(format!(
            "block_number {} does not fit in i64",
            msg.block_number
        ))
    })?;

    let mut out = Vec::new();
    let push = |out: &mut Vec<NewHandle>, handle_id: &str, op_tag: &str| {
        out.push(NewHandle {
            handle_id: handle_id.to_string(),
            chain_id: chain_id_i32,
            operator: op_tag.to_string(),
            caller: Some(caller.clone()),
            tx_hash: Some(msg.transaction_hash.clone()),
            block_timestamp: None,
            block_number: Some(block_number),
            resolved_at: None,
            processed_by_subgraph: false,
            processed_by_s3: false,
            processed_by_nats: true,
        });
    };

    for event in &msg.events {
        let operator_tag = event.operator.wire_tag();
        for handle_id in event.operator.emitted_handles() {
            push(&mut out, handle_id, operator_tag);
        }
    }
    Ok(out)
}

/// Classify an `async_nats` pull error by its message text, for use as a
/// tracing-field label only.
fn classify_pull_error<E: std::fmt::Display>(e: &E) -> &'static str {
    let s = e.to_string().to_lowercase();
    if s.contains("timed out") || s.contains("timeout") {
        "timeout"
    } else if s.contains("disconnect") || s.contains("connection") {
        "disconnect"
    } else if s.contains("serde") || s.contains("deserialize") || s.contains("invalid") {
        "deserialize"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Shared test constants ────────────────────────────────────────────────
    // Anvil dev account #0; lowercase form is what extract_handles should emit.
    const CALLER_RAW: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cfFFb92266";
    const CALLER_LOWERCASE: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    const TEST_CHAIN_ID: u32 = 1;
    const TEST_BLOCK_NUMBER: u64 = 10;
    const TEST_TX_HASH: &str = "0xdead";

    // Operand / result handle placeholders reused across operator-family fixtures.
    const HANDLE_L: &str = "0x1";
    const HANDLE_R: &str = "0x2";
    const HANDLE_RES: &str = "0xcc";
    const HANDLE_SUCCESS: &str = "0xdd";
    // Positional placeholders for 4-6 handle variants (select/transfer/mint/burn).
    const HANDLE_1: &str = "0x1";
    const HANDLE_2: &str = "0x2";
    const HANDLE_3: &str = "0x3";
    const HANDLE_4: &str = "0x4";
    const HANDLE_5: &str = "0x5";
    const HANDLE_6: &str = "0x6";

    fn parse(json: &str) -> TransactionMessage {
        serde_json::from_str(json).expect("payload must deserialize")
    }

    /// Wrap an events-JSON fragment in the TransactionMessage envelope using
    /// the default test chain_id. Centralises the boilerplate every test repeats.
    fn make_tx_message_json(events_json: &str) -> String {
        make_tx_message_json_with_chain_id(u64::from(TEST_CHAIN_ID), events_json)
    }

    /// Takes `chain_id` as `u64` only so the overflow test can pass `u32::MAX`
    /// literally. The boundary under test is the `i32` narrowing inside
    /// `extract_handles`, NOT JSON→u32 parsing: a `chainId` above `u32::MAX`
    /// would fail to deserialize into the u32 field before `extract_handles` runs.
    fn make_tx_message_json_with_chain_id(chain_id: u64, events_json: &str) -> String {
        format!(
            r#"{{
                "chainId": {chain_id}, "blockNumber": {TEST_BLOCK_NUMBER},
                "caller": "{CALLER_RAW}", "transactionHash": "{TEST_TX_HASH}",
                "events": [{events_json}]
            }}"#
        )
    }

    #[test]
    fn extract_handles_returns_one_handle_when_event_is_wrap_as_public_handle() {
        let json = make_tx_message_json(
            r#"{"logIndex":0,"type":"wrap_as_public_handle",
                 "value":"42","teeType":1,"handle":"0xaaaa"}"#,
        );
        let rows = extract_handles(&parse(&json)).expect("ok");
        assert_eq!(1, rows.len());
        assert_eq!("0xaaaa", rows[0].handle_id);
        assert_eq!("wrap_as_public_handle", rows[0].operator);
        assert_eq!(Some(CALLER_LOWERCASE.to_string()), rows[0].caller);
        assert_eq!(i32::try_from(TEST_CHAIN_ID).unwrap(), rows[0].chain_id);
        assert_eq!(
            Some(i64::try_from(TEST_BLOCK_NUMBER).unwrap()),
            rows[0].block_number
        );
        assert!(rows[0].processed_by_nats);
        assert!(!rows[0].processed_by_subgraph);
        assert!(!rows[0].processed_by_s3);
        assert!(rows[0].block_timestamp.is_none());
        assert_eq!(Some(TEST_TX_HASH.to_string()), rows[0].tx_hash);
    }

    #[test]
    fn extract_handles_returns_one_result_handle_when_event_is_arithmetic() {
        for op in ["add", "sub", "mul", "div"] {
            let json = make_tx_message_json(&format!(
                r#"{{"logIndex":0,"type":"{op}",
                     "leftHandOperand":"{HANDLE_L}","rightHandOperand":"{HANDLE_R}","result":"{HANDLE_RES}"}}"#
            ));
            let rows = extract_handles(&parse(&json)).expect("ok");
            assert_eq!(1, rows.len(), "{op} should emit exactly 1 handle");
            assert_eq!(HANDLE_RES, rows[0].handle_id);
            assert_eq!(op, rows[0].operator);
        }
    }

    #[test]
    fn extract_handles_returns_success_and_result_when_event_is_safe_arithmetic() {
        for op in ["safe_add", "safe_sub", "safe_mul", "safe_div"] {
            let json = make_tx_message_json(&format!(
                r#"{{"logIndex":0,"type":"{op}",
                     "leftHandOperand":"{HANDLE_L}","rightHandOperand":"{HANDLE_R}",
                     "success":"{HANDLE_SUCCESS}","result":"{HANDLE_RES}"}}"#
            ));
            let rows = extract_handles(&parse(&json)).expect("ok");
            assert_eq!(2, rows.len(), "{op} should emit 2 handles");
            assert_eq!(HANDLE_SUCCESS, rows[0].handle_id);
            assert_eq!(HANDLE_RES, rows[1].handle_id);
            assert!(rows.iter().all(|r| r.operator == op));
        }
    }

    #[test]
    fn extract_handles_returns_one_result_handle_when_event_is_boolean() {
        for op in ["eq", "ne", "ge", "gt", "le", "lt"] {
            let json = make_tx_message_json(&format!(
                r#"{{"logIndex":0,"type":"{op}",
                     "leftHandOperand":"{HANDLE_L}","rightHandOperand":"{HANDLE_R}","result":"{HANDLE_RES}"}}"#
            ));
            let rows = extract_handles(&parse(&json)).expect("ok");
            assert_eq!(1, rows.len());
            assert_eq!(HANDLE_RES, rows[0].handle_id);
            assert_eq!(op, rows[0].operator);
        }
    }

    #[test]
    fn extract_handles_returns_one_result_handle_when_event_is_select() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex":0,"type":"select",
                 "condition":"{HANDLE_1}","ifTrue":"{HANDLE_2}","ifFalse":"{HANDLE_3}","result":"{HANDLE_4}"}}"#
        ));
        let rows = extract_handles(&parse(&json)).expect("ok");
        assert_eq!(1, rows.len());
        assert_eq!(HANDLE_4, rows[0].handle_id);
        assert_eq!("select", rows[0].operator);
    }

    #[test]
    fn extract_handles_returns_success_and_new_balances_when_event_is_transfer() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex":0,"type":"transfer",
                 "balanceFrom":"{HANDLE_1}","balanceTo":"{HANDLE_2}","amount":"{HANDLE_3}",
                 "success":"{HANDLE_4}","newBalanceFrom":"{HANDLE_5}","newBalanceTo":"{HANDLE_6}"}}"#
        ));
        let rows = extract_handles(&parse(&json)).expect("ok");
        assert_eq!(3, rows.len());
        assert_eq!(HANDLE_4, rows[0].handle_id);
        assert_eq!(HANDLE_5, rows[1].handle_id);
        assert_eq!(HANDLE_6, rows[2].handle_id);
        assert!(rows.iter().all(|r| r.operator == "transfer"));
    }

    #[test]
    fn extract_handles_returns_success_and_new_balance_and_total_supply_when_event_is_mint() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex":0,"type":"mint",
                 "balanceTo":"{HANDLE_1}","amount":"{HANDLE_2}","totalSupply":"{HANDLE_3}",
                 "success":"{HANDLE_4}","newBalanceTo":"{HANDLE_5}","newTotalSupply":"{HANDLE_6}"}}"#
        ));
        let rows = extract_handles(&parse(&json)).expect("ok");
        assert_eq!(3, rows.len());
        assert_eq!(HANDLE_4, rows[0].handle_id);
        assert_eq!(HANDLE_5, rows[1].handle_id);
        assert_eq!(HANDLE_6, rows[2].handle_id);
        assert!(rows.iter().all(|r| r.operator == "mint"));
    }

    #[test]
    fn extract_handles_returns_success_and_new_balance_and_total_supply_when_event_is_burn() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex":0,"type":"burn",
                 "balanceFrom":"{HANDLE_1}","amount":"{HANDLE_2}","totalSupply":"{HANDLE_3}",
                 "success":"{HANDLE_4}","newBalanceFrom":"{HANDLE_5}","newTotalSupply":"{HANDLE_6}"}}"#
        ));
        let rows = extract_handles(&parse(&json)).expect("ok");
        assert_eq!(3, rows.len());
        assert_eq!(HANDLE_4, rows[0].handle_id);
        assert_eq!(HANDLE_5, rows[1].handle_id);
        assert_eq!(HANDLE_6, rows[2].handle_id);
        assert!(rows.iter().all(|r| r.operator == "burn"));
    }

    #[test]
    fn extract_handles_returns_all_handles_in_order_when_tx_has_multiple_events() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex":0,"type":"wrap_as_public_handle","value":"1","teeType":1,"handle":"0xa"}},
               {{"logIndex":1,"type":"add","leftHandOperand":"{HANDLE_1}","rightHandOperand":"{HANDLE_2}","result":"0xb"}},
               {{"logIndex":2,"type":"safe_add","leftHandOperand":"{HANDLE_1}","rightHandOperand":"{HANDLE_2}","success":"0xc","result":"0xd"}}"#
        ));
        let rows = extract_handles(&parse(&json)).expect("ok");
        assert_eq!(4, rows.len(), "1 + 1 + 2 = 4 handles");
        assert_eq!(
            vec!["0xa", "0xb", "0xc", "0xd"],
            rows.iter()
                .map(|r| r.handle_id.as_str())
                .collect::<Vec<_>>()
        );
        // All rows share tx-level metadata
        assert!(
            rows.iter()
                .all(|r| r.tx_hash == Some(TEST_TX_HASH.to_string()))
        );
        assert!(
            rows.iter()
                .all(|r| r.caller == Some(CALLER_LOWERCASE.to_string()))
        );
    }

    #[test]
    fn extract_handles_returns_err_when_chain_id_overflows_i32() {
        let json = make_tx_message_json_with_chain_id(
            u64::from(u32::MAX),
            &format!(
                r#"{{"logIndex":0,"type":"add",
                     "leftHandOperand":"{HANDLE_L}","rightHandOperand":"{HANDLE_R}","result":"{HANDLE_RES}"}}"#
            ),
        );
        let result = extract_handles(&parse(&json));
        assert!(result.is_err(), "u32::MAX chain_id should overflow i32");
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("chain_id"),
            "error message mentions chain_id: {err}"
        );
    }
}
