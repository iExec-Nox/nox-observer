//! Supervision helpers for the multichain subgraph poller fleet.
//!
//! `Application` owns the `JoinSet` and the cancellation token; this module
//! provides the per-task race wrapper (`PollerOutcome`), the drain routine
//! used when one task forces global shutdown, and the mapping from a
//! `JoinSet` exit back to an `anyhow::Error` enriched with the offending
//! `chain_id`.

use std::collections::HashMap;

use anyhow::anyhow;
use tokio::task::{Id as TaskId, JoinSet};
use tracing::{error, info, warn};

use crate::errors::SubgraphPollerError;

/// Outcome of one subgraph poller task wrapped around a cancellation race.
pub enum PollerOutcome {
    Cancelled,
    Exited(Result<(), SubgraphPollerError>),
}

/// Await every remaining poller after cancellation has been requested.
///
/// Logs each task's terminal state with its `chain_id`. Never returns an error:
/// the original exit reason is reported separately by `map_first_poller_exit`,
/// and we want to give every task a chance to flush before the process dies.
pub async fn drain_poller_set(
    set: &mut JoinSet<PollerOutcome>,
    task_to_chain: &HashMap<TaskId, i32>,
) {
    while let Some(res) = set.join_next_with_id().await {
        match res {
            Ok((id, PollerOutcome::Cancelled)) => {
                info!(chain_id = ?task_to_chain.get(&id), "subgraph poller stopped cleanly");
            }
            Ok((id, PollerOutcome::Exited(Ok(())))) => {
                warn!(
                    chain_id = ?task_to_chain.get(&id),
                    "subgraph poller exited with Ok before cancellation took effect (run should be infinite)"
                );
            }
            Ok((id, PollerOutcome::Exited(Err(e)))) => {
                error!(
                    chain_id = ?task_to_chain.get(&id),
                    "subgraph poller failed during drain: {e:#}"
                );
            }
            Err(join_err) => {
                error!(
                    chain_id = ?task_to_chain.get(&join_err.id()),
                    "subgraph poller task panicked during drain: {join_err}"
                );
            }
        }
    }
}

/// Convert the first poller-task exit picked up by `select!` into an
/// `anyhow::Error` for `Application::run` to bubble up. `Ok(())` from `run`
/// is treated as an error because pollers are expected to loop forever.
pub fn map_first_poller_exit(
    maybe: Option<Result<(TaskId, PollerOutcome), tokio::task::JoinError>>,
    task_to_chain: &HashMap<TaskId, i32>,
) -> anyhow::Error {
    match maybe {
        Some(Ok((id, PollerOutcome::Cancelled))) => {
            let chain = task_to_chain.get(&id);
            anyhow!("subgraph poller for chain {chain:?} reported Cancelled without prior signal")
        }
        Some(Ok((id, PollerOutcome::Exited(Ok(()))))) => {
            let chain = task_to_chain.get(&id);
            anyhow!(
                "subgraph poller for chain {chain:?} exited unexpectedly with Ok (run() should be infinite)"
            )
        }
        Some(Ok((id, PollerOutcome::Exited(Err(e))))) => {
            let chain = task_to_chain.get(&id);
            anyhow::Error::new(e).context(format!("subgraph poller for chain {chain:?} failed"))
        }
        Some(Err(join_err)) => {
            let chain = task_to_chain.get(&join_err.id());
            anyhow!("subgraph poller task panicked (chain {chain:?}): {join_err}")
        }
        None => anyhow!("no subgraph poller was spawned"),
    }
}
