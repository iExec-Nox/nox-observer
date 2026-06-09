//! Supervision of the multichain subgraph poller fleet.
//!
//! `SubgraphPollerSupervisor` owns the `JoinSet`, the cancellation token, and
//! the task → chain_id map. `Application` only sees three operations:
//!
//! - `spawn(pollers)`  : start every poller, each wrapped in a cancel-vs-run race.
//! - `wait_for_exit()` : future that resolves when the first task exits (always
//!   treated as fatal: pollers are supposed to loop forever).
//! - `shutdown()`      : cancel + drain every remaining task with structured logs.

use std::collections::HashMap;

use anyhow::anyhow;
use tokio::task::{Id as TaskId, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::SubgraphPoller;
use crate::errors::SubgraphPollerError;

/// Outcome of one supervised poller task wrapped around a cancellation race.
enum PollerOutcome {
    Cancelled,
    Exited(Result<(), SubgraphPollerError>),
}

pub struct SubgraphPollerSupervisor {
    set: JoinSet<PollerOutcome>,
    cancel: CancellationToken,
    task_to_chain: HashMap<TaskId, i32>,
}

impl SubgraphPollerSupervisor {
    /// Spawn one task per poller, each wrapped in a biased cancel-vs-run race.
    pub fn spawn(pollers: Vec<(i32, SubgraphPoller)>) -> Self {
        let cancel = CancellationToken::new();
        let mut set: JoinSet<PollerOutcome> = JoinSet::new();
        let mut task_to_chain: HashMap<TaskId, i32> = HashMap::new();

        for (chain_id, poller) in pollers {
            let token = cancel.clone();
            let handle = set.spawn(async move {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => PollerOutcome::Cancelled,
                    res = poller.run() => PollerOutcome::Exited(res),
                }
            });
            task_to_chain.insert(handle.id(), chain_id);
        }

        Self {
            set,
            cancel,
            task_to_chain,
        }
    }

    /// Future that resolves on the first poller-task exit. The returned
    /// `anyhow::Error` is enriched with the offending `chain_id`. Designed to
    /// be raced against the other long-running services in `tokio::select!`.
    pub async fn wait_for_exit(&mut self) -> anyhow::Error {
        let exit = self.set.join_next_with_id().await;
        self.map_first_exit(exit)
    }

    /// Convert the first poller-task exit picked up by `wait_for_exit` into an
    /// `anyhow::Error` enriched with the offending `chain_id`. `Ok(())` from
    /// `run` is treated as an error because pollers are expected to loop forever.
    fn map_first_exit(
        &self,
        maybe: Option<Result<(TaskId, PollerOutcome), tokio::task::JoinError>>,
    ) -> anyhow::Error {
        match maybe {
            Some(Ok((id, PollerOutcome::Cancelled))) => {
                let chain = self.task_to_chain.get(&id);
                anyhow!(
                    "subgraph poller for chain {chain:?} reported Cancelled without prior signal"
                )
            }
            Some(Ok((id, PollerOutcome::Exited(Ok(()))))) => {
                let chain = self.task_to_chain.get(&id);
                anyhow!(
                    "subgraph poller for chain {chain:?} exited unexpectedly with Ok (run() should be infinite)"
                )
            }
            Some(Ok((id, PollerOutcome::Exited(Err(e))))) => {
                let chain = self.task_to_chain.get(&id);
                anyhow::Error::new(e).context(format!("subgraph poller for chain {chain:?} failed"))
            }
            Some(Err(join_err)) => {
                let chain = self.task_to_chain.get(&join_err.id());
                anyhow!("subgraph poller task panicked (chain {chain:?}): {join_err}")
            }
            None => anyhow!("no subgraph poller was spawned"),
        }
    }

    /// Cancel the token, then await every remaining task. Logs each terminal
    /// state (`Cancelled`, unexpected `Ok`, error, panic) with its `chain_id`.
    /// Never returns an error: the original exit reason is already captured by
    /// `wait_for_exit`, and we want every task to flush before the process dies.
    pub async fn shutdown(mut self) {
        info!("triggering graceful shutdown of remaining subgraph pollers");
        self.cancel.cancel();
        while let Some(res) = self.set.join_next_with_id().await {
            match res {
                Ok((id, PollerOutcome::Cancelled)) => {
                    info!(chain_id = ?self.task_to_chain.get(&id), "subgraph poller stopped cleanly");
                }
                Ok((id, PollerOutcome::Exited(Ok(())))) => {
                    warn!(
                        chain_id = ?self.task_to_chain.get(&id),
                        "subgraph poller exited with Ok before cancellation took effect (run should be infinite)"
                    );
                }
                Ok((id, PollerOutcome::Exited(Err(e)))) => {
                    error!(
                        chain_id = ?self.task_to_chain.get(&id),
                        "subgraph poller failed during drain: {e:#}"
                    );
                }
                Err(join_err) => {
                    error!(
                        chain_id = ?self.task_to_chain.get(&join_err.id()),
                        "subgraph poller task panicked during drain: {join_err}"
                    );
                }
            }
        }
    }
}

