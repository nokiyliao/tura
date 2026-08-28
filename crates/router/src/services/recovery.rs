//! Router restart recovery hooks.
//!
//! Startup first reaps same-instance orphan workers, then this module closes
//! every durable runtime row that did not reach a terminal state. The router
//! does not begin accepting traffic until each row has one exact disposition.

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};
use session_log_contract::{
    client::call_service, GetRuntimeLeaseRequest, ListSessionsRequest,
    RecoveryCloseRuntimeOutcome, RecoveryCloseRuntimeReason, SessionLogCommand,
    SessionLogResponse, RuntimeLeaseSnapshot,
};
use std::collections::BTreeSet;

use crate::app::AppState;

const RECOVERY_PAGE_SIZE: u64 = 100;

pub async fn recover_after_start(state: &AppState) -> Result<Value> {
    let session_db_status = state.session_db.start()?;
    let (runtime_rows_inspected, runtime_rows_recovered) = recover_runtime_rows(state).await?;
    Ok(json!({
        "session_db": session_db_status,
        "queue_replay": "requested",
        "runtime_reattach": false,
        "orphan_policy": "close_with_durable_runtime_terminal_feed",
        "runtime_rows_inspected": runtime_rows_inspected,
        "runtime_rows_recovered": runtime_rows_recovered,
        "command_execution_recovery": "reconcile_on_session_admission",
        "replay_policy": "diagnosed_only_after_no_authoritative_publication_or_idempotent_cas_proof"
    }))
}

async fn recover_runtime_rows(state: &AppState) -> Result<(u64, Vec<Value>)> {
    let workspaces = match call_service(&SessionLogCommand::ListWorkspaces)? {
        SessionLogResponse::Workspaces { workspaces } => workspaces,
        SessionLogResponse::Error { error } => return Err(anyhow!(error)),
        other => bail!("unexpected list_workspaces response during recovery: {other:?}"),
    };
    let mut seen_runtime_ids = BTreeSet::new();
    let mut inspected = 0_u64;
    let mut recovered = Vec::new();

    for workspace in workspaces {
        let mut page = 0_u64;
        let mut seen_sessions = 0_u64;
        loop {
            let response = call_service(&SessionLogCommand::ListSessions(ListSessionsRequest {
                workspace: workspace.directory.clone(),
                page,
                page_size: RECOVERY_PAGE_SIZE,
            }))?;
            let (page_info, sessions) = match response {
                SessionLogResponse::Sessions { page, sessions } => (page, sessions),
                SessionLogResponse::Error { error } => return Err(anyhow!(error)),
                other => bail!("unexpected list_sessions response during recovery: {other:?}"),
            };
            let session_count = sessions.len() as u64;
            for session in sessions {
                for runtime_id in session.lifecycle_projection.runtime_ids {
                    if !seen_runtime_ids.insert(runtime_id.clone()) {
                        continue;
                    }
                    let snapshot = read_runtime_snapshot(&runtime_id)?;
                    inspected = inspected.saturating_add(1);
                    if snapshot.terminal && !snapshot.lease_active {
                        if snapshot.lifecycle.is_some()
                            && let Some(delivery) = state
                                .execution
                                .reconcile_durable_terminal_callback(&snapshot)?
                        {
                            recovered.push(json!({
                                "runtime_id": snapshot.runtime_id,
                                "session_id": snapshot.session_id,
                                "recovery_action": "callback_reconciled",
                                "transaction_id": delivery.transaction_id,
                                "event_id": delivery.event_id,
                                "terminal": true,
                                "lease_active": false,
                            }));
                        }
                        continue;
                    }
                    let reason =
                        startup_recovery_reason(snapshot.revision, snapshot.last_event_seq);
                    let receipt_id = format!(
                        "router-startup-recovery:{}:{}:{}",
                        snapshot.runtime_id, snapshot.revision, snapshot.last_event_seq
                    );
                    let value = state
                        .execution
                        .recovery_close_runtime(
                            state,
                            json!({
                                "receipt_id": receipt_id,
                                "database_path": snapshot.database_path,
                                "runtime_id": snapshot.runtime_id,
                                "session_id": snapshot.session_id,
                                "lease_id": snapshot.lease_id,
                                "expected_lease_active": snapshot.lease_active,
                                "expected_revision": snapshot.revision,
                                "expected_last_event_seq": snapshot.last_event_seq,
                                "expected_session_event_seq": snapshot.session_event_seq,
                                "expected_session_state": snapshot.session_state,
                                "reason": reason,
                            }),
                        )
                        .await?;
                    let outcome: RecoveryCloseRuntimeOutcome = serde_json::from_value(
                        value
                            .get("result")
                            .cloned()
                            .ok_or_else(|| anyhow!("startup recovery result is missing"))?,
                    )?;
                    match outcome {
                        RecoveryCloseRuntimeOutcome::Closed { receipt }
                        | RecoveryCloseRuntimeOutcome::AlreadyClosed { receipt }
                            if receipt.terminal && !receipt.lease_active =>
                        {
                            let post_snapshot = read_runtime_snapshot(&receipt.runtime_id)?;
                            let delivery = if post_snapshot.lifecycle.is_some() {
                                state
                                    .execution
                                    .reconcile_durable_terminal_callback(&post_snapshot)?
                            } else {
                                None
                            };
                            recovered.push(json!({
                                "runtime_id": receipt.runtime_id,
                                "session_id": receipt.session_id,
                                "receipt_id": receipt.receipt_id,
                                "reason": receipt.reason,
                                "terminal": receipt.terminal,
                                "lease_active": receipt.lease_active,
                                "callback_transaction_id": delivery.as_ref().map(|value| &value.transaction_id),
                                "callback_event_id": delivery.as_ref().map(|value| &value.event_id),
                            }));
                        }
                        other => {
                            bail!(
                                "STARTUP_RUNTIME_RECOVERY_NOT_TERMINAL:{}:{other:?}",
                                runtime_id
                            )
                        }
                    }
                }
            }
            seen_sessions = seen_sessions.saturating_add(session_count);
            if session_count == 0 || seen_sessions >= page_info.total {
                break;
            }
            page = page.saturating_add(1);
        }
    }

    Ok((inspected, recovered))
}

fn read_runtime_snapshot(runtime_id: &str) -> Result<RuntimeLeaseSnapshot> {
    match call_service(&SessionLogCommand::GetRuntimeLease(
        GetRuntimeLeaseRequest {
            runtime_id: runtime_id.to_string(),
            database_path: None,
        },
    ))? {
        SessionLogResponse::RuntimeLeaseRead {
            runtime: Some(runtime),
        } => Ok(runtime),
        SessionLogResponse::RuntimeLeaseRead { runtime: None } => {
            bail!("STARTUP_RECOVERY_RUNTIME_NOT_FOUND:{runtime_id}")
        }
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => bail!("unexpected runtime lease response during recovery: {other:?}"),
    }
}

fn startup_recovery_reason(revision: u64, last_event_seq: u64) -> RecoveryCloseRuntimeReason {
    if revision == 0 && last_event_seq == 0 {
        RecoveryCloseRuntimeReason::UnbornRuntime
    } else {
        RecoveryCloseRuntimeReason::OrphanedRuntime
    }
}

#[cfg(test)]
mod tests {
    use super::startup_recovery_reason;
    use session_log_contract::RecoveryCloseRuntimeReason;

    #[test]
    fn startup_recovery_classifies_unborn_and_published_runtime_shapes() {
        assert_eq!(
            startup_recovery_reason(0, 0),
            RecoveryCloseRuntimeReason::UnbornRuntime
        );
        assert_eq!(
            startup_recovery_reason(1, 1),
            RecoveryCloseRuntimeReason::OrphanedRuntime
        );
        assert_eq!(
            startup_recovery_reason(7, 7),
            RecoveryCloseRuntimeReason::OrphanedRuntime
        );
    }
}
