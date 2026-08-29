//! Router restart recovery hooks.
//!
//! Startup first reaps same-instance orphan workers, then this module closes
//! every durable runtime row that did not reach a terminal state. The router
//! does not begin accepting traffic until each row has one exact disposition.

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use session_log_contract::{
    GetRuntimeLeaseRequest, ListRuntimeLocationsRequest, RecoveryCloseRuntimeOutcome,
    RecoveryCloseRuntimeReason, RuntimeLeaseSnapshot, RuntimeLocation, SessionLogCommand,
    SessionLogResponse, client::call_service,
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
    let mut seen_runtime_ids = BTreeSet::new();
    let mut inspected = 0_u64;
    let mut recovered = Vec::new();
    let mut page = 0_u64;
    let mut seen_locations = 0_u64;
    loop {
        let response = call_service(&SessionLogCommand::ListRuntimeLocations(
            ListRuntimeLocationsRequest {
                page,
                page_size: RECOVERY_PAGE_SIZE,
            },
        ))?;
        let (page_info, locations) = match response {
            SessionLogResponse::RuntimeLocations { page, locations } => (page, locations),
            SessionLogResponse::Error { error } => return Err(anyhow!(error)),
            other => bail!("unexpected list_runtime_locations response during recovery: {other:?}"),
        };
        let location_count = locations.len() as u64;
        for location in locations {
            let runtime_id = location.runtime_id.clone();
            if !seen_runtime_ids.insert(runtime_id.clone()) {
                continue;
            }
            let snapshot = read_runtime_snapshot(&location)?;
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
            let reason = startup_recovery_reason(snapshot.revision, snapshot.last_event_seq);
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
                    let post_snapshot = read_runtime_snapshot(&location)?;
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
        seen_locations = seen_locations.saturating_add(location_count);
        if location_count == 0 || seen_locations >= page_info.total {
            break;
        }
        page = page.saturating_add(1);
    }

    Ok((inspected, recovered))
}

fn read_runtime_snapshot(location: &RuntimeLocation) -> Result<RuntimeLeaseSnapshot> {
    let runtime_id = &location.runtime_id;
    let database_path = canonical_runtime_database_path(location)?;
    match call_service(&SessionLogCommand::GetRuntimeLease(
        GetRuntimeLeaseRequest {
            runtime_id: runtime_id.clone(),
            database_path: Some(database_path),
        },
    ))? {
        SessionLogResponse::RuntimeLeaseRead {
            runtime: Some(runtime),
        } => validate_runtime_location(location, runtime),
        SessionLogResponse::RuntimeLeaseRead { runtime: None } => {
            bail!("STARTUP_RECOVERY_RUNTIME_NOT_FOUND:{runtime_id}")
        }
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => bail!("unexpected runtime lease response during recovery: {other:?}"),
    }
}

fn canonical_runtime_database_path(location: &RuntimeLocation) -> Result<String> {
    std::fs::canonicalize(&location.workspace_db_path)
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|error| {
            anyhow!(
                "STARTUP_RECOVERY_RUNTIME_DATABASE_UNAVAILABLE:{}:{}:{}",
                location.runtime_id,
                location.workspace_db_path,
                error
            )
        })
}

fn validate_runtime_location(
    location: &RuntimeLocation,
    runtime: RuntimeLeaseSnapshot,
) -> Result<RuntimeLeaseSnapshot> {
    let location_database_path = canonical_runtime_database_path(location)?;
    if runtime.runtime_id != location.runtime_id
        || runtime.session_id != location.session_id
        || runtime.database_path != location_database_path
    {
        bail!(
            "STARTUP_RECOVERY_RUNTIME_LOCATION_MISMATCH:{}:{}:{}:{}:{}:{}",
            location.runtime_id,
            location.session_id,
            runtime.session_id,
            location.workspace_db_path,
            runtime.database_path,
            runtime.runtime_id
        );
    }
    Ok(runtime)
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
    use super::{
        canonical_runtime_database_path, startup_recovery_reason, validate_runtime_location,
    };
    use lifecycle::SessionState;
    use session_log_contract::{RecoveryCloseRuntimeReason, RuntimeLeaseSnapshot, RuntimeLocation};

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

    #[test]
    fn startup_recovery_rejects_runtime_location_identity_mismatch() {
        let database_path = std::env::current_exe()
            .expect("current test executable")
            .to_string_lossy()
            .into_owned();
        let location = RuntimeLocation {
            runtime_id: "runtime-1".to_string(),
            session_id: "session-1".to_string(),
            workspace_db_path: database_path.clone(),
        };
        let runtime = RuntimeLeaseSnapshot {
            database_path: database_path.clone(),
            runtime_id: "runtime-1".to_string(),
            session_id: "session-drift".to_string(),
            lifecycle: None,
            lease_id: None,
            lease_active: true,
            revision: 0,
            last_event_seq: 0,
            terminal: false,
            session_event_seq: 2,
            session_state: SessionState::Running,
            runtime_state: None,
        };
        let error = validate_runtime_location(&location, runtime)
            .expect_err("identity drift must block startup recovery");
        assert_eq!(
            error.to_string(),
            format!(
                "STARTUP_RECOVERY_RUNTIME_LOCATION_MISMATCH:runtime-1:session-1:session-drift:{database_path}:{database_path}:runtime-1"
            )
        );
    }

    #[test]
    fn startup_recovery_rejects_missing_registered_database() {
        let location = RuntimeLocation {
            runtime_id: "runtime-missing".to_string(),
            session_id: "session-missing".to_string(),
            workspace_db_path: "/definitely-missing/tura/session_log.sqlite3".to_string(),
        };
        let error = canonical_runtime_database_path(&location)
            .expect_err("missing registered database must block startup recovery");
        assert!(
            error.to_string().starts_with(
                "STARTUP_RECOVERY_RUNTIME_DATABASE_UNAVAILABLE:runtime-missing:/definitely-missing/tura/session_log.sqlite3:"
            ),
            "unexpected blocker: {error:#}"
        );
    }
}
