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
use std::io::ErrorKind;

use crate::app::AppState;

const RECOVERY_PAGE_SIZE: u64 = 100;

#[derive(Debug, PartialEq, Eq)]
enum RuntimeLocationRecoveryState {
    ActiveActionable,
    TerminalProven,
}

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
            if runtime_location_recovery_state(&location)?
                == RuntimeLocationRecoveryState::TerminalProven
            {
                recovered.push(json!({
                    "runtime_id": runtime_id,
                    "recovery_action": "terminal_proven",
                    "terminal_proven": true,
                    "effect_authorized": false,
                }));
                continue;
            }
            let Some(database_path) = checked_runtime_database_path(&location)? else {
                recovered.push(quarantined_missing_database_result(&location));
                continue;
            };
            let snapshot = read_runtime_snapshot(&location, database_path)?;
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
                    let post_snapshot = read_runtime_snapshot(
                        &location,
                        checked_runtime_database_path(&location)?.ok_or_else(|| {
                            anyhow!(
                                "STARTUP_RECOVERY_RUNTIME_DATABASE_DISAPPEARED:{}:{}",
                                location.runtime_id,
                                location.workspace_db_path
                            )
                        })?,
                    )?;
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

fn read_runtime_snapshot(
    location: &RuntimeLocation,
    database_path: String,
) -> Result<RuntimeLeaseSnapshot> {
    let runtime_id = &location.runtime_id;
    match call_service(&SessionLogCommand::GetRuntimeLease(
        GetRuntimeLeaseRequest {
            runtime_id: runtime_id.clone(),
            database_path: Some(database_path.clone()),
        },
    ))? {
        SessionLogResponse::RuntimeLeaseRead {
            runtime: Some(runtime),
        } => validate_runtime_location(location, runtime, &database_path),
        SessionLogResponse::RuntimeLeaseRead { runtime: None } => {
            bail!("STARTUP_RECOVERY_RUNTIME_NOT_FOUND:{runtime_id}")
        }
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => bail!("unexpected runtime lease response during recovery: {other:?}"),
    }
}

fn checked_runtime_database_path(location: &RuntimeLocation) -> Result<Option<String>> {
    match std::fs::metadata(&location.workspace_db_path) {
        Ok(_) => std::fs::canonicalize(&location.workspace_db_path)
            .map(|path| path.to_string_lossy().into_owned())
            .map(Some)
            .map_err(|error| {
                anyhow!(
                    "STARTUP_RECOVERY_RUNTIME_DATABASE_UNAVAILABLE:{}:{}:{}",
                    location.runtime_id,
                    location.workspace_db_path,
                    error
                )
            }),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(anyhow!(
            "STARTUP_RECOVERY_RUNTIME_DATABASE_UNAVAILABLE:{}:{}:{}",
            location.runtime_id,
            location.workspace_db_path,
            error
        )),
    }
}

fn runtime_location_recovery_state(
    location: &RuntimeLocation,
) -> Result<RuntimeLocationRecoveryState> {
    let proof_complete = location.terminal_revision.is_some()
        && location.terminal_event_seq.is_some()
        && location
            .terminal_evidence_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
    let proof_fields_present = location.terminal_revision.is_some()
        || location.terminal_event_seq.is_some()
        || location.terminal_evidence_id.is_some();
    if location.terminal_proven && proof_complete {
        return Ok(RuntimeLocationRecoveryState::TerminalProven);
    }
    if location.terminal_proven || proof_fields_present {
        bail!(
            "STARTUP_RECOVERY_MALFORMED_TERMINAL_PROOF:{}",
            location.runtime_id
        );
    }
    Ok(RuntimeLocationRecoveryState::ActiveActionable)
}

fn quarantined_missing_database_result(location: &RuntimeLocation) -> Value {
    json!({
        "runtime_id": location.runtime_id,
        "session_id": location.session_id,
        "recovery_action": "quarantined_missing_database",
        "terminal_proven": false,
        "effect_authorized": false,
    })
}

fn validate_runtime_location(
    location: &RuntimeLocation,
    runtime: RuntimeLeaseSnapshot,
    location_database_path: &str,
) -> Result<RuntimeLeaseSnapshot> {
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
        RuntimeLocationRecoveryState, checked_runtime_database_path,
        quarantined_missing_database_result, runtime_location_recovery_state,
        startup_recovery_reason, validate_runtime_location,
    };
    use lifecycle::SessionState;
    use session_log_contract::{RecoveryCloseRuntimeReason, RuntimeLeaseSnapshot, RuntimeLocation};

    fn location(database_path: String) -> RuntimeLocation {
        RuntimeLocation {
            runtime_id: "runtime-1".to_string(),
            session_id: "session-1".to_string(),
            workspace_db_path: database_path,
            terminal_proven: false,
            terminal_revision: None,
            terminal_event_seq: None,
            terminal_evidence_id: None,
        }
    }

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
        let location = location(database_path.clone());
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
        let error = validate_runtime_location(&location, runtime, &database_path)
            .expect_err("identity drift must block startup recovery");
        assert_eq!(
            error.to_string(),
            format!(
                "STARTUP_RECOVERY_RUNTIME_LOCATION_MISMATCH:runtime-1:session-1:session-drift:{database_path}:{database_path}:runtime-1"
            )
        );
    }

    #[test]
    fn startup_recovery_quarantines_missing_registered_database_without_terminal_proof() {
        let mut location = location("/definitely-missing/tura/session_log.sqlite3".to_string());
        location.runtime_id = "runtime-missing".to_string();
        location.session_id = "session-missing".to_string();
        assert_eq!(
            checked_runtime_database_path(&location).expect("definite absence is nonblocking"),
            None
        );
        assert_eq!(
            quarantined_missing_database_result(&location),
            serde_json::json!({
                "runtime_id": "runtime-missing",
                "session_id": "session-missing",
                "recovery_action": "quarantined_missing_database",
                "terminal_proven": false,
                "effect_authorized": false,
            })
        );
    }

    #[test]
    fn startup_recovery_blocks_database_io_errors() {
        let location = location(
            std::env::current_exe()
                .expect("current test executable")
                .join("not-a-database")
                .to_string_lossy()
                .into_owned(),
        );
        let error = checked_runtime_database_path(&location)
            .expect_err("non-not-found metadata errors must block startup recovery");
        assert!(
            error
                .to_string()
                .starts_with("STARTUP_RECOVERY_RUNTIME_DATABASE_UNAVAILABLE:runtime-1:")
        );
    }

    #[test]
    fn startup_recovery_skips_only_complete_terminal_proof() {
        let mut location = location("/unused".to_string());
        location.terminal_proven = true;
        location.terminal_revision = Some(4);
        location.terminal_event_seq = Some(9);
        location.terminal_evidence_id = Some("receipt-4".to_string());
        assert_eq!(
            runtime_location_recovery_state(&location).expect("complete proof is skippable"),
            RuntimeLocationRecoveryState::TerminalProven
        );

        location.terminal_evidence_id = None;
        let error = runtime_location_recovery_state(&location)
            .expect_err("malformed terminal proof must block");
        assert_eq!(
            error.to_string(),
            "STARTUP_RECOVERY_MALFORMED_TERMINAL_PROOF:runtime-1"
        );
    }
}
