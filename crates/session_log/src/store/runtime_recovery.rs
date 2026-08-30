use super::SessionLogStore;
use super::feed::append_session_feed_event_tx;
use super::helpers::{append_session_event, replay_session_events};
use super::runtime_events::{load_session_projection_row, persist_session_projection};
use anyhow::{Context, Result};
use lifecycle::{
    RuntimeAggregate, RuntimeError, RuntimeEvent, RuntimeState, SessionCommand, SessionQuery,
    SessionState,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;
use session_log_contract::{
    GetRuntimeLeaseRequest, RecoveryCloseRuntimeOutcome, RecoveryCloseRuntimeReason,
    RecoveryCloseRuntimeRequest, RuntimeLeaseSnapshot, RuntimeRecoveryReceipt, SessionFeedEvent,
    recovery_terminal_projection_event_id,
};
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct RuntimeRow {
    session_id: String,
    fallback_from_id: Option<String>,
    lifecycle: Option<session_log_contract::RuntimeLifecycleIdentity>,
    lease_id: Option<String>,
    lease_active: bool,
    revision: u64,
    last_event_seq: u64,
    terminal: bool,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalRecoveryRequest<'a> {
    database_path: &'a str,
    runtime_id: &'a str,
    session_id: &'a str,
    lease_id: Option<&'a str>,
    expected_lease_active: bool,
    expected_revision: u64,
    expected_last_event_seq: u64,
    expected_session_event_seq: u64,
    expected_session_state: SessionState,
    reason: RecoveryCloseRuntimeReason,
    convergence_proof_sha256: Option<&'a str>,
}

impl SessionLogStore {
    pub fn get_runtime_lease(
        &self,
        request: GetRuntimeLeaseRequest,
    ) -> Result<Option<RuntimeLeaseSnapshot>> {
        if request.runtime_id.trim().is_empty() {
            anyhow::bail!("runtime_id must be non-empty");
        }
        let workspace_db_path = match request.database_path.as_deref() {
            Some(database_path) => exact_database_path(database_path)?,
            None => {
                let Some(path) = self.runtime_workspace_db_path(&request.runtime_id)? else {
                    return Ok(None);
                };
                path
            }
        };
        let database_path = std::fs::canonicalize(&workspace_db_path)
            .with_context(|| {
                format!(
                    "failed to canonicalize runtime database {}",
                    workspace_db_path.display()
                )
            })?
            .to_string_lossy()
            .into_owned();
        self.with_workspace_connection(&workspace_db_path, |conn| {
            let Some(row) = load_runtime_row(conn, &request.runtime_id)? else {
                return Ok(None);
            };
            let session = replay_session_events(conn, &row.session_id)?;
            let session_event_seq = load_session_event_seq(conn, &row.session_id)?;
            let runtime_events = load_runtime_events(conn, &request.runtime_id)?;
            let runtime_state = if runtime_events.is_empty() {
                None
            } else {
                Some(
                    RuntimeAggregate::replay(request.runtime_id.clone(), runtime_events)
                        .map_err(anyhow::Error::msg)
                        .context("persisted runtime lifecycle is invalid")?
                        .state,
                )
            };
            Ok(Some(RuntimeLeaseSnapshot {
                database_path: database_path.clone(),
                runtime_id: request.runtime_id.clone(),
                session_id: row.session_id,
                lifecycle: row.lifecycle,
                lease_id: row.lease_id,
                lease_active: row.lease_active,
                revision: row.revision,
                last_event_seq: row.last_event_seq,
                terminal: row.terminal,
                session_event_seq,
                session_state: session.state,
                runtime_state,
            }))
        })
    }

    pub fn recovery_close_runtime(
        &self,
        request: RecoveryCloseRuntimeRequest,
    ) -> Result<RecoveryCloseRuntimeOutcome> {
        validate_request(&request)?;
        let workspace_db_path = exact_database_path(&request.database_path)?;
        let outcome = self.with_workspace_connection(&workspace_db_path, |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let canonical_request = serde_json::to_string(&CanonicalRecoveryRequest {
                database_path: &request.database_path,
                runtime_id: &request.runtime_id,
                session_id: &request.session_id,
                lease_id: request.lease_id.as_deref(),
                expected_lease_active: request.expected_lease_active,
                expected_revision: request.expected_revision,
                expected_last_event_seq: request.expected_last_event_seq,
                expected_session_event_seq: request.expected_session_event_seq,
                expected_session_state: request.expected_session_state,
                reason: request.reason,
                convergence_proof_sha256: request.convergence_proof_sha256.as_deref(),
            })?;

            if let Some(existing) = load_recovery_receipt(&tx, &request.receipt_id)? {
                if existing.session_id != request.session_id
                    || existing.request_json != canonical_request
                {
                    return Ok(RecoveryCloseRuntimeOutcome::ReceiptConflict);
                }
                let receipt: RuntimeRecoveryReceipt = serde_json::from_str(&existing.result_json)
                    .with_context(|| {
                    format!("invalid runtime recovery receipt {}", request.receipt_id)
                })?;
                if receipt.receipt_id != request.receipt_id
                    || receipt.database_path != request.database_path
                    || receipt.runtime_id != request.runtime_id
                    || receipt.session_id != request.session_id
                    || receipt.lease_id != request.lease_id
                    || receipt.reason != request.reason
                    || receipt.convergence_proof_sha256 != request.convergence_proof_sha256
                    || receipt.lease_active
                    || !receipt.terminal
                {
                    return Ok(RecoveryCloseRuntimeOutcome::ReceiptConflict);
                }
                let Some(row) = load_runtime_row(&tx, &request.runtime_id)? else {
                    return Ok(RecoveryCloseRuntimeOutcome::RuntimeNotFound);
                };
                if row.session_id != request.session_id || row.lease_id != request.lease_id {
                    return Ok(RecoveryCloseRuntimeOutcome::IdentityMismatch {
                        field: "durable_receipt_runtime_identity".to_string(),
                    });
                }
                if row.revision != receipt.revision || row.last_event_seq != receipt.last_event_seq
                {
                    return Ok(RecoveryCloseRuntimeOutcome::CasConflict {
                        current_revision: row.revision,
                        current_last_event_seq: row.last_event_seq,
                    });
                }
                if row.lease_active || !row.terminal {
                    return Ok(RecoveryCloseRuntimeOutcome::LeaseStateConflict {
                        lease_active: row.lease_active,
                        terminal: row.terminal,
                    });
                }
                let session = replay_session_events(&tx, &request.session_id)?;
                let session_event_seq = load_session_event_seq(&tx, &request.session_id)?;
                if session_event_seq != receipt.session_event_seq
                    || session.state != receipt.session_state
                {
                    return Ok(RecoveryCloseRuntimeOutcome::SessionCasConflict {
                        current_event_seq: session_event_seq,
                        current_state: session.state,
                    });
                }
                let projection = session.query(SessionQuery::Lifecycle);
                ensure_recovery_terminal_feed(
                    &tx,
                    &receipt.receipt_id,
                    &receipt.session_id,
                    &receipt.runtime_id,
                    &projection,
                    receipt.closed_at,
                )?;
                tx.commit()?;
                return Ok(RecoveryCloseRuntimeOutcome::AlreadyClosed { receipt });
            }

            if !request.quiescence.is_quiescent() {
                return Ok(RecoveryCloseRuntimeOutcome::RuntimeLive {
                    proof: request.quiescence.clone(),
                });
            }

            let Some(row) = load_runtime_row(&tx, &request.runtime_id)? else {
                return Ok(RecoveryCloseRuntimeOutcome::RuntimeNotFound);
            };
            if row.session_id != request.session_id {
                return Ok(RecoveryCloseRuntimeOutcome::IdentityMismatch {
                    field: "session_id".to_string(),
                });
            }
            if row.lease_id != request.lease_id {
                return Ok(RecoveryCloseRuntimeOutcome::IdentityMismatch {
                    field: "lease_id".to_string(),
                });
            }
            if row.revision != request.expected_revision
                || row.last_event_seq != request.expected_last_event_seq
            {
                return Ok(RecoveryCloseRuntimeOutcome::CasConflict {
                    current_revision: row.revision,
                    current_last_event_seq: row.last_event_seq,
                });
            }
            if row.lease_active != request.expected_lease_active || row.terminal {
                return Ok(RecoveryCloseRuntimeOutcome::LeaseStateConflict {
                    lease_active: row.lease_active,
                    terminal: row.terminal,
                });
            }

            let mut session = replay_session_events(&tx, &request.session_id)?;
            let session_event_seq = load_session_event_seq(&tx, &request.session_id)?;
            if session_event_seq != request.expected_session_event_seq
                || session.state != request.expected_session_state
            {
                return Ok(RecoveryCloseRuntimeOutcome::SessionCasConflict {
                    current_event_seq: session_event_seq,
                    current_state: session.state,
                });
            }
            if !session
                .runtime_ids
                .iter()
                .any(|runtime_id| runtime_id == &request.runtime_id)
            {
                return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                    error: "interrupted session does not contain the requested runtime".to_string(),
                });
            }
            match session.state {
                SessionState::Running | SessionState::Paused => {
                    if session.active_runtime_id.as_deref() != Some(request.runtime_id.as_str()) {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error:
                                "recoverable session does not actively own the requested runtime"
                                    .to_string(),
                        });
                    }
                }
                state if state.is_terminal() => {
                    if session.active_runtime_id.is_some() {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: "terminal session still owns an active runtime".to_string(),
                        });
                    }
                }
                state => {
                    return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                        error: format!("session state {state:?} is not recoverable or terminal"),
                    });
                }
            }

            let mut events = load_runtime_events(&tx, &request.runtime_id)?;
            if events.len() as u64 != row.last_event_seq {
                return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                    error: format!(
                        "runtime event count {} does not match last_event_seq {}",
                        events.len(),
                        row.last_event_seq
                    ),
                });
            }
            match request.reason {
                RecoveryCloseRuntimeReason::UnbornRuntime => {
                    if row.revision != 0 || row.last_event_seq != 0 || !events.is_empty() {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: "unborn runtime must have revision 0 and no runtime events"
                                .to_string(),
                        });
                    }
                }
                RecoveryCloseRuntimeReason::OrphanedRuntime => {
                    if events.is_empty() {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: "orphaned runtime must have a replayable event stream"
                                .to_string(),
                        });
                    }
                    let aggregate = RuntimeAggregate::replay(
                        request.runtime_id.clone(),
                        events.iter().cloned(),
                    )
                    .map_err(anyhow::Error::msg)
                    .context("invalid runtime event stream during recovery")?;
                    if aggregate.session_id != request.session_id {
                        return Ok(RecoveryCloseRuntimeOutcome::IdentityMismatch {
                            field: "runtime_event_session_id".to_string(),
                        });
                    }
                    if aggregate.fallback_from_id != row.fallback_from_id {
                        return Ok(RecoveryCloseRuntimeOutcome::IdentityMismatch {
                            field: "runtime_event_fallback_from_id".to_string(),
                        });
                    }
                    if !aggregate.state.is_live() {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: format!(
                                "runtime state {:?} is not live recovery residue",
                                aggregate.state
                            ),
                        });
                    }
                    events.push(RuntimeEvent::RuntimeFailed {
                        finished_at: chrono::Utc::now(),
                        error: RuntimeError {
                            error_code: Some("runtime_recovery_cancelled".to_string()),
                            error_text: Some(
                                "runtime cancelled after exact ownerless recovery".to_string(),
                            ),
                            retry_allowed: false,
                            fallback_allowed: false,
                            fallback_to_id: None,
                        },
                        state: RuntimeState::Cancelled,
                        usage: aggregate.usage.clone(),
                    });
                    let recovered =
                        RuntimeAggregate::replay(request.runtime_id.clone(), events.clone())
                            .map_err(anyhow::Error::msg)
                            .context("runtime recovery cancellation event is invalid")?;
                    if recovered.state != RuntimeState::Cancelled {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: "runtime recovery did not produce cancelled state".to_string(),
                        });
                    }
                }
                RecoveryCloseRuntimeReason::CommanderConvergenceProven => {
                    if events.is_empty() {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: "proven Commander convergence requires a replayable runtime"
                                .to_string(),
                        });
                    }
                    let aggregate = RuntimeAggregate::replay(
                        request.runtime_id.clone(),
                        events.iter().cloned(),
                    )
                    .map_err(anyhow::Error::msg)
                    .context("invalid runtime event stream during proven convergence recovery")?;
                    if aggregate.session_id != request.session_id {
                        return Ok(RecoveryCloseRuntimeOutcome::IdentityMismatch {
                            field: "runtime_event_session_id".to_string(),
                        });
                    }
                    if aggregate.fallback_from_id != row.fallback_from_id {
                        return Ok(RecoveryCloseRuntimeOutcome::IdentityMismatch {
                            field: "runtime_event_fallback_from_id".to_string(),
                        });
                    }
                    if !aggregate.state.is_live() || aggregate.output.is_some() {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: "proven Commander convergence recovery requires live runtime without output"
                                .to_string(),
                        });
                    }
                    events.push(RuntimeEvent::RuntimeFailed {
                        finished_at: chrono::Utc::now(),
                        error: RuntimeError {
                            error_code: Some(
                                "commander_convergence_proven_before_runtime_output".to_string(),
                            ),
                            error_text: Some(
                                "provider target turn completed before runtime output became durable"
                                    .to_string(),
                            ),
                            retry_allowed: true,
                            fallback_allowed: false,
                            fallback_to_id: None,
                        },
                        state: RuntimeState::Failed,
                        usage: aggregate.usage.clone(),
                    });
                    let recovered = RuntimeAggregate::replay(
                        request.runtime_id.clone(),
                        events.clone(),
                    )
                    .map_err(anyhow::Error::msg)
                    .context("proven Commander convergence recovery event is invalid")?;
                    if recovered.state != RuntimeState::Failed {
                        return Ok(RecoveryCloseRuntimeOutcome::InvalidRecoveryShape {
                            error: "proven Commander convergence did not produce failed state"
                                .to_string(),
                        });
                    }
                }
            }

            let appends_runtime_event = matches!(
                request.reason,
                RecoveryCloseRuntimeReason::OrphanedRuntime
                    | RecoveryCloseRuntimeReason::CommanderConvergenceProven
            );
            let post_revision = row.revision + u64::from(appends_runtime_event);
            let post_last_event_seq = row.last_event_seq + u64::from(appends_runtime_event);
            if let Some(event) = events.last().filter(|_| appends_runtime_event) {
                tx.execute(
                    "INSERT INTO runtime_events(
                        runtime_id, event_seq, revision, idempotency_key, event_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        request.runtime_id,
                        post_last_event_seq,
                        post_revision,
                        format!("runtime-recovery:{}:terminal", request.receipt_id),
                        serde_json::to_string(event)?,
                    ],
                )?;
            }
            let changed = tx.execute(
                "UPDATE runtimes
                 SET lease_active = 0, terminal = 1, revision = ?6, last_event_seq = ?7
                 WHERE runtime_id = ?1 AND session_id = ?2 AND lease_id IS ?3
                   AND revision = ?4 AND last_event_seq = ?5
                   AND lease_active = ?8 AND terminal = 0",
                params![
                    request.runtime_id,
                    request.session_id,
                    request.lease_id,
                    request.expected_revision,
                    request.expected_last_event_seq,
                    post_revision,
                    post_last_event_seq,
                    request.expected_lease_active,
                ],
            )?;
            if changed != 1 {
                return Ok(RecoveryCloseRuntimeOutcome::CasConflict {
                    current_revision: row.revision,
                    current_last_event_seq: row.last_event_seq,
                });
            }

            let now_ms = chrono::Utc::now().timestamp_millis();
            let post_session_event_seq = if session.state.is_recoverable_running() {
                let mut projection_row = load_session_projection_row(&tx, &request.session_id)?
                    .with_context(|| format!("session {} not found", request.session_id))?;
                let command = if request.reason
                    == RecoveryCloseRuntimeReason::CommanderConvergenceProven
                {
                    SessionCommand::RuntimeFailed {
                        runtime_id: request.runtime_id.clone(),
                    }
                } else {
                    SessionCommand::InterruptSession
                };
                let event = session.execute(command)?;
                persist_session_projection(
                    &tx,
                    &request.session_id,
                    &session,
                    &mut projection_row,
                    now_ms,
                )?;
                append_session_event(&tx, &request.session_id, &event)?
            } else {
                session_event_seq
            };
            let projection = session.query(SessionQuery::Lifecycle);
            ensure_recovery_terminal_feed(
                &tx,
                &request.receipt_id,
                &request.session_id,
                &request.runtime_id,
                &projection,
                now_ms,
            )?;
            let receipt = RuntimeRecoveryReceipt {
                receipt_id: request.receipt_id.clone(),
                database_path: request.database_path.clone(),
                runtime_id: request.runtime_id.clone(),
                session_id: request.session_id.clone(),
                lease_id: request.lease_id.clone(),
                revision: post_revision,
                last_event_seq: post_last_event_seq,
                session_event_seq: post_session_event_seq,
                reason: request.reason,
                convergence_proof_sha256: request.convergence_proof_sha256.clone(),
                lease_active: false,
                terminal: true,
                session_state: projection.state,
                quiescence: request.quiescence.clone(),
                closed_at: now_ms,
            };
            tx.execute(
                "INSERT INTO session_command_receipts(
                    command_id, session_id, request_json, result_json
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    request.receipt_id,
                    request.session_id,
                    canonical_request,
                    serde_json::to_string(&receipt)?,
                ],
            )?;
            tx.commit()?;
            Ok(RecoveryCloseRuntimeOutcome::Closed { receipt })
        })?;
        if let RecoveryCloseRuntimeOutcome::Closed { receipt }
        | RecoveryCloseRuntimeOutcome::AlreadyClosed { receipt } = &outcome
        {
            let snapshot = self
                .get_runtime_lease(GetRuntimeLeaseRequest {
                    runtime_id: receipt.runtime_id.clone(),
                    database_path: Some(workspace_db_path.to_string_lossy().into_owned()),
                })?
                .context("terminal recovery runtime disappeared before global proof projection")?;
            self.project_terminal_runtime_location(&snapshot, &receipt.receipt_id)?;
        }
        Ok(outcome)
    }
}

fn ensure_recovery_terminal_feed(
    tx: &Transaction<'_>,
    receipt_id: &str,
    session_id: &str,
    runtime_id: &str,
    projection: &lifecycle::SessionProjection,
    updated_at: i64,
) -> Result<()> {
    let event_id = recovery_terminal_projection_event_id(receipt_id);
    let exists = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_feed_events WHERE event_id = ?1)",
        params![event_id],
        |row| row.get::<_, bool>(0),
    )?;
    if !exists {
        append_session_feed_event_tx(
            tx,
            session_id,
            Some(runtime_id),
            &event_id,
            &SessionFeedEvent::SessionProjectionUpdated {
                projection: projection.clone(),
                session_name: None,
                updated_at,
            },
        )?;
    }
    Ok(())
}

fn validate_request(request: &RecoveryCloseRuntimeRequest) -> Result<()> {
    for (name, value) in [
        ("receipt_id", request.receipt_id.as_str()),
        ("database_path", request.database_path.as_str()),
        ("runtime_id", request.runtime_id.as_str()),
        ("session_id", request.session_id.as_str()),
    ] {
        if value.trim().is_empty() {
            anyhow::bail!("{name} must be non-empty");
        }
    }
    if request
        .lease_id
        .as_deref()
        .is_some_and(|lease_id| lease_id.trim().is_empty())
    {
        anyhow::bail!("lease_id must be null or non-empty");
    }
    if request.expected_lease_active && request.lease_id.is_none() {
        anyhow::bail!("an active recovery lease must have a lease_id");
    }
    match request.reason {
        RecoveryCloseRuntimeReason::CommanderConvergenceProven => {
            let digest = request.convergence_proof_sha256.as_deref().ok_or_else(|| {
                anyhow::anyhow!("proven Commander convergence requires proof SHA-256")
            })?;
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                anyhow::bail!("Commander convergence proof digest is not lowercase SHA-256");
            }
        }
        RecoveryCloseRuntimeReason::OrphanedRuntime | RecoveryCloseRuntimeReason::UnbornRuntime => {
            if request.convergence_proof_sha256.is_some() {
                anyhow::bail!("generic runtime recovery cannot carry Commander proof");
            }
        }
    }
    Ok(())
}

fn exact_database_path(database_path: &str) -> Result<PathBuf> {
    let requested = Path::new(database_path);
    if !requested.is_absolute() {
        anyhow::bail!("database_path must be absolute");
    }
    let metadata = std::fs::metadata(requested)
        .with_context(|| format!("recovery database {} does not exist", requested.display()))?;
    if !metadata.is_file() {
        anyhow::bail!("recovery database {} is not a file", requested.display());
    }
    let canonical = std::fs::canonicalize(requested)
        .with_context(|| format!("failed to canonicalize {}", requested.display()))?;
    if canonical != requested {
        anyhow::bail!(
            "database_path must be canonical: expected {}",
            canonical.display()
        );
    }
    Ok(canonical)
}

fn load_runtime_row(conn: &rusqlite::Connection, runtime_id: &str) -> Result<Option<RuntimeRow>> {
    conn.query_row(
        "SELECT session_id, fallback_from_id, lease_id, lease_active, revision,
                last_event_seq, terminal, lifecycle_json
         FROM runtimes WHERE runtime_id = ?1",
        params![runtime_id],
        |row| {
            let lifecycle_json = row.get::<_, Option<String>>(7)?;
            let lifecycle = lifecycle_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        7,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
            Ok(RuntimeRow {
                session_id: row.get(0)?,
                fallback_from_id: row.get(1)?,
                lease_id: row.get(2)?,
                lease_active: row.get(3)?,
                revision: row.get(4)?,
                last_event_seq: row.get(5)?,
                terminal: row.get(6)?,
                lifecycle,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn load_session_event_seq(conn: &rusqlite::Connection, session_id: &str) -> Result<u64> {
    conn.query_row(
        "SELECT COUNT(*) FROM session_events WHERE session_id = ?1",
        params![session_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn load_runtime_events(conn: &rusqlite::Connection, runtime_id: &str) -> Result<Vec<RuntimeEvent>> {
    let mut statement = conn.prepare(
        "SELECT event_seq, event_json FROM runtime_events
         WHERE runtime_id = ?1 ORDER BY event_seq",
    )?;
    let rows = statement.query_map(params![runtime_id], |row| {
        Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut events = Vec::new();
    for (index, row) in rows.enumerate() {
        let (event_seq, json) = row?;
        let expected = index as u64 + 1;
        if event_seq != expected {
            anyhow::bail!(
                "runtime {runtime_id} event sequence is not contiguous: expected {expected}, found {event_seq}"
            );
        }
        events.push(serde_json::from_str(&json).with_context(|| {
            format!("invalid runtime event while recovering runtime {runtime_id}")
        })?);
    }
    Ok(events)
}

struct StoredRecoveryReceipt {
    session_id: String,
    request_json: String,
    result_json: String,
}

fn load_recovery_receipt(
    tx: &Transaction<'_>,
    receipt_id: &str,
) -> Result<Option<StoredRecoveryReceipt>> {
    tx.query_row(
        "SELECT session_id, request_json, result_json
         FROM session_command_receipts WHERE command_id = ?1",
        params![receipt_id],
        |row| {
            Ok(StoredRecoveryReceipt {
                session_id: row.get(0)?,
                request_json: row.get(1)?,
                result_json: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lifecycle::{
        ProviderConfig, RuntimeAggregate, RuntimeProviderConfig, SessionCommand, SessionEvent,
        TaskPlan, ToolChoice,
    };
    use session_log_contract::{
        ActivateRuntimeLeaseRequest, CommitRuntimeEventRequest, CreateSessionRequest,
        DeleteSessionRequest, DeleteWorkspaceRequest, ExecuteSessionCommandRequest,
        GetSessionRequest, ListRuntimeLocationsRequest, MarkSessionInterruptedRequest,
        ReadSessionFeedRequest, RegisterRuntimeRequest, ReplayRuntimeRequest,
        RuntimeEventCommitOutcome, RuntimeRecoveryQuiescenceProof, RuntimeRegistrationOutcome,
    };

    #[derive(Debug, PartialEq)]
    struct SessionLedgerSnapshot {
        projection_row: Vec<rusqlite::types::Value>,
        events: Vec<(u64, String)>,
    }

    struct RecoveryFixture {
        _root: tempfile::TempDir,
        _workspace: tempfile::TempDir,
        store: SessionLogStore,
        database_path: String,
        session_id: String,
        runtime_id: String,
        lease_id: String,
    }

    impl RecoveryFixture {
        fn new(label: &str) -> Self {
            Self::new_with_active_lease(label, true)
        }

        fn new_without_active_lease(label: &str) -> Self {
            Self::new_with_active_lease(label, false)
        }

        fn new_with_active_lease(label: &str, activate_lease: bool) -> Self {
            let root = tempfile::tempdir().expect("session store root");
            let workspace = tempfile::tempdir().expect("session workspace");
            let store = SessionLogStore::open(root.path().join("db")).expect("session store");
            let session_id = format!("recovery-session-{label}");
            let runtime_id = format!("recovery-runtime-{label}");
            let lease_id = format!("recovery-lease-{label}");
            let workspace_text = workspace.path().to_string_lossy().to_string();
            store
                .create_session(CreateSessionRequest {
                    command_id: format!("create:{session_id}"),
                    session_id: session_id.clone(),
                    creation_command: SessionCommand::CreateSession {
                        task_plan: TaskPlan::default(),
                    },
                    copy_context: false,
                    workspace: workspace_text.clone(),
                    session_directory: workspace_text.clone(),
                    name: "runtime recovery fixture".to_string(),
                    created_at: 1,
                    model: None,
                    agent: None,
                    session_type: "coding".to_string(),
                    kill_processes_on_start: false,
                    validator_enabled: false,
                    force_planning: false,
                    model_variant: None,
                    model_acceleration_enabled: false,
                    disable_permission_restrictions: false,
                    use_last_tool_call_response: false,
                    auto_session_name: false,
                    initial_task_plan_patch: None,
                })
                .expect("create recovery session");
            assert!(matches!(
                store
                    .register_runtime(RegisterRuntimeRequest {
                        runtime_id: runtime_id.clone(),
                        session_id: session_id.clone(),
                        fallback_from_id: None,
                        lifecycle: None,
                    })
                    .expect("register recovery runtime"),
                RuntimeRegistrationOutcome::Registered { .. }
            ));
            if activate_lease {
                store
                    .activate_runtime_lease(ActivateRuntimeLeaseRequest {
                        runtime_id: runtime_id.clone(),
                        lease_id: lease_id.clone(),
                    })
                    .expect("activate recovery lease");
            }
            let database_path =
                std::fs::canonicalize(crate::path::workspace_session_log_db(&workspace_text))
                    .expect("canonical recovery database path")
                    .to_string_lossy()
                    .into_owned();
            Self {
                _root: root,
                _workspace: workspace,
                store,
                database_path,
                session_id,
                runtime_id,
                lease_id,
            }
        }

        fn commit_events(&self, published: bool) -> u64 {
            let mut runtime = runtime_aggregate(&self.runtime_id, &self.session_id);
            runtime
                .mark_called(runtime.created_at + chrono::Duration::milliseconds(1))
                .expect("runtime called");
            runtime
                .mark_waiting_first_token()
                .expect("runtime waiting first token");
            if published {
                runtime
                    .mark_first_token(runtime.created_at + chrono::Duration::milliseconds(2))
                    .expect("runtime first token");
                runtime
                    .append_text("authoritative output")
                    .expect("append authoritative output");
            }
            let events = runtime.take_uncommitted_events();
            for (index, event) in events.iter().cloned().enumerate() {
                let event_seq = index as u64 + 1;
                assert!(matches!(
                    self.store
                        .commit_runtime_event(CommitRuntimeEventRequest {
                            runtime_id: self.runtime_id.clone(),
                            event_seq,
                            expected_revision: event_seq - 1,
                            lease_id: self.lease_id.clone(),
                            idempotency_key: format!("{}:{event_seq}", self.runtime_id),
                            event,
                        })
                        .expect("commit recovery runtime event"),
                    RuntimeEventCommitOutcome::Applied { .. }
                ));
            }
            events.len() as u64
        }

        fn interrupt(&self) {
            assert!(
                self.store
                    .mark_session_interrupted(MarkSessionInterruptedRequest {
                        session_id: self.session_id.clone(),
                    })
                    .expect("interrupt recovery session")
            );
        }

        fn set_owner_state(&self, state: SessionState) {
            let session_command = match state {
                SessionState::Running => return,
                SessionState::Paused => SessionCommand::ApplyRuntimeState { state },
                SessionState::Completed => SessionCommand::RuntimeCompleted {
                    runtime_id: self.runtime_id.clone(),
                },
                SessionState::Failed => SessionCommand::RuntimeFailed {
                    runtime_id: self.runtime_id.clone(),
                },
                SessionState::Cancelled => SessionCommand::RuntimeCancelled {
                    runtime_id: self.runtime_id.clone(),
                },
                SessionState::Interrupted => SessionCommand::InterruptSession,
                SessionState::Created => panic!("registered runtime owner cannot remain created"),
            };
            let result = self
                .store
                .execute_session_command(ExecuteSessionCommandRequest {
                    command_id: format!("owner-state-{:?}:{}", state, self.session_id),
                    session_id: self.session_id.clone(),
                    session_command,
                    message_projection: None,
                })
                .expect("set recovery owner state");
            assert_eq!(result.projection.state, state);
        }

        fn session_ledger_snapshot(&self) -> SessionLedgerSnapshot {
            self.store
                .with_workspace_connection(Path::new(&self.database_path), |conn| {
                    let mut projection =
                        conn.prepare("SELECT * FROM sessions WHERE session_id = ?1")?;
                    let column_count = projection.column_count();
                    let projection_row = projection.query_row(params![self.session_id], |row| {
                        (0..column_count)
                            .map(|index| row.get(index))
                            .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                    })?;
                    let mut events = conn.prepare(
                        "SELECT event_seq, event_json FROM session_events
                         WHERE session_id = ?1 ORDER BY event_seq",
                    )?;
                    let events = events
                        .query_map(params![self.session_id], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })?
                        .collect::<rusqlite::Result<Vec<(u64, String)>>>()?;
                    Ok(SessionLedgerSnapshot {
                        projection_row,
                        events,
                    })
                })
                .expect("snapshot recovery owner ledger")
        }

        fn runtime_event_rows(&self) -> Vec<(u64, u64, String, String)> {
            self.store
                .with_workspace_connection(Path::new(&self.database_path), |conn| {
                    let mut statement = conn.prepare(
                        "SELECT event_seq, revision, idempotency_key, event_json
                         FROM runtime_events WHERE runtime_id = ?1 ORDER BY event_seq",
                    )?;
                    Ok(statement
                        .query_map(params![self.runtime_id], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?)
                })
                .expect("snapshot runtime event ledger")
        }

        fn recovery_receipt_count(&self, receipt_id: &str) -> u64 {
            self.store
                .with_workspace_connection(Path::new(&self.database_path), |conn| {
                    conn.query_row(
                        "SELECT COUNT(*) FROM session_command_receipts WHERE command_id = ?1",
                        params![receipt_id],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
                })
                .expect("count recovery receipts")
        }

        fn request(
            &self,
            receipt_id: &str,
            reason: RecoveryCloseRuntimeReason,
            revision: u64,
        ) -> RecoveryCloseRuntimeRequest {
            let (
                lease_id,
                expected_lease_active,
                expected_session_event_seq,
                expected_session_state,
            ) = self
                .store
                .with_workspace_connection(Path::new(&self.database_path), |conn| {
                    let session = replay_session_events(conn, &self.session_id)?;
                    let row = load_runtime_row(conn, &self.runtime_id)?
                        .expect("recovery fixture runtime exists");
                    Ok((
                        row.lease_id,
                        row.lease_active,
                        load_session_event_seq(conn, &self.session_id)?,
                        session.state,
                    ))
                })
                .expect("read recovery session CAS");
            RecoveryCloseRuntimeRequest {
                receipt_id: receipt_id.to_string(),
                database_path: self.database_path.clone(),
                runtime_id: self.runtime_id.clone(),
                session_id: self.session_id.clone(),
                lease_id,
                expected_lease_active,
                expected_revision: revision,
                expected_last_event_seq: revision,
                expected_session_event_seq,
                expected_session_state,
                reason,
                convergence_proof_sha256: (reason
                    == RecoveryCloseRuntimeReason::CommanderConvergenceProven)
                    .then(|| "a".repeat(64)),
                quiescence: quiescent_proof(),
            }
        }
    }

    #[test]
    fn runtime_lease_snapshot_exposes_exact_recovery_preimage() {
        let fixture = RecoveryFixture::new("lease-snapshot");
        let snapshot = fixture
            .store
            .get_runtime_lease(GetRuntimeLeaseRequest {
                runtime_id: fixture.runtime_id.clone(),
                database_path: None,
            })
            .expect("read runtime lease snapshot")
            .expect("runtime lease snapshot exists");

        assert_eq!(snapshot.database_path, fixture.database_path);
        assert_eq!(snapshot.runtime_id, fixture.runtime_id);
        assert_eq!(snapshot.session_id, fixture.session_id);
        assert_eq!(
            snapshot.lease_id.as_deref(),
            Some(fixture.lease_id.as_str())
        );
        assert!(snapshot.lease_active);
        assert!(!snapshot.terminal);
        assert_eq!(snapshot.revision, 0);
        assert_eq!(snapshot.last_event_seq, 0);
        assert_eq!(snapshot.session_event_seq, 2);
        assert_eq!(snapshot.session_state, SessionState::Running);
    }

    #[test]
    fn runtime_location_only_row_is_discovered_and_terminalized_exactly_once() {
        let fixture = RecoveryFixture::new("location-only");
        fixture.interrupt();
        fixture
            .store
            .with_index_connection(|conn| {
                conn.execute(
                    "DELETE FROM sessions WHERE session_id = ?1",
                    params![fixture.session_id],
                )?;
                Ok(())
            })
            .expect("remove derived session index row");

        assert!(
            fixture
                .store
                .list_workspaces()
                .expect("list workspaces")
                .is_empty()
        );
        let (page, locations) = fixture
            .store
            .list_runtime_locations(ListRuntimeLocationsRequest {
                page: 0,
                page_size: 1,
            })
            .expect("list authoritative runtime registrations");
        assert_eq!(page.total, 1);
        assert_eq!(locations.len(), 1);
        let location = &locations[0];
        assert_eq!(location.runtime_id, fixture.runtime_id);
        assert_eq!(location.session_id, fixture.session_id);
        assert_eq!(
            std::fs::canonicalize(&location.workspace_db_path)
                .expect("canonical indexed runtime database")
                .to_string_lossy(),
            fixture.database_path
        );

        let snapshot = fixture
            .store
            .get_runtime_lease(GetRuntimeLeaseRequest {
                runtime_id: location.runtime_id.clone(),
                database_path: Some(
                    std::fs::canonicalize(&location.workspace_db_path)
                        .expect("canonical registered runtime database")
                        .to_string_lossy()
                        .into_owned(),
                ),
            })
            .expect("read exact registered runtime")
            .expect("registered runtime exists");
        let receipt_id = "runtime-location-only-recovery";
        let request = fixture.request(
            receipt_id,
            RecoveryCloseRuntimeReason::UnbornRuntime,
            snapshot.revision,
        );
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(request.clone())
                .expect("close runtime location-only row"),
            RecoveryCloseRuntimeOutcome::Closed { ref receipt }
                if receipt.runtime_id == fixture.runtime_id
                    && receipt.session_id == fixture.session_id
                    && receipt.terminal
                    && !receipt.lease_active
        ));
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(request)
                .expect("replay runtime location-only close"),
            RecoveryCloseRuntimeOutcome::AlreadyClosed { .. }
        ));
        assert_eq!(fixture.recovery_receipt_count(receipt_id), 1);

        let (terminal, lease_active, terminal_projection_count) = fixture
            .store
            .with_workspace_connection(Path::new(&fixture.database_path), |conn| {
                let (terminal, lease_active) = conn.query_row(
                    "SELECT terminal, lease_active FROM runtimes WHERE runtime_id = ?1",
                    params![fixture.runtime_id],
                    |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
                )?;
                let terminal_projection_count = conn.query_row(
                    "SELECT COUNT(*) FROM session_feed_events WHERE event_id = ?1",
                    ["runtime-recovery:runtime-location-only-recovery:session-projection"],
                    |row| row.get::<_, u64>(0),
                )?;
                Ok((terminal, lease_active, terminal_projection_count))
            })
            .expect("read exact location-only terminal ledger");
        assert!(terminal);
        assert!(!lease_active);
        assert_eq!(terminal_projection_count, 1);
    }

    #[test]
    fn runtime_location_listing_filters_complete_terminal_proof_before_pagination() {
        let fixture = RecoveryFixture::new("bounded-listing");
        fixture
            .store
            .with_index_connection(|conn| {
                let tx = conn.transaction()?;
                for index in 0..1_000_u64 {
                    tx.execute(
                        "INSERT INTO runtime_locations(
                            runtime_id, session_id, workspace_db_path, terminal_proven,
                            terminal_revision, terminal_event_seq, terminal_evidence_id
                         ) VALUES (?1, ?2, ?3, 1, 1, 1, ?4)",
                        params![
                            format!("proven-{index:04}"),
                            format!("historical-session-{index:04}"),
                            fixture.database_path,
                            format!("terminal-proof-{index:04}")
                        ],
                    )?;
                }
                tx.execute(
                    "INSERT INTO runtime_locations(
                        runtime_id, session_id, workspace_db_path, terminal_proven,
                        terminal_revision, terminal_event_seq, terminal_evidence_id
                     ) VALUES ('malformed-proof', 'malformed-session', ?1, 1, 1, NULL, 'evidence')",
                    params![fixture.database_path],
                )?;
                tx.commit()?;
                Ok(())
            })
            .expect("seed terminal history");

        let (page, locations) = fixture
            .store
            .list_runtime_locations(ListRuntimeLocationsRequest {
                page: 0,
                page_size: 500,
            })
            .expect("list actionable locations");
        assert_eq!(page.total, 2);
        assert_eq!(locations.len(), 2);
        assert!(
            locations
                .iter()
                .any(|location| location.runtime_id == fixture.runtime_id)
        );
        assert!(
            locations
                .iter()
                .any(|location| location.runtime_id == "malformed-proof")
        );
        assert!(
            locations
                .iter()
                .all(|location| !location.runtime_id.starts_with("proven-"))
        );
    }

    #[test]
    fn deleting_session_removes_only_its_derived_runtime_locations() {
        let fixture = RecoveryFixture::new("delete-derived-location");
        fixture
            .store
            .with_index_connection(|conn| {
                conn.execute(
                    "INSERT INTO runtime_locations(runtime_id, session_id, workspace_db_path)
                     VALUES ('unrelated-runtime', 'unrelated-session', '/tmp/unrelated.sqlite3')",
                    [],
                )?;
                Ok(())
            })
            .expect("seed unrelated location");

        fixture
            .store
            .delete_session(DeleteSessionRequest {
                session_id: fixture.session_id.clone(),
            })
            .expect("delete indexed session");

        fixture
            .store
            .with_index_connection(|conn| {
                let target_count = conn.query_row(
                    "SELECT COUNT(*) FROM runtime_locations WHERE session_id = ?1",
                    params![fixture.session_id],
                    |row| row.get::<_, u64>(0),
                )?;
                let unrelated_count = conn.query_row(
                    "SELECT COUNT(*) FROM runtime_locations WHERE runtime_id = 'unrelated-runtime'",
                    [],
                    |row| row.get::<_, u64>(0),
                )?;
                assert_eq!(target_count, 0);
                assert_eq!(unrelated_count, 1);
                Ok(())
            })
            .expect("verify derived location cleanup");
    }

    #[test]
    fn deleting_workspace_removes_only_its_derived_runtime_locations() {
        let fixture = RecoveryFixture::new("delete-workspace-derived-location");
        fixture
            .store
            .with_index_connection(|conn| {
                conn.execute(
                    "INSERT INTO runtime_locations(runtime_id, session_id, workspace_db_path)
                     VALUES ('unrelated-workspace-runtime', 'unrelated-workspace-session',
                             '/tmp/unrelated-workspace.sqlite3')",
                    [],
                )?;
                Ok(())
            })
            .expect("seed unrelated workspace location");

        fixture
            .store
            .delete_workspace(DeleteWorkspaceRequest {
                workspace: fixture._workspace.path().to_string_lossy().into_owned(),
            })
            .expect("delete indexed workspace");

        fixture
            .store
            .with_index_connection(|conn| {
                let target_count = conn.query_row(
                    "SELECT COUNT(*) FROM runtime_locations WHERE session_id = ?1",
                    params![fixture.session_id],
                    |row| row.get::<_, u64>(0),
                )?;
                let unrelated_count = conn.query_row(
                    "SELECT COUNT(*) FROM runtime_locations
                     WHERE runtime_id = 'unrelated-workspace-runtime'",
                    [],
                    |row| row.get::<_, u64>(0),
                )?;
                assert_eq!(target_count, 0);
                assert_eq!(unrelated_count, 1);
                Ok(())
            })
            .expect("verify workspace derived location cleanup");
    }

    #[test]
    fn orphaned_runtime_appends_cancelled_failure_and_closes_exactly_once() {
        let fixture = RecoveryFixture::new("orphaned");
        let revision = fixture.commit_events(false);
        fixture.interrupt();
        let request = fixture.request(
            "recovery-receipt-orphaned",
            RecoveryCloseRuntimeReason::OrphanedRuntime,
            revision,
        );
        let before_replay = fixture
            .store
            .replay_runtime(ReplayRuntimeRequest {
                runtime_id: fixture.runtime_id.clone(),
            })
            .expect("replay orphan before recovery")
            .expect("orphan event stream exists");

        let first = fixture
            .store
            .recovery_close_runtime(request.clone())
            .expect("close orphaned runtime");
        assert!(matches!(
            first,
            RecoveryCloseRuntimeOutcome::Closed { ref receipt }
                if !receipt.lease_active
                    && receipt.terminal
                    && receipt.revision == revision + 1
                    && receipt.last_event_seq == revision + 1
                    && receipt.session_state == SessionState::Interrupted
        ));
        let snapshot = fixture
            .store
            .get_runtime_lease(GetRuntimeLeaseRequest {
                runtime_id: fixture.runtime_id.clone(),
                database_path: Some(fixture.database_path.clone()),
            })
            .expect("read closed runtime")
            .expect("closed runtime exists");
        assert!(!snapshot.lease_active);
        assert!(snapshot.terminal);
        assert_eq!(snapshot.revision, revision + 1);
        assert_eq!(snapshot.last_event_seq, revision + 1);
        let after_replay = fixture
            .store
            .replay_runtime(ReplayRuntimeRequest {
                runtime_id: fixture.runtime_id.clone(),
            })
            .expect("replay orphan after recovery")
            .expect("orphan event stream remains");
        assert_eq!(after_replay.revision, before_replay.revision + 1);
        assert_eq!(
            after_replay.next_event_seq,
            before_replay.next_event_seq + 1
        );
        assert_eq!(after_replay.aggregate.state, RuntimeState::Cancelled);
        assert!(matches!(
            after_replay.aggregate.error,
            Some(lifecycle::RuntimeError {
                ref error_code,
                retry_allowed: false,
                fallback_allowed: false,
                ..
            }) if error_code.as_deref() == Some("runtime_recovery_cancelled")
        ));
        let session = fixture
            .store
            .get_session(GetSessionRequest {
                session_id: fixture.session_id.clone(),
            })
            .expect("read recovered session")
            .expect("recovered session exists");
        assert_eq!(
            session.lifecycle_projection.state,
            SessionState::Interrupted
        );
        assert!(session.lifecycle_projection.active_runtime_id.is_none());
        let (feed, _) = fixture
            .store
            .read_session_feed(ReadSessionFeedRequest {
                session_id: fixture.session_id.clone(),
                after_cursor: 0,
                limit: 100,
            })
            .expect("read recovery terminal feed");
        assert_eq!(
            feed.iter()
                .filter(|entry| {
                    entry.event_id
                        == "runtime-recovery:recovery-receipt-orphaned:session-projection"
                })
                .count(),
            1
        );

        let mut replay = request;
        replay.quiescence.active_turn = true;
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(replay)
                .expect("replay recovery receipt"),
            RecoveryCloseRuntimeOutcome::AlreadyClosed { .. }
        ));
        let (feed, _) = fixture
            .store
            .read_session_feed(ReadSessionFeedRequest {
                session_id: fixture.session_id.clone(),
                after_cursor: 0,
                limit: 100,
            })
            .expect("read replayed recovery terminal feed");
        assert_eq!(
            feed.iter()
                .filter(|entry| {
                    entry.event_id
                        == "runtime-recovery:recovery-receipt-orphaned:session-projection"
                })
                .count(),
            1
        );
    }

    #[test]
    fn proven_commander_convergence_closes_as_failed_and_is_retryable_exactly_once() {
        let fixture = RecoveryFixture::new("commander-proof");
        let revision = fixture.commit_events(false);
        let request = fixture.request(
            "recovery-receipt-commander-proof",
            RecoveryCloseRuntimeReason::CommanderConvergenceProven,
            revision,
        );

        let first = fixture
            .store
            .recovery_close_runtime(request.clone())
            .expect("close proven Commander convergence runtime");
        assert!(matches!(
            first,
            RecoveryCloseRuntimeOutcome::Closed { ref receipt }
                if receipt.session_state == SessionState::Failed
                    && receipt.convergence_proof_sha256.as_deref()
                        == Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    && receipt.terminal
                    && !receipt.lease_active
        ));
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(request)
                .expect("replay proven Commander convergence close"),
            RecoveryCloseRuntimeOutcome::AlreadyClosed { .. }
        ));
        assert_eq!(
            fixture.recovery_receipt_count("recovery-receipt-commander-proof"),
            1
        );

        let runtime = fixture
            .store
            .replay_runtime(ReplayRuntimeRequest {
                runtime_id: fixture.runtime_id.clone(),
            })
            .expect("replay proven Commander convergence runtime")
            .expect("proven Commander convergence runtime exists")
            .aggregate;
        assert_eq!(runtime.state, RuntimeState::Failed);
        assert!(matches!(
            runtime.error,
            Some(lifecycle::RuntimeError {
                ref error_code,
                retry_allowed: true,
                fallback_allowed: false,
                ..
            }) if error_code.as_deref()
                == Some("commander_convergence_proven_before_runtime_output")
        ));
        let session = fixture
            .store
            .get_session(GetSessionRequest {
                session_id: fixture.session_id.clone(),
            })
            .expect("read failed Commander convergence session")
            .expect("Commander convergence session exists");
        assert_eq!(session.lifecycle_projection.state, SessionState::Failed);
        assert!(session.lifecycle_projection.active_runtime_id.is_none());
    }

    #[test]
    fn proven_commander_convergence_without_proof_is_rejected_without_mutation() {
        let fixture = RecoveryFixture::new("commander-proof-missing");
        let revision = fixture.commit_events(false);
        let mut request = fixture.request(
            "recovery-receipt-commander-proof-missing",
            RecoveryCloseRuntimeReason::CommanderConvergenceProven,
            revision,
        );
        request.convergence_proof_sha256 = None;
        let before = fixture.runtime_event_rows();

        let error = fixture
            .store
            .recovery_close_runtime(request)
            .expect_err("missing proof must fail before mutation");
        assert!(error.to_string().contains("requires proof SHA-256"));
        assert_eq!(fixture.runtime_event_rows(), before);
        assert_eq!(
            fixture.recovery_receipt_count("recovery-receipt-commander-proof-missing"),
            0
        );
    }

    #[test]
    fn terminalization_matrix_closes_only_the_registered_runtime_exactly_once() {
        let rows = [
            ("pre_provider_dispatch_failure", false),
            ("provider_error", true),
            ("provider_interruption", true),
            ("provider_timeout", true),
            ("tool_failure", true),
            ("post_tool_feed_projection_failure", true),
            ("explicit_cancel", true),
            ("client_disconnect_durable_turn", true),
            ("killed_runtime_worker", true),
            ("router_restart_recovery", true),
            ("legacy_orphan_or_stale_runtime", true),
        ];

        for (source, has_runtime_events) in rows {
            let fixture = RecoveryFixture::new(&format!("matrix-{source}"));
            let revision = has_runtime_events
                .then(|| fixture.commit_events(false))
                .unwrap_or_default();
            fixture.interrupt();
            let reason = if has_runtime_events {
                RecoveryCloseRuntimeReason::OrphanedRuntime
            } else {
                RecoveryCloseRuntimeReason::UnbornRuntime
            };
            let receipt_id = format!("terminalization-matrix-{source}");
            let request = fixture.request(&receipt_id, reason, revision);

            let receipt = match fixture
                .store
                .recovery_close_runtime(request.clone())
                .expect("close matrix runtime")
            {
                RecoveryCloseRuntimeOutcome::Closed { receipt } => receipt,
                other => panic!("matrix row {source} did not close: {other:?}"),
            };
            assert_eq!(receipt.runtime_id, fixture.runtime_id, "row={source}");
            assert_eq!(receipt.session_id, fixture.session_id, "row={source}");
            assert!(receipt.terminal, "row={source}");
            assert!(!receipt.lease_active, "row={source}");

            let runtime = fixture
                .store
                .get_runtime_lease(GetRuntimeLeaseRequest {
                    runtime_id: fixture.runtime_id.clone(),
                    database_path: Some(fixture.database_path.clone()),
                })
                .expect("read matrix runtime")
                .expect("matrix runtime exists");
            assert!(runtime.terminal, "row={source}");
            assert!(!runtime.lease_active, "row={source}");

            let (feed, _) = fixture
                .store
                .read_session_feed(ReadSessionFeedRequest {
                    session_id: fixture.session_id.clone(),
                    after_cursor: 0,
                    limit: 100,
                })
                .expect("read matrix terminal feed");
            let terminal_projection_count = feed
                .iter()
                .filter(|entry| {
                    entry.event_id == format!("runtime-recovery:{receipt_id}:session-projection")
                        && matches!(
                            entry.event,
                            SessionFeedEvent::SessionProjectionUpdated { .. }
                        )
                })
                .count();
            assert_eq!(terminal_projection_count, 1, "row={source}");
            assert_eq!(
                fixture.recovery_receipt_count(&receipt_id),
                1,
                "row={source}"
            );

            let other_terminal_count = fixture
                .store
                .with_workspace_connection(Path::new(&fixture.database_path), |conn| {
                    conn.query_row(
                        "SELECT COUNT(*) FROM runtimes WHERE terminal = 1 AND runtime_id != ?1",
                        params![fixture.runtime_id],
                        |row| row.get::<_, u64>(0),
                    )
                    .map_err(Into::into)
                })
                .expect("count non-target terminal runtimes");
            assert_eq!(other_terminal_count, 0, "row={source}");

            assert!(matches!(
                fixture
                    .store
                    .recovery_close_runtime(request)
                    .expect("replay matrix runtime"),
                RecoveryCloseRuntimeOutcome::AlreadyClosed { ref receipt }
                    if receipt.runtime_id == fixture.runtime_id
            ));
            assert_eq!(
                fixture.recovery_receipt_count(&receipt_id),
                1,
                "row={source}"
            );
        }
    }

    #[test]
    fn unborn_runtime_ignores_unrelated_global_activity_but_rejects_target_liveness() {
        let fixture = RecoveryFixture::new("unborn");
        fixture.interrupt();
        assert!(
            fixture
                .store
                .replay_runtime(ReplayRuntimeRequest {
                    runtime_id: fixture.runtime_id.clone(),
                })
                .expect("replay unborn before recovery")
                .is_none()
        );
        let mut request = fixture.request(
            "recovery-receipt-unborn",
            RecoveryCloseRuntimeReason::UnbornRuntime,
            0,
        );
        request.quiescence.global_active_session_count = 3;
        request.quiescence.global_retained_slot_count = 2;
        request.quiescence.global_active_command_runs = 1;
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(request.clone())
                .expect("close target despite unrelated global activity"),
            RecoveryCloseRuntimeOutcome::Closed { .. }
        ));
        let closed = fixture
            .store
            .get_runtime_lease(GetRuntimeLeaseRequest {
                runtime_id: fixture.runtime_id.clone(),
                database_path: Some(fixture.database_path.clone()),
            })
            .expect("read closed unborn runtime")
            .expect("closed unborn runtime exists");
        assert!(!closed.lease_active);
        assert!(closed.terminal);
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(request)
                .expect("replay unborn recovery"),
            RecoveryCloseRuntimeOutcome::AlreadyClosed { .. }
        ));
        assert!(
            fixture
                .store
                .replay_runtime(ReplayRuntimeRequest {
                    runtime_id: fixture.runtime_id.clone(),
                })
                .expect("replay unborn after recovery")
                .is_none()
        );

        let live = RecoveryFixture::new("target-live");
        live.interrupt();
        let mut live_request = live.request(
            "recovery-receipt-target-live",
            RecoveryCloseRuntimeReason::UnbornRuntime,
            0,
        );
        live_request.quiescence.active_turn = true;
        assert!(matches!(
            live.store
                .recovery_close_runtime(live_request)
                .expect("reject target liveness"),
            RecoveryCloseRuntimeOutcome::RuntimeLive { .. }
        ));
    }

    #[test]
    fn published_orphan_appends_cancelled_failure_without_rewriting_publication() {
        let fixture = RecoveryFixture::new("published");
        let revision = fixture.commit_events(true);
        fixture.interrupt();
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(fixture.request(
                    "recovery-receipt-published",
                    RecoveryCloseRuntimeReason::OrphanedRuntime,
                    revision,
                ))
                .expect("published recovery decision"),
            RecoveryCloseRuntimeOutcome::Closed { .. }
        ));
        let snapshot = fixture
            .store
            .get_runtime_lease(GetRuntimeLeaseRequest {
                runtime_id: fixture.runtime_id.clone(),
                database_path: Some(fixture.database_path.clone()),
            })
            .expect("read published runtime")
            .expect("published runtime exists");
        assert!(!snapshot.lease_active);
        assert!(snapshot.terminal);
        assert_eq!(snapshot.revision, revision + 1);
        let replay = fixture
            .store
            .replay_runtime(ReplayRuntimeRequest {
                runtime_id: fixture.runtime_id.clone(),
            })
            .expect("replay published recovery")
            .expect("published runtime remains replayable");
        assert_eq!(replay.aggregate.text, "authoritative output");
        assert_eq!(replay.aggregate.state, RuntimeState::Cancelled);
    }

    #[test]
    fn conflicting_retry_and_preclosure_cas_conflict_fail_closed() {
        let closed = RecoveryFixture::new("receipt-conflict");
        closed.interrupt();
        let request = closed.request(
            "recovery-receipt-conflict",
            RecoveryCloseRuntimeReason::UnbornRuntime,
            0,
        );
        assert!(matches!(
            closed
                .store
                .recovery_close_runtime(request.clone())
                .expect("first recovery close"),
            RecoveryCloseRuntimeOutcome::Closed { .. }
        ));
        let mut conflicting = request;
        conflicting.reason = RecoveryCloseRuntimeReason::OrphanedRuntime;
        assert_eq!(
            closed
                .store
                .recovery_close_runtime(conflicting)
                .expect("conflicting receipt replay"),
            RecoveryCloseRuntimeOutcome::ReceiptConflict
        );

        let stale = RecoveryFixture::new("cas-conflict");
        stale.interrupt();
        assert_eq!(
            stale
                .store
                .recovery_close_runtime(stale.request(
                    "recovery-receipt-cas",
                    RecoveryCloseRuntimeReason::UnbornRuntime,
                    1,
                ))
                .expect("stale CAS decision"),
            RecoveryCloseRuntimeOutcome::CasConflict {
                current_revision: 0,
                current_last_event_seq: 0,
            }
        );
        let snapshot = stale
            .store
            .get_runtime_lease(GetRuntimeLeaseRequest {
                runtime_id: stale.runtime_id.clone(),
                database_path: Some(stale.database_path.clone()),
            })
            .expect("read CAS-conflicted runtime")
            .expect("CAS-conflicted runtime exists");
        assert!(snapshot.lease_active);
        assert!(!snapshot.terminal);
        assert!(matches!(
            stale
                .store
                .recovery_close_runtime(stale.request(
                    "recovery-receipt-cas",
                    RecoveryCloseRuntimeReason::UnbornRuntime,
                    0,
                ))
                .expect("corrected CAS recovery"),
            RecoveryCloseRuntimeOutcome::Closed { .. }
        ));
    }

    #[test]
    fn exact_database_path_recovers_when_the_derived_runtime_route_is_missing() {
        let fixture = RecoveryFixture::new("exact-path");
        fixture.interrupt();
        fixture
            .store
            .with_index_connection(|conn| {
                conn.execute(
                    "DELETE FROM runtime_locations WHERE runtime_id = ?1",
                    params![fixture.runtime_id],
                )?;
                Ok(())
            })
            .expect("remove stale derived runtime route");

        assert!(
            fixture
                .store
                .get_runtime_lease(GetRuntimeLeaseRequest {
                    runtime_id: fixture.runtime_id.clone(),
                    database_path: None,
                })
                .expect("read through missing derived route")
                .is_none()
        );
        assert!(
            fixture
                .store
                .get_runtime_lease(GetRuntimeLeaseRequest {
                    runtime_id: fixture.runtime_id.clone(),
                    database_path: Some(fixture.database_path.clone()),
                })
                .expect("read through exact recovery path")
                .is_some()
        );
        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(fixture.request(
                    "recovery-receipt-exact-path",
                    RecoveryCloseRuntimeReason::UnbornRuntime,
                    0,
                ))
                .expect("recover through exact database path"),
            RecoveryCloseRuntimeOutcome::Closed { .. }
        ));
    }

    #[test]
    fn inactive_runtime_without_a_lease_closes_under_exact_null_identity() {
        let fixture = RecoveryFixture::new_without_active_lease("null-lease");
        fixture.interrupt();
        let request = fixture.request(
            "recovery-receipt-null-lease",
            RecoveryCloseRuntimeReason::UnbornRuntime,
            0,
        );
        assert_eq!(request.lease_id, None);
        assert!(!request.expected_lease_active);

        let outcome = fixture
            .store
            .recovery_close_runtime(request)
            .expect("close inactive null-lease runtime");
        assert!(matches!(
            outcome,
            RecoveryCloseRuntimeOutcome::Closed { ref receipt }
                if receipt.lease_id.is_none()
                    && !receipt.lease_active
                    && receipt.terminal
        ));
    }

    #[test]
    fn stale_session_cas_leaves_runtime_and_session_streams_untouched() {
        let fixture = RecoveryFixture::new("session-cas");
        let revision = fixture.commit_events(false);
        fixture.interrupt();
        let session_before = fixture.session_ledger_snapshot();
        let runtime_before = fixture.runtime_event_rows();
        let receipt_id = "recovery-receipt-session-cas";
        let mut request = fixture.request(
            receipt_id,
            RecoveryCloseRuntimeReason::OrphanedRuntime,
            revision,
        );
        request.expected_session_event_seq -= 1;

        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(request)
                .expect("reject stale session CAS"),
            RecoveryCloseRuntimeOutcome::SessionCasConflict { .. }
        ));
        assert_eq!(fixture.session_ledger_snapshot(), session_before);
        assert_eq!(fixture.runtime_event_rows(), runtime_before);
        assert_eq!(fixture.recovery_receipt_count(receipt_id), 0);
        let runtime = fixture
            .store
            .get_runtime_lease(GetRuntimeLeaseRequest {
                runtime_id: fixture.runtime_id.clone(),
                database_path: Some(fixture.database_path.clone()),
            })
            .expect("read runtime after stale session CAS")
            .expect("runtime remains present");
        assert!(runtime.lease_active);
        assert!(!runtime.terminal);
    }

    #[test]
    fn runtime_update_cas_failure_rolls_back_the_preceding_event_append() {
        let fixture = RecoveryFixture::new("transaction-rollback");
        let revision = fixture.commit_events(false);
        fixture.interrupt();
        let session_before = fixture.session_ledger_snapshot();
        let runtime_before = fixture.runtime_event_rows();
        let receipt_id = "recovery-receipt-transaction-rollback";
        fixture
            .store
            .with_workspace_connection(Path::new(&fixture.database_path), |conn| {
                conn.execute_batch(
                    "CREATE TRIGGER reject_recovery_runtime_update
                     BEFORE UPDATE OF lease_active, terminal ON runtimes
                     WHEN NEW.terminal = 1
                     BEGIN
                         SELECT RAISE(IGNORE);
                     END;",
                )?;
                Ok(())
            })
            .expect("install deterministic CAS conflict trigger");

        assert!(matches!(
            fixture
                .store
                .recovery_close_runtime(fixture.request(
                    receipt_id,
                    RecoveryCloseRuntimeReason::OrphanedRuntime,
                    revision,
                ))
                .expect("surface runtime update CAS conflict"),
            RecoveryCloseRuntimeOutcome::CasConflict { .. }
        ));
        assert_eq!(fixture.session_ledger_snapshot(), session_before);
        assert_eq!(fixture.runtime_event_rows(), runtime_before);
        assert_eq!(fixture.recovery_receipt_count(receipt_id), 0);
    }

    #[test]
    fn recovery_covers_all_six_owner_states_without_rewriting_terminal_owners() {
        for owner_state in [
            SessionState::Running,
            SessionState::Paused,
            SessionState::Completed,
            SessionState::Failed,
            SessionState::Cancelled,
            SessionState::Interrupted,
        ] {
            let fixture = RecoveryFixture::new(&format!("owner-{owner_state:?}"));
            fixture.set_owner_state(owner_state);
            let before = fixture.session_ledger_snapshot();
            let before_event_count = before.events.len();
            let receipt_id = format!("recovery-receipt-owner-{owner_state:?}");

            let outcome = fixture
                .store
                .recovery_close_runtime(fixture.request(
                    &receipt_id,
                    RecoveryCloseRuntimeReason::UnbornRuntime,
                    0,
                ))
                .expect("recover owner-state residue");
            let expected_state = if owner_state.is_recoverable_running() {
                SessionState::Interrupted
            } else {
                owner_state
            };
            assert!(matches!(
                outcome,
                RecoveryCloseRuntimeOutcome::Closed { ref receipt }
                    if receipt.session_state == expected_state
            ));
            let after = fixture.session_ledger_snapshot();
            if owner_state.is_recoverable_running() {
                assert_eq!(after.events.len(), before_event_count + 1);
                let event: SessionEvent = serde_json::from_str(&after.events.last().unwrap().1)
                    .expect("decode recovery interruption event");
                assert!(matches!(
                    event,
                    SessionEvent::SessionInterrupted {
                        state: SessionState::Interrupted,
                        ..
                    }
                ));
            } else {
                assert_eq!(after, before, "terminal owner {owner_state:?} changed");
            }
            let runtime = fixture
                .store
                .get_runtime_lease(GetRuntimeLeaseRequest {
                    runtime_id: fixture.runtime_id.clone(),
                    database_path: Some(fixture.database_path.clone()),
                })
                .expect("read recovered owner runtime")
                .expect("owner runtime remains present");
            assert!(!runtime.lease_active);
            assert!(runtime.terminal);
        }
    }

    #[test]
    fn recovery_reason_must_match_runtime_event_shape() {
        let unborn = RecoveryFixture::new("wrong-unborn-reason");
        unborn.interrupt();
        assert!(matches!(
            unborn
                .store
                .recovery_close_runtime(unborn.request(
                    "recovery-receipt-wrong-unborn",
                    RecoveryCloseRuntimeReason::OrphanedRuntime,
                    0,
                ))
                .expect("unborn reason decision"),
            RecoveryCloseRuntimeOutcome::InvalidRecoveryShape { .. }
        ));

        let orphan = RecoveryFixture::new("wrong-orphan-reason");
        let revision = orphan.commit_events(false);
        orphan.interrupt();
        assert!(matches!(
            orphan
                .store
                .recovery_close_runtime(orphan.request(
                    "recovery-receipt-wrong-orphan",
                    RecoveryCloseRuntimeReason::UnbornRuntime,
                    revision,
                ))
                .expect("orphan reason decision"),
            RecoveryCloseRuntimeOutcome::InvalidRecoveryShape { .. }
        ));
    }

    fn quiescent_proof() -> RuntimeRecoveryQuiescenceProof {
        RuntimeRecoveryQuiescenceProof {
            active_turn: false,
            queued_turn: false,
            running_turn: false,
            worker_alive: false,
            active_command_runs: 0,
            retained_process_scopes: 0,
            retained_slot: false,
            global_active_session_count: 0,
            global_retained_slot_count: 0,
            global_active_command_runs: 0,
        }
    }

    fn runtime_aggregate(runtime_id: &str, session_id: &str) -> RuntimeAggregate {
        RuntimeAggregate::new(
            runtime_id.to_string(),
            session_id.to_string(),
            "recovery-test-agent".to_string(),
            RuntimeProviderConfig {
                base: ProviderConfig {
                    tura_llm_name: "recovery-test-provider".to_string(),
                    default_model_tier: None,
                    current_model: Some("recovery-test-model".to_string()),
                    stream: true,
                    temperature: 0.0,
                    max_tokens: 1024,
                    tool_choice: ToolChoice::Auto,
                    time_out_ms: 1_000,
                },
                thinking: false,
                provider_name: "recovery-test-provider".to_string(),
                model_name: "recovery-test-model".to_string(),
                provider_url_name: "recovery-test-provider".to_string(),
                llm_provider_name: "recovery-test-provider".to_string(),
            },
            chrono::Utc::now(),
        )
    }
}
