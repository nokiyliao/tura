//! Router-owned execution supervision.
//!
//! This module owns runtime worker lifecycle decisions. Gateway may enqueue or
//! cancel turns, but must not spawn runtime workers directly.

use anyhow::{anyhow, Result};
use lifecycle::{RuntimeAggregate, RuntimeId, RuntimeState, SessionState};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Notify, RwLock};

use crate::services::runtime_workers::{runtime_worker_limit, MAX_QUEUED_RUNTIME_TURNS};
use crate::{dispatch_run_agent_with_runtime_slot, AppState};
use router_contract::{CancelRuntimeRequest, EnqueueTurnRequest, ProbeSessionsRequest};
use runtime_contract::{LifecycleExecutionContext, RunAgentRequest};
use session_lifecycle::{
    commander_store_path, IntakeOutcome, LifecycleConfig, LiveEffectEvidence, ReclaimOutcome,
    SessionLifecycleStore, TerminalReceipt, TerminalReceiptIdentity, TerminalState,
};
use session_log_contract::{
    recovery_terminal_projection_event_id, ActivateRuntimeLeaseRequest, GetRuntimeLeaseRequest,
    GetSessionRequest, RecoveryCloseRuntimeOutcome, RecoveryCloseRuntimeReason,
    RecoveryCloseRuntimeRequest, RegisterRuntimeRequest, ReplayRuntimeRequest,
    RuntimeLeaseOutcome, RuntimeLeaseSnapshot, RuntimeLifecycleIdentity,
    RuntimeRecoveryQuiescenceProof, RuntimeRecoveryReceipt, RuntimeRegistrationOutcome,
    SessionFeedEntry, SessionFeedEvent, SessionLogCommand, SessionLogResponse,
};

#[derive(Clone)]
pub struct ExecutionService {
    admission: Arc<RwLock<()>>,
    sessions: Arc<Mutex<HashMap<String, RuntimeLease>>>,
    runtime_slots: RuntimeSlotGate,
    retained_slots: Arc<Mutex<HashMap<String, RuntimeSlotPermit>>>,
    retained_watchers: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeLease {
    runtime_id: RuntimeId,
    lease_id: String,
    commander_session_id: String,
    transaction_id: String,
    task_id: Option<String>,
    goal_id: Option<String>,
    operator_override: bool,
    receipt_event_seq: u64,
    slot_acquired: bool,
    terminalizing: bool,
}

fn active_turn_conflict(session_id: &str, active: &RuntimeLease) -> Value {
    json!({
        "ok": false,
        "code": "session_active_turn",
        "session_id": session_id,
        "runtime_id": active.runtime_id,
        "error": format!("session {session_id} already has an active turn"),
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RouterRecoveryCloseRuntimeRequest {
    receipt_id: String,
    database_path: String,
    runtime_id: String,
    session_id: String,
    lease_id: Option<String>,
    expected_lease_active: bool,
    expected_revision: u64,
    expected_last_event_seq: u64,
    expected_session_event_seq: u64,
    expected_session_state: lifecycle::SessionState,
    reason: RecoveryCloseRuntimeReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TerminalDeliveryIdentity {
    pub(crate) commander_session_id: String,
    pub(crate) transaction_id: String,
    pub(crate) event_id: String,
    pub(crate) runtime_id: String,
}

impl ExecutionService {
    pub fn new() -> Self {
        Self {
            admission: Arc::new(RwLock::new(())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            runtime_slots: RuntimeSlotGate::default(),
            retained_slots: Arc::new(Mutex::new(HashMap::new())),
            retained_watchers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn enqueue_turn_request(
        &self,
        state: &AppState,
        input: Value,
        request_id: &str,
    ) -> Result<Value> {
        let _admission = self.admission.read().await;
        let request: EnqueueTurnRequest = serde_json::from_value(input)?;
        if let Some(active) = self.sessions.lock().get(&request.session_id) {
            return Ok(active_turn_conflict(&request.session_id, active));
        }
        let lease_id = format!("lease-{}", uuid::Uuid::new_v4());
        state.session_db.start()?;
        let mut run_request = payload_to_run_agent_request(&request, &lease_id, None)?;
        let requested_prompt = run_request
            .prompt
            .as_deref()
            .or(run_request.message.as_deref())
            .or_else(|| run_request.input.as_ref().and_then(Value::as_str));
        let fallback_from_id =
            runtime_registration_fallback(&request.session_id, requested_prompt)?;
        run_request.fallback_from_id.clone_from(&fallback_from_id);
        let commander_session_id = run_request
            .parent_session_id
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| request.session_id.clone());
        let supplied_lifecycle = run_request.lifecycle.take();
        let task_id = supplied_lifecycle
            .as_ref()
            .and_then(|context| context.task_id.clone())
            .or_else(|| run_request.task_id.clone());
        let goal_id = supplied_lifecycle
            .as_ref()
            .and_then(|context| context.goal_id.clone())
            .or_else(|| run_request.goal_id.clone());
        let operator_override = supplied_lifecycle
            .as_ref()
            .map(|context| context.operator_override)
            .unwrap_or(run_request.operator_override);
        run_request.lifecycle = Some(LifecycleExecutionContext {
            transaction_id: request_id.to_string(),
            commander_session_id: commander_session_id.clone(),
            task_id: task_id.clone(),
            goal_id: goal_id.clone(),
            operator_override,
        });
        let durable_lifecycle = RuntimeLifecycleIdentity {
            commander_session_id: commander_session_id.clone(),
            transaction_id: request_id.to_string(),
            task_id: task_id.clone(),
            goal_id: goal_id.clone(),
            operator_override,
            dispatch_runtime_id: request.runtime_id.clone(),
            dispatch_lease_id: lease_id.clone(),
            receipt_event_seq: 0,
        };
        let maximum_parallel_runtime_workers =
            runtime_worker_limit(run_request.maximum_parallel_runtime_workers);
        if debug_runtime_enabled() {
            eprintln!(
                "router debug: enqueue_turn start session_id={} runtime_id={}",
                request.session_id, request.runtime_id
            );
        }
        {
            let mut sessions = self.sessions.lock();
            if let Some(active) = sessions.get(&request.session_id) {
                return Ok(active_turn_conflict(&request.session_id, active));
            }
            let queued = sessions
                .values()
                .filter(|lease| !lease.slot_acquired && !lease.terminalizing)
                .count();
            if queued >= MAX_QUEUED_RUNTIME_TURNS {
                return Err(anyhow!(
                    "runtime turn queue is full ({queued}/{MAX_QUEUED_RUNTIME_TURNS})"
                ));
            }
            sessions.insert(
                request.session_id.clone(),
                RuntimeLease {
                    runtime_id: request.runtime_id.clone(),
                    lease_id: lease_id.clone(),
                    commander_session_id,
                    transaction_id: request_id.to_string(),
                    task_id,
                    goal_id,
                    operator_override,
                    receipt_event_seq: 0,
                    slot_acquired: false,
                    terminalizing: false,
                },
            );
        }
        let active_guard = ActiveSessionGuard::new(
            Arc::clone(&self.sessions),
            &request.session_id,
            &request.runtime_id,
        );
        let permit = self
            .acquire_runtime_slot(&request.session_id, maximum_parallel_runtime_workers)
            .await?;
        if !self.mark_slot_acquired(&request.session_id, &request.runtime_id) {
            return Err(anyhow!(
                "session {} was cancelled before runtime dispatch",
                request.session_id
            ));
        }
        register_and_activate_runtime(
            &request.session_id,
            &request.runtime_id,
            &lease_id,
            fallback_from_id,
            Some(durable_lifecycle),
        )?;
        if debug_runtime_enabled() {
            eprintln!(
                "router debug: enqueue_turn dispatch session_id={}",
                request.session_id
            );
        }
        let (status, body) =
            dispatch_run_agent_with_runtime_slot(state, run_request, request_id.to_string()).await;
        let delivery = match self.ensure_terminal_receipt(
            &request.session_id,
            &request.runtime_id,
            request_id,
        ) {
            Ok(delivery) => delivery,
            Err(error) => {
                let missing_terminal_feed = error
                    .to_string()
                    .starts_with("TERMINAL_FEED_EVENT_NOT_FOUND:");
                if missing_terminal_feed {
                    match self
                        .terminalize_registered_runtime(
                            state,
                            &request.session_id,
                            &request.runtime_id,
                        )
                        .await
                    {
                        Ok(()) => match self.ensure_terminal_receipt(
                            &request.session_id,
                            &request.runtime_id,
                            request_id,
                        ) {
                            Ok(delivery) => delivery,
                            Err(receipt_error) => {
                                match self.retain_runtime_slot_if_current(
                                    &request.session_id,
                                    &request.runtime_id,
                                    permit,
                                ) {
                                    Ok(()) => active_guard.retain(),
                                    Err(permit) => {
                                        active_guard.finish();
                                        drop(permit);
                                    }
                                }
                                return Err(anyhow!(
                                    "TERMINAL_RECEIPT_NOT_DURABLE:{error:#}:RUNTIME_TERMINALIZED_BUT_RECEIPT_NOT_DURABLE:{receipt_error:#}"
                                ));
                            }
                        },
                        Err(terminalization_error) => {
                            match self.retain_runtime_slot_if_current(
                                &request.session_id,
                                &request.runtime_id,
                                permit,
                            ) {
                                Ok(()) => active_guard.retain(),
                                Err(permit) => {
                                    active_guard.finish();
                                    drop(permit);
                                }
                            }
                            return Err(anyhow!(
                                "TERMINAL_RECEIPT_NOT_DURABLE:{error:#}:AUTO_TERMINALIZATION_BLOCKED:{terminalization_error:#}"
                            ));
                        }
                    }
                } else {
                    match self.retain_runtime_slot_if_current(
                        &request.session_id,
                        &request.runtime_id,
                        permit,
                    ) {
                        Ok(()) => active_guard.retain(),
                        Err(permit) => {
                            active_guard.finish();
                            drop(permit);
                        }
                    }
                    return Err(anyhow!("TERMINAL_RECEIPT_NOT_DURABLE:{error:#}"));
                }
            }
        };
        state
            .command_run
            .wait_for_session_idle(&request.session_id)
            .await;
        let evidence = self.live_effect_evidence(state, &request.session_id).await;
        match lifecycle_store(&delivery.commander_session_id)?.reclaim_terminal_slot(
            &delivery.transaction_id,
            &delivery.event_id,
            evidence,
        )? {
            ReclaimOutcome::Released | ReclaimOutcome::AlreadyReleased => {
                active_guard.finish();
                drop(permit);
            }
            ReclaimOutcome::Retained { blocker } => {
                if let Err(permit) = self.retain_runtime_slot_if_current(
                    &request.session_id,
                    &request.runtime_id,
                    permit,
                ) {
                    active_guard.finish();
                    drop(permit);
                    return Err(anyhow!(
                        "RUNTIME_CANCELLED_BEFORE_SLOT_RETAIN:session={},runtime={}",
                        request.session_id,
                        request.runtime_id
                    ));
                }
                self.spawn_retained_reclaimer(
                    state.clone(),
                    request.session_id.clone(),
                    delivery.clone(),
                );
                self.wait_for_retained_release(&request.session_id).await;
                if self.retained_slots.lock().contains_key(&request.session_id) {
                    active_guard.retain();
                    return Err(anyhow!(blocker));
                }
                active_guard.finish();
            }
        }
        if debug_runtime_enabled() {
            eprintln!(
                "router debug: enqueue_turn finished session_id={} status={} body={}",
                request.session_id, status, body
            );
        }
        if status >= 400 {
            return Err(anyhow!(
                "{}",
                body.pointer("/result/error")
                    .or_else(|| body.get("error"))
                    .and_then(Value::as_str)
                    .unwrap_or("runtime worker failed")
            ));
        }
        Ok(json!({
            "status": "finished",
            "runtime_id": request.runtime_id,
            "session_id": request.session_id,
            "result": body
        }))
    }

    pub async fn command_run_request(
        &self,
        state: &AppState,
        input: Value,
        request_id: &str,
    ) -> Result<Value> {
        let nested_session = input
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let nested_reservation = {
            let sessions = self.sessions.lock();
            nested_session.and_then(|session_id| {
                sessions
                    .contains_key(session_id)
                    .then(|| state.command_run.reserve_for_session(Some(session_id)))
            })
        };
        if let Some(reservation) = nested_reservation {
            return state
                .command_run
                .execute_with_reserved_session(input, Some(request_id), reservation)
                .await;
        }

        let _admission = self.admission.read().await;
        state
            .command_run
            .execute_with_request_id(input, Some(request_id))
            .await
    }

    pub async fn get_runtime_lease(&self, state: &AppState, input: Value) -> Result<Value> {
        let request: GetRuntimeLeaseRequest = serde_json::from_value(input)?;
        state.session_db.start()?;
        match session_log_contract::client::call_service(&SessionLogCommand::GetRuntimeLease(
            request,
        ))? {
            SessionLogResponse::RuntimeLeaseRead { runtime } => Ok(json!({
                "status": "ok",
                "runtime": runtime,
            })),
            SessionLogResponse::Error { error } => Err(anyhow!(error)),
            other => Err(anyhow!("unexpected get_runtime_lease response: {other:?}")),
        }
    }

    pub(crate) fn reconcile_durable_terminal_callback(
        &self,
        snapshot: &RuntimeLeaseSnapshot,
    ) -> Result<Option<TerminalDeliveryIdentity>> {
        if !snapshot.terminal || snapshot.lease_active {
            return Err(anyhow!(
                "RUNTIME_CALLBACK_RECONCILIATION_REQUIRES_CLOSED_RUNTIME:runtime={},terminal={},lease_active={}",
                snapshot.runtime_id,
                snapshot.terminal,
                snapshot.lease_active
            ));
        }
        let lease = runtime_lease_from_snapshot(snapshot)?;
        let entry = terminal_feed_entry_for_runtime(&snapshot.session_id, &snapshot.runtime_id)?;
        let SessionFeedEvent::SessionProjectionUpdated { projection, .. } = &entry.event else {
            return Err(anyhow!(
                "RUNTIME_CALLBACK_TERMINAL_PROJECTION_MISSING:{}",
                snapshot.runtime_id
            ));
        };
        if !terminal_runtime_is_current(
            projection,
            &snapshot.session_id,
            &lease.runtime_id,
            &snapshot.runtime_id,
        )? {
            return Ok(None);
        }
        let expected_terminal_state = runtime_terminal_state_from_snapshot(snapshot, projection)?;
        self.write_snapshot_terminal_receipt(&lease, snapshot, &entry, expected_terminal_state)?;
        let store = lifecycle_store(&lease.commander_session_id)?;
        let delivery = intake_terminal_receipt(
            &store,
            &entry,
            &snapshot.runtime_id,
            &lease.transaction_id,
            &lease,
            expected_terminal_state,
        )?;
        if let Some(delivery) = delivery.as_ref() {
            match store.reclaim_terminal_slot(
                &delivery.transaction_id,
                &delivery.event_id,
                LiveEffectEvidence::default(),
            )? {
                ReclaimOutcome::Released | ReclaimOutcome::AlreadyReleased => {}
                ReclaimOutcome::Retained { blocker } => return Err(anyhow!(blocker)),
            }
        }
        Ok(delivery)
    }

    pub async fn recovery_close_runtime(&self, state: &AppState, input: Value) -> Result<Value> {
        let request: RouterRecoveryCloseRuntimeRequest = serde_json::from_value(input)?;
        let _admission = self.admission.write().await;

        let lease = self.sessions.lock().get(&request.session_id).cloned();
        let queued_turn = lease
            .as_ref()
            .is_some_and(|lease| !lease.slot_acquired && !lease.terminalizing);
        let running_turn = lease
            .as_ref()
            .is_some_and(|lease| lease.slot_acquired && !lease.terminalizing);
        let active_turn = lease.as_ref().is_some_and(|lease| !lease.terminalizing);
        let worker_alive = state
            .manager
            .worker_alive_by_key(&format!("runtime_worker:{}", request.session_id))
            .await;
        let retained_process_scopes =
            code_tools::shell_executor::retained_shell_process_scope_count_for_scope(
                &request.session_id,
            );
        let retained_slot = self.retained_slots.lock().contains_key(&request.session_id);
        let proof = RuntimeRecoveryQuiescenceProof {
            active_turn,
            queued_turn,
            running_turn,
            worker_alive,
            active_command_runs: state
                .command_run
                .active_count_for_session(&request.session_id)
                as u64,
            retained_process_scopes: retained_process_scopes as u64,
            retained_slot,
            global_active_session_count: self.sessions.lock().len() as u64,
            global_retained_slot_count: self.retained_slots.lock().len() as u64,
            global_active_command_runs: state.command_run.active_count() as u64,
        };
        if !proof.is_quiescent() {
            return Ok(json!({
                "status": "ok",
                "result": session_log_contract::RecoveryCloseRuntimeOutcome::RuntimeLive {
                    proof,
                },
            }));
        }

        state.session_db.start()?;
        let recovery = RecoveryCloseRuntimeRequest {
            receipt_id: request.receipt_id,
            database_path: request.database_path,
            runtime_id: request.runtime_id,
            session_id: request.session_id,
            lease_id: request.lease_id,
            expected_lease_active: request.expected_lease_active,
            expected_revision: request.expected_revision,
            expected_last_event_seq: request.expected_last_event_seq,
            expected_session_event_seq: request.expected_session_event_seq,
            expected_session_state: request.expected_session_state,
            reason: request.reason,
            quiescence: proof,
        };
        match session_log_contract::client::call_service(&SessionLogCommand::RecoveryCloseRuntime(
            recovery,
        ))? {
            SessionLogResponse::RuntimeRecoveryClosed { result } => Ok(json!({
                "status": "ok",
                "result": result,
            })),
            SessionLogResponse::Error { error } => Err(anyhow!(error)),
            other => Err(anyhow!(
                "unexpected recovery_close_runtime response: {other:?}"
            )),
        }
    }

    pub async fn cancel_turn(&self, state: &AppState, input: Value) -> Value {
        let request = match serde_json::from_value::<CancelRuntimeRequest>(input) {
            Ok(request)
                if !request.session_id.trim().is_empty()
                    && !request.runtime_id.trim().is_empty() =>
            {
                request
            }
            Ok(_) => {
                return json!({
                    "status": "error",
                    "error": "session_id and runtime_id must be non-empty",
                    "stopped_worker": false,
                });
            }
            Err(error) => {
                return json!({
                    "status": "error",
                    "error": format!("invalid cancel runtime request: {error}"),
                    "stopped_worker": false,
                });
            }
        };
        let session_id = request.session_id;
        let runtime_id = request.runtime_id;
        let lease = self
            .sessions
            .lock()
            .get(&session_id)
            .filter(|lease| lease.runtime_id == runtime_id)
            .cloned();
        let Some(lease) = lease else {
            return json!({
                "status": "idle",
                "session_id": session_id,
                "runtime_id": runtime_id,
                "stopped_worker": false,
                "active_command_runs_cancelled": 0,
            });
        };
        if let Err(error) = self.mark_terminalizing(&session_id, &runtime_id) {
            return json!({
                "status": "error",
                "session_id": session_id,
                "runtime_id": runtime_id,
                "stopped_worker": false,
                "active_command_runs_cancelled": 0,
                "runtime_terminalized": false,
                "terminalization_pending": false,
                "terminalization_error": error.to_string(),
            });
        }
        let stopped_worker = state
            .manager
            .stop_worker_by_key(&format!("runtime_worker:{session_id}"))
            .await;
        let active_command_runs_cancelled = state.command_run.cancel_session(&session_id);
        let command_runs_drained = tokio::time::timeout(
            Duration::from_secs(10),
            state.command_run.wait_for_session_idle(&session_id),
        )
        .await
        .is_ok();
        if !command_runs_drained {
            return json!({
                "status": "error",
                "error": "TURA_SESSION_ACTIVE_COMMAND_CANCELLATION_DID_NOT_DRAIN",
                "session_id": session_id,
                "runtime_id": runtime_id,
                "stopped_worker": stopped_worker,
                "active_command_runs_cancelled": active_command_runs_cancelled,
                "active_command_runs_remaining": state.command_run.active_count_for_session(&session_id),
            });
        }
        let retained_process_scopes_terminated =
            code_tools::shell_executor::terminate_retained_shell_process_scopes_for_scope(
                &session_id,
            );
        self.retained_slots.lock().remove(&session_id);
        if let Some(notify) = self.retained_watchers.lock().remove(&session_id) {
            notify.notify_one();
        }
        let terminalization = self
            .terminalize_cancelled_runtime(state, &session_id, &runtime_id, &lease)
            .await;
        let terminalization_error = terminalization.as_ref().err().map(ToString::to_string);
        let runtime_terminalized = terminalization.is_ok();
        json!({
            "status": if runtime_terminalized { "cancelled" } else { "error" },
            "session_id": session_id,
            "runtime_id": runtime_id,
            "stopped_worker": stopped_worker,
            "active_command_runs_cancelled": active_command_runs_cancelled,
            "active_command_runs_remaining": state.command_run.active_count_for_session(&session_id),
            "retained_process_scopes_terminated": retained_process_scopes_terminated,
            "runtime_terminalized": runtime_terminalized,
            "terminalization_pending": !runtime_terminalized,
            "active_turn_removed": false,
            "terminalization_error": terminalization_error
        })
    }

    pub async fn kill_session_workers(&self, state: &AppState, input: Value) -> Value {
        let session_id = input
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let Some(session_id) = session_id else {
            let stopped = state
                .manager
                .stop_workers_with_prefix("runtime_worker:")
                .await;
            let leases = self.sessions.lock().clone();
            let mut terminalized_count = 0;
            let mut terminalization_failures = Vec::new();
            for (session_id, lease) in leases {
                state.command_run.cancel_session(&session_id);
                let drained = tokio::time::timeout(
                    Duration::from_secs(10),
                    state.command_run.wait_for_session_idle(&session_id),
                )
                .await
                .is_ok();
                code_tools::shell_executor::terminate_retained_shell_process_scopes_for_scope(
                    &session_id,
                );
                self.retained_slots.lock().remove(&session_id);
                if let Some(notify) = self.retained_watchers.lock().remove(&session_id) {
                    notify.notify_one();
                }
                let terminalization = if drained {
                    self.terminalize_cancelled_runtime(
                        state,
                        &session_id,
                        &lease.runtime_id,
                        &lease,
                    )
                    .await
                } else {
                    Err(anyhow!(
                        "RUNTIME_TERMINALIZATION_COMMAND_RUN_DID_NOT_DRAIN:session={session_id},runtime={}",
                        lease.runtime_id
                    ))
                };
                match terminalization {
                    Ok(()) => terminalized_count += 1,
                    Err(error) => terminalization_failures.push(json!({
                        "session_id": session_id,
                        "runtime_id": lease.runtime_id,
                        "error": error.to_string()
                    })),
                }
            }
            return json!({
                "status": if terminalization_failures.is_empty() { "stopped" } else { "error" },
                "stopped": stopped,
                "stopped_worker": stopped > 0,
                "active_turns_removed": 0,
                "terminalized_count": terminalized_count,
                "terminalizing_count": 0,
                "terminalization_failures": terminalization_failures
            });
        };

        let lease = self.sessions.lock().get(&session_id).cloned();
        let stopped_worker = state
            .manager
            .stop_worker_by_key(&format!("runtime_worker:{session_id}"))
            .await;
        let active_command_runs_cancelled = state.command_run.cancel_session(&session_id);
        let command_runs_drained = tokio::time::timeout(
            Duration::from_secs(10),
            state.command_run.wait_for_session_idle(&session_id),
        )
        .await
        .is_ok();
        let retained_process_scopes_terminated =
            code_tools::shell_executor::terminate_retained_shell_process_scopes_for_scope(
                &session_id,
            );
        self.retained_slots.lock().remove(&session_id);
        if let Some(notify) = self.retained_watchers.lock().remove(&session_id) {
            notify.notify_one();
        }
        let terminalization = match lease.as_ref() {
            Some(lease) if command_runs_drained => {
                self.terminalize_cancelled_runtime(state, &session_id, &lease.runtime_id, lease)
                    .await
            }
            Some(lease) => Err(anyhow!(
                "RUNTIME_TERMINALIZATION_COMMAND_RUN_DID_NOT_DRAIN:session={session_id},runtime={}",
                lease.runtime_id
            )),
            None => Ok(()),
        };
        let terminalization_error = terminalization.as_ref().err().map(ToString::to_string);
        let runtime_terminalized =
            lease.is_none() || (command_runs_drained && terminalization.is_ok());
        json!({
            "status": if runtime_terminalized { "stopped" } else { "error" },
            "session_id": session_id,
            "stopped": usize::from(stopped_worker),
            "stopped_worker": stopped_worker,
            "active_turn_removed": false,
            "active_command_runs_cancelled": active_command_runs_cancelled,
            "active_command_runs_remaining": state.command_run.active_count_for_session(&session_id),
            "retained_process_scopes_terminated": retained_process_scopes_terminated,
            "runtime_terminalized": runtime_terminalized,
            "terminalization_pending": false,
            "terminalization_error": terminalization_error
        })
    }

    pub async fn probe_sessions(&self, state: &AppState, input: Value) -> Result<Value> {
        let request: ProbeSessionsRequest = serde_json::from_value(input)?;
        let states = self.sessions.lock().clone();
        let mut sessions = Vec::new();
        for session_id in request
            .session_ids
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            let lease = states.get(&session_id);
            let queued_turn =
                lease.is_some_and(|lease| !lease.slot_acquired && !lease.terminalizing);
            let running_turn =
                lease.is_some_and(|lease| lease.slot_acquired && !lease.terminalizing);
            let active_turn = lease.is_some_and(|lease| !lease.terminalizing);
            let worker_alive = state
                .manager
                .worker_alive_by_key(&format!("runtime_worker:{session_id}"))
                .await;
            let status = if lease.is_some_and(|lease| lease.terminalizing) {
                "terminalizing"
            } else if queued_turn {
                "queued"
            } else if running_turn || worker_alive {
                "running"
            } else {
                "inactive"
            };
            sessions.push(json!({
                "session_id": session_id,
                "runtime_id": lease.map(|lease| lease.runtime_id.clone()),
                "active_turn": active_turn,
                "queued_turn": queued_turn,
                "running_turn": running_turn,
                "worker_alive": worker_alive,
                "status": status
            }));
        }
        Ok(json!({ "sessions": sessions }))
    }

    pub async fn status(&self, state: &AppState) -> Value {
        let leases = self.sessions.lock().clone();
        let retained = self
            .retained_slots
            .lock()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut sessions = Vec::with_capacity(leases.len());
        for (session_id, lease) in leases {
            let worker_alive = state
                .manager
                .worker_alive_by_key(&format!("runtime_worker:{session_id}"))
                .await;
            sessions.push(json!({
                "session_id": session_id,
                "runtime_id": lease.runtime_id,
                "transaction_id": lease.transaction_id,
                "slot_acquired": lease.slot_acquired,
                "terminalizing": lease.terminalizing,
                "worker_alive": worker_alive,
                "active_command_runs": state.command_run.active_count_for_session(&session_id),
                "retained_process_scopes": code_tools::shell_executor::retained_shell_process_scope_count_for_scope(&session_id),
                "retained_slot": retained.iter().any(|value| value == &session_id)
            }));
        }
        json!({
            "status": "ok",
            "active_session_count": sessions.len(),
            "retained_slot_count": retained.len(),
            "active_command_runs": state.command_run.active_count(),
            "sessions": sessions
        })
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions.lock().len()
    }

    pub(crate) fn intake_terminal_feed_entry(
        &self,
        entry: &SessionFeedEntry,
        transaction_id: &str,
    ) -> Result<Option<TerminalDeliveryIdentity>> {
        let SessionFeedEvent::SessionProjectionUpdated { projection, .. } = &entry.event else {
            return Ok(None);
        };
        if !projection.state.is_terminal() {
            return Ok(None);
        }
        let Some(runtime_id) = entry
            .runtime_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            // Session commands also publish terminal projections. They carry no
            // runtime identity and therefore cannot own a terminal receipt.
            return Ok(None);
        };
        let lease = self
            .sessions
            .lock()
            .get(&entry.session_id)
            .cloned()
            .ok_or_else(|| anyhow!("TERMINAL_FEED_LEASE_NOT_FOUND:{}", entry.session_id))?;
        if lease.transaction_id != transaction_id {
            return Err(anyhow!(
                "TERMINAL_FEED_IDENTITY_MISMATCH:session={},runtime={},transaction={}",
                entry.session_id,
                runtime_id,
                transaction_id
            ));
        }
        if !terminal_runtime_is_current(
            projection,
            &entry.session_id,
            &lease.runtime_id,
            runtime_id,
        )? {
            return Ok(None);
        }
        let snapshot = read_runtime_lease_snapshot(runtime_id)?;
        let expected_terminal_state = runtime_terminal_state_from_snapshot(&snapshot, projection)?;
        let store = lifecycle_store(&lease.commander_session_id)?;
        intake_terminal_receipt(
            &store,
            entry,
            runtime_id,
            transaction_id,
            &lease,
            expected_terminal_state,
        )
    }

    pub(crate) fn acknowledge_terminal_delivery(
        &self,
        delivery: &TerminalDeliveryIdentity,
    ) -> Result<()> {
        lifecycle_store(&delivery.commander_session_id)?.acknowledge(
            &delivery.transaction_id,
            &delivery.event_id,
            &delivery.transaction_id,
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_session_lease_for_test(&self, session_id: &str, slot_acquired: bool) {
        self.sessions.lock().insert(
            session_id.to_string(),
            RuntimeLease {
                runtime_id: format!("runtime-{session_id}"),
                lease_id: format!("lease-{session_id}"),
                commander_session_id: session_id.to_string(),
                transaction_id: format!("transaction-{session_id}"),
                task_id: None,
                goal_id: None,
                operator_override: false,
                receipt_event_seq: 0,
                slot_acquired,
                terminalizing: false,
            },
        );
    }
    async fn acquire_runtime_slot(
        &self,
        session_id: &str,
        maximum_parallel_runtime_workers: usize,
    ) -> Result<RuntimeSlotPermit> {
        if debug_runtime_enabled() {
            eprintln!(
                "router debug: enqueue_turn waiting for runtime slot session_id={session_id} limit={maximum_parallel_runtime_workers}"
            );
        }
        let permit = self
            .runtime_slots
            .acquire(maximum_parallel_runtime_workers)
            .await;
        if debug_runtime_enabled() {
            eprintln!("router debug: enqueue_turn acquired runtime slot session_id={session_id}");
        }
        Ok(permit)
    }

    fn mark_slot_acquired(&self, session_id: &str, runtime_id: &str) -> bool {
        let mut sessions = self.sessions.lock();
        let Some(lease) = sessions.get_mut(session_id) else {
            return false;
        };
        if lease.runtime_id != runtime_id {
            return false;
        }
        lease.slot_acquired = true;
        true
    }

    fn mark_terminalizing(&self, session_id: &str, runtime_id: &str) -> Result<RuntimeLease> {
        let mut sessions = self.sessions.lock();
        let lease = sessions
            .get_mut(session_id)
            .ok_or_else(|| anyhow!("RUNTIME_TERMINALIZATION_LEASE_NOT_FOUND:{session_id}"))?;
        if lease.runtime_id != runtime_id {
            return Err(anyhow!(
                "RUNTIME_TERMINALIZATION_IDENTITY_MISMATCH:session={session_id},expected_runtime={runtime_id},current_runtime={}",
                lease.runtime_id
            ));
        }
        lease.terminalizing = true;
        Ok(lease.clone())
    }

    async fn terminalization_quiescence_proof(
        &self,
        state: &AppState,
        session_id: &str,
        runtime_id: &str,
    ) -> Result<RuntimeRecoveryQuiescenceProof> {
        let terminalizing = self
            .sessions
            .lock()
            .get(session_id)
            .is_some_and(|lease| lease.runtime_id == runtime_id && lease.terminalizing);
        if !terminalizing {
            return Err(anyhow!(
                "RUNTIME_TERMINALIZATION_PHASE_NOT_OWNED:session={session_id},runtime={runtime_id}"
            ));
        }
        let worker_alive = state
            .manager
            .worker_alive_by_key(&format!("runtime_worker:{session_id}"))
            .await;
        let retained_process_scopes =
            code_tools::shell_executor::retained_shell_process_scope_count_for_scope(session_id);
        let retained_slot = self.retained_slots.lock().contains_key(session_id);
        let global_active_session_count = self.sessions.lock().len() as u64;
        let global_retained_slot_count = self.retained_slots.lock().len() as u64;
        let global_active_command_runs = state.command_run.active_count() as u64;
        let proof = RuntimeRecoveryQuiescenceProof {
            active_turn: false,
            queued_turn: false,
            running_turn: false,
            worker_alive,
            active_command_runs: state.command_run.active_count_for_session(session_id) as u64,
            retained_process_scopes: retained_process_scopes as u64,
            retained_slot,
            global_active_session_count,
            global_retained_slot_count,
            global_active_command_runs,
        };
        if !proof.is_quiescent() {
            return Err(anyhow!(
                "RUNTIME_TERMINALIZATION_NOT_QUIESCENT:{}",
                serde_json::to_string(&proof)?
            ));
        }
        Ok(proof)
    }

    async fn terminalize_registered_runtime(
        &self,
        state: &AppState,
        session_id: &str,
        runtime_id: &str,
    ) -> Result<()> {
        let lease = self.mark_terminalizing(session_id, runtime_id)?;
        tokio::time::timeout(
            Duration::from_secs(10),
            state.command_run.wait_for_session_idle(session_id),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "RUNTIME_TERMINALIZATION_COMMAND_RUN_DID_NOT_DRAIN:session={session_id},runtime={runtime_id}"
            )
        })?;
        let proof = self
            .terminalization_quiescence_proof(state, session_id, runtime_id)
            .await?;
        state.session_db.start()?;
        let snapshot = match session_log_contract::client::call_service(
            &SessionLogCommand::GetRuntimeLease(GetRuntimeLeaseRequest {
                runtime_id: runtime_id.to_string(),
                database_path: None,
            }),
        )? {
            SessionLogResponse::RuntimeLeaseRead {
                runtime: Some(runtime),
            } => runtime,
            SessionLogResponse::RuntimeLeaseRead { runtime: None } => {
                return Err(anyhow!(
                    "RUNTIME_TERMINALIZATION_DURABLE_LEASE_NOT_FOUND:{runtime_id}"
                ));
            }
            SessionLogResponse::Error { error } => return Err(anyhow!(error)),
            other => {
                return Err(anyhow!(
                    "RUNTIME_TERMINALIZATION_LEASE_READ_UNEXPECTED:{other:?}"
                ));
            }
        };
        validate_terminalization_identity(&snapshot, &lease, session_id, runtime_id)?;
        if snapshot.terminal {
            if snapshot.lease_active {
                return Err(anyhow!(
                    "RUNTIME_TERMINALIZATION_DURABLE_STATE_CONFLICT:runtime={runtime_id},terminal=true,lease_active=true"
                ));
            }
            return Ok(());
        }
        if !snapshot.lease_active {
            return Err(anyhow!(
                "RUNTIME_TERMINALIZATION_DURABLE_STATE_CONFLICT:runtime={runtime_id},terminal=false,lease_active=false"
            ));
        }
        let reason = if snapshot.revision == 0 && snapshot.last_event_seq == 0 {
            RecoveryCloseRuntimeReason::UnbornRuntime
        } else {
            RecoveryCloseRuntimeReason::OrphanedRuntime
        };
        let recovery = RecoveryCloseRuntimeRequest {
            receipt_id: format!(
                "router-auto-terminalize:{}:{}",
                lease.transaction_id, runtime_id
            ),
            database_path: snapshot.database_path.clone(),
            runtime_id: runtime_id.to_string(),
            session_id: session_id.to_string(),
            lease_id: snapshot.lease_id.clone(),
            expected_lease_active: snapshot.lease_active,
            expected_revision: snapshot.revision,
            expected_last_event_seq: snapshot.last_event_seq,
            expected_session_event_seq: snapshot.session_event_seq,
            expected_session_state: snapshot.session_state,
            reason,
            quiescence: proof,
        };
        let result = match session_log_contract::client::call_service(
            &SessionLogCommand::RecoveryCloseRuntime(recovery),
        )? {
            SessionLogResponse::RuntimeRecoveryClosed { result } => result,
            SessionLogResponse::Error { error } => return Err(anyhow!(error)),
            other => {
                return Err(anyhow!(
                    "RUNTIME_TERMINALIZATION_RECOVERY_UNEXPECTED:{other:?}"
                ));
            }
        };
        match result {
            RecoveryCloseRuntimeOutcome::Closed { receipt }
            | RecoveryCloseRuntimeOutcome::AlreadyClosed { receipt }
                if receipt.runtime_id == runtime_id
                    && receipt.session_id == session_id
                    && receipt.lease_id == snapshot.lease_id
                    && receipt.terminal
                    && !receipt.lease_active =>
            {
                self.write_recovery_terminal_receipt(&lease, &receipt)?;
                Ok(())
            }
            other => Err(anyhow!(
                "RUNTIME_TERMINALIZATION_RECOVERY_REJECTED:{other:?}"
            )),
        }
    }

    async fn terminalize_cancelled_runtime(
        &self,
        state: &AppState,
        session_id: &str,
        runtime_id: &str,
        lease: &RuntimeLease,
    ) -> Result<()> {
        let terminalization = self
            .terminalize_registered_runtime(state, session_id, runtime_id)
            .await;
        if let Err(error) = terminalization {
            if !error
                .to_string()
                .starts_with("RUNTIME_TERMINALIZATION_LEASE_NOT_FOUND:")
            {
                return Err(error);
            }
            self.confirm_runtime_durably_closed(state, session_id, runtime_id, lease)
                .map_err(|readback_error| {
                    anyhow!(
                        "RUNTIME_CANCEL_TERMINALIZATION_FAILED:{error:#}:DURABLE_READBACK_FAILED:{readback_error:#}"
                    )
                })?;
        }
        Ok(())
    }

    fn write_recovery_terminal_receipt(
        &self,
        lease: &RuntimeLease,
        recovery: &RuntimeRecoveryReceipt,
    ) -> Result<()> {
        let receipt_lease_id = recovery.lease_id.as_deref().ok_or_else(|| {
            anyhow!(
                "RUNTIME_RECOVERY_TERMINAL_RECEIPT_LEASE_MISSING:{}",
                recovery.runtime_id
            )
        })?;
        let event_id = recovery_terminal_projection_event_id(&recovery.receipt_id);
        let mut receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                &lease.transaction_id,
                event_id,
                lease.receipt_event_seq,
                &lease.commander_session_id,
                &recovery.session_id,
                &recovery.runtime_id,
                receipt_lease_id,
            ),
            recovery_terminal_state(recovery),
            recovery.closed_at,
        );
        receipt.task_id.clone_from(&lease.task_id);
        receipt.goal_id.clone_from(&lease.goal_id);
        receipt.operator_override = lease.operator_override;
        receipt.audit_metadata.insert(
            "runtime_event_seq".to_string(),
            json!(recovery.last_event_seq),
        );
        receipt.audit_metadata.insert(
            "runtime_expected_revision".to_string(),
            json!(recovery.revision.saturating_sub(1)),
        );
        receipt.audit_metadata.insert(
            "runtime_state".to_string(),
            recovery_runtime_state(recovery),
        );
        receipt.audit_metadata.insert(
            "session_state".to_string(),
            json!(recovery.session_state),
        );
        receipt.audit_metadata.insert(
            "dispatch_runtime_id".to_string(),
            json!(lease.runtime_id),
        );
        receipt.audit_metadata.insert(
            "dispatch_lease_id".to_string(),
            json!(lease.lease_id),
        );
        lifecycle_store(&lease.commander_session_id)?
            .write_terminal_receipt(&receipt)
            .map_err(|error| anyhow!(error.to_string()))?;
        Ok(())
    }

    fn write_snapshot_terminal_receipt(
        &self,
        lease: &RuntimeLease,
        snapshot: &RuntimeLeaseSnapshot,
        entry: &SessionFeedEntry,
        terminal_state: TerminalState,
    ) -> Result<()> {
        let store = lifecycle_store(&lease.commander_session_id)?;
        Self::ensure_snapshot_terminal_receipt(
            &store,
            lease,
            snapshot,
            entry,
            terminal_state,
        )
    }

    fn ensure_snapshot_terminal_receipt(
        store: &SessionLifecycleStore,
        lease: &RuntimeLease,
        snapshot: &RuntimeLeaseSnapshot,
        entry: &SessionFeedEntry,
        terminal_state: TerminalState,
    ) -> Result<()> {
        match store.terminal_receipt(&lease.transaction_id, &entry.event_id) {
            Ok(_) => return Ok(()),
            Err(error) if error.code == "TERMINAL_RECEIPT_NOT_FOUND" => {}
            Err(error) => return Err(anyhow!(error.to_string())),
        }
        let receipt = Self::snapshot_terminal_receipt(lease, snapshot, entry, terminal_state)?;
        store
            .write_terminal_receipt(&receipt)
            .map_err(|error| anyhow!(error.to_string()))?;
        Ok(())
    }

    fn snapshot_terminal_receipt(
        lease: &RuntimeLease,
        snapshot: &RuntimeLeaseSnapshot,
        entry: &SessionFeedEntry,
        terminal_state: TerminalState,
    ) -> Result<TerminalReceipt> {
        let receipt_lease_id = snapshot.lease_id.as_deref().ok_or_else(|| {
            anyhow!(
                "RUNTIME_SNAPSHOT_TERMINAL_RECEIPT_LEASE_MISSING:{}",
                snapshot.runtime_id
            )
        })?;
        let finished_at_ms = match &entry.event {
            SessionFeedEvent::SessionProjectionUpdated { updated_at, .. } => *updated_at,
            _ => {
                return Err(anyhow!(
                    "RUNTIME_CALLBACK_TERMINAL_PROJECTION_MISSING:{}",
                    snapshot.runtime_id
                ));
            }
        };
        let mut receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                &lease.transaction_id,
                &entry.event_id,
                lease.receipt_event_seq,
                &lease.commander_session_id,
                &snapshot.session_id,
                &snapshot.runtime_id,
                receipt_lease_id,
            ),
            terminal_state,
            finished_at_ms,
        );
        receipt.task_id.clone_from(&lease.task_id);
        receipt.goal_id.clone_from(&lease.goal_id);
        receipt.operator_override = lease.operator_override;
        receipt.audit_metadata.insert(
            "runtime_event_seq".to_string(),
            json!(snapshot.last_event_seq),
        );
        receipt.audit_metadata.insert(
            "runtime_expected_revision".to_string(),
            json!(snapshot.revision.saturating_sub(1)),
        );
        receipt.audit_metadata.insert(
            "runtime_state".to_string(),
            snapshot.runtime_state.map_or(Value::Null, |state| json!(state)),
        );
        receipt.audit_metadata.insert(
            "dispatch_runtime_id".to_string(),
            json!(lease.runtime_id),
        );
        receipt.audit_metadata.insert(
            "dispatch_lease_id".to_string(),
            json!(lease.lease_id),
        );
        Ok(receipt)
    }

    fn confirm_runtime_durably_closed(
        &self,
        state: &AppState,
        session_id: &str,
        runtime_id: &str,
        lease: &RuntimeLease,
    ) -> Result<()> {
        state.session_db.start()?;
        let snapshot = match session_log_contract::client::call_service(
            &SessionLogCommand::GetRuntimeLease(GetRuntimeLeaseRequest {
                runtime_id: runtime_id.to_string(),
                database_path: None,
            }),
        )? {
            SessionLogResponse::RuntimeLeaseRead {
                runtime: Some(runtime),
            } => runtime,
            SessionLogResponse::RuntimeLeaseRead { runtime: None } => {
                return Err(anyhow!(
                    "RUNTIME_CANCEL_DURABLE_LEASE_NOT_FOUND:{runtime_id}"
                ));
            }
            SessionLogResponse::Error { error } => return Err(anyhow!(error)),
            other => {
                return Err(anyhow!(
                    "RUNTIME_CANCEL_LEASE_READ_UNEXPECTED:{other:?}"
                ));
            }
        };
        validate_terminalization_identity(&snapshot, lease, session_id, runtime_id)?;
        if !snapshot.terminal || snapshot.lease_active {
            return Err(anyhow!(
                "RUNTIME_CANCEL_DURABLE_STATE_CONFLICT:runtime={runtime_id},terminal={},lease_active={}",
                snapshot.terminal,
                snapshot.lease_active
            ));
        }
        Ok(())
    }

    fn ensure_terminal_receipt(
        &self,
        session_id: &str,
        dispatch_runtime_id: &str,
        transaction_id: &str,
    ) -> Result<TerminalDeliveryIdentity> {
        let mut after_cursor = 0;
        let mut latest = None;
        loop {
            let response = session_log_contract::client::call_service(
                &SessionLogCommand::ReadSessionFeed(session_log_contract::ReadSessionFeedRequest {
                    session_id: session_id.to_string(),
                    after_cursor,
                    limit: 1_000,
                }),
            )?;
            let SessionLogResponse::SessionFeed {
                entries,
                next_cursor,
            } = response
            else {
                return Err(anyhow!("TERMINAL_FEED_READ_FAILED:{response:?}"));
            };
            for entry in &entries {
                if let Some(delivery) = self.intake_terminal_feed_entry(entry, transaction_id)? {
                    latest = Some(delivery);
                }
            }
            if next_cursor <= after_cursor {
                return latest.ok_or_else(|| {
                    anyhow!(
                        "TERMINAL_FEED_EVENT_NOT_FOUND:session={session_id},runtime={dispatch_runtime_id}"
                    )
                });
            }
            after_cursor = next_cursor;
        }
    }

    fn retain_runtime_slot_if_current(
        &self,
        session_id: &str,
        runtime_id: &str,
        permit: RuntimeSlotPermit,
    ) -> std::result::Result<(), RuntimeSlotPermit> {
        let sessions = self.sessions.lock();
        if !sessions
            .get(session_id)
            .is_some_and(|lease| lease.runtime_id == runtime_id)
        {
            return Err(permit);
        }
        self.retained_slots
            .lock()
            .insert(session_id.to_string(), permit);
        Ok(())
    }

    async fn live_effect_evidence(&self, state: &AppState, session_id: &str) -> LiveEffectEvidence {
        LiveEffectEvidence {
            runtime_worker_alive: state
                .manager
                .worker_alive_by_key(&format!("runtime_worker:{session_id}"))
                .await,
            active_tool_calls: 0,
            active_command_runs: state.command_run.active_count_for_session(session_id),
            live_effect_processes:
                code_tools::shell_executor::retained_shell_process_scope_count_for_scope(session_id),
            pending_init: false,
        }
    }

    fn spawn_retained_reclaimer(
        &self,
        state: AppState,
        session_id: String,
        delivery: TerminalDeliveryIdentity,
    ) {
        let notify = Arc::new(Notify::new());
        if self
            .retained_watchers
            .lock()
            .insert(session_id.clone(), Arc::clone(&notify))
            .is_some()
        {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if !service.retained_slots.lock().contains_key(&session_id) {
                    if let Some(notify) = service.retained_watchers.lock().remove(&session_id) {
                        notify.notify_one();
                    }
                    return;
                }
                let evidence = service.live_effect_evidence(&state, &session_id).await;
                let outcome = match lifecycle_store(&delivery.commander_session_id).and_then(
                    |store| {
                        store
                            .reclaim_terminal_slot(
                                &delivery.transaction_id,
                                &delivery.event_id,
                                evidence,
                            )
                            .map_err(|error| anyhow!(error.to_string()))
                    },
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        eprintln!(
                            "router retained execution reclaimer deferred session={session_id}: {error:#}"
                        );
                        continue;
                    }
                };
                match outcome {
                    ReclaimOutcome::Released | ReclaimOutcome::AlreadyReleased => {
                        service.retained_slots.lock().remove(&session_id);
                        service.retained_watchers.lock().remove(&session_id);
                        notify.notify_one();
                        return;
                    }
                    ReclaimOutcome::Retained { .. } => {}
                }
            }
        });
    }

    async fn wait_for_retained_release(&self, session_id: &str) {
        loop {
            let Some(notify) = self.retained_watchers.lock().get(session_id).cloned() else {
                return;
            };
            let notified = notify.notified();
            if !self.retained_slots.lock().contains_key(session_id) {
                return;
            }
            notified.await;
        }
    }
}

fn intake_terminal_receipt(
    store: &SessionLifecycleStore,
    entry: &SessionFeedEntry,
    runtime_id: &str,
    transaction_id: &str,
    lease: &RuntimeLease,
    expected_terminal_state: TerminalState,
) -> Result<Option<TerminalDeliveryIdentity>> {
    let receipt = store.terminal_receipt(transaction_id, &entry.event_id)?;
    if receipt.commander_session_id != lease.commander_session_id
        || receipt.child_session_id != entry.session_id
        || receipt.runtime_id != runtime_id
        || receipt.terminal_state != expected_terminal_state
        || receipt.task_id != lease.task_id
        || receipt.goal_id != lease.goal_id
        || receipt.operator_override != lease.operator_override
        || receipt
            .audit_metadata
            .get("dispatch_runtime_id")
            .and_then(Value::as_str)
            != Some(lease.runtime_id.as_str())
        || receipt
            .audit_metadata
            .get("dispatch_lease_id")
            .and_then(Value::as_str)
            != Some(lease.lease_id.as_str())
        || (runtime_id == lease.runtime_id && receipt.lease_id != lease.lease_id)
    {
        return Err(anyhow!(
            "TERMINAL_RECEIPT_DISPATCH_IDENTITY_MISMATCH:session={},runtime={},transaction={},event={}",
            entry.session_id,
            runtime_id,
            transaction_id,
            entry.event_id
        ));
    }
    match store.intake(transaction_id, &entry.event_id)? {
        IntakeOutcome::Applied { .. } | IntakeOutcome::Duplicate { .. } => {
            Ok(Some(TerminalDeliveryIdentity {
                commander_session_id: lease.commander_session_id.clone(),
                transaction_id: transaction_id.to_string(),
                event_id: entry.event_id.clone(),
                runtime_id: runtime_id.to_string(),
            }))
        }
        IntakeOutcome::Pending { blocker } => Err(anyhow!(blocker)),
    }
}

fn read_runtime_lease_snapshot(runtime_id: &str) -> Result<RuntimeLeaseSnapshot> {
    match session_log_contract::client::call_service(&SessionLogCommand::GetRuntimeLease(
        GetRuntimeLeaseRequest {
            runtime_id: runtime_id.to_string(),
            database_path: None,
        },
    ))? {
        SessionLogResponse::RuntimeLeaseRead {
            runtime: Some(snapshot),
        } => Ok(snapshot),
        SessionLogResponse::RuntimeLeaseRead { runtime: None } => Err(anyhow!(
            "RUNTIME_CALLBACK_DURABLE_LEASE_NOT_FOUND:{runtime_id}"
        )),
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => Err(anyhow!(
            "RUNTIME_CALLBACK_LEASE_READ_UNEXPECTED:{other:?}"
        )),
    }
}

fn runtime_lease_from_snapshot(snapshot: &RuntimeLeaseSnapshot) -> Result<RuntimeLease> {
    let lifecycle = snapshot.lifecycle.as_ref().ok_or_else(|| {
        anyhow!(
            "CRASH_RECOVERY_LACKS_DURABLE_LIFECYCLE_IDENTITY:{}",
            snapshot.runtime_id
        )
    })?;
    lifecycle
        .validate()
        .map_err(anyhow::Error::msg)
        .map_err(|error| {
            anyhow!(
                "RUNTIME_CALLBACK_DURABLE_LIFECYCLE_IDENTITY_INVALID:{}:{error}",
                snapshot.runtime_id
            )
        })?;
    let runtime_lease_id = snapshot.lease_id.as_deref().ok_or_else(|| {
        anyhow!(
            "RUNTIME_CALLBACK_DURABLE_LEASE_ID_MISSING:{}",
            snapshot.runtime_id
        )
    })?;
    if lifecycle.dispatch_runtime_id == snapshot.runtime_id
        && lifecycle.dispatch_lease_id != runtime_lease_id
    {
        return Err(anyhow!(
            "RUNTIME_CALLBACK_DISPATCH_LEASE_IDENTITY_MISMATCH:runtime={},durable_lease={},dispatch_lease={}",
            snapshot.runtime_id,
            runtime_lease_id,
            lifecycle.dispatch_lease_id
        ));
    }
    Ok(RuntimeLease {
        runtime_id: lifecycle.dispatch_runtime_id.clone(),
        lease_id: lifecycle.dispatch_lease_id.clone(),
        commander_session_id: lifecycle.commander_session_id.clone(),
        transaction_id: lifecycle.transaction_id.clone(),
        task_id: lifecycle.task_id.clone(),
        goal_id: lifecycle.goal_id.clone(),
        operator_override: lifecycle.operator_override,
        receipt_event_seq: lifecycle.receipt_event_seq,
        slot_acquired: false,
        terminalizing: true,
    })
}

fn runtime_terminal_state_from_snapshot(
    snapshot: &RuntimeLeaseSnapshot,
    projection: &lifecycle::SessionProjection,
) -> Result<TerminalState> {
    if snapshot.session_state != projection.state {
        return Err(anyhow!(
            "RUNTIME_CALLBACK_SESSION_STATE_MISMATCH:runtime={},snapshot={:?},projection={:?}",
            snapshot.runtime_id,
            snapshot.session_state,
            projection.state
        ));
    }
    match snapshot.runtime_state {
        Some(state) => runtime_terminal_state(state),
        None => terminal_state(projection.state),
    }
}

fn terminal_feed_entry_for_runtime(
    session_id: &str,
    runtime_id: &str,
) -> Result<SessionFeedEntry> {
    let mut after_cursor = 0;
    let mut latest = None;
    loop {
        let response = session_log_contract::client::call_service(
            &SessionLogCommand::ReadSessionFeed(session_log_contract::ReadSessionFeedRequest {
                session_id: session_id.to_string(),
                after_cursor,
                limit: 1_000,
            }),
        )?;
        let SessionLogResponse::SessionFeed {
            entries,
            next_cursor,
        } = response
        else {
            return Err(anyhow!("TERMINAL_FEED_READ_FAILED:{response:?}"));
        };
        for entry in entries {
            let is_terminal = entry.runtime_id.as_deref() == Some(runtime_id)
                && matches!(
                    &entry.event,
                    SessionFeedEvent::SessionProjectionUpdated { projection, .. }
                        if projection.state.is_terminal()
                );
            if is_terminal {
                latest = Some(entry);
            }
        }
        if next_cursor <= after_cursor {
            break;
        }
        after_cursor = next_cursor;
    }
    latest.ok_or_else(|| {
        anyhow!(
            "TERMINAL_FEED_EVENT_NOT_FOUND:session={session_id},runtime={runtime_id}"
        )
    })
}

fn terminal_runtime_is_current(
    projection: &lifecycle::SessionProjection,
    session_id: &str,
    dispatch_runtime_id: &str,
    runtime_id: &str,
) -> Result<bool> {
    let Some(dispatch_index) = projection
        .runtime_ids
        .iter()
        .position(|candidate| candidate == dispatch_runtime_id)
    else {
        return Ok(false);
    };
    let Some(runtime_index) = projection
        .runtime_ids
        .iter()
        .position(|candidate| candidate == runtime_id)
    else {
        return Err(anyhow!(
            "TERMINAL_FEED_RUNTIME_NOT_IN_SESSION:session={session_id},runtime={runtime_id}"
        ));
    };
    if runtime_index < dispatch_index {
        return Ok(false);
    }
    if projection.runtime_ids.last().map(String::as_str) != Some(runtime_id)
        || projection.session_id != session_id
    {
        return Err(anyhow!(
            "TERMINAL_FEED_RUNTIME_CHAIN_MISMATCH:session={session_id},dispatch_runtime={dispatch_runtime_id},runtime={runtime_id}"
        ));
    }
    Ok(true)
}

#[derive(Clone, Default)]
struct RuntimeSlotGate {
    active: Arc<Mutex<usize>>,
    notify: Arc<Notify>,
}

impl RuntimeSlotGate {
    async fn acquire(&self, limit: usize) -> RuntimeSlotPermit {
        loop {
            let notified = self.notify.notified();
            {
                let mut active = self.active.lock();
                if *active < limit {
                    *active += 1;
                    return RuntimeSlotPermit { gate: self.clone() };
                }
            }
            notified.await;
        }
    }
}

struct RuntimeSlotPermit {
    gate: RuntimeSlotGate,
}

impl Drop for RuntimeSlotPermit {
    fn drop(&mut self) {
        let mut active = self.gate.active.lock();
        *active = active.saturating_sub(1);
        drop(active);
        self.gate.notify.notify_waiters();
    }
}

struct ActiveSessionGuard {
    sessions: Arc<Mutex<HashMap<String, RuntimeLease>>>,
    session_id: String,
    runtime_id: RuntimeId,
    active: std::sync::atomic::AtomicBool,
}

impl ActiveSessionGuard {
    fn new(
        sessions: Arc<Mutex<HashMap<String, RuntimeLease>>>,
        session_id: &str,
        runtime_id: &str,
    ) -> Self {
        Self {
            sessions,
            session_id: session_id.to_string(),
            runtime_id: runtime_id.to_string(),
            active: std::sync::atomic::AtomicBool::new(true),
        }
    }

    fn finish(&self) {
        self.active
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.remove_matching_lease();
    }

    fn retain(&self) {
        self.active
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn remove_matching_lease(&self) {
        let mut sessions = self.sessions.lock();
        if sessions
            .get(&self.session_id)
            .is_some_and(|lease| lease.runtime_id == self.runtime_id)
        {
            sessions.remove(&self.session_id);
        }
    }
}

impl Drop for ActiveSessionGuard {
    fn drop(&mut self) {
        if self.active.load(std::sync::atomic::Ordering::SeqCst) {
            self.remove_matching_lease();
        }
    }
}

fn payload_to_run_agent_request(
    request: &EnqueueTurnRequest,
    lease_id: &str,
    fallback_from_id: Option<String>,
) -> Result<RunAgentRequest> {
    let mut value = request.payload.clone();
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "session_id".to_string(),
            Value::String(request.session_id.clone()),
        );
        object.insert(
            "runtime_id".to_string(),
            Value::String(request.runtime_id.clone()),
        );
        object.insert("lease_id".to_string(), Value::String(lease_id.to_string()));
        match fallback_from_id {
            Some(fallback_from_id) => {
                object.insert(
                    "fallback_from_id".to_string(),
                    Value::String(fallback_from_id),
                );
            }
            None => {
                object.remove("fallback_from_id");
            }
        }
    }
    serde_json::from_value(value).map_err(|error| {
        anyhow!(
            "invalid run-agent payload for runtime {} session {}: {error}",
            request.runtime_id,
            request.session_id
        )
    })
}

fn validate_terminalization_identity(
    snapshot: &RuntimeLeaseSnapshot,
    lease: &RuntimeLease,
    session_id: &str,
    runtime_id: &str,
) -> Result<()> {
    if snapshot.runtime_id != runtime_id
        || snapshot.session_id != session_id
        || snapshot.lease_id.as_deref() != Some(lease.lease_id.as_str())
    {
        return Err(anyhow!(
            "RUNTIME_TERMINALIZATION_DURABLE_IDENTITY_MISMATCH:session={session_id},runtime={runtime_id},snapshot_session={},snapshot_runtime={},snapshot_lease={:?},router_lease={}",
            snapshot.session_id,
            snapshot.runtime_id,
            snapshot.lease_id,
            lease.lease_id
        ));
    }
    if snapshot.database_path.trim().is_empty() {
        return Err(anyhow!(
            "RUNTIME_TERMINALIZATION_DATABASE_PATH_MISSING:{runtime_id}"
        ));
    }
    Ok(())
}

fn lifecycle_store(commander_session_id: &str) -> Result<SessionLifecycleStore> {
    let base = session_log_contract::client::default_db_dir().join("session_lifecycle_v1");
    let root = commander_store_path(&base, commander_session_id)?;
    Ok(SessionLifecycleStore::open(
        root,
        commander_session_id,
        LifecycleConfig::default(),
    )?)
}

fn terminal_state(state: SessionState) -> Result<TerminalState> {
    match state {
        SessionState::Completed => Ok(TerminalState::Completed),
        SessionState::Failed => Ok(TerminalState::Failed),
        SessionState::Cancelled => Ok(TerminalState::Cancelled),
        SessionState::Interrupted => Ok(TerminalState::Interrupted),
        other => Err(anyhow!("SESSION_STATE_NOT_TERMINAL:{other:?}")),
    }
}

fn runtime_terminal_state(state: RuntimeState) -> Result<TerminalState> {
    match state {
        RuntimeState::Finished => Ok(TerminalState::Completed),
        RuntimeState::Failed | RuntimeState::TimedOut => Ok(TerminalState::Failed),
        RuntimeState::Cancelled => Ok(TerminalState::Cancelled),
        other => Err(anyhow!("RUNTIME_STATE_NOT_TERMINAL:{other:?}")),
    }
}

fn recovery_terminal_state(recovery: &RuntimeRecoveryReceipt) -> TerminalState {
    match recovery.reason {
        RecoveryCloseRuntimeReason::OrphanedRuntime => TerminalState::Cancelled,
        RecoveryCloseRuntimeReason::UnbornRuntime => TerminalState::Interrupted,
    }
}

fn recovery_runtime_state(recovery: &RuntimeRecoveryReceipt) -> Value {
    match recovery.reason {
        RecoveryCloseRuntimeReason::OrphanedRuntime => json!(RuntimeState::Cancelled),
        RecoveryCloseRuntimeReason::UnbornRuntime => Value::Null,
    }
}

fn register_and_activate_runtime(
    session_id: &str,
    runtime_id: &str,
    lease_id: &str,
    fallback_from_id: Option<String>,
    lifecycle: Option<RuntimeLifecycleIdentity>,
) -> Result<()> {
    let response = session_log_contract::client::call_service(
        &SessionLogCommand::RegisterRuntime(RegisterRuntimeRequest {
            runtime_id: runtime_id.to_string(),
            session_id: session_id.to_string(),
            fallback_from_id,
            lifecycle,
        }),
    )?;
    match response {
        SessionLogResponse::RuntimeRegistered {
            result:
                RuntimeRegistrationOutcome::Registered { .. }
                | RuntimeRegistrationOutcome::AlreadyRegistered { .. },
        } => {}
        SessionLogResponse::RuntimeRegistered { result } => {
            return Err(anyhow!(
                "session_db rejected runtime {runtime_id} registration for session {session_id}: {result:?}"
            ));
        }
        SessionLogResponse::Error { error } => {
            return Err(anyhow!(
                "session_db failed runtime {runtime_id} registration for session {session_id}: {error}"
            ));
        }
        other => {
            return Err(anyhow!(
                "unexpected session_db registration response for runtime {runtime_id}: {other:?}"
            ));
        }
    }

    let response = session_log_contract::client::call_service(
        &SessionLogCommand::ActivateRuntimeLease(ActivateRuntimeLeaseRequest {
            runtime_id: runtime_id.to_string(),
            lease_id: lease_id.to_string(),
        }),
    )?;
    match response {
        SessionLogResponse::RuntimeLeaseActivated {
            result: RuntimeLeaseOutcome::Activated | RuntimeLeaseOutcome::AlreadyActive,
        } => Ok(()),
        SessionLogResponse::RuntimeLeaseActivated { result } => Err(anyhow!(
            "session_db rejected lease {lease_id} for runtime {runtime_id}: {result:?}"
        )),
        SessionLogResponse::Error { error } => Err(anyhow!(
            "session_db failed to activate lease for runtime {runtime_id}: {error}"
        )),
        other => Err(anyhow!(
            "unexpected session_db lease response for runtime {runtime_id}: {other:?}"
        )),
    }
}

fn runtime_registration_fallback(
    session_id: &str,
    expected_user_input: Option<&str>,
) -> Result<Option<String>> {
    let response = session_log_contract::client::call_service(&SessionLogCommand::GetSession(
        GetSessionRequest {
            session_id: session_id.to_string(),
        },
    ))?;
    match response {
        SessionLogResponse::Session {
            session: Some(session),
        } => {
            let Some(latest_runtime_id) =
                failed_session_runtime_fallback(&session.lifecycle_projection)?
            else {
                return Ok(None);
            };
            let expected_user_input = expected_user_input
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "FAILED_SESSION_RETRY_INPUT_MISSING:{}",
                        session.lifecycle_projection.session_id
                    )
                })?;
            failed_session_retry_root(
                &session.lifecycle_projection,
                &latest_runtime_id,
                expected_user_input,
                replay_runtime_identity,
            )
            .map(Some)
        }
        SessionLogResponse::Session { session: None } => Err(anyhow!(
            "session_db cannot register runtime for missing session {session_id}"
        )),
        SessionLogResponse::Error { error } => Err(anyhow!(
            "session_db failed to read session {session_id} before runtime registration: {error}"
        )),
        other => Err(anyhow!(
            "unexpected session_db response while reading session {session_id}: {other:?}"
        )),
    }
}

const MAX_FAILED_RUNTIME_RECONCILIATION_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetryRuntimeIdentity {
    runtime_id: String,
    session_id: String,
    state: RuntimeState,
    latest_user_input: Option<String>,
}

fn replay_runtime_identity(runtime_id: &str) -> Result<Option<RetryRuntimeIdentity>> {
    let response = session_log_contract::client::call_service(&SessionLogCommand::ReplayRuntime(
        ReplayRuntimeRequest {
            runtime_id: runtime_id.to_string(),
        },
    ))?;
    match response {
        SessionLogResponse::RuntimeReplayed {
            runtime: Some(runtime),
        } => Ok(Some(retry_runtime_identity(&runtime.aggregate))),
        SessionLogResponse::RuntimeReplayed { runtime: None } => Ok(None),
        SessionLogResponse::Error { error } => Err(anyhow!(
            "session_db failed to replay runtime {runtime_id}: {error}"
        )),
        other => Err(anyhow!(
            "unexpected session_db response while replaying runtime {runtime_id}: {other:?}"
        )),
    }
}

fn retry_runtime_identity(runtime: &RuntimeAggregate) -> RetryRuntimeIdentity {
    let latest_user_input = runtime
        .input
        .as_ref()
        .and_then(|input| input.get("messages"))
        .and_then(Value::as_array)
        .and_then(|messages| {
            messages.iter().rev().find_map(|message| {
                (message.get("role").and_then(Value::as_str) == Some("user"))
                    .then(|| message.get("content").and_then(Value::as_str))
                    .flatten()
                    .map(str::to_string)
            })
        });
    RetryRuntimeIdentity {
        runtime_id: runtime.runtime_id.clone(),
        session_id: runtime.session_id.clone(),
        state: runtime.state,
        latest_user_input,
    }
}

fn failed_session_retry_root<F>(
    projection: &lifecycle::SessionProjection,
    latest_runtime_id: &str,
    expected_user_input: &str,
    mut replay_runtime: F,
) -> Result<String>
where
    F: FnMut(&str) -> Result<Option<RetryRuntimeIdentity>>,
{
    let latest_index = projection
        .runtime_ids
        .iter()
        .position(|runtime_id| runtime_id == latest_runtime_id)
        .ok_or_else(|| {
            anyhow!(
                "FAILED_SESSION_RETRY_SOURCE_NOT_IN_PROJECTION:{}:{}",
                projection.session_id,
                latest_runtime_id
            )
        })?;
    let mut root = None;
    for runtime_id in projection.runtime_ids[..=latest_index]
        .iter()
        .rev()
        .take(MAX_FAILED_RUNTIME_RECONCILIATION_DEPTH)
    {
        let identity = replay_runtime(runtime_id)?.ok_or_else(|| {
            anyhow!(
                "FAILED_SESSION_RETRY_SOURCE_MISSING:{}:{}",
                projection.session_id,
                runtime_id
            )
        })?;
        if identity.runtime_id != *runtime_id || identity.session_id != projection.session_id {
            return Err(anyhow!(
                "FAILED_SESSION_RETRY_SOURCE_IDENTITY_MISMATCH:{}:{}",
                projection.session_id,
                runtime_id
            ));
        }
        if !matches!(
            identity.state,
            RuntimeState::Failed | RuntimeState::TimedOut
        ) {
            break;
        }
        root = Some(identity);
    }
    let root = root.ok_or_else(|| {
        anyhow!(
            "FAILED_SESSION_RETRY_ROOT_NOT_FOUND:{}:{}",
            projection.session_id,
            latest_runtime_id
        )
    })?;
    if root.latest_user_input.as_deref().map(str::trim) != Some(expected_user_input.trim()) {
        return Err(anyhow!(
            "FAILED_SESSION_RETRY_ROOT_INPUT_MISMATCH:{}:{}",
            projection.session_id,
            root.runtime_id
        ));
    }
    Ok(root.runtime_id)
}

fn failed_session_runtime_fallback(
    projection: &lifecycle::SessionProjection,
) -> Result<Option<String>> {
    if projection.state != SessionState::Failed {
        return Ok(None);
    }
    projection
        .runtime_ids
        .last()
        .cloned()
        .map(Some)
        .ok_or_else(|| {
            anyhow!(
                "FAILED_SESSION_MISSING_RUNTIME_LINEAGE:{}",
                projection.session_id
            )
        })
}

fn debug_runtime_enabled() -> bool {
    std::env::var("TURA_DEBUG_RUNTIME")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{
        failed_session_retry_root, failed_session_runtime_fallback, intake_terminal_receipt,
        payload_to_run_agent_request, runtime_lease_from_snapshot,
        runtime_terminal_state_from_snapshot, terminal_runtime_is_current,
        validate_terminalization_identity, EnqueueTurnRequest, ExecutionService,
        RetryRuntimeIdentity, RouterRecoveryCloseRuntimeRequest, RuntimeLease,
    };
    use crate::{build_state, services::manager::ServiceManager};
    use lifecycle::{RuntimeState, SessionProjection, SessionState, TaskPlan};
    use serde_json::json;
    use session_lifecycle::{
        LifecycleConfig, SessionLifecycleStore, TerminalReceipt, TerminalReceiptIdentity,
        TerminalState,
    };
    use session_log_contract::{
        RuntimeLeaseSnapshot, RuntimeLifecycleIdentity, SessionFeedEntry, SessionFeedEvent,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn failed_session_registration_reuses_exact_latest_runtime_lineage() {
        let mut projection = SessionProjection {
            session_id: "session-retry".to_string(),
            state: SessionState::Failed,
            parent_id: None,
            task_plan: TaskPlan::default(),
            pending_user_inputs: Vec::new(),
            cancelled: false,
            runtime_ids: vec!["runtime-old".to_string(), "runtime-failed".to_string()],
            active_runtime_id: None,
        };

        assert_eq!(
            failed_session_runtime_fallback(&projection).expect("failed session fallback"),
            Some("runtime-failed".to_string())
        );

        projection.state = SessionState::Completed;
        assert_eq!(
            failed_session_runtime_fallback(&projection).expect("completed session starts fresh"),
            None
        );

        projection.state = SessionState::Failed;
        projection.runtime_ids.clear();
        assert!(failed_session_runtime_fallback(&projection)
            .expect_err("failed session without lineage must fail closed")
            .to_string()
            .contains("FAILED_SESSION_MISSING_RUNTIME_LINEAGE"));
    }

    #[test]
    fn failed_session_retry_recovers_root_before_legacy_unlinked_attempts() {
        let projection = SessionProjection {
            session_id: "session-retry".to_string(),
            state: SessionState::Failed,
            parent_id: None,
            task_plan: TaskPlan::default(),
            pending_user_inputs: Vec::new(),
            cancelled: false,
            runtime_ids: vec![
                "runtime-completed".to_string(),
                "runtime-root".to_string(),
                "runtime-legacy-retry-1".to_string(),
                "runtime-legacy-retry-2".to_string(),
            ],
            active_runtime_id: None,
        };
        let identities = std::collections::HashMap::from([
            (
                "runtime-completed",
                RetryRuntimeIdentity {
                    runtime_id: "runtime-completed".to_string(),
                    session_id: "session-retry".to_string(),
                    state: RuntimeState::Finished,
                    latest_user_input: Some("older completed task".to_string()),
                },
            ),
            (
                "runtime-root",
                RetryRuntimeIdentity {
                    runtime_id: "runtime-root".to_string(),
                    session_id: "session-retry".to_string(),
                    state: RuntimeState::Failed,
                    latest_user_input: Some("exact root task".to_string()),
                },
            ),
            (
                "runtime-legacy-retry-1",
                RetryRuntimeIdentity {
                    runtime_id: "runtime-legacy-retry-1".to_string(),
                    session_id: "session-retry".to_string(),
                    state: RuntimeState::Failed,
                    latest_user_input: Some("legacy wrapper prompt".to_string()),
                },
            ),
            (
                "runtime-legacy-retry-2",
                RetryRuntimeIdentity {
                    runtime_id: "runtime-legacy-retry-2".to_string(),
                    session_id: "session-retry".to_string(),
                    state: RuntimeState::Failed,
                    latest_user_input: Some("legacy wrapper prompt".to_string()),
                },
            ),
        ]);

        let root = failed_session_retry_root(
            &projection,
            "runtime-legacy-retry-2",
            "exact root task",
            |runtime_id| Ok(identities.get(runtime_id).cloned()),
        )
        .expect("legacy unlinked retries should reconcile to their failed root");
        assert_eq!(root, "runtime-root");

        let error = failed_session_retry_root(
            &projection,
            "runtime-legacy-retry-2",
            "legacy wrapper prompt",
            |runtime_id| Ok(identities.get(runtime_id).cloned()),
        )
        .expect_err("a rewritten retry prompt must not replace canonical root input");
        assert!(error
            .to_string()
            .contains("FAILED_SESSION_RETRY_ROOT_INPUT_MISMATCH"));
    }
    use std::sync::Arc;

    #[test]
    fn recovery_router_payload_rejects_caller_supplied_quiescence() {
        let request = json!({
            "receipt_id": "receipt-1",
            "database_path": "/tmp/session_log.sqlite3",
            "runtime_id": "runtime-1",
            "session_id": "session-1",
            "lease_id": "lease-1",
            "expected_lease_active": true,
            "expected_revision": 0,
            "expected_last_event_seq": 0,
            "expected_session_event_seq": 1,
            "expected_session_state": "interrupted",
            "reason": "unborn_runtime",
            "quiescence": {
                "active_turn": false
            }
        });

        let error = serde_json::from_value::<RouterRecoveryCloseRuntimeRequest>(request)
            .expect_err("caller-supplied quiescence proof must be rejected");
        assert!(error.to_string().contains("unknown field `quiescence`"));
    }

    #[tokio::test]
    async fn queued_recovery_writer_does_not_deadlock_nested_command_run() {
        let workspace = tempfile::tempdir().expect("nested command workspace");
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("nested-session", true);
        let outer_turn_lease = service.admission.read().await;
        let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
        let writer_gate = Arc::clone(&service.admission);
        let writer = tokio::spawn(async move {
            queued_tx.send(()).expect("signal queued recovery writer");
            let _recovery = writer_gate.write().await;
        });
        queued_rx.await.expect("recovery writer queued");
        tokio::task::yield_now().await;
        assert!(!writer.is_finished());

        let nested = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.command_run_request(
                &state,
                json!({
                    "session_id": "nested-session",
                    "runtime_id": "runtime-nested-session",
                    "session_directory": workspace.path().display().to_string(),
                    "arguments": {
                        "commands": [{
                            "command": "task_status",
                            "command_line": json!({
                                "status": "done",
                                "task_group": "nested admission canary"
                            }).to_string()
                        }]
                    },
                    "allowed_commands": ["task_status"]
                }),
                "nested-command-run-canary",
            ),
        )
        .await
        .expect("nested command_run must not wait behind recovery writer")
        .expect("nested command_run should finish");
        assert_eq!(nested["result"]["results"][0]["success"], true);

        drop(outer_turn_lease);
        tokio::time::timeout(std::time::Duration::from_secs(1), writer)
            .await
            .expect("recovery writer should acquire after outer turn release")
            .expect("recovery writer task should join");
    }

    #[tokio::test]
    async fn active_router_turn_denies_recovery_before_session_db_mutation() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("active-recovery-session", true);

        let response = service
            .recovery_close_runtime(
                &state,
                json!({
                    "receipt_id": "active-recovery-receipt",
                    "database_path": "/tmp/session_log.sqlite3",
                    "runtime_id": "runtime-active-recovery-session",
                    "session_id": "active-recovery-session",
                    "lease_id": "lease-active-recovery-session",
                    "expected_lease_active": true,
                    "expected_revision": 0,
                    "expected_last_event_seq": 0,
                    "expected_session_event_seq": 1,
                    "expected_session_state": "interrupted",
                    "reason": "unborn_runtime"
                }),
            )
            .await
            .expect("active recovery denial");

        assert_eq!(response["result"]["outcome"], "runtime_live");
        assert_eq!(response["result"]["proof"]["active_turn"], true);
        assert_eq!(response["result"]["proof"]["running_turn"], true);
        assert_eq!(
            response["result"]["proof"]["global_active_session_count"],
            1
        );
    }

    #[tokio::test]
    async fn terminalizing_phase_is_quiescent_but_keeps_session_reserved() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("terminalizing-session", true);
        service
            .mark_terminalizing("terminalizing-session", "runtime-terminalizing-session")
            .expect("mark exact runtime terminalizing");

        let proof = service
            .terminalization_quiescence_proof(
                &state,
                "terminalizing-session",
                "runtime-terminalizing-session",
            )
            .await
            .expect("terminalizing runtime should be quiescent without effects");
        assert!(proof.is_quiescent());
        assert_eq!(proof.global_active_session_count, 1);
        assert!(service
            .sessions
            .lock()
            .contains_key("terminalizing-session"));

        let probe = service
            .probe_sessions(&state, json!({ "session_ids": ["terminalizing-session"] }))
            .await
            .expect("probe terminalizing session");
        assert_eq!(probe["sessions"][0]["status"], "terminalizing");
        assert_eq!(probe["sessions"][0]["active_turn"], false);
    }

    #[test]
    fn terminalization_identity_requires_exact_database_runtime_session_and_lease() {
        let lease = RuntimeLease {
            runtime_id: "runtime-exact".to_string(),
            lease_id: "lease-exact".to_string(),
            commander_session_id: "commander-exact".to_string(),
            transaction_id: "transaction-exact".to_string(),
            task_id: None,
            goal_id: None,
            operator_override: false,
            receipt_event_seq: 0,
            slot_acquired: true,
            terminalizing: true,
        };
        let mut snapshot = RuntimeLeaseSnapshot {
            database_path: "/tmp/session_log.sqlite3".to_string(),
            runtime_id: "runtime-exact".to_string(),
            session_id: "session-exact".to_string(),
            lifecycle: None,
            lease_id: Some("lease-exact".to_string()),
            lease_active: true,
            revision: 0,
            last_event_seq: 0,
            terminal: false,
            session_event_seq: 2,
            session_state: SessionState::Running,
            runtime_state: None,
        };
        validate_terminalization_identity(&snapshot, &lease, "session-exact", "runtime-exact")
            .expect("exact durable identity should pass");

        snapshot.lease_id = Some("lease-drift".to_string());
        assert!(validate_terminalization_identity(
            &snapshot,
            &lease,
            "session-exact",
            "runtime-exact",
        )
        .expect_err("lease drift must fail closed")
        .to_string()
        .contains("RUNTIME_TERMINALIZATION_DURABLE_IDENTITY_MISMATCH"));
    }

    #[test]
    fn durable_lifecycle_snapshot_restores_callback_identity_and_runtime_terminal_semantics() {
        let lifecycle = RuntimeLifecycleIdentity {
            commander_session_id: "commander-durable".to_string(),
            transaction_id: "transaction-durable".to_string(),
            task_id: Some("task-durable".to_string()),
            goal_id: Some("goal-durable".to_string()),
            operator_override: true,
            dispatch_runtime_id: "runtime-durable".to_string(),
            dispatch_lease_id: "lease-durable".to_string(),
            receipt_event_seq: 2,
        };
        let snapshot = RuntimeLeaseSnapshot {
            database_path: "/tmp/session_log.sqlite3".to_string(),
            runtime_id: "runtime-durable".to_string(),
            session_id: "child-durable".to_string(),
            lifecycle: Some(lifecycle),
            lease_id: Some("lease-durable".to_string()),
            lease_active: false,
            revision: 7,
            last_event_seq: 7,
            terminal: true,
            session_event_seq: 3,
            session_state: SessionState::Interrupted,
            runtime_state: Some(RuntimeState::Cancelled),
        };
        let projection = SessionProjection {
            session_id: "child-durable".to_string(),
            state: SessionState::Interrupted,
            parent_id: Some("commander-durable".to_string()),
            task_plan: TaskPlan::default(),
            pending_user_inputs: Vec::new(),
            cancelled: false,
            runtime_ids: vec!["runtime-durable".to_string()],
            active_runtime_id: None,
        };

        let lease = runtime_lease_from_snapshot(&snapshot)
            .expect("durable lifecycle identity should reconstruct the callback owner");
        assert_eq!(lease.transaction_id, "transaction-durable");
        assert_eq!(lease.commander_session_id, "commander-durable");
        assert_eq!(lease.receipt_event_seq, 2);
        assert_eq!(
            runtime_terminal_state_from_snapshot(&snapshot, &projection)
                .expect("runtime terminal state should be authoritative"),
            TerminalState::Cancelled
        );
    }

    #[test]
    fn snapshot_terminal_receipt_replays_or_reuses_runtime_writer_receipt() {
        let lifecycle = RuntimeLifecycleIdentity {
            commander_session_id: "commander-replay".to_string(),
            transaction_id: "transaction-replay".to_string(),
            task_id: Some("task-replay".to_string()),
            goal_id: Some("goal-replay".to_string()),
            operator_override: true,
            dispatch_runtime_id: "runtime-replay".to_string(),
            dispatch_lease_id: "lease-replay".to_string(),
            receipt_event_seq: 0,
        };
        let snapshot = RuntimeLeaseSnapshot {
            database_path: "/tmp/session_log.sqlite3".to_string(),
            runtime_id: "runtime-replay".to_string(),
            session_id: "child-replay".to_string(),
            lifecycle: Some(lifecycle),
            lease_id: Some("lease-replay".to_string()),
            lease_active: false,
            revision: 8,
            last_event_seq: 8,
            terminal: true,
            session_event_seq: 4,
            session_state: SessionState::Failed,
            runtime_state: Some(RuntimeState::Failed),
        };
        let lease = runtime_lease_from_snapshot(&snapshot).expect("durable callback identity");
        let event_id = "runtime-replay:8:session-projection";
        let entry = SessionFeedEntry {
            session_id: "child-replay".to_string(),
            cursor: 12,
            runtime_id: Some("runtime-replay".to_string()),
            event_id: event_id.to_string(),
            event: SessionFeedEvent::SessionProjectionUpdated {
                projection: SessionProjection {
                    session_id: "child-replay".to_string(),
                    state: SessionState::Failed,
                    parent_id: Some("commander-replay".to_string()),
                    task_plan: TaskPlan::default(),
                    pending_user_inputs: Vec::new(),
                    cancelled: false,
                    runtime_ids: vec!["runtime-replay".to_string()],
                    active_runtime_id: None,
                },
                session_name: None,
                updated_at: 1_787_970_243_996,
            },
        };

        let replay = ExecutionService::snapshot_terminal_receipt(
            &lease,
            &snapshot,
            &entry,
            TerminalState::Failed,
        )
        .expect("snapshot receipt");
        let mut original = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                "transaction-replay",
                event_id,
                0,
                "commander-replay",
                "child-replay",
                "runtime-replay",
                "lease-replay",
            ),
            TerminalState::Failed,
            1_787_970_243_996,
        );
        original.task_id = Some("task-replay".to_string());
        original.goal_id = Some("goal-replay".to_string());
        original.operator_override = true;
        original
            .audit_metadata
            .insert("runtime_event_seq".to_string(), json!(8));
        original
            .audit_metadata
            .insert("runtime_expected_revision".to_string(), json!(7));
        original
            .audit_metadata
            .insert("runtime_state".to_string(), json!(RuntimeState::Failed));
        original
            .audit_metadata
            .insert("dispatch_runtime_id".to_string(), json!("runtime-replay"));
        original
            .audit_metadata
            .insert("dispatch_lease_id".to_string(), json!("lease-replay"));

        assert_eq!(replay, original);
        assert!(!replay.audit_metadata.contains_key("session_state"));
        let root = tempfile::tempdir().expect("lifecycle root");
        let store = SessionLifecycleStore::open(
            root.path(),
            "commander-replay",
            LifecycleConfig::default(),
        )
        .expect("lifecycle store");
        store
            .write_terminal_receipt(&original)
            .expect("original runtime receipt");
        store
            .write_terminal_receipt(&replay)
            .expect("snapshot replay must be already durable, not conflicting");

        let mut delayed_projection = entry.clone();
        match &mut delayed_projection.event {
            SessionFeedEvent::SessionProjectionUpdated { updated_at, .. } => {
                *updated_at += 140;
            }
            _ => panic!("fixture must remain a projection event"),
        }
        let reconstructed = ExecutionService::snapshot_terminal_receipt(
            &lease,
            &snapshot,
            &delayed_projection,
            TerminalState::Failed,
        )
        .expect("delayed projection receipt");
        assert_ne!(reconstructed, original);
        ExecutionService::ensure_snapshot_terminal_receipt(
            &store,
            &lease,
            &snapshot,
            &delayed_projection,
            TerminalState::Failed,
        )
        .expect("the durable runtime receipt must outrank a later projection timestamp");
        assert_eq!(
            store
                .terminal_receipt("transaction-replay", event_id)
                .expect("durable runtime receipt"),
            original
        );
        let delivery = intake_terminal_receipt(
            &store,
            &delayed_projection,
            "runtime-replay",
            "transaction-replay",
            &lease,
            TerminalState::Failed,
        )
        .expect("existing durable receipt must remain intake-compatible")
        .expect("terminal delivery identity");
        assert_eq!(delivery.runtime_id, "runtime-replay");
        assert_eq!(store.readback().expect("readback").applied_receipts, 1);
    }

    #[test]
    fn payload_to_run_agent_request_injects_authoritative_session_id() {
        let request = EnqueueTurnRequest {
            runtime_id: "runtime-1".to_string(),
            session_id: "session-authoritative".to_string(),
            payload: json!({
                "session_id": "stale-session",
                "prompt": "hello",
                "model": "openai/gpt-test",
                "worker_env": { "TURA_REASONING_EFFORT": "low" }
            }),
        };

        let run = payload_to_run_agent_request(
            &request,
            "lease-test",
            Some("runtime-failed".to_string()),
        )
        .expect("valid enqueue payload should become run-agent request");

        assert_eq!(run.session_id.as_deref(), Some("session-authoritative"));
        assert_eq!(run.runtime_id, "runtime-1");
        assert_eq!(run.fallback_from_id.as_deref(), Some("runtime-failed"));
        assert_eq!(run.prompt.as_deref(), Some("hello"));
        assert_eq!(run.model.as_deref(), Some("openai/gpt-test"));
        assert_eq!(
            run.worker_env
                .get("TURA_REASONING_EFFORT")
                .map(String::as_str),
            Some("low")
        );
    }

    #[test]
    fn payload_to_run_agent_request_reports_invalid_payload_shape() {
        let request = EnqueueTurnRequest {
            runtime_id: "runtime-invalid".to_string(),
            session_id: "session-invalid".to_string(),
            payload: json!({
                "worker_env": "not-an-object"
            }),
        };

        let error = payload_to_run_agent_request(&request, "lease-test", None)
            .expect_err("invalid worker_env shape should be rejected");

        assert!(
            error.to_string().contains("invalid run-agent payload")
                && error.to_string().contains("runtime-invalid")
                && error.to_string().contains("session-invalid"),
            "invalid payload error should include runtime and session context: {error}"
        );
    }

    #[test]
    fn terminal_runtime_chain_accepts_fallback_and_ignores_prior_turns() {
        let projection = SessionProjection {
            session_id: "child-1".to_string(),
            state: SessionState::Completed,
            parent_id: Some("commander-1".to_string()),
            task_plan: TaskPlan::default(),
            pending_user_inputs: Vec::new(),
            cancelled: false,
            runtime_ids: vec![
                "old-runtime".to_string(),
                "dispatch-runtime".to_string(),
                "fallback-runtime".to_string(),
            ],
            active_runtime_id: None,
        };

        assert!(!terminal_runtime_is_current(
            &projection,
            "child-1",
            "dispatch-runtime",
            "old-runtime",
        )
        .expect("historical runtime should be ignored"));
        assert!(terminal_runtime_is_current(
            &projection,
            "child-1",
            "dispatch-runtime",
            "fallback-runtime",
        )
        .expect("latest fallback should be accepted"));
        let error = terminal_runtime_is_current(
            &projection,
            "child-1",
            "dispatch-runtime",
            "dispatch-runtime",
        )
        .expect_err("non-latest dispatch runtime must not terminate a fallback chain");
        assert!(error
            .to_string()
            .contains("TERMINAL_FEED_RUNTIME_CHAIN_MISMATCH"));
    }

    #[test]
    fn fallback_receipt_intake_is_exactly_once_and_preserves_dispatch_identity() {
        let root = tempfile::tempdir().expect("lifecycle root");
        let store =
            SessionLifecycleStore::open(root.path(), "commander-1", LifecycleConfig::default())
                .expect("lifecycle store");
        let lease = RuntimeLease {
            runtime_id: "dispatch-runtime".to_string(),
            lease_id: "dispatch-lease".to_string(),
            commander_session_id: "commander-1".to_string(),
            transaction_id: "transaction-1".to_string(),
            task_id: Some("task-1".to_string()),
            goal_id: Some("goal-1".to_string()),
            operator_override: true,
            receipt_event_seq: 0,
            slot_acquired: true,
            terminalizing: false,
        };
        let projection = SessionProjection {
            session_id: "child-1".to_string(),
            state: SessionState::Completed,
            parent_id: Some("commander-1".to_string()),
            task_plan: TaskPlan::default(),
            pending_user_inputs: Vec::new(),
            cancelled: false,
            runtime_ids: vec![
                "dispatch-runtime".to_string(),
                "fallback-runtime".to_string(),
            ],
            active_runtime_id: None,
        };
        let entry = SessionFeedEntry {
            session_id: "child-1".to_string(),
            cursor: 9,
            runtime_id: Some("fallback-runtime".to_string()),
            event_id: "fallback-runtime:5:session-projection".to_string(),
            event: SessionFeedEvent::SessionProjectionUpdated {
                projection: projection.clone(),
                session_name: None,
                updated_at: 10,
            },
        };
        let mut receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                "transaction-1",
                entry.event_id.clone(),
                0,
                "commander-1",
                "child-1",
                "fallback-runtime",
                "fallback-lease",
            ),
            TerminalState::Completed,
            10,
        );
        receipt.task_id = Some("task-1".to_string());
        receipt.goal_id = Some("goal-1".to_string());
        receipt.operator_override = true;
        receipt
            .audit_metadata
            .insert("dispatch_runtime_id".to_string(), json!("dispatch-runtime"));
        receipt
            .audit_metadata
            .insert("dispatch_lease_id".to_string(), json!("dispatch-lease"));
        store
            .write_terminal_receipt(&receipt)
            .expect("durable fallback receipt");

        let first = intake_terminal_receipt(
            &store,
            &entry,
            "fallback-runtime",
            "transaction-1",
            &lease,
            TerminalState::Completed,
        )
        .expect("first intake")
        .expect("terminal delivery");
        let duplicate = intake_terminal_receipt(
            &store,
            &entry,
            "fallback-runtime",
            "transaction-1",
            &lease,
            TerminalState::Completed,
        )
        .expect("duplicate intake")
        .expect("duplicate terminal delivery");
        assert_eq!(first, duplicate);
        assert_eq!(first.runtime_id, "fallback-runtime");
        assert_eq!(store.readback().expect("readback").applied_receipts, 1);
        assert_eq!(store.readback().expect("readback").pending_receipts, 0);
    }

    #[test]
    fn terminal_session_command_projection_without_runtime_is_not_a_receipt() {
        let service = ExecutionService::new();
        let entry = SessionFeedEntry {
            session_id: "child-1".to_string(),
            cursor: 20,
            runtime_id: None,
            event_id: "session-command-1:session-projection".to_string(),
            event: SessionFeedEvent::SessionProjectionUpdated {
                projection: SessionProjection {
                    session_id: "child-1".to_string(),
                    state: SessionState::Completed,
                    parent_id: Some("commander-1".to_string()),
                    task_plan: TaskPlan::default(),
                    pending_user_inputs: Vec::new(),
                    cancelled: false,
                    runtime_ids: vec!["dispatch-runtime".to_string()],
                    active_runtime_id: None,
                },
                session_name: None,
                updated_at: 10,
            },
        };

        assert_eq!(
            service
                .intake_terminal_feed_entry(&entry, "transaction-1")
                .expect("session-level terminal projections are not receipt failures"),
            None
        );
    }

    #[tokio::test]
    async fn cancel_idle_turn_reports_idle_without_worker_stop() {
        let state = build_state();
        let response = ExecutionService::new()
            .cancel_turn(
                &state,
                json!({
                    "session_id": "idle-session",
                    "runtime_id": "runtime-idle-session"
                }),
            )
            .await;

        assert_eq!(response["status"], "idle");
        assert_eq!(response["session_id"], "idle-session");
        assert_eq!(response["stopped_worker"], false);
    }

    #[tokio::test]
    async fn cancel_active_turn_fails_closed_without_durable_runtime() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("active-session", true);

        let response = service
            .cancel_turn(
                &state,
                json!({
                    "session_id": "active-session",
                    "runtime_id": "runtime-active-session"
                }),
            )
            .await;

        assert_eq!(response["status"], "error");
        assert_eq!(response["session_id"], "active-session");
        assert_eq!(response["stopped_worker"], false);
        assert_eq!(response["runtime_terminalized"], false);
        assert_eq!(response["terminalization_pending"], true);
        assert!(response["terminalization_error"]
            .as_str()
            .is_some_and(|error| !error.trim().is_empty()));
        assert!(service
            .sessions
            .lock()
            .get("active-session")
            .is_some_and(|lease| lease.terminalizing));
    }

    #[tokio::test]
    async fn cancel_active_turn_drains_router_owned_command_run() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("command-session", true);
        let workspace = tempfile::tempdir().expect("workspace");
        let command = if cfg!(windows) {
            "Test-Path .; Start-Sleep -Seconds 5".to_string()
        } else {
            "find . -maxdepth 0; sleep 5".to_string()
        };
        let request = json!({
            "session_id": "command-session",
            "runtime_id": "runtime-command-session",
            "session_directory": workspace.path().display().to_string(),
            "arguments": {
                "commands": [{
                    "command": "shell_command",
                    "command_line": json!({
                        "command": command,
                        "timeout_ms": 30_000
                    }).to_string()
                }]
            }
        });
        let running = {
            let command_run = state.command_run.clone();
            tokio::spawn(async move {
                command_run
                    .execute_with_request_id(request, Some("command-session-execution"))
                    .await
            })
        };
        let started = Instant::now();
        while state
            .command_run
            .active_count_for_session("command-session")
            == 0
            && started.elapsed() < Duration::from_secs(2)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let response = service
            .cancel_turn(
                &state,
                json!({
                    "session_id": "command-session",
                    "runtime_id": "runtime-command-session"
                }),
            )
            .await;

        assert_eq!(response["status"], "error");
        assert_eq!(response["active_command_runs_cancelled"], 1);
        assert_eq!(response["active_command_runs_remaining"], 0);
        assert_eq!(response["runtime_terminalized"], false);
        assert_eq!(response["terminalization_pending"], true);
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .expect("cancelled command task should terminate promptly")
            .expect("cancelled command task should join")
            .expect("cancelled command response should remain deterministic");
        assert!(service
            .sessions
            .lock()
            .get("command-session")
            .is_some_and(|lease| lease.terminalizing));
        assert!(!service
            .retained_slots
            .lock()
            .contains_key("command-session"));
    }

    #[tokio::test]
    async fn cancelled_runtime_cannot_publish_a_retained_slot() {
        let service = ExecutionService::new();
        service.set_session_lease_for_test("cancelled-session", true);
        let permit = service.runtime_slots.acquire(1).await;
        service.sessions.lock().remove("cancelled-session");

        let permit = service
            .retain_runtime_slot_if_current(
                "cancelled-session",
                "runtime-cancelled-session",
                permit,
            )
            .expect_err("removed lease must reject retained-slot publication");
        drop(permit);

        assert!(!service
            .retained_slots
            .lock()
            .contains_key("cancelled-session"));
    }

    #[tokio::test]
    async fn stale_runtime_cancel_does_not_remove_the_current_lease() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("active-session", true);

        let response = service
            .cancel_turn(
                &state,
                json!({
                    "session_id": "active-session",
                    "runtime_id": "runtime-stale"
                }),
            )
            .await;

        assert_eq!(response["status"], "idle");
        assert!(service.sessions.lock().contains_key("active-session"));
    }

    #[test]
    fn execution_service_starts_with_no_active_runtime_workers() {
        let manager = ServiceManager::new();

        assert_eq!(manager.count_workers_with_prefix("runtime_worker:"), 0);
    }

    #[tokio::test]
    async fn probe_sessions_reports_active_and_inactive_sessions() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("active-session", true);

        let response = service
            .probe_sessions(
                &state,
                json!({ "session_ids": ["active-session", "inactive-session"] }),
            )
            .await
            .expect("probe sessions");

        let sessions = response["sessions"]
            .as_array()
            .expect("sessions array should be present");
        assert_eq!(sessions[0]["session_id"], "active-session");
        assert_eq!(sessions[0]["status"], "running");
        assert_eq!(sessions[0]["active_turn"], true);
        assert_eq!(sessions[0]["running_turn"], true);
        assert_eq!(sessions[1]["session_id"], "inactive-session");
        assert_eq!(sessions[1]["status"], "inactive");
    }

    #[tokio::test]
    async fn probe_sessions_reports_queued_turns_as_active_without_worker() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("queued-session", false);

        let response = service
            .probe_sessions(&state, json!({ "session_ids": ["queued-session"] }))
            .await
            .expect("probe sessions");

        let sessions = response["sessions"]
            .as_array()
            .expect("sessions array should be present");
        assert_eq!(sessions[0]["session_id"], "queued-session");
        assert_eq!(sessions[0]["runtime_id"], "runtime-queued-session");
        assert_eq!(sessions[0]["status"], "queued");
        assert_eq!(sessions[0]["active_turn"], true);
        assert_eq!(sessions[0]["queued_turn"], true);
        assert_eq!(sessions[0]["worker_alive"], false);
    }

    #[tokio::test]
    async fn execution_status_exposes_retained_and_command_liveness_evidence() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("status-session", true);

        let response = service.status(&state).await;

        assert_eq!(response["status"], "ok");
        assert_eq!(response["active_session_count"], 1);
        assert_eq!(response["active_command_runs"], 0);
        assert_eq!(response["sessions"][0]["session_id"], "status-session");
        assert_eq!(response["sessions"][0]["slot_acquired"], true);
        assert_eq!(response["sessions"][0]["terminalizing"], false);
        assert_eq!(response["sessions"][0]["retained_slot"], false);
    }

    #[tokio::test]
    async fn enqueue_turn_reports_active_session_as_structured_payload_without_dispatch() {
        let state = build_state();
        let service = ExecutionService::new();
        service.set_session_lease_for_test("active-session", true);

        let response = service
            .enqueue_turn_request(
                &state,
                json!({
                    "runtime_id": "active-runtime-2",
                    "session_id": "active-session",
                    "payload": {
                        "prompt": "append instead of failing"
                    }
                }),
                "active-session-test",
            )
            .await
            .expect("active-session rejection is a gateway-handled payload");

        assert_eq!(response["ok"], false);
        assert_eq!(response["code"], "session_active_turn");
        assert_eq!(response["session_id"], "active-session");
        assert_eq!(response["runtime_id"], "runtime-active-session");
        assert!(service.sessions.lock().contains_key("active-session"));
        assert_eq!(
            state.manager.count_workers_with_prefix("runtime_worker:"),
            0
        );
    }

    #[tokio::test]
    async fn acquire_runtime_slot_queues_above_runtime_worker_limit_instead_of_rejecting() {
        let service = Arc::new(ExecutionService::new());
        let mut permits = Vec::new();
        let configured_limit = 6;
        for index in 0..configured_limit {
            permits.push(
                service
                    .acquire_runtime_slot(&format!("running-{index}"), configured_limit)
                    .await
                    .expect("initial runtime slots should be available"),
            );
        }

        service.set_session_lease_for_test("queued-session", false);
        let queued_service = Arc::clone(&service);
        let queued = tokio::spawn(async move {
            queued_service
                .acquire_runtime_slot("queued-session", configured_limit)
                .await
                .expect("queued turn should acquire the released runtime slot")
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !queued.is_finished(),
            "turn above the runtime worker limit should wait in the queue, not fail immediately"
        );

        drop(permits.pop());
        let permit = tokio::time::timeout(std::time::Duration::from_secs(1), queued)
            .await
            .expect("queued turn should resume after a runtime slot is released")
            .expect("queued task should not panic");
        drop(permit);
    }
}
