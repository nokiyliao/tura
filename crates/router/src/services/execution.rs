//! Router-owned execution supervision.
//!
//! This module owns runtime worker lifecycle decisions. Gateway may enqueue or
//! cancel turns, but must not spawn runtime workers directly.

use anyhow::{Result, anyhow};
use lifecycle::{RuntimeAggregate, RuntimeId, RuntimeState, SessionCommand, SessionState};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Notify, RwLock};

use crate::ipc_handlers::enqueue_turn_identity;
use crate::services::runtime_workers::{MAX_QUEUED_RUNTIME_TURNS, runtime_worker_limit};
use crate::{AppState, dispatch_run_agent_with_runtime_slot};
use router_contract::{
    CancelRuntimeRequest, EnqueueTurnRequest, IpcRequest, ProbeSessionsRequest,
    RegisterChildSessionOutcome, RegisterChildSessionRequest, RegisterChildSessionResponse,
};
use runtime_contract::{LifecycleExecutionContext, RunAgentRequest};
use session_lifecycle::{
    CallbackEffectIdentity, ChildAdmissionOutcome, ChildAdmissionRecord,
    ContinuationDispatchRecord, ContinuationWriteOutcome, DurableCallbackRecord, IntakeOutcome,
    LifecycleConfig, LiveEffectEvidence, ReclaimOutcome, SessionLifecycleStore, TerminalReceipt,
    TerminalReceiptIdentity, TerminalState, canonical_value_sha256, commander_store_path,
};
use session_log_contract::{
    ActivateRuntimeLeaseRequest, CreateSessionRequest, GetRuntimeLeaseRequest, GetSessionRequest,
    RecoveryCloseRuntimeOutcome, RecoveryCloseRuntimeReason, RecoveryCloseRuntimeRequest,
    RegisterRuntimeRequest, ReplayRuntimeRequest, RuntimeLeaseOutcome, RuntimeLeaseSnapshot,
    RuntimeLifecycleIdentity, RuntimeRecoveryQuiescenceProof, RuntimeRecoveryReceipt,
    RuntimeRegistrationOutcome, SessionFeedEntry, SessionFeedEvent, SessionLogCommand,
    SessionLogResponse, SessionSnapshot, recovery_terminal_projection_event_id,
};

#[derive(Clone)]
pub struct ExecutionService {
    admission: Arc<RwLock<()>>,
    child_admission: Arc<tokio::sync::Mutex<()>>,
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
    parent_mission_revision_sha256: Option<String>,
    delegated_input_sha256: Option<String>,
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
    pub(crate) callback_payload_sha256: Option<String>,
    pub(crate) callback_effect_identity: Option<CallbackEffectIdentity>,
}

impl ExecutionService {
    pub fn new() -> Self {
        Self {
            admission: Arc::new(RwLock::new(())),
            child_admission: Arc::new(tokio::sync::Mutex::new(())),
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
        let lease_id = format!("lease-{}", uuid::Uuid::new_v4());
        self.enqueue_turn_request_with_identity(state, input, request_id, lease_id, None)
            .await
    }

    pub async fn register_child_session_request(
        &self,
        state: &AppState,
        input: Value,
    ) -> Result<Value> {
        let request: RegisterChildSessionRequest = serde_json::from_value(input)?;
        request.validate().map_err(anyhow::Error::msg)?;
        let _admission = self.child_admission.lock().await;
        state.session_db.start()?;

        let parent = read_session_snapshot(&request.parent_session_id)?
            .ok_or_else(|| anyhow!("CHILD_ADMISSION_PARENT_SESSION_NOT_FOUND:{}", request.parent_session_id))?;
        let existing_child = read_session_snapshot(&request.child_session_id)?;
        let store = lifecycle_store(&request.parent_session_id)?;
        if existing_child.is_some() && store.child_admission(&request.child_session_id)?.is_none() {
            return Err(anyhow!(
                "CHILD_ADMISSION_EXISTING_CHILD_WITHOUT_IDENTITY:{}",
                request.child_session_id
            ));
        }

        let record = child_admission_record(&request);
        let admitted = store.admit_child(&record)?;
        match existing_child {
            Some(child) => ensure_child_parent_identity(&child, &request.parent_session_id)?,
            None => create_child_session(&parent, &request)?,
        }

        if !child_runtime_is_registered(&request)? {
            let payload = child_execution_payload(&request)?;
            let response = self
                .enqueue_turn_request_with_identity(
                    state,
                    serde_json::to_value(EnqueueTurnRequest {
                        runtime_id: request.child_runtime_id.clone(),
                        session_id: request.child_session_id.clone(),
                        payload,
                    })?,
                    &request.child_transaction_id,
                    request.child_lease_id.clone(),
                    None,
                )
                .await?;
            if response.get("ok").and_then(Value::as_bool) == Some(false) {
                return Err(anyhow!(
                    "CHILD_ADMISSION_EXECUTION_REJECTED:{}",
                    response
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("router execution rejected")
                ));
            }
        }

        serde_json::to_value(RegisterChildSessionResponse {
            outcome: match admitted {
                ChildAdmissionOutcome::Admitted => RegisterChildSessionOutcome::Admitted,
                ChildAdmissionOutcome::AlreadyAdmitted => {
                    RegisterChildSessionOutcome::AlreadyAdmitted
                }
            },
            parent_session_id: request.parent_session_id,
            child_session_id: request.child_session_id,
            child_runtime_id: request.child_runtime_id,
            child_transaction_id: request.child_transaction_id,
            callback_request_id: request.callback_request_id,
            effect_id: request.effect_id,
        })
        .map_err(Into::into)
    }

    async fn enqueue_turn_request_with_identity(
        &self,
        state: &AppState,
        input: Value,
        request_id: &str,
        lease_id: String,
        continuation: Option<ContinuationDispatchRecord>,
    ) -> Result<Value> {
        let _admission = self.admission.read().await;
        let request: EnqueueTurnRequest = serde_json::from_value(input)?;
        if let Some(active) = self.sessions.lock().get(&request.session_id) {
            return Ok(active_turn_conflict(&request.session_id, active));
        }
        state.session_db.start()?;
        let mut run_request = payload_to_run_agent_request(&request, &lease_id, None)?;
        let requested_prompt = run_request.effective_prompt();
        let fallback_from_id =
            runtime_registration_fallback(&request.session_id, requested_prompt)?;
        run_request.fallback_from_id.clone_from(&fallback_from_id);
        let commander_session_id = run_request
            .parent_session_id
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| request.session_id.clone());
        let delegated = run_request
            .parent_session_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
        validate_delegated_input_digest(&mut run_request, delegated)?;
        run_request
            .validate_delegated_identity()
            .map_err(anyhow::Error::msg)?;
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
            parent_mission_revision_sha256: run_request.parent_mission_revision_sha256.clone(),
            delegated_input_sha256: run_request.delegated_input_sha256.clone(),
            task_id: task_id.clone(),
            goal_id: goal_id.clone(),
            operator_override,
        });
        let durable_lifecycle = RuntimeLifecycleIdentity {
            commander_session_id: commander_session_id.clone(),
            transaction_id: request_id.to_string(),
            parent_mission_revision_sha256: run_request.parent_mission_revision_sha256.clone(),
            delegated_input_sha256: run_request.delegated_input_sha256.clone(),
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
                    parent_mission_revision_sha256: run_request
                        .parent_mission_revision_sha256
                        .clone(),
                    delegated_input_sha256: run_request.delegated_input_sha256.clone(),
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
        if let Some(record) = continuation.as_ref() {
            let store = lifecycle_store(&record.commander_session_id)?;
            store.mark_callback_continuation_dispatched(record)?;
        }
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
        require_successful_runtime_dispatch(status, &body)?;
        if let Some(record) = continuation.as_ref() {
            let store = lifecycle_store(&record.commander_session_id)?;
            if !store.callback_continuation_completion_proven(record)? {
                return Err(anyhow!(
                    "CONTINUATION_SUCCESS_EVIDENCE_NOT_DURABLE:{}:{}:{}",
                    record.request_id,
                    record.runtime_id,
                    record.lease_id
                ));
            }
            complete_and_ack_callback_continuation(&store, record)?;
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
        let mut delivery = intake_terminal_receipt(
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
        if let Some(current) = delivery.take() {
            delivery = match publish_terminal_failure_callback_from_store(&store, current.clone())?
            {
                Some((_transport, failure_delivery)) => Some(failure_delivery),
                None => Some(current),
            };
        }
        Ok(delivery)
    }

    pub(crate) fn historical_terminal_state_mismatch(
        &self,
        snapshot: &RuntimeLeaseSnapshot,
    ) -> Result<bool> {
        let entry = terminal_feed_entry_for_runtime(&snapshot.session_id, &snapshot.runtime_id)?;
        let SessionFeedEvent::SessionProjectionUpdated { projection, .. } = entry.event else {
            return Ok(false);
        };
        Ok(is_historical_terminal_runtime(
            projection.active_runtime_id.as_deref(),
            &snapshot.runtime_id,
        ))
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

    #[allow(
        dead_code,
        reason = "reserved for an explicit Commander consumer acknowledgement"
    )]
    pub(crate) fn acknowledge_terminal_delivery(
        &self,
        delivery: &TerminalDeliveryIdentity,
    ) -> Result<()> {
        let store = lifecycle_store(&delivery.commander_session_id)?;
        if let (Some(payload_sha256), Some(effect_identity)) = (
            delivery.callback_payload_sha256.as_deref(),
            delivery.callback_effect_identity.as_ref(),
        ) {
            store.acknowledge_callback(
                &delivery.transaction_id,
                &delivery.event_id,
                payload_sha256,
                effect_identity,
            )?;
        }
        store.acknowledge(
            &delivery.transaction_id,
            &delivery.event_id,
            &delivery.transaction_id,
        )?;
        Ok(())
    }

    pub(crate) async fn continue_terminal_delivery(
        &self,
        state: &AppState,
        delivery: &TerminalDeliveryIdentity,
    ) -> Result<Value> {
        let store = lifecycle_store(&delivery.commander_session_id)?;
        self.continue_terminal_delivery_with_store(state, delivery, &store)
            .await
    }

    async fn continue_terminal_delivery_with_store(
        &self,
        state: &AppState,
        delivery: &TerminalDeliveryIdentity,
        store: &SessionLifecycleStore,
    ) -> Result<Value> {
        let persisted = store
            .callback_continuations_for_replay()?
            .into_iter()
            .find(|record| {
                record.child_transaction_id == delivery.transaction_id
                    && record.child_event_id == delivery.event_id
            });
        let (continuation, callback_to_intake) = if let Some(record) = persisted {
            (record, None)
        } else {
            let callback = store
                .callbacks_for_replay()?
                .into_iter()
                .find(|record| {
                    record.transaction_id == delivery.transaction_id
                        && record.event_id == delivery.event_id
                })
                .ok_or_else(|| {
                    anyhow!(
                        "PERSISTED_CALLBACK_FOR_CONTINUATION_NOT_FOUND:{}:{}",
                        delivery.transaction_id,
                        delivery.event_id
                    )
                })?;
            let continuation = ContinuationDispatchRecord::from_callback(&callback)?;
            (continuation, Some(callback))
        };
        if continuation.commander_session_id != delivery.commander_session_id
            || continuation.child_transaction_id != delivery.transaction_id
            || continuation.child_event_id != delivery.event_id
            || continuation.child_runtime_id != delivery.runtime_id
            || delivery.callback_payload_sha256.as_deref()
                != Some(continuation.callback_payload_sha256.as_str())
            || delivery.callback_effect_identity.as_ref() != Some(&continuation.effect_identity)
        {
            return Err(anyhow!(
                "CONTINUATION_DELIVERY_IDENTITY_MISMATCH:{}:{}",
                delivery.transaction_id,
                delivery.event_id
            ));
        }
        if let Some(callback) = callback_to_intake {
            store.mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )?;
        }
        match store.prepare_callback_continuation(&continuation)? {
            ContinuationWriteOutcome::AlreadyAcknowledged => {
                return Ok(continuation_result(&continuation, "already_acknowledged"));
            }
            ContinuationWriteOutcome::AlreadyCompleted => {
                complete_and_ack_callback_continuation(store, &continuation)?;
                return Ok(continuation_result(
                    &continuation,
                    "acknowledged_after_restart",
                ));
            }
            ContinuationWriteOutcome::AlreadyDispatched => {
                if store.callback_continuation_completion_proven(&continuation)? {
                    complete_and_ack_callback_continuation(store, &continuation)?;
                    return Ok(continuation_result(
                        &continuation,
                        "reconciled_and_acknowledged_after_restart",
                    ));
                }
                return Err(anyhow!(
                    "CONTINUATION_DISPATCHED_RECONCILIATION_REQUIRED:{}:{}:{}",
                    continuation.request_id,
                    continuation.runtime_id,
                    continuation.lease_id
                ));
            }
            ContinuationWriteOutcome::Prepared | ContinuationWriteOutcome::AlreadyPrepared => {}
        }
        let prompt = serde_json::to_string(&continuation.parent_input)?;
        let input = json!({
            "runtime_id": continuation.runtime_id,
            "session_id": continuation.commander_session_id,
            "payload": {
                "prompt": prompt,
                "operator_override": false
            }
        });
        let request_id = continuation.request_id.clone();
        let internal_request = IpcRequest {
            request_id: request_id.clone(),
            kind: "call".to_string(),
            method: "execution.enqueue_turn".to_string(),
            payload: input,
            deadline_ms: None,
        };
        let identity = enqueue_turn_identity(&internal_request)
            .ok_or_else(|| anyhow!("CONTINUATION_ENQUEUE_IDENTITY_MISSING:{request_id}"))?;
        if identity.commander_session_id != continuation.commander_session_id
            || identity.child_session_id != continuation.commander_session_id
            || identity.runtime_id != continuation.runtime_id
            || identity.transaction_id != continuation.request_id
        {
            return Err(anyhow!(
                "CONTINUATION_ENQUEUE_IDENTITY_MISMATCH:{}",
                continuation.request_id
            ));
        }
        self.enqueue_turn_request_with_identity(
            state,
            internal_request.payload,
            &request_id,
            continuation.lease_id.clone(),
            Some(continuation),
        )
        .await
    }

    pub(crate) async fn recover_callback_continuations(
        &self,
        state: &AppState,
        commander_session_id: &str,
    ) -> Result<Vec<Value>> {
        let Some(store) = lifecycle_store_if_exists(commander_session_id)? else {
            return Ok(Vec::new());
        };
        let callbacks = store.callbacks_for_replay()?;
        let continuations = store.callback_continuations_for_replay()?;
        let mut deliveries = Vec::new();
        for callback in callbacks {
            deliveries.push(TerminalDeliveryIdentity {
                commander_session_id: callback.commander_session_id.clone(),
                transaction_id: callback.transaction_id.clone(),
                event_id: callback.event_id.clone(),
                runtime_id: callback.runtime_id.clone(),
                callback_payload_sha256: Some(callback.callback_payload_sha256.clone()),
                callback_effect_identity: Some(callback.effect_identity.clone()),
            });
        }
        for continuation in continuations {
            if deliveries.iter().any(|delivery| {
                delivery.transaction_id == continuation.child_transaction_id
                    && delivery.event_id == continuation.child_event_id
            }) {
                continue;
            }
            deliveries.push(TerminalDeliveryIdentity {
                commander_session_id: continuation.commander_session_id.clone(),
                transaction_id: continuation.child_transaction_id.clone(),
                event_id: continuation.child_event_id.clone(),
                runtime_id: continuation.child_runtime_id.clone(),
                callback_payload_sha256: Some(continuation.callback_payload_sha256.clone()),
                callback_effect_identity: Some(continuation.effect_identity.clone()),
            });
        }
        let mut recovered = Vec::new();
        for delivery in deliveries {
            recovered.push(self.continue_terminal_delivery(state, &delivery).await?);
        }
        Ok(recovered)
    }

    pub(crate) fn publish_terminal_callback(
        &self,
        mut delivery: TerminalDeliveryIdentity,
        transport_payload: Value,
    ) -> Result<(Value, TerminalDeliveryIdentity)> {
        let store = lifecycle_store(&delivery.commander_session_id)?;
        Self::publish_terminal_callback_from_store(&store, &mut delivery, transport_payload)
    }

    fn publish_terminal_callback_from_store(
        store: &SessionLifecycleStore,
        delivery: &mut TerminalDeliveryIdentity,
        transport_payload: Value,
    ) -> Result<(Value, TerminalDeliveryIdentity)> {
        let receipt = store.terminal_receipt(&delivery.transaction_id, &delivery.event_id)?;
        if receipt.child_session_id == receipt.commander_session_id {
            return Ok((transport_payload, delivery.clone()));
        }
        let parent_mission_revision_sha256 = receipt
            .audit_metadata
            .get("parent_mission_revision_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!("DELEGATED_LIFECYCLE_IDENTITY_MISSING:parent_mission_revision_sha256")
            })?;
        let delegated_input_sha256 = receipt
            .audit_metadata
            .get("delegated_input_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!("DELEGATED_LIFECYCLE_IDENTITY_MISSING:delegated_input_sha256")
            })?;
        let callback_payload = transport_payload
            .pointer("/payload/body/item/text")
            .cloned()
            .ok_or_else(|| anyhow!("TERMINAL_CALLBACK_PAYLOAD_MISSING:{}", delivery.event_id))?;
        let effect_id = transport_payload
            .pointer("/payload/body/item/id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "TERMINAL_CALLBACK_EFFECT_IDENTITY_MISSING:{}",
                    delivery.event_id
                )
            })?
            .to_string();
        require_terminal_callback_admission(
            store,
            &receipt,
            parent_mission_revision_sha256,
            delegated_input_sha256,
            Some(&effect_id),
        )?;
        let record = DurableCallbackRecord::new(
            &receipt,
            callback_payload,
            transport_payload,
            parent_mission_revision_sha256,
            delegated_input_sha256,
            CallbackEffectIdentity::Exact { effect_id },
        )?;
        store.publish_callback(&record)?;
        store.mark_callback_intaken(
            &record.transaction_id,
            &record.event_id,
            &record.callback_payload_sha256,
        )?;
        delivery.callback_payload_sha256 = Some(record.callback_payload_sha256.clone());
        delivery.callback_effect_identity = Some(record.effect_identity.clone());
        Ok((record.transport_payload, delivery.clone()))
    }

    pub(crate) fn replay_terminal_callbacks(
        &self,
        commander_session_id: &str,
        child_session_id: &str,
        transaction_id: &str,
    ) -> Result<Vec<(Value, TerminalDeliveryIdentity)>> {
        let Some(store) = lifecycle_store_if_exists(commander_session_id)? else {
            return Ok(Vec::new());
        };
        replay_terminal_callbacks_from_store(
            &store,
            commander_session_id,
            child_session_id,
            transaction_id,
        )
    }

    pub(crate) fn publish_terminal_failure_callback(
        &self,
        delivery: TerminalDeliveryIdentity,
    ) -> Result<Option<(Value, TerminalDeliveryIdentity)>> {
        let store = lifecycle_store(&delivery.commander_session_id)?;
        publish_terminal_failure_callback_from_store(&store, delivery)
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
                parent_mission_revision_sha256: None,
                delegated_input_sha256: None,
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
        receipt
            .audit_metadata
            .insert("session_state".to_string(), json!(recovery.session_state));
        receipt
            .audit_metadata
            .insert("dispatch_runtime_id".to_string(), json!(lease.runtime_id));
        receipt
            .audit_metadata
            .insert("dispatch_lease_id".to_string(), json!(lease.lease_id));
        if let Some(value) = &lease.parent_mission_revision_sha256 {
            receipt
                .audit_metadata
                .insert("parent_mission_revision_sha256".to_string(), json!(value));
        }
        if let Some(value) = &lease.delegated_input_sha256 {
            receipt
                .audit_metadata
                .insert("delegated_input_sha256".to_string(), json!(value));
        }
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
        Self::ensure_snapshot_terminal_receipt(&store, lease, snapshot, entry, terminal_state)
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
            snapshot
                .runtime_state
                .map_or(Value::Null, |state| json!(state)),
        );
        receipt
            .audit_metadata
            .insert("dispatch_runtime_id".to_string(), json!(lease.runtime_id));
        receipt
            .audit_metadata
            .insert("dispatch_lease_id".to_string(), json!(lease.lease_id));
        if let Some(value) = &lease.parent_mission_revision_sha256 {
            receipt
                .audit_metadata
                .insert("parent_mission_revision_sha256".to_string(), json!(value));
        }
        if let Some(value) = &lease.delegated_input_sha256 {
            receipt
                .audit_metadata
                .insert("delegated_input_sha256".to_string(), json!(value));
        }
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
                return Err(anyhow!("RUNTIME_CANCEL_LEASE_READ_UNEXPECTED:{other:?}"));
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

fn continuation_result(record: &ContinuationDispatchRecord, status: &str) -> Value {
    json!({
        "status": status,
        "request_id": record.request_id,
        "runtime_id": record.runtime_id,
        "lease_id": record.lease_id,
        "session_id": record.commander_session_id,
    })
}

fn require_successful_runtime_dispatch(status: u16, body: &Value) -> Result<()> {
    if status < 400 {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        body.pointer("/result/error")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("runtime worker failed")
    ))
}

fn complete_and_ack_callback_continuation(
    store: &SessionLifecycleStore,
    record: &ContinuationDispatchRecord,
) -> Result<()> {
    store.mark_callback_continuation_completed(record)?;
    store.acknowledge_callback(
        &record.child_transaction_id,
        &record.child_event_id,
        &record.callback_payload_sha256,
        &record.effect_identity,
    )?;
    store.acknowledge(
        &record.child_transaction_id,
        &record.child_event_id,
        &record.request_id,
    )?;
    store.mark_callback_continuation_acknowledged(record)?;
    Ok(())
}

fn replay_terminal_callbacks_from_store(
    store: &SessionLifecycleStore,
    commander_session_id: &str,
    child_session_id: &str,
    transaction_id: &str,
) -> Result<Vec<(Value, TerminalDeliveryIdentity)>> {
    let mut matching = Vec::new();
    for record in store.callbacks_for_replay()? {
        if record.transaction_id != transaction_id {
            continue;
        }
        if record.commander_session_id != commander_session_id
            || record.child_session_id != child_session_id
        {
            return Err(anyhow!(
                "CALLBACK_REPLAY_IDENTITY_MISMATCH:commander={commander_session_id},session={child_session_id},transaction={transaction_id}"
            ));
        }
        store.mark_callback_intaken(
            &record.transaction_id,
            &record.event_id,
            &record.callback_payload_sha256,
        )?;
        matching.push(record);
    }
    matching
        .into_iter()
        .map(|record| {
            let delivery = TerminalDeliveryIdentity {
                commander_session_id: record.commander_session_id.clone(),
                transaction_id: record.transaction_id.clone(),
                event_id: record.event_id.clone(),
                runtime_id: record.runtime_id.clone(),
                callback_payload_sha256: Some(record.callback_payload_sha256.clone()),
                callback_effect_identity: Some(record.effect_identity.clone()),
            };
            Ok((record.transport_payload, delivery))
        })
        .collect()
}

fn publish_terminal_failure_callback_from_store(
    store: &SessionLifecycleStore,
    mut delivery: TerminalDeliveryIdentity,
) -> Result<Option<(Value, TerminalDeliveryIdentity)>> {
    let receipt = store.terminal_receipt(&delivery.transaction_id, &delivery.event_id)?;
    if receipt.child_session_id == receipt.commander_session_id
        || receipt.terminal_state == TerminalState::Completed
    {
        return Ok(None);
    }
    let parent_mission_revision_sha256 = receipt
        .audit_metadata
        .get("parent_mission_revision_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("DELEGATED_LIFECYCLE_IDENTITY_MISSING:parent_mission_revision_sha256")
        })?;
    let delegated_input_sha256 = receipt
        .audit_metadata
        .get("delegated_input_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("DELEGATED_LIFECYCLE_IDENTITY_MISSING:delegated_input_sha256"))?;
    require_terminal_callback_admission(
        store,
        &receipt,
        parent_mission_revision_sha256,
        delegated_input_sha256,
        None,
    )?;
    let receipt_sha256 = session_lifecycle::terminal_receipt_sha256(&receipt)?;
    let callback_payload = json!({
        "type": "terminal_failure",
        "classification": "UNSETTLED_EFFECT",
        "terminal_receipt_sha256": receipt_sha256,
        "runtime_id": receipt.runtime_id,
        "lease_id": receipt.lease_id,
        "terminal_state": receipt.terminal_state,
        "parent_mission_revision_sha256": parent_mission_revision_sha256,
    });
    let transport_payload = json!({
        "request_id": delivery.transaction_id,
        "kind": "gateway.callback",
        "method": "session.terminal_failure",
        "payload": {
            "session_id": receipt.child_session_id,
            "runtime_id": receipt.runtime_id,
            "body": { "item": callback_payload }
        }
    });
    let record = DurableCallbackRecord::new(
        &receipt,
        callback_payload,
        transport_payload,
        parent_mission_revision_sha256,
        delegated_input_sha256,
        CallbackEffectIdentity::UnsettledEffect {
            classification: "terminal_receipt_without_settled_effect_evidence".to_string(),
            evidence_sha256: receipt_sha256,
        },
    )?;
    store.publish_callback(&record)?;
    store.mark_callback_intaken(
        &record.transaction_id,
        &record.event_id,
        &record.callback_payload_sha256,
    )?;
    delivery.callback_payload_sha256 = Some(record.callback_payload_sha256.clone());
    delivery.callback_effect_identity = Some(record.effect_identity.clone());
    Ok(Some((record.transport_payload, delivery)))
}

fn require_terminal_callback_admission(
    store: &SessionLifecycleStore,
    receipt: &TerminalReceipt,
    parent_mission_revision_sha256: &str,
    delegated_input_sha256: &str,
    exact_effect_id: Option<&str>,
) -> Result<ChildAdmissionRecord> {
    let admission = store
        .child_admission(&receipt.child_session_id)?
        .ok_or_else(|| {
            anyhow!(
                "TERMINAL_CALLBACK_CHILD_ADMISSION_NOT_DURABLE:{}",
                receipt.child_session_id
            )
        })?;
    if admission.parent_session_id != receipt.commander_session_id
        || admission.parent_mission_revision_sha256 != parent_mission_revision_sha256
        || admission.child_runtime_id != receipt.runtime_id
        || admission.child_transaction_id != receipt.transaction_id
        || admission.child_lease_id != receipt.lease_id
        || admission.callback_request_id != receipt.transaction_id
        || admission.delegated_input_sha256 != delegated_input_sha256
    {
        return Err(anyhow!(
            "TERMINAL_CALLBACK_ADMISSION_IDENTITY_MISMATCH:{}",
            receipt.child_session_id
        ));
    }
    if let Some(effect_id) = exact_effect_id
        && admission.effect_id != effect_id
    {
        return Err(anyhow!(
            "TERMINAL_CALLBACK_EFFECT_IDENTITY_CONFLICT:expected={},actual={effect_id}",
            admission.effect_id
        ));
    }
    Ok(admission)
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
                callback_payload_sha256: None,
                callback_effect_identity: None,
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
        other => Err(anyhow!("RUNTIME_CALLBACK_LEASE_READ_UNEXPECTED:{other:?}")),
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
        parent_mission_revision_sha256: lifecycle.parent_mission_revision_sha256.clone(),
        delegated_input_sha256: lifecycle.delegated_input_sha256.clone(),
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
    match snapshot.runtime_state {
        Some(state) => runtime_terminal_state(state),
        None => {
            if snapshot.session_state != projection.state {
                return Err(anyhow!(
                    "RUNTIME_CALLBACK_SESSION_STATE_MISMATCH:runtime={},snapshot={:?},projection={:?}",
                    snapshot.runtime_id,
                    snapshot.session_state,
                    projection.state
                ));
            }
            terminal_state(projection.state)
        }
    }
}

fn terminal_feed_entry_for_runtime(session_id: &str, runtime_id: &str) -> Result<SessionFeedEntry> {
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
        anyhow!("TERMINAL_FEED_EVENT_NOT_FOUND:session={session_id},runtime={runtime_id}")
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

fn is_historical_terminal_runtime(active_runtime_id: Option<&str>, runtime_id: &str) -> bool {
    active_runtime_id != Some(runtime_id)
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

fn validate_delegated_input_digest(request: &mut RunAgentRequest, delegated: bool) -> Result<()> {
    if !delegated {
        return Ok(());
    }
    let recomputed_digest = request
        .effective_prompt_sha256()
        .ok_or_else(|| anyhow!("DELEGATED_LIFECYCLE_IDENTITY_MISSING:effective_prompt"))?;
    if request.delegated_input_sha256.as_deref() != Some(&recomputed_digest) {
        return Err(anyhow!(
            "DELEGATED_INPUT_SHA256_MISMATCH:expected={recomputed_digest},actual={}",
            request
                .delegated_input_sha256
                .as_deref()
                .unwrap_or("missing")
        ));
    }
    request.delegated_input_sha256 = Some(recomputed_digest);
    Ok(())
}

fn read_session_snapshot(session_id: &str) -> Result<Option<SessionSnapshot>> {
    match session_log_contract::client::call_service(&SessionLogCommand::GetSession(
        GetSessionRequest {
            session_id: session_id.to_string(),
        },
    ))? {
        SessionLogResponse::Session { session } => Ok(session.map(|session| *session)),
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => Err(anyhow!(
            "unexpected session_db response while reading {session_id}: {other:?}"
        )),
    }
}

fn create_child_session(
    parent: &SessionSnapshot,
    request: &RegisterChildSessionRequest,
) -> Result<()> {
    let metadata = &parent.metadata;
    let command = SessionLogCommand::CreateSession(Box::new(CreateSessionRequest {
        command_id: format!("child-admission-create-{}", request.callback_request_id),
        session_id: request.child_session_id.clone(),
        creation_command: SessionCommand::RegisterChildSession {
            parent_id: request.parent_session_id.clone(),
        },
        copy_context: false,
        workspace: parent.workspace.clone(),
        session_directory: request.session_directory.clone(),
        name: request.session_name.clone(),
        created_at: request.created_at_ms,
        model: metadata.model.clone(),
        agent: metadata.agent.clone(),
        session_type: metadata.session_type.clone(),
        kill_processes_on_start: metadata.kill_processes_on_start,
        validator_enabled: metadata.validator_enabled,
        force_planning: metadata.force_planning,
        model_variant: metadata.model_variant.clone(),
        model_acceleration_enabled: metadata.model_acceleration_enabled,
        disable_permission_restrictions: metadata.disable_permission_restrictions,
        use_last_tool_call_response: metadata.use_last_tool_call_response,
        auto_session_name: false,
        initial_task_plan_patch: None,
    }));
    match session_log_contract::client::call_service(&command)? {
        SessionLogResponse::SessionCommandApplied { result }
            if result.projection.parent_id.as_deref()
                == Some(request.parent_session_id.as_str()) =>
        {
            Ok(())
        }
        SessionLogResponse::SessionCommandApplied { .. } => Err(anyhow!(
            "CHILD_ADMISSION_PARENT_IDENTITY_MISMATCH:{}",
            request.child_session_id
        )),
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => Err(anyhow!(
            "unexpected session_db child creation response: {other:?}"
        )),
    }
}

fn ensure_child_parent_identity(child: &SessionSnapshot, parent_session_id: &str) -> Result<()> {
    if child.lifecycle_projection.parent_id.as_deref() == Some(parent_session_id) {
        return Ok(());
    }
    Err(anyhow!(
        "CHILD_ADMISSION_PARENT_IDENTITY_MISMATCH:{}",
        child.session_id
    ))
}

fn child_runtime_is_registered(request: &RegisterChildSessionRequest) -> Result<bool> {
    match session_log_contract::client::call_service(&SessionLogCommand::GetRuntimeLease(
        GetRuntimeLeaseRequest {
            runtime_id: request.child_runtime_id.clone(),
            database_path: None,
        },
    ))? {
        SessionLogResponse::RuntimeLeaseRead {
            runtime: Some(runtime),
        } => {
            validate_child_runtime_identity(&runtime, request)?;
            Ok(true)
        }
        SessionLogResponse::RuntimeLeaseRead { runtime: None } => Ok(false),
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => Err(anyhow!(
            "unexpected session_db runtime read response for {}: {other:?}",
            request.child_runtime_id
        )),
    }
}

fn validate_child_runtime_identity(
    runtime: &RuntimeLeaseSnapshot,
    request: &RegisterChildSessionRequest,
) -> Result<()> {
    let lifecycle = runtime.lifecycle.as_ref();
    let exact = runtime.runtime_id == request.child_runtime_id
        && runtime.session_id == request.child_session_id
        && runtime.lease_id.as_deref() == Some(request.child_lease_id.as_str())
        && lifecycle.is_some_and(|identity| {
            identity.commander_session_id == request.parent_session_id
                && identity.transaction_id == request.child_transaction_id
                && identity.parent_mission_revision_sha256.as_deref()
                    == Some(request.parent_mission_revision_sha256.as_str())
                && identity.delegated_input_sha256.as_deref()
                    == Some(request.delegated_input_sha256.as_str())
                && identity.dispatch_runtime_id == request.child_runtime_id
                && identity.dispatch_lease_id == request.child_lease_id
        });
    if exact {
        Ok(())
    } else {
        Err(anyhow!(
            "CHILD_ADMISSION_RUNTIME_IDENTITY_CONFLICT:{}",
            request.child_runtime_id
        ))
    }
}

fn child_admission_record(request: &RegisterChildSessionRequest) -> ChildAdmissionRecord {
    ChildAdmissionRecord::new(
        &request.parent_session_id,
        &request.parent_mission_revision_sha256,
        &request.child_session_id,
        &request.child_runtime_id,
        &request.child_transaction_id,
        &request.child_lease_id,
        &request.callback_request_id,
        &request.effect_id,
        &request.delegated_input_sha256,
        canonical_value_sha256(&request.execution_payload),
        &request.session_directory,
        &request.session_name,
        request.created_at_ms,
    )
}

fn child_execution_payload(request: &RegisterChildSessionRequest) -> Result<Value> {
    let mut payload = request.execution_payload.clone();
    let object = payload
        .as_object_mut()
        .ok_or_else(|| anyhow!("CHILD_ADMISSION_PAYLOAD_INVALID:execution_payload"))?;
    object.insert(
        "parent_session_id".to_string(),
        Value::String(request.parent_session_id.clone()),
    );
    object.insert(
        "parent_mission_revision_sha256".to_string(),
        Value::String(request.parent_mission_revision_sha256.clone()),
    );
    object.insert(
        "delegated_input_sha256".to_string(),
        Value::String(request.delegated_input_sha256.clone()),
    );
    object.insert(
        "lifecycle".to_string(),
        json!({
            "transaction_id": request.child_transaction_id,
            "commander_session_id": request.parent_session_id,
            "parent_mission_revision_sha256": request.parent_mission_revision_sha256,
            "delegated_input_sha256": request.delegated_input_sha256,
            "task_id": null,
            "goal_id": null,
            "operator_override": false,
        }),
    );
    Ok(payload)
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

fn lifecycle_store_if_exists(commander_session_id: &str) -> Result<Option<SessionLifecycleStore>> {
    let base = session_log_contract::client::default_db_dir().join("session_lifecycle_v1");
    let root = commander_store_path(&base, commander_session_id)?;
    if !root.exists() {
        return Ok(None);
    }
    Ok(Some(SessionLifecycleStore::open(
        root,
        commander_session_id,
        LifecycleConfig::default(),
    )?))
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
        EnqueueTurnRequest, ExecutionService, RetryRuntimeIdentity,
        RouterRecoveryCloseRuntimeRequest, RuntimeLease, TerminalDeliveryIdentity,
        complete_and_ack_callback_continuation, failed_session_retry_root,
        failed_session_runtime_fallback, intake_terminal_receipt, is_historical_terminal_runtime,
        payload_to_run_agent_request, publish_terminal_failure_callback_from_store,
        replay_terminal_callbacks_from_store, require_successful_runtime_dispatch,
        runtime_lease_from_snapshot, runtime_terminal_state_from_snapshot,
        terminal_runtime_is_current, validate_delegated_input_digest,
        validate_child_runtime_identity, validate_terminalization_identity,
    };
    use crate::{build_state, services::manager::ServiceManager};
    use lifecycle::{RuntimeState, SessionProjection, SessionState, TaskPlan};
    use runtime_contract::RunAgentRequest;
    use serde_json::json;
    use session_lifecycle::{
        CallbackEffectIdentity, ChildAdmissionRecord, ContinuationDispatchRecord,
        DurableCallbackRecord, LifecycleConfig, SessionLifecycleStore, TerminalReceipt,
        TerminalReceiptIdentity, TerminalState, commander_store_path,
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
        assert!(
            failed_session_runtime_fallback(&projection)
                .expect_err("failed session without lineage must fail closed")
                .to_string()
                .contains("FAILED_SESSION_MISSING_RUNTIME_LINEAGE")
        );
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
        assert!(
            error
                .to_string()
                .contains("FAILED_SESSION_RETRY_ROOT_INPUT_MISMATCH")
        );
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
        assert!(
            service
                .sessions
                .lock()
                .contains_key("terminalizing-session")
        );

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
            parent_mission_revision_sha256: None,
            delegated_input_sha256: None,
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
        assert!(
            validate_terminalization_identity(&snapshot, &lease, "session-exact", "runtime-exact",)
                .expect_err("lease drift must fail closed")
                .to_string()
                .contains("RUNTIME_TERMINALIZATION_DURABLE_IDENTITY_MISMATCH")
        );
    }

    #[test]
    fn child_runtime_replay_requires_exact_durable_lifecycle_identity() {
        let request = router_contract::RegisterChildSessionRequest {
            parent_session_id: "commander-exact".to_string(),
            parent_mission_revision_sha256: "a".repeat(64),
            child_session_id: "child-exact".to_string(),
            child_runtime_id: "runtime-exact".to_string(),
            child_transaction_id: "transaction-exact".to_string(),
            child_lease_id: "lease-exact".to_string(),
            callback_request_id: "transaction-exact".to_string(),
            effect_id: "runtime-exact.message".to_string(),
            delegated_input_sha256: "b".repeat(64),
            session_directory: "/tmp/child-exact".to_string(),
            session_name: "child exact".to_string(),
            created_at_ms: 1_786_845_600_000,
            execution_payload: json!({"prompt": "delegated prompt"}),
        };
        let lifecycle = RuntimeLifecycleIdentity {
            commander_session_id: request.parent_session_id.clone(),
            transaction_id: request.child_transaction_id.clone(),
            parent_mission_revision_sha256: Some(
                request.parent_mission_revision_sha256.clone(),
            ),
            delegated_input_sha256: Some(request.delegated_input_sha256.clone()),
            task_id: None,
            goal_id: None,
            operator_override: false,
            dispatch_runtime_id: request.child_runtime_id.clone(),
            dispatch_lease_id: request.child_lease_id.clone(),
            receipt_event_seq: 0,
        };
        let mut snapshot = RuntimeLeaseSnapshot {
            database_path: "/tmp/session.sqlite3".to_string(),
            runtime_id: request.child_runtime_id.clone(),
            session_id: request.child_session_id.clone(),
            lifecycle: Some(lifecycle),
            lease_id: Some(request.child_lease_id.clone()),
            lease_active: false,
            revision: 1,
            last_event_seq: 1,
            terminal: true,
            session_event_seq: 1,
            session_state: SessionState::Running,
            runtime_state: Some(RuntimeState::Cancelled),
        };
        validate_child_runtime_identity(&snapshot, &request)
            .expect("exact runtime replay identity");

        snapshot.lease_id = Some("changed-lease".to_string());
        assert!(
            validate_child_runtime_identity(&snapshot, &request)
                .expect_err("changed runtime lease must conflict")
                .to_string()
                .contains("CHILD_ADMISSION_RUNTIME_IDENTITY_CONFLICT")
        );
    }

    #[test]
    fn durable_lifecycle_snapshot_restores_callback_identity_and_runtime_terminal_semantics() {
        let lifecycle = RuntimeLifecycleIdentity {
            commander_session_id: "commander-durable".to_string(),
            transaction_id: "transaction-durable".to_string(),
            parent_mission_revision_sha256: None,
            delegated_input_sha256: None,
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
            session_state: SessionState::Running,
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
                .expect("runtime terminal state must survive a later session continuation"),
            TerminalState::Cancelled
        );

        let mut legacy_snapshot = snapshot.clone();
        legacy_snapshot.runtime_state = None;
        let error = runtime_terminal_state_from_snapshot(&legacy_snapshot, &projection)
            .expect_err("legacy snapshots still require matching session projections");
        assert!(
            error
                .to_string()
                .contains("RUNTIME_CALLBACK_SESSION_STATE_MISMATCH")
        );
    }

    #[test]
    fn historical_terminal_runtime_requires_no_active_runtime_ownership() {
        assert!(is_historical_terminal_runtime(None, "runtime-old"));
        assert!(is_historical_terminal_runtime(
            Some("runtime-current"),
            "runtime-old"
        ));
        assert!(!is_historical_terminal_runtime(
            Some("runtime-current"),
            "runtime-current"
        ));
    }

    #[test]
    fn snapshot_terminal_receipt_replays_or_reuses_runtime_writer_receipt() {
        let lifecycle = RuntimeLifecycleIdentity {
            commander_session_id: "commander-replay".to_string(),
            transaction_id: "transaction-replay".to_string(),
            parent_mission_revision_sha256: None,
            delegated_input_sha256: None,
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
        assert!(
            terminal_runtime_is_current(
                &projection,
                "child-1",
                "dispatch-runtime",
                "fallback-runtime",
            )
            .expect("latest fallback should be accepted")
        );
        let error = terminal_runtime_is_current(
            &projection,
            "child-1",
            "dispatch-runtime",
            "dispatch-runtime",
        )
        .expect_err("non-latest dispatch runtime must not terminate a fallback chain");
        assert!(
            error
                .to_string()
                .contains("TERMINAL_FEED_RUNTIME_CHAIN_MISMATCH")
        );
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
            parent_mission_revision_sha256: None,
            delegated_input_sha256: None,
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
        assert!(
            response["terminalization_error"]
                .as_str()
                .is_some_and(|error| !error.trim().is_empty())
        );
        assert!(
            service
                .sessions
                .lock()
                .get("active-session")
                .is_some_and(|lease| lease.terminalizing)
        );
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
        assert!(
            service
                .sessions
                .lock()
                .get("command-session")
                .is_some_and(|lease| lease.terminalizing)
        );
        assert!(
            !service
                .retained_slots
                .lock()
                .contains_key("command-session")
        );
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

        assert!(
            !service
                .retained_slots
                .lock()
                .contains_key("cancelled-session")
        );
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

    fn callback_receipt(terminal_state: TerminalState) -> TerminalReceipt {
        let mut receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                "transaction-callback",
                "event-callback",
                0,
                "commander-callback",
                "child-callback",
                "runtime-callback",
                "lease-callback",
            ),
            terminal_state,
            1_786_845_600_000,
        );
        receipt.audit_metadata.insert(
            "parent_mission_revision_sha256".to_string(),
            json!("a".repeat(64)),
        );
        receipt.audit_metadata.insert(
            "delegated_input_sha256".to_string(),
            json!(session_lifecycle::canonical_value_sha256(&json!(
                "delegated prompt"
            ))),
        );
        receipt
    }

    fn callback_delivery() -> TerminalDeliveryIdentity {
        TerminalDeliveryIdentity {
            commander_session_id: "commander-callback".to_string(),
            transaction_id: "transaction-callback".to_string(),
            event_id: "event-callback".to_string(),
            runtime_id: "runtime-callback".to_string(),
            callback_payload_sha256: None,
            callback_effect_identity: None,
        }
    }

    fn durable_callback_fixture(
        root: &std::path::Path,
        terminal_state: TerminalState,
    ) -> (SessionLifecycleStore, TerminalDeliveryIdentity) {
        let store =
            SessionLifecycleStore::open(root, "commander-callback", LifecycleConfig::default())
                .expect("callback store");
        let receipt = callback_receipt(terminal_state);
        store
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        store
            .intake("transaction-callback", "event-callback")
            .expect("receipt intake");
        (store, callback_delivery())
    }

    fn admit_callback_child(
        store: &SessionLifecycleStore,
        parent_mission_revision_sha256: &str,
    ) -> ChildAdmissionRecord {
        let admission = ChildAdmissionRecord::new(
            "commander-callback",
            parent_mission_revision_sha256,
            "child-callback",
            "runtime-callback",
            "transaction-callback",
            "lease-callback",
            "transaction-callback",
            "runtime-callback.message",
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            session_lifecycle::canonical_value_sha256(&json!({"prompt": "delegated prompt"})),
            "/tmp/child-callback",
            "delegated child callback",
            1_786_845_600_000,
        );
        store
            .admit_child(&admission)
            .expect("durable child admission");
        admission
    }

    #[test]
    fn public_child_terminal_callback_requires_admission_and_intakes_once() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let (store, delivery) = durable_callback_fixture(root.path(), TerminalState::Completed);
        let transport_payload = json!({
            "request_id": "transaction-callback",
            "kind": "gateway.callback",
            "method": "session.agent_message",
            "payload": {
                "session_id": "child-callback",
                "runtime_id": "runtime-callback",
                "body": {
                    "type": "item.completed",
                    "item": {
                        "id": "runtime-callback.message",
                        "type": "agent_message",
                        "text": "child result"
                    }
                }
            }
        });

        let mut missing_admission_delivery = delivery.clone();
        assert_eq!(
            ExecutionService::publish_terminal_callback_from_store(
                &store,
                &mut missing_admission_delivery,
                transport_payload.clone(),
            )
            .expect_err("unadmitted child callback must fail closed")
            .to_string(),
            "TERMINAL_CALLBACK_CHILD_ADMISSION_NOT_DURABLE:child-callback"
        );
        assert_eq!(
            store
                .readback()
                .expect("pre-admission readback")
                .pending_callbacks,
            0
        );

        admit_callback_child(&store, &"a".repeat(64));

        let mut first_delivery = delivery.clone();
        let first = ExecutionService::publish_terminal_callback_from_store(
            &store,
            &mut first_delivery,
            transport_payload.clone(),
        )
        .expect("first callback publication");
        let mut replay_delivery = delivery;
        let replay = ExecutionService::publish_terminal_callback_from_store(
            &store,
            &mut replay_delivery,
            transport_payload,
        )
        .expect("identical callback replay");
        assert_eq!(replay, first);
        assert_eq!(
            first_delivery.callback_effect_identity,
            Some(CallbackEffectIdentity::Exact {
                effect_id: "runtime-callback.message".to_string(),
            })
        );
        let readback = store.readback().expect("callback intake readback");
        assert_eq!(readback.pending_callbacks, 0);
        assert_eq!(readback.intaken_callbacks, 1);
        assert_eq!(readback.acknowledged_callbacks, 0);

        let mut conflicting_delivery = first_delivery;
        let conflict = json!({
            "payload": {"body": {"item": {
                "id": "foreign.message",
                "text": "child result"
            }}}
        });
        assert!(
            ExecutionService::publish_terminal_callback_from_store(
                &store,
                &mut conflicting_delivery,
                conflict,
            )
            .expect_err("changed effect identity must conflict")
            .to_string()
            .contains("TERMINAL_CALLBACK_EFFECT_IDENTITY_CONFLICT")
        );
    }

    #[test]
    fn active_child_replay_after_forwarder_loss_converges_without_second_execution() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = SessionLifecycleStore::open(
            root.path(),
            "commander-callback",
            LifecycleConfig::default(),
        )
        .expect("callback store");
        admit_callback_child(&store, &"a".repeat(64));
        assert!(store.callbacks_for_replay().expect("active child callbacks").is_empty());

        let original_router = ExecutionService::new();
        assert!(original_router.sessions.lock().is_empty());
        drop(original_router);

        let receipt = callback_receipt(TerminalState::Completed);
        store
            .write_terminal_receipt(&receipt)
            .expect("later terminal receipt");
        store
            .intake(&receipt.transaction_id, &receipt.event_id)
            .expect("later terminal intake");
        let transport = json!({
            "request_id": "transaction-callback",
            "kind": "gateway.callback",
            "method": "session.agent_message",
            "payload": {"body": {"item": {
                "id": "runtime-callback.message",
                "text": "child result"
            }}}
        });
        let mut first_delivery = callback_delivery();
        ExecutionService::publish_terminal_callback_from_store(
            &store,
            &mut first_delivery,
            transport.clone(),
        )
        .expect("replacement forwarder terminal publication");
        let mut replay_delivery = callback_delivery();
        ExecutionService::publish_terminal_callback_from_store(
            &store,
            &mut replay_delivery,
            transport,
        )
        .expect("identical forwarder replay");

        let callbacks = store.callbacks_for_replay().expect("single callback replay");
        assert_eq!(callbacks.len(), 1);
        let continuation =
            ContinuationDispatchRecord::from_callback(&callbacks[0]).expect("continuation");
        store
            .prepare_callback_continuation(&continuation)
            .expect("prepare continuation");
        store
            .mark_callback_continuation_dispatched(&continuation)
            .expect("dispatch continuation");
        store
            .mark_callback_continuation_completed(&continuation)
            .expect("complete continuation");
        complete_and_ack_callback_continuation(&store, &continuation).expect("first ack");
        complete_and_ack_callback_continuation(&store, &continuation).expect("replayed ack");

        let restarted_router = ExecutionService::new();
        assert!(restarted_router.sessions.lock().is_empty());
        let readback = store.readback().expect("terminal convergence readback");
        assert_eq!(readback.intaken_callbacks, 1);
        assert_eq!(readback.acknowledged_callbacks, 1);
        assert_eq!(readback.acknowledged_receipts, 1);
        assert!(store.callbacks_for_replay().expect("post-ack callbacks").is_empty());
        assert!(
            store
                .callback_continuations_for_replay()
                .expect("post-ack continuations")
                .is_empty()
        );
    }

    #[test]
    fn fresh_turn_and_router_restart_replay_without_in_memory_lease() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let (store, delivery) = durable_callback_fixture(root.path(), TerminalState::Completed);
        let record = DurableCallbackRecord::new(
            &store
                .terminal_receipt(&delivery.transaction_id, &delivery.event_id)
                .expect("receipt"),
            json!("child result"),
            json!({"kind": "gateway.callback", "payload": {"body": {"item": {"id": "message-1", "text": "child result"}}}}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".to_string(),
            },
        )
        .expect("callback");
        store.publish_callback(&record).expect("pending callback");
        let fresh_service = ExecutionService::new();
        assert!(fresh_service.sessions.lock().is_empty());

        let first = replay_terminal_callbacks_from_store(
            &store,
            "commander-callback",
            "child-callback",
            "transaction-callback",
        )
        .expect("fresh replay before lease");
        assert_eq!(first.len(), 1);
        let readback = store.readback().expect("durable intake readback");
        assert_eq!(readback.pending_callbacks, 0);
        assert_eq!(readback.intaken_callbacks, 1);
        assert_eq!(readback.acknowledged_callbacks, 0);

        let restarted_service = ExecutionService::new();
        assert!(restarted_service.sessions.lock().is_empty());
        let duplicate = replay_terminal_callbacks_from_store(
            &store,
            "commander-callback",
            "child-callback",
            "transaction-callback",
        )
        .expect("restart replay without lease");
        assert_eq!(duplicate, first);
        assert_eq!(store.readback().expect("readback").intaken_callbacks, 1);
    }

    #[test]
    fn callback_continuation_completes_and_acks_exactly_once() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let (store, delivery) = durable_callback_fixture(root.path(), TerminalState::Completed);
        let callback = DurableCallbackRecord::new(
            &store
                .terminal_receipt(&delivery.transaction_id, &delivery.event_id)
                .expect("receipt"),
            json!("child result"),
            json!({"kind": "gateway.callback", "payload": {"body": {"item": {"id": "message-1", "text": "child result"}}}}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".to_string(),
            },
        )
        .expect("callback");
        store.publish_callback(&callback).expect("publish callback");
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake callback");
        let continuation =
            ContinuationDispatchRecord::from_callback(&callback).expect("continuation");
        store
            .prepare_callback_continuation(&continuation)
            .expect("prepare continuation");

        store
            .mark_callback_continuation_dispatched(&continuation)
            .expect("parent dispatch");
        complete_and_ack_callback_continuation(&store, &continuation).expect("first completion");
        complete_and_ack_callback_continuation(&store, &continuation).expect("duplicate replay");

        let readback = store.readback().expect("readback");
        assert_eq!(readback.acknowledged_callbacks, 1);
        assert_eq!(readback.acknowledged_receipts, 1);
        assert!(
            store
                .callbacks_for_replay()
                .expect("callback replay")
                .is_empty()
        );
        assert!(
            store
                .callback_continuations_for_replay()
                .expect("continuation replay")
                .is_empty()
        );
        assert_eq!(
            store
                .prepare_callback_continuation(&continuation)
                .expect("duplicate successful replay"),
            session_lifecycle::ContinuationWriteOutcome::AlreadyAcknowledged
        );
    }

    #[test]
    fn registered_parent_failure_remains_dispatched_and_unacknowledged() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let (store, delivery) = durable_callback_fixture(root.path(), TerminalState::Completed);
        let callback = DurableCallbackRecord::new(
            &store
                .terminal_receipt(&delivery.transaction_id, &delivery.event_id)
                .expect("receipt"),
            json!("child result"),
            json!({"kind": "gateway.callback", "payload": {"body": {"item": {"id": "message-1", "text": "child result"}}}}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".to_string(),
            },
        )
        .expect("callback");
        store.publish_callback(&callback).expect("publish callback");
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake callback");
        let continuation =
            ContinuationDispatchRecord::from_callback(&callback).expect("continuation");
        store
            .prepare_callback_continuation(&continuation)
            .expect("prepare continuation");
        assert_eq!(
            store
                .callback_continuations_for_replay()
                .expect("pre-registration continuation")[0]
                .state,
            session_lifecycle::ContinuationDispatchState::Prepared
        );
        assert_eq!(
            store
                .readback()
                .expect("registration failure readback")
                .acknowledged_callbacks,
            0
        );
        store
            .mark_callback_continuation_dispatched(&continuation)
            .expect("runtime registered and activated");
        assert_eq!(
            require_successful_runtime_dispatch(
                500,
                &json!({"result": {"error": "provider failed"}}),
            )
            .expect_err("provider failure must stop before completion")
            .to_string(),
            "provider failed"
        );

        let readback = store.readback().expect("failure readback");
        assert_eq!(readback.acknowledged_callbacks, 0);
        assert_eq!(readback.acknowledged_receipts, 0);
        assert_eq!(
            store
                .callback_continuations_for_replay()
                .expect("uncertain continuation")[0]
                .state,
            session_lifecycle::ContinuationDispatchState::Dispatched
        );
    }

    #[tokio::test]
    async fn callback_ack_completed_restart_recovers_without_second_execution() {
        let commander_session_id = format!("commander-callback-{}", uuid::Uuid::new_v4());
        let default_store_path = commander_store_path(
            &session_log_contract::client::default_db_dir().join("session_lifecycle_v1"),
            &commander_session_id,
        )
        .expect("default callback store path");
        assert!(
            !default_store_path.exists(),
            "test identity must not preexist in the default lifecycle root"
        );
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = SessionLifecycleStore::open(
            root.path(),
            &commander_session_id,
            LifecycleConfig::default(),
        )
        .expect("callback store");
        let mut receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                "transaction-callback",
                "event-callback",
                0,
                &commander_session_id,
                "child-callback",
                "runtime-callback",
                "lease-callback",
            ),
            TerminalState::Completed,
            1_786_845_600_000,
        );
        receipt.audit_metadata.insert(
            "parent_mission_revision_sha256".to_string(),
            json!("a".repeat(64)),
        );
        receipt.audit_metadata.insert(
            "delegated_input_sha256".to_string(),
            json!(session_lifecycle::canonical_value_sha256(&json!(
                "delegated prompt"
            ))),
        );
        store
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        store
            .intake("transaction-callback", "event-callback")
            .expect("receipt intake");
        let callback = DurableCallbackRecord::new(
            &receipt,
            json!("child result"),
            json!({"kind": "gateway.callback", "payload": {"body": {"item": {"id": "message-1", "text": "child result"}}}}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".to_string(),
            },
        )
        .expect("callback");
        store.publish_callback(&callback).expect("publish callback");
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake callback");
        let continuation =
            ContinuationDispatchRecord::from_callback(&callback).expect("continuation");
        store
            .prepare_callback_continuation(&continuation)
            .expect("prepare continuation");
        store
            .mark_callback_continuation_dispatched(&continuation)
            .expect("durable parent dispatch before completion");
        store
            .mark_callback_continuation_completed(&continuation)
            .expect("durable parent completion before crash");
        store
            .acknowledge_callback(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
                &callback.effect_identity,
            )
            .expect("durable callback ack before crash");
        store
            .acknowledge(
                &callback.transaction_id,
                &callback.event_id,
                &continuation.request_id,
            )
            .expect("durable receipt ack before crash");
        let delivery = TerminalDeliveryIdentity {
            commander_session_id: commander_session_id.clone(),
            transaction_id: callback.transaction_id.clone(),
            event_id: callback.event_id.clone(),
            runtime_id: callback.runtime_id.clone(),
            callback_payload_sha256: Some(callback.callback_payload_sha256.clone()),
            callback_effect_identity: Some(callback.effect_identity.clone()),
        };
        drop(store);
        let store = SessionLifecycleStore::open(
            root.path(),
            &commander_session_id,
            LifecycleConfig::default(),
        )
        .expect("reopen callback store after restart");

        let state = build_state();
        let service = ExecutionService::new();
        for (field, changed) in [
            (
                "transaction",
                TerminalDeliveryIdentity {
                    transaction_id: "changed-transaction".to_string(),
                    ..delivery.clone()
                },
            ),
            (
                "event",
                TerminalDeliveryIdentity {
                    event_id: "changed-event".to_string(),
                    ..delivery.clone()
                },
            ),
            (
                "runtime",
                TerminalDeliveryIdentity {
                    runtime_id: "changed-runtime".to_string(),
                    ..delivery.clone()
                },
            ),
            (
                "payload",
                TerminalDeliveryIdentity {
                    callback_payload_sha256: Some("b".repeat(64)),
                    ..delivery.clone()
                },
            ),
            (
                "effect",
                TerminalDeliveryIdentity {
                    callback_effect_identity: Some(CallbackEffectIdentity::Exact {
                        effect_id: "changed-effect".to_string(),
                    }),
                    ..delivery.clone()
                },
            ),
        ] {
            let error = service
                .continue_terminal_delivery_with_store(&state, &changed, &store)
                .await
                .expect_err("changed recovery identity must fail closed");
            assert!(
                error
                    .to_string()
                    .starts_with(if field == "transaction" || field == "event" {
                        "PERSISTED_CALLBACK_FOR_CONTINUATION_NOT_FOUND:"
                    } else {
                        "CONTINUATION_DELIVERY_IDENTITY_MISMATCH:"
                    }),
                "unexpected {field} identity error: {error}"
            );
        }
        assert_eq!(
            service.sessions.lock().len(),
            0,
            "enqueue count before recovery"
        );

        let result = service
            .continue_terminal_delivery_with_store(&state, &delivery, &store)
            .await
            .expect("restart finishes ack through formal recovery path");
        assert_eq!(result["status"], "acknowledged_after_restart");
        assert_eq!(
            service.sessions.lock().len(),
            0,
            "provider/enqueue execution delta"
        );

        drop(store);
        let reopened = SessionLifecycleStore::open(
            root.path(),
            &commander_session_id,
            LifecycleConfig::default(),
        )
        .expect("reopen after recovery");
        let readback = reopened.readback().expect("readback");
        assert_eq!(readback.acknowledged_callbacks, 1);
        assert_eq!(readback.acknowledged_receipts, 1);
        assert!(
            reopened
                .callbacks_for_replay()
                .expect("callback replay")
                .is_empty()
        );
        assert!(
            reopened
                .callback_continuations_for_replay()
                .expect("continuation replay")
                .is_empty()
        );
        assert_eq!(
            reopened
                .prepare_callback_continuation(&continuation)
                .expect("acknowledged continuation readback"),
            session_lifecycle::ContinuationWriteOutcome::AlreadyAcknowledged
        );
        assert!(
            service
                .recover_callback_continuations(&state, &commander_session_id)
                .await
                .expect("formal replay after recovery")
                .is_empty()
        );
        assert_eq!(service.sessions.lock().len(), 0, "final enqueue count");
        assert!(
            !default_store_path.exists(),
            "test must not create its identity in the default lifecycle root"
        );
    }

    #[test]
    fn missing_commander_store_is_an_empty_replay() {
        let service = ExecutionService::new();
        let commander_session_id = format!("missing-commander-{}", uuid::Uuid::new_v4());
        let replay = service
            .replay_terminal_callbacks(
                &commander_session_id,
                "missing-child",
                "missing-transaction",
            )
            .expect("missing store is not fatal");
        assert!(replay.is_empty());
        assert!(service.sessions.lock().is_empty());
    }

    #[test]
    fn replay_changed_immutable_identity_conflicts() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let (store, delivery) = durable_callback_fixture(root.path(), TerminalState::Completed);
        let record = DurableCallbackRecord::new(
            &store
                .terminal_receipt(&delivery.transaction_id, &delivery.event_id)
                .expect("receipt"),
            json!("child result"),
            json!({"payload": {"body": {"item": {"id": "message-1", "text": "child result"}}}}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".to_string(),
            },
        )
        .expect("callback");
        store.publish_callback(&record).expect("pending callback");

        let error = replay_terminal_callbacks_from_store(
            &store,
            "commander-callback",
            "different-child",
            "transaction-callback",
        )
        .expect_err("changed child identity must fail closed");
        assert!(
            error
                .to_string()
                .starts_with("CALLBACK_REPLAY_IDENTITY_MISMATCH:")
        );
    }

    #[test]
    fn delegated_prompt_digest_is_recomputed_and_mismatch_rejected() {
        let mut request = RunAgentRequest {
            prompt: Some("authoritative prompt".to_string()),
            message: Some("ignored message".to_string()),
            delegated_input_sha256: Some("b".repeat(64)),
            ..Default::default()
        };
        let error = validate_delegated_input_digest(&mut request, true)
            .expect_err("caller digest mismatch");
        assert!(
            error
                .to_string()
                .starts_with("DELEGATED_INPUT_SHA256_MISMATCH:")
        );

        request.delegated_input_sha256 = request.effective_prompt_sha256();
        validate_delegated_input_digest(&mut request, true).expect("matching digest");
        assert_eq!(
            request.delegated_input_sha256,
            request.effective_prompt_sha256()
        );
    }

    #[test]
    fn terminal_failure_and_cancellation_require_admission_and_remain_unsettled() {
        for terminal_state in [TerminalState::Failed, TerminalState::Cancelled] {
            let root = tempfile::tempdir().expect("temp lifecycle root");
            let (store, delivery) = durable_callback_fixture(root.path(), terminal_state);
            assert_eq!(
                publish_terminal_failure_callback_from_store(&store, delivery.clone())
                    .expect_err("unadmitted failure must fail closed")
                    .to_string(),
                "TERMINAL_CALLBACK_CHILD_ADMISSION_NOT_DURABLE:child-callback"
            );
            assert_eq!(store.readback().expect("missing admission readback").intaken_callbacks, 0);

            admit_callback_child(&store, &"a".repeat(64));
            let (transport, delivery) =
                publish_terminal_failure_callback_from_store(&store, delivery)
                    .expect("failure callback")
                    .expect("delegated failure callback");

            assert_eq!(transport["method"], "session.terminal_failure");
            assert_eq!(
                transport["payload"]["body"]["item"]["classification"],
                "UNSETTLED_EFFECT"
            );
            assert!(matches!(
                delivery.callback_effect_identity,
                Some(CallbackEffectIdentity::UnsettledEffect { .. })
            ));
            let readback = store.readback().expect("readback");
            assert_eq!(readback.pending_callbacks, 0);
            assert_eq!(readback.intaken_callbacks, 1);
            assert_eq!(readback.acknowledged_callbacks, 0);
            assert_eq!(readback.acknowledged_receipts, 0);
        }

        let root = tempfile::tempdir().expect("mismatched admission root");
        let (store, delivery) = durable_callback_fixture(root.path(), TerminalState::Failed);
        admit_callback_child(&store, &"b".repeat(64));
        assert_eq!(
            publish_terminal_failure_callback_from_store(&store, delivery)
                .expect_err("mismatched admission must fail closed")
                .to_string(),
            "TERMINAL_CALLBACK_ADMISSION_IDENTITY_MISMATCH:child-callback"
        );
        assert_eq!(store.readback().expect("mismatch readback").intaken_callbacks, 0);
    }
}
