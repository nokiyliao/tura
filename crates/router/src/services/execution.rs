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
use tura_llm_rust::official_codex_app_server::load_terminal_commander_convergence_ledger;

use crate::ipc_handlers::enqueue_turn_identity;
use crate::services::runtime_workers::{MAX_QUEUED_RUNTIME_TURNS, runtime_worker_limit};
use crate::{AppState, dispatch_run_agent_with_runtime_slot};
use router_contract::{
    AcknowledgeChildCallbackEffectIdentity, AcknowledgeChildCallbackOutcome,
    AcknowledgeChildCallbackRequest, AcknowledgeChildCallbackResponse, CancelRuntimeRequest,
    EnqueueTurnRequest, IpcRequest, ProbeSessionsRequest, RegisterChildSessionOutcome,
    RegisterChildSessionRequest, RegisterChildSessionResponse,
};
use runtime_contract::{
    CommanderContinuationBinding, CommanderConvergenceProof, LifecycleExecutionContext,
    RunAgentRequest, TaskContextCapsule,
};
use session_lifecycle::{
    AckOutcome, CallbackEffectIdentity, ChildAdmissionOutcome, ChildAdmissionRecord,
    ContinuationDispatchRecord, ContinuationDispatchState, ContinuationWriteOutcome,
    DurableCallbackRecord, IntakeOutcome, LifecycleConfig, LiveEffectEvidence, ReclaimOutcome,
    SessionLifecycleStore, TerminalReceipt, TerminalReceiptIdentity, TerminalState,
    canonical_value_sha256, commander_store_path,
};
use session_log_contract::{
    ActivateRuntimeLeaseRequest, CreateSessionRequest, GetRuntimeLeaseRequest, GetSessionRequest,
    RecoveryCloseRuntimeOutcome, RecoveryCloseRuntimeReason, RecoveryCloseRuntimeRequest,
    RegisterRuntimeRequest, ReplayRuntimeRequest, RuntimeLeaseOutcome, RuntimeLeaseSnapshot,
    RuntimeLifecycleIdentity, RuntimeRecoveryQuiescenceProof, RuntimeRecoveryReceipt,
    RuntimeRegistrationOutcome, SessionFeedEntry, SessionFeedEvent, SessionLogCommand,
    SessionLogResponse, SessionMetadata, SessionSnapshot, recovery_terminal_projection_event_id,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommanderConvergenceRecoveryEvidence {
    proof_sha256: String,
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
        let payload = validated_child_execution_payload(&request)?;
        state.session_db.start()?;

        let replay_store = lifecycle_store(&request.parent_session_id)?;
        let replay_record = child_admission_record(&request);
        if replay_store
            .child_admission(&request.child_session_id)?
            .is_some()
        {
            match replay_store.admit_child(&replay_record)? {
                ChildAdmissionOutcome::AlreadyAdmitted => {
                    if child_runtime_is_exact_active_nonterminal(&request)? {
                        return register_child_session_response(
                            request,
                            RegisterChildSessionOutcome::AlreadyAdmitted,
                        );
                    }
                }
                ChildAdmissionOutcome::Admitted => {
                    unreachable!("an existing durable child admission cannot be newly admitted")
                }
            }
        }

        let _admission = self.child_admission.lock().await;

        let parent = read_session_snapshot(&request.parent_session_id)?.ok_or_else(|| {
            anyhow!(
                "CHILD_ADMISSION_PARENT_SESSION_NOT_FOUND:{}",
                request.parent_session_id
            )
        })?;
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

        register_child_session_response(
            request,
            match admitted {
                ChildAdmissionOutcome::Admitted => RegisterChildSessionOutcome::Admitted,
                ChildAdmissionOutcome::AlreadyAdmitted => {
                    RegisterChildSessionOutcome::AlreadyAdmitted
                }
            },
        )
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
        let requested_continuation_fallback = request
            .payload
            .get("fallback_from_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        if requested_continuation_fallback.is_some() && continuation.is_none() {
            return Err(anyhow!(
                "CALLBACK_CONTINUATION_FALLBACK_WITHOUT_DURABLE_RECORD:{}",
                request.runtime_id
            ));
        }
        if let Some(fallback_from_id) = requested_continuation_fallback.as_deref() {
            validate_continuation_fallback_source(&request.session_id, fallback_from_id)?;
        }
        let mut run_request = payload_to_run_agent_request(
            &request,
            &lease_id,
            requested_continuation_fallback.clone(),
        )?;
        let requested_prompt = run_request.effective_prompt();
        let fallback_from_id = match requested_continuation_fallback {
            Some(fallback_from_id) => Some(fallback_from_id),
            None => runtime_registration_fallback(&request.session_id, requested_prompt)?,
        };
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
            commander_continuation: supplied_lifecycle
                .as_ref()
                .and_then(|context| context.commander_continuation.clone()),
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
                    let convergence_recovery = continuation
                        .as_ref()
                        .and_then(|record| commander_continuation_binding(record).ok())
                        .and_then(|binding| {
                            read_session_snapshot(&request.session_id)
                                .ok()
                                .flatten()
                                .and_then(|snapshot| {
                                    terminal_commander_convergence_recovery_evidence(
                                        std::path::Path::new(&snapshot.metadata.session_directory),
                                        &request.session_id,
                                        &request.runtime_id,
                                        &binding,
                                    )
                                    .ok()
                                    .flatten()
                                })
                        });
                    match self
                        .terminalize_registered_runtime(
                            state,
                            &request.session_id,
                            &request.runtime_id,
                            convergence_recovery.as_ref(),
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
            let bound =
                bind_commander_convergence_proof_from_runtime(&store, record, &request.runtime_id)?;
            if !store.callback_continuation_completion_proven(&bound)? {
                return Err(anyhow!(
                    "CONTINUATION_SUCCESS_EVIDENCE_NOT_DURABLE:{}:{}:{}",
                    record.request_id,
                    record.runtime_id,
                    record.lease_id
                ));
            }
            complete_and_ack_callback_continuation(&store, &bound)?;
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

    pub(crate) fn acknowledge_child_callback_request(&self, input: Value) -> Result<Value> {
        let request: AcknowledgeChildCallbackRequest = serde_json::from_value(input)?;
        request.validate().map_err(anyhow::Error::msg)?;
        let store = lifecycle_store(&request.parent_session_id)?;
        acknowledge_child_callback_from_store(&store, request)
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
        let convergence_recovery = if request.reason == RecoveryCloseRuntimeReason::OrphanedRuntime
        {
            let matching = lifecycle_store_if_exists(&request.session_id)?
                .map(|store| store.callback_continuations_for_replay())
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .filter(|record| {
                    record.runtime_id == request.runtime_id
                        && record.state == ContinuationDispatchState::Dispatched
                        && record.commander_thread_id.is_some()
                })
                .collect::<Vec<_>>();
            if matching.len() > 1 {
                return Err(anyhow!(
                    "COMMANDER_CONVERGENCE_STARTUP_BINDING_CONFLICT:{}",
                    request.runtime_id
                ));
            }
            match matching.first() {
                Some(record) => {
                    let binding = commander_continuation_binding(record)?;
                    match read_session_snapshot(&request.session_id)? {
                        Some(snapshot) => terminal_commander_convergence_recovery_evidence(
                            std::path::Path::new(&snapshot.metadata.session_directory),
                            &request.session_id,
                            &request.runtime_id,
                            &binding,
                        )?,
                        None => None,
                    }
                }
                None => None,
            }
        } else {
            None
        };
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
            reason: if convergence_recovery.is_some() {
                RecoveryCloseRuntimeReason::CommanderConvergenceProven
            } else {
                request.reason
            },
            convergence_proof_sha256: convergence_recovery.map(|evidence| evidence.proof_sha256),
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
                if let Ok(bound) = bind_commander_convergence_proof_from_runtime(
                    store,
                    &continuation,
                    &continuation.runtime_id,
                ) {
                    if store.callback_continuation_completion_proven(&bound)? {
                        complete_and_ack_callback_continuation(store, &bound)?;
                        return Ok(continuation_result(
                            &continuation,
                            "reconciled_and_acknowledged_after_restart",
                        ));
                    }
                }
                if let Some((request, lease_id)) =
                    commander_convergence_fallback_request(&continuation)?
                {
                    let request_id = request.request_id.clone();
                    return self
                        .enqueue_turn_request_with_identity(
                            state,
                            request.payload,
                            &request_id,
                            lease_id,
                            Some(continuation),
                        )
                        .await;
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
        let commander_continuation = continuation
            .commander_thread_id
            .as_ref()
            .map(|_| commander_continuation_binding(&continuation))
            .transpose()?;
        let snapshot =
            read_session_snapshot(&continuation.commander_session_id)?.ok_or_else(|| {
                anyhow!(
                    "CALLBACK_CONTINUATION_PARENT_SESSION_NOT_FOUND:{}",
                    continuation.commander_session_id
                )
            })?;
        let input = json!({
            "runtime_id": continuation.runtime_id,
            "session_id": continuation.commander_session_id,
            "payload": callback_continuation_payload(
                &continuation,
                &snapshot.metadata,
                commander_continuation.as_ref(),
            )?,
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
        let admission = require_terminal_callback_admission(
            store,
            &receipt,
            parent_mission_revision_sha256,
            delegated_input_sha256,
            Some(&effect_id),
        )?;
        let mut record = DurableCallbackRecord::new(
            &receipt,
            callback_payload,
            transport_payload,
            parent_mission_revision_sha256,
            delegated_input_sha256,
            CallbackEffectIdentity::Exact { effect_id },
        )?;
        record.commander_thread_id = admission.commander_thread_id;
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
        convergence_recovery: Option<&CommanderConvergenceRecoveryEvidence>,
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
        let reason = if convergence_recovery.is_some() {
            RecoveryCloseRuntimeReason::CommanderConvergenceProven
        } else if snapshot.revision == 0 && snapshot.last_event_seq == 0 {
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
            convergence_proof_sha256: convergence_recovery
                .map(|evidence| evidence.proof_sha256.clone()),
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
            .terminalize_registered_runtime(state, session_id, runtime_id, None)
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

fn commander_continuation_binding(
    record: &ContinuationDispatchRecord,
) -> Result<CommanderContinuationBinding> {
    let target_thread_id = record.commander_thread_id.clone().ok_or_else(|| {
        anyhow!(
            "COMMANDER_CONTINUATION_TARGET_MISSING:{}",
            record.request_id
        )
    })?;
    let effect = serde_json::to_value(&record.effect_identity)?;
    let binding = CommanderContinuationBinding {
        target_thread_id,
        requested_action: record.requested_action.clone(),
        continuation_request_id: record.request_id.clone(),
        child_session_id: record.child_session_id.clone(),
        child_transaction_id: record.child_transaction_id.clone(),
        child_runtime_id: record.child_runtime_id.clone(),
        callback_payload_sha256: record.callback_payload_sha256.clone(),
        effect_identity_sha256: canonical_value_sha256(&effect),
        pre_revision_sha256: record.parent_mission_revision_sha256.clone(),
    };
    binding.validate().map_err(anyhow::Error::msg)?;
    Ok(binding)
}

fn terminal_commander_convergence_recovery_evidence(
    session_directory: &std::path::Path,
    session_id: &str,
    runtime_id: &str,
    binding: &CommanderContinuationBinding,
) -> Result<Option<CommanderConvergenceRecoveryEvidence>> {
    let Some(ledger) = load_terminal_commander_convergence_ledger(
        session_directory,
        session_id,
        runtime_id,
        binding,
    )
    .map_err(anyhow::Error::msg)?
    else {
        return Ok(None);
    };
    let proof = ledger
        .commander_convergence_proof
        .as_ref()
        .ok_or_else(|| anyhow!("COMMANDER_CONVERGENCE_LEDGER_PROOF_MISSING:{runtime_id}"))?;
    Ok(Some(CommanderConvergenceRecoveryEvidence {
        proof_sha256: canonical_value_sha256(&serde_json::to_value(proof)?),
    }))
}

fn commander_convergence_fallback_request(
    record: &ContinuationDispatchRecord,
) -> Result<Option<(IpcRequest, String)>> {
    if record.commander_thread_id.is_none() {
        return Ok(None);
    }
    let Some(snapshot) = read_session_snapshot(&record.commander_session_id)? else {
        return Ok(None);
    };
    if snapshot.lifecycle_projection.state != SessionState::Failed
        || snapshot.lifecycle_projection.active_runtime_id.is_some()
    {
        return Ok(None);
    }
    let Some(latest_runtime_id) = snapshot.lifecycle_projection.runtime_ids.last() else {
        return Ok(None);
    };
    let response = session_log_contract::client::call_service(&SessionLogCommand::ReplayRuntime(
        ReplayRuntimeRequest {
            runtime_id: latest_runtime_id.clone(),
        },
    ))?;
    let attempt = match response {
        SessionLogResponse::RuntimeReplayed {
            runtime: Some(runtime),
        } => runtime.aggregate,
        SessionLogResponse::RuntimeReplayed { runtime: None } => return Ok(None),
        SessionLogResponse::Error { error } => return Err(anyhow!(error)),
        other => return Err(anyhow!("unexpected runtime replay response: {other:?}")),
    };
    let attempt_binds_continuation = attempt.runtime_id == record.runtime_id
        || attempt.fallback_from_id.as_deref() == Some(record.runtime_id.as_str())
        || (attempt
            .runtime_id
            .starts_with("callback-continuation-recovery-runtime-")
            && attempt.fallback_from_id.as_deref().is_some_and(|fallback| {
                fallback.starts_with("callback-continuation-recovery-runtime-")
            }));
    if attempt.runtime_id != *latest_runtime_id
        || attempt.session_id != record.commander_session_id
        || attempt.state != RuntimeState::Failed
        || !attempt_binds_continuation
    {
        return Ok(None);
    }
    let binding = commander_continuation_binding(record)?;
    let recovery_digest = if attempt.runtime_id == record.runtime_id
        && attempt.output.is_none()
        && attempt
            .error
            .as_ref()
            .and_then(|error| error.error_code.as_deref())
            == Some("commander_convergence_proven_before_runtime_output")
    {
        let Some(ledger) = load_terminal_commander_convergence_ledger(
            std::path::Path::new(&snapshot.metadata.session_directory),
            &record.commander_session_id,
            &record.runtime_id,
            &binding,
        )
        .map_err(anyhow::Error::msg)?
        else {
            return Ok(None);
        };
        let proof = ledger.commander_convergence_proof.as_ref().ok_or_else(|| {
            anyhow!(
                "COMMANDER_CONVERGENCE_LEDGER_PROOF_MISSING:{}",
                record.runtime_id
            )
        })?;
        let proof_sha256 = canonical_value_sha256(&serde_json::to_value(proof)?);
        canonical_value_sha256(&json!({
            "request_id": record.request_id,
            "original_runtime_id": record.runtime_id,
            "proof_sha256": proof_sha256,
        }))
    } else if attempt.runtime_id == record.runtime_id
        && let Some(evidence_sha256) = pre_provider_zero_effect_failure_evidence(&attempt)
    {
        canonical_value_sha256(&json!({
            "request_id": record.request_id,
            "original_runtime_id": record.runtime_id,
            "recovery_class": "pre_provider_zero_effect_failure",
            "evidence_sha256": evidence_sha256,
        }))
    } else if let Some((recovery_class, evidence_sha256)) =
        pre_provider_commander_active_writer_evidence(&attempt)
            .map(|evidence| ("commander_active_writer_pre_submit", evidence))
            .or_else(|| {
                pre_provider_commander_binding_recovery_evidence(&attempt)
                    .map(|evidence| ("commander_binding_pre_submit", evidence))
            })
            .or_else(|| {
                pre_provider_commander_ledger_chain_evidence(&attempt)
                    .map(|evidence| ("commander_ledger_chain_pre_submit", evidence))
            })
    {
        let prior_recovery_attempts = snapshot
            .lifecycle_projection
            .runtime_ids
            .iter()
            .filter(|runtime_id| runtime_id.starts_with("callback-continuation-recovery-runtime-"))
            .count();
        if prior_recovery_attempts >= 7 {
            return Ok(None);
        }
        canonical_value_sha256(&json!({
            "request_id": record.request_id,
            "original_runtime_id": record.runtime_id,
            "failed_attempt_runtime_id": attempt.runtime_id,
            "recovery_class": recovery_class,
            "attempt_number": prior_recovery_attempts + 1,
            "evidence_sha256": evidence_sha256,
        }))
    } else {
        return Ok(None);
    };
    let runtime_id = format!("callback-continuation-recovery-runtime-{recovery_digest}");
    let lease_id = format!("callback-continuation-recovery-lease-{recovery_digest}");
    let mut payload = callback_continuation_payload(record, &snapshot.metadata, Some(&binding))?;
    payload
        .as_object_mut()
        .ok_or_else(|| {
            anyhow!(
                "CALLBACK_CONTINUATION_PAYLOAD_NOT_OBJECT:{}",
                record.request_id
            )
        })?
        .insert(
            "fallback_from_id".to_string(),
            Value::String(attempt.runtime_id.clone()),
        );
    let input = json!({
        "runtime_id": runtime_id,
        "session_id": record.commander_session_id,
        "payload": payload,
    });
    Ok(Some((
        IpcRequest {
            request_id: record.request_id.clone(),
            kind: "call".to_string(),
            method: "execution.enqueue_turn".to_string(),
            payload: input,
            deadline_ms: None,
        },
        lease_id,
    )))
}

fn pre_provider_zero_effect_failure_evidence(runtime: &RuntimeAggregate) -> Option<String> {
    pre_provider_zero_effect_evidence(runtime, "PROVIDER_ROUTE_ADMISSION_REJECTED")
}

fn pre_provider_commander_active_writer_evidence(runtime: &RuntimeAggregate) -> Option<String> {
    let error_text = runtime.error.as_ref()?.error_text.as_deref()?;
    if runtime.provider.llm_provider_name != "official_codex_app_server"
        || !error_text.starts_with("official Codex App Server returned an error for thread/resume:")
        || !error_text.contains("\"code\":-32600")
        || !error_text.contains("already has an active writer")
    {
        return None;
    }
    pre_provider_zero_effect_evidence(runtime, "OFFICIAL_CODEX_APP_SERVER_FAILED")
}

fn pre_provider_commander_binding_recovery_evidence(runtime: &RuntimeAggregate) -> Option<String> {
    let error_text = runtime.error.as_ref()?.error_text.as_deref()?;
    if runtime.provider.llm_provider_name != "official_codex_app_server"
        || error_text != "COMMANDER_CONTINUATION_BINDING_INVALID: runtime/request identity mismatch"
    {
        return None;
    }
    pre_provider_zero_effect_evidence(runtime, "OFFICIAL_CODEX_APP_SERVER_FAILED")
}

fn pre_provider_commander_ledger_chain_evidence(runtime: &RuntimeAggregate) -> Option<String> {
    let error_text = runtime.error.as_ref()?.error_text.as_deref()?;
    if runtime.provider.llm_provider_name != "official_codex_app_server"
        || !error_text.starts_with(
            "OFFICIAL_CODEX_INTERRUPTED_RECOVERY_UNCERTAIN_EFFECT: effect 0 is not durably reconciled:",
        )
        || !error_text.ends_with("execution ledger fallback source is not durable")
    {
        return None;
    }
    pre_provider_zero_effect_evidence(runtime, "OFFICIAL_CODEX_APP_SERVER_FAILED")
}

fn pre_provider_zero_effect_evidence(
    runtime: &RuntimeAggregate,
    accepted_error_code: &str,
) -> Option<String> {
    let error = runtime.error.as_ref()?;
    let error_text = error.error_text.as_deref()?;
    let output = runtime.output.as_ref()?.as_object()?;
    if runtime.state != RuntimeState::Failed
        || runtime.called_at.is_none()
        || runtime.call_finished_at.is_none()
        || runtime.first_token_at.is_some()
        || runtime.usage.is_some()
        || runtime.context_tokens.input != 0
        || runtime.reasoning.is_some()
        || runtime.reasoning_hash.is_some()
        || !runtime.text.is_empty()
        || !runtime.tool_call.is_empty()
        || error.error_code.as_deref() != Some(accepted_error_code)
        || error.retry_allowed
        || error.fallback_allowed
        || error.fallback_to_id.is_some()
        || output.len() != 1
        || output.get("error").and_then(Value::as_str) != Some(error_text)
    {
        return None;
    }
    Some(canonical_value_sha256(&json!({
        "runtime_id": runtime.runtime_id,
        "session_id": runtime.session_id,
        "state": runtime.state,
        "called_at": runtime.called_at,
        "call_finished_at": runtime.call_finished_at,
        "error": error,
        "output": runtime.output,
        "context_tokens": runtime.context_tokens,
        "usage": runtime.usage,
        "first_token_at": runtime.first_token_at,
        "reasoning": runtime.reasoning,
        "reasoning_hash": runtime.reasoning_hash,
        "text": runtime.text,
        "tool_call": runtime.tool_call,
    })))
}

fn callback_continuation_payload(
    record: &ContinuationDispatchRecord,
    parent: &SessionMetadata,
    commander_continuation: Option<&CommanderContinuationBinding>,
) -> Result<Value> {
    Ok(json!({
        "prompt": serde_json::to_string(&record.parent_input)?,
        "directory": parent.session_directory,
        "model": parent.model,
        "agent": parent.agent,
        "session_type": parent.session_type,
        "parent_mission_revision_sha256": record.parent_mission_revision_sha256,
        "delegated_input_sha256": record.delegated_input_sha256,
        "lifecycle": {
            "transaction_id": record.request_id,
            "commander_session_id": record.commander_session_id,
            "parent_mission_revision_sha256": record.parent_mission_revision_sha256,
            "delegated_input_sha256": record.delegated_input_sha256,
            "task_id": null,
            "goal_id": null,
            "operator_override": false,
            "commander_continuation": commander_continuation,
        },
        "operator_override": false,
    }))
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

fn bind_commander_convergence_proof_from_runtime(
    store: &SessionLifecycleStore,
    record: &ContinuationDispatchRecord,
    completion_runtime_id: &str,
) -> Result<ContinuationDispatchRecord> {
    if record.commander_thread_id.is_none() {
        return Ok(record.clone());
    }
    let response = session_log_contract::client::call_service(&SessionLogCommand::ReplayRuntime(
        ReplayRuntimeRequest {
            runtime_id: completion_runtime_id.to_string(),
        },
    ))?;
    let runtime = match response {
        SessionLogResponse::RuntimeReplayed {
            runtime: Some(runtime),
        } => runtime.aggregate,
        SessionLogResponse::RuntimeReplayed { runtime: None } => {
            return Err(anyhow!(
                "COMMANDER_CONVERGENCE_RUNTIME_NOT_FOUND:{}",
                completion_runtime_id
            ));
        }
        SessionLogResponse::Error { error } => {
            return Err(anyhow!(
                "COMMANDER_CONVERGENCE_RUNTIME_REPLAY_FAILED:{}:{error}",
                completion_runtime_id
            ));
        }
        other => {
            return Err(anyhow!(
                "COMMANDER_CONVERGENCE_RUNTIME_REPLAY_UNEXPECTED:{}:{other:?}",
                completion_runtime_id
            ));
        }
    };
    let proof = commander_convergence_proof_from_runtime(record, &runtime)?;
    store.bind_callback_continuation_convergence_proof(record, &proof)?;
    let mut bound = record.clone();
    bound.convergence_proof = Some(proof);
    Ok(bound)
}

fn commander_convergence_proof_from_runtime(
    record: &ContinuationDispatchRecord,
    runtime: &RuntimeAggregate,
) -> Result<CommanderConvergenceProof> {
    let original_or_bound_fallback = runtime.runtime_id == record.runtime_id
        || runtime.fallback_from_id.as_deref() == Some(record.runtime_id.as_str())
        || (runtime
            .runtime_id
            .starts_with("callback-continuation-recovery-runtime-")
            && runtime.fallback_from_id.as_deref().is_some_and(|fallback| {
                fallback.starts_with("callback-continuation-recovery-runtime-")
            }));
    if !original_or_bound_fallback
        || runtime.session_id != record.commander_session_id
        || runtime.state != RuntimeState::Finished
    {
        return Err(anyhow!(
            "COMMANDER_CONVERGENCE_RUNTIME_IDENTITY_MISMATCH:{}",
            record.runtime_id
        ));
    }
    let output = runtime.output.as_ref().ok_or_else(|| {
        anyhow!(
            "COMMANDER_CONVERGENCE_RUNTIME_OUTPUT_MISSING:{}",
            record.runtime_id
        )
    })?;
    let proof: CommanderConvergenceProof = serde_json::from_value(
        output
            .get("commander_convergence_proof")
            .cloned()
            .ok_or_else(|| anyhow!("COMMANDER_CONVERGENCE_PROOF_MISSING:{}", record.runtime_id))?,
    )
    .map_err(|error| {
        anyhow!(
            "COMMANDER_CONVERGENCE_PROOF_MALFORMED:{}:{error}",
            record.runtime_id
        )
    })?;
    let final_content = output.get("content").ok_or_else(|| {
        anyhow!(
            "COMMANDER_CONVERGENCE_FINAL_ASSISTANT_MISSING:{}",
            record.runtime_id
        )
    })?;
    if canonical_value_sha256(final_content) != proof.final_assistant_sha256 {
        return Err(anyhow!(
            "COMMANDER_CONVERGENCE_FINAL_ASSISTANT_HASH_MISMATCH:{}",
            record.runtime_id
        ));
    }
    proof.validate_shape().map_err(anyhow::Error::msg)?;
    Ok(proof)
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
    let admission = require_terminal_callback_admission(
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
    let mut record = DurableCallbackRecord::new(
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
    record.commander_thread_id = admission.commander_thread_id;
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

fn child_runtime_is_exact_active_nonterminal(
    request: &RegisterChildSessionRequest,
) -> Result<bool> {
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
            Ok(runtime.lease_active && !runtime.terminal)
        }
        SessionLogResponse::RuntimeLeaseRead { runtime: None } => Ok(false),
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => Err(anyhow!(
            "unexpected session_db runtime read response for {}: {other:?}",
            request.child_runtime_id
        )),
    }
}

fn register_child_session_response(
    request: RegisterChildSessionRequest,
    outcome: RegisterChildSessionOutcome,
) -> Result<Value> {
    serde_json::to_value(RegisterChildSessionResponse {
        outcome,
        parent_session_id: request.parent_session_id,
        child_session_id: request.child_session_id,
        child_runtime_id: request.child_runtime_id,
        child_transaction_id: request.child_transaction_id,
        callback_request_id: request.callback_request_id,
        effect_id: request.effect_id,
    })
    .map_err(Into::into)
}

fn acknowledge_child_callback_from_store(
    store: &SessionLifecycleStore,
    request: AcknowledgeChildCallbackRequest,
) -> Result<Value> {
    let admission = store
        .child_admission(&request.child_session_id)?
        .ok_or_else(|| {
            anyhow!(
                "CHILD_CALLBACK_ACK_ADMISSION_NOT_FOUND:{}",
                request.child_session_id
            )
        })?;
    if admission.parent_session_id != request.parent_session_id
        || admission.parent_mission_revision_sha256 != request.parent_mission_revision_sha256
        || admission.commander_thread_id.as_deref() != Some(request.commander_thread_id.as_str())
        || admission.child_session_id != request.child_session_id
        || admission.child_runtime_id != request.child_runtime_id
        || admission.child_transaction_id != request.transaction_id
        || admission.child_lease_id != request.child_lease_id
        || admission.callback_request_id != request.transaction_id
    {
        return Err(anyhow!(
            "CHILD_CALLBACK_ACK_ADMISSION_IDENTITY_MISMATCH:{}",
            request.child_session_id
        ));
    }

    let effect_identity = callback_effect_identity(&request.effect_identity);
    if let CallbackEffectIdentity::Exact { effect_id } = &effect_identity
        && admission.effect_id != *effect_id
    {
        return Err(anyhow!(
            "CHILD_CALLBACK_ACK_ADMISSION_EFFECT_IDENTITY_MISMATCH:{}",
            request.child_session_id
        ));
    }

    let callback = store
        .intaken_callback(&request.transaction_id, &request.event_id)?
        .ok_or_else(|| {
            anyhow!(
                "CHILD_CALLBACK_ACK_INTAKEN_CALLBACK_NOT_FOUND:{}:{}",
                request.transaction_id,
                request.event_id
            )
        })?;
    if callback.commander_session_id != request.parent_session_id
        || callback.parent_mission_revision_sha256 != request.parent_mission_revision_sha256
        || callback.commander_thread_id.as_deref() != Some(request.commander_thread_id.as_str())
        || callback.child_session_id != request.child_session_id
        || callback.runtime_id != request.child_runtime_id
        || callback.lease_id != request.child_lease_id
        || callback.transaction_id != request.transaction_id
        || callback.event_id != request.event_id
        || callback.callback_payload_sha256 != request.callback_payload_sha256
        || callback.effect_identity != effect_identity
    {
        return Err(anyhow!(
            "CHILD_CALLBACK_ACK_CALLBACK_IDENTITY_MISMATCH:{}:{}",
            request.transaction_id,
            request.event_id
        ));
    }

    let outcome = match store.acknowledge_callback(
        &request.transaction_id,
        &request.event_id,
        &request.callback_payload_sha256,
        &effect_identity,
    )? {
        AckOutcome::Acknowledged => AcknowledgeChildCallbackOutcome::Acknowledged,
        AckOutcome::AlreadyAcknowledged => AcknowledgeChildCallbackOutcome::AlreadyAcknowledged,
    };
    serde_json::to_value(AcknowledgeChildCallbackResponse {
        outcome,
        parent_session_id: request.parent_session_id,
        parent_mission_revision_sha256: request.parent_mission_revision_sha256,
        commander_thread_id: request.commander_thread_id,
        child_session_id: request.child_session_id,
        child_runtime_id: request.child_runtime_id,
        child_lease_id: request.child_lease_id,
        transaction_id: request.transaction_id,
        event_id: request.event_id,
        callback_payload_sha256: request.callback_payload_sha256,
        effect_identity: request.effect_identity,
    })
    .map_err(Into::into)
}

fn callback_effect_identity(
    identity: &AcknowledgeChildCallbackEffectIdentity,
) -> CallbackEffectIdentity {
    match identity {
        AcknowledgeChildCallbackEffectIdentity::Exact { effect_id } => {
            CallbackEffectIdentity::Exact {
                effect_id: effect_id.clone(),
            }
        }
        AcknowledgeChildCallbackEffectIdentity::ProvenZeroEffect {
            classification,
            evidence_sha256,
        } => CallbackEffectIdentity::ProvenZeroEffect {
            classification: classification.clone(),
            evidence_sha256: evidence_sha256.clone(),
        },
        AcknowledgeChildCallbackEffectIdentity::UnsettledEffect {
            classification,
            evidence_sha256,
        } => CallbackEffectIdentity::UnsettledEffect {
            classification: classification.clone(),
            evidence_sha256: evidence_sha256.clone(),
        },
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
        request.commander_thread_id.clone(),
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

fn validated_child_execution_payload(request: &RegisterChildSessionRequest) -> Result<Value> {
    let payload = child_execution_payload(request)?;
    let enqueue = EnqueueTurnRequest {
        runtime_id: request.child_runtime_id.clone(),
        session_id: request.child_session_id.clone(),
        payload: payload.clone(),
    };
    let mut run_request = payload_to_run_agent_request(&enqueue, &request.child_lease_id, None)?;
    run_request
        .validate_delegated_identity()
        .map_err(anyhow::Error::msg)?;
    validate_delegated_input_digest(&mut run_request, true)?;

    if run_request.runtime_context.is_some() && run_request.task_context_capsule.is_some() {
        return Err(anyhow!(
            "TASK_CONTEXT_CAPSULE_CONFLICT: runtime_context and task_context_capsule are mutually exclusive"
        ));
    }
    if let Some(value) = run_request.task_context_capsule.as_ref() {
        let capsule = TaskContextCapsule::from_value(value.clone()).map_err(anyhow::Error::msg)?;
        capsule
            .bind_jspace(run_request.jspace_contract.as_ref())
            .map_err(anyhow::Error::msg)?;
    }

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
        RecoveryCloseRuntimeReason::CommanderConvergenceProven => TerminalState::Failed,
    }
}

fn recovery_runtime_state(recovery: &RuntimeRecoveryReceipt) -> Value {
    match recovery.reason {
        RecoveryCloseRuntimeReason::OrphanedRuntime => json!(RuntimeState::Cancelled),
        RecoveryCloseRuntimeReason::UnbornRuntime => Value::Null,
        RecoveryCloseRuntimeReason::CommanderConvergenceProven => json!(RuntimeState::Failed),
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

fn validate_continuation_fallback_source(session_id: &str, fallback_from_id: &str) -> Result<()> {
    let response = session_log_contract::client::call_service(&SessionLogCommand::GetSession(
        GetSessionRequest {
            session_id: session_id.to_string(),
        },
    ))?;
    match response {
        SessionLogResponse::Session {
            session: Some(session),
        } if session.lifecycle_projection.state == SessionState::Failed
            && session.lifecycle_projection.active_runtime_id.is_none()
            && session
                .lifecycle_projection
                .runtime_ids
                .last()
                .map(String::as_str)
                == Some(fallback_from_id) =>
        {
            Ok(())
        }
        SessionLogResponse::Session {
            session: Some(session),
        } => Err(anyhow!(
            "CALLBACK_CONTINUATION_FALLBACK_SOURCE_NOT_LATEST_FAILED:{}:{}:{:?}:{:?}",
            session_id,
            fallback_from_id,
            session.lifecycle_projection.state,
            session.lifecycle_projection.runtime_ids.last()
        )),
        SessionLogResponse::Session { session: None } => Err(anyhow!(
            "CALLBACK_CONTINUATION_FALLBACK_SESSION_NOT_FOUND:{}",
            session_id
        )),
        SessionLogResponse::Error { error } => Err(anyhow!(error)),
        other => Err(anyhow!(
            "unexpected session_db response while validating callback fallback: {other:?}"
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
        acknowledge_child_callback_from_store, callback_continuation_payload,
        commander_continuation_binding, commander_convergence_proof_from_runtime,
        complete_and_ack_callback_continuation, failed_session_retry_root,
        failed_session_runtime_fallback, intake_terminal_receipt, is_historical_terminal_runtime,
        lifecycle_store, payload_to_run_agent_request,
        pre_provider_commander_active_writer_evidence,
        pre_provider_commander_binding_recovery_evidence,
        pre_provider_commander_ledger_chain_evidence, pre_provider_zero_effect_failure_evidence,
        publish_terminal_failure_callback_from_store, read_session_snapshot,
        register_and_activate_runtime, replay_terminal_callbacks_from_store,
        require_successful_runtime_dispatch, runtime_lease_from_snapshot,
        runtime_terminal_state_from_snapshot, terminal_commander_convergence_recovery_evidence,
        terminal_runtime_is_current, validate_child_runtime_identity,
        validate_delegated_input_digest, validate_terminalization_identity,
        validated_child_execution_payload,
    };
    use crate::{build_state, services::manager::ServiceManager};
    use chrono::Utc;
    use lifecycle::{
        ProviderConfig, RuntimeAggregate, RuntimeError, RuntimeProviderConfig, RuntimeState,
        SessionProjection, SessionState, TaskPlan, ToolChoice,
    };
    use router_contract::{
        AcknowledgeChildCallbackEffectIdentity, AcknowledgeChildCallbackOutcome,
        AcknowledgeChildCallbackRequest, AcknowledgeChildCallbackResponse,
        RegisterChildSessionRequest,
    };
    use runtime_contract::{CommanderConvergenceProof, RunAgentRequest};
    use serde_json::json;
    use session_lifecycle::{
        CallbackEffectIdentity, ChildAdmissionRecord, ContinuationDispatchRecord,
        ContinuationDispatchState, DurableCallbackRecord, LifecycleConfig, SessionLifecycleStore,
        TerminalReceipt, TerminalReceiptIdentity, TerminalState, commander_store_path,
    };
    use session_log_contract::{
        RuntimeLeaseSnapshot, RuntimeLifecycleIdentity, SessionFeedEntry, SessionFeedEvent,
        SessionMetadata,
    };
    use std::sync::Arc;
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
            commander_thread_id: Some("commander-thread-exact".to_string()),
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
            parent_mission_revision_sha256: Some(request.parent_mission_revision_sha256.clone()),
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

    fn callback_continuation_record() -> ContinuationDispatchRecord {
        let receipt = callback_receipt(TerminalState::Completed);
        let mut callback = DurableCallbackRecord::new(
            &receipt,
            json!("child result"),
            json!({"kind": "gateway.callback"}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".to_string(),
            },
        )
        .expect("callback");
        callback.commander_thread_id = Some("commander-thread-1".to_string());
        ContinuationDispatchRecord::from_callback(&callback).expect("continuation")
    }

    fn callback_parent_metadata() -> SessionMetadata {
        SessionMetadata {
            session_directory: "/tmp/parent-official-codex".to_string(),
            model: Some("official_codex_app_server/gpt-5.6-sol".to_string()),
            agent: Some("balanced".to_string()),
            session_type: "coding".to_string(),
            kill_processes_on_start: false,
            validator_enabled: false,
            force_planning: false,
            model_variant: None,
            model_acceleration_enabled: false,
            disable_permission_restrictions: true,
            use_last_tool_call_response: false,
            auto_session_name: false,
            context_tokens: lifecycle::ContextTokenStats::default(),
            runtime_usage: json!({}),
        }
    }

    fn assert_callback_parent_identity(payload: &serde_json::Value) {
        assert_eq!(payload["directory"], "/tmp/parent-official-codex");
        assert_eq!(payload["model"], "official_codex_app_server/gpt-5.6-sol");
        assert_eq!(payload["agent"], "balanced");
        assert_eq!(payload["session_type"], "coding");
    }

    #[test]
    fn callback_continuation_first_dispatch_inherits_parent_provider_identity() {
        let record = callback_continuation_record();
        let payload = callback_continuation_payload(&record, &callback_parent_metadata(), None)
            .expect("first continuation payload");

        assert_callback_parent_identity(&payload);
        assert_eq!(payload["lifecycle"]["transaction_id"], record.request_id);
        assert_eq!(
            payload["lifecycle"]["commander_session_id"],
            record.commander_session_id
        );
        assert!(payload["lifecycle"]["commander_continuation"].is_null());
    }

    #[test]
    fn callback_continuation_fallback_inherits_parent_provider_identity() {
        let record = callback_continuation_record();
        let binding = commander_continuation_binding(&record).expect("continuation binding");
        let payload =
            callback_continuation_payload(&record, &callback_parent_metadata(), Some(&binding))
                .expect("fallback continuation payload");

        assert_callback_parent_identity(&payload);
        assert_eq!(
            payload["lifecycle"]["commander_continuation"],
            serde_json::to_value(binding).expect("serialized binding")
        );
        assert_eq!(payload["lifecycle"]["transaction_id"], record.request_id);
    }

    fn pre_provider_route_admission_failure() -> RuntimeAggregate {
        let now = Utc::now();
        let mut runtime = RuntimeAggregate::new(
            "callback-continuation-runtime-pre-provider".to_string(),
            "commander-callback".to_string(),
            "balanced".to_string(),
            RuntimeProviderConfig {
                base: ProviderConfig {
                    tura_llm_name: "codex".to_string(),
                    default_model_tier: None,
                    current_model: Some("gpt-5.6-luna".to_string()),
                    stream: true,
                    temperature: 0.0,
                    max_tokens: 256,
                    tool_choice: ToolChoice::Auto,
                    time_out_ms: 1_000,
                },
                thinking: false,
                provider_name: "codex".to_string(),
                model_name: "gpt-5.6-luna".to_string(),
                provider_url_name: "local".to_string(),
                llm_provider_name: "codex".to_string(),
            },
            now,
        );
        runtime.mark_called(now).expect("call started");
        runtime
            .mark_waiting_first_token()
            .expect("waiting first token");
        runtime
            .set_input(json!({"prompt": "continue callback"}))
            .expect("input captured");
        let message = "official Codex admission rejected route: config error: legacy provider 'codex' is disabled; use 'official_codex_app_server'";
        runtime
            .set_output(json!({"error": message}))
            .expect("local diagnostic captured");
        runtime
            .finish_failure(
                now,
                RuntimeError {
                    error_code: Some("PROVIDER_ROUTE_ADMISSION_REJECTED".to_string()),
                    error_text: Some(message.to_string()),
                    retry_allowed: false,
                    fallback_allowed: false,
                    fallback_to_id: None,
                },
                RuntimeState::Failed,
                None,
            )
            .expect("runtime failed");
        runtime
    }

    #[test]
    fn provider_route_admission_failure_proves_pre_provider_zero_effect() {
        let runtime = pre_provider_route_admission_failure();
        let evidence = pre_provider_zero_effect_failure_evidence(&runtime)
            .expect("exact local admission rejection is recoverable");
        assert_eq!(
            pre_provider_zero_effect_failure_evidence(&runtime),
            Some(evidence),
            "evidence identity must be deterministic"
        );
    }

    #[test]
    fn commander_active_writer_rejection_proves_pre_submit_zero_effect() {
        let mut runtime = pre_provider_route_admission_failure();
        runtime.provider.provider_name = "official_codex_app_server/gpt-5.6-sol".to_string();
        runtime.provider.llm_provider_name = "official_codex_app_server".to_string();
        let message = "official Codex App Server returned an error for thread/resume: {\"code\":-32600,\"message\":\"thread commander-thread-1 already has an active writer\"}";
        runtime.output = Some(json!({"error": message}));
        let error = runtime.error.as_mut().expect("runtime error");
        error.error_code = Some("OFFICIAL_CODEX_APP_SERVER_FAILED".to_string());
        error.error_text = Some(message.to_string());

        assert!(
            pre_provider_commander_active_writer_evidence(&runtime).is_some(),
            "exact server-side writer rejection is deferred before provider effects"
        );

        runtime.provider.llm_provider_name = "openai".to_string();
        assert!(
            pre_provider_commander_active_writer_evidence(&runtime).is_none(),
            "a non-official provider must not borrow the writer-busy recovery class"
        );
    }

    #[test]
    fn commander_binding_mismatch_is_pre_submit_zero_effect() {
        let mut runtime = pre_provider_route_admission_failure();
        runtime.provider.provider_name = "official_codex_app_server/gpt-5.6-sol".to_string();
        runtime.provider.llm_provider_name = "official_codex_app_server".to_string();
        let message = "COMMANDER_CONTINUATION_BINDING_INVALID: runtime/request identity mismatch";
        runtime.output = Some(json!({"error": message}));
        let error = runtime.error.as_mut().expect("runtime error");
        error.error_code = Some("OFFICIAL_CODEX_APP_SERVER_FAILED".to_string());
        error.error_text = Some(message.to_string());

        assert!(
            pre_provider_commander_binding_recovery_evidence(&runtime).is_some(),
            "local binding validation fails before provider submission"
        );
    }

    #[test]
    fn commander_preledger_chain_rejection_is_pre_submit_zero_effect() {
        let mut runtime = pre_provider_route_admission_failure();
        runtime.provider.provider_name = "official_codex_app_server/gpt-5.6-sol".to_string();
        runtime.provider.llm_provider_name = "official_codex_app_server".to_string();
        let message = "OFFICIAL_CODEX_INTERRUPTED_RECOVERY_UNCERTAIN_EFFECT: effect 0 is not durably reconciled: runtime execution ledger: execution ledger fallback source is not durable";
        runtime.output = Some(json!({"error": message}));
        let error = runtime.error.as_mut().expect("runtime error");
        error.error_code = Some("OFFICIAL_CODEX_APP_SERVER_FAILED".to_string());
        error.error_text = Some(message.to_string());

        assert!(
            pre_provider_commander_ledger_chain_evidence(&runtime).is_some(),
            "the old preledger validator failed before provider submission"
        );
    }

    #[test]
    fn ambiguous_or_provider_observed_failure_is_not_recoverable() {
        let baseline = pre_provider_route_admission_failure();
        let mut variants = Vec::new();

        let mut first_token = baseline.clone();
        first_token.first_token_at = Some(Utc::now());
        variants.push(first_token);

        let mut token_usage = baseline.clone();
        token_usage.context_tokens.input = 1;
        variants.push(token_usage);

        let mut assistant_text = baseline.clone();
        assistant_text.text = "provider output".to_string();
        variants.push(assistant_text);

        let mut reasoning = baseline.clone();
        reasoning.reasoning = Some("provider reasoning".to_string());
        variants.push(reasoning);

        let mut changed_output = baseline.clone();
        changed_output.output = Some(json!({"error": "different diagnostic"}));
        variants.push(changed_output);

        let mut extra_output = baseline.clone();
        extra_output.output = Some(
            json!({"error": baseline.error.as_ref().and_then(|error| error.error_text.as_deref()), "provider_result": true}),
        );
        variants.push(extra_output);

        let mut retryable = baseline;
        retryable
            .error
            .as_mut()
            .expect("runtime error")
            .retry_allowed = true;
        variants.push(retryable);

        for variant in variants {
            assert!(
                pre_provider_zero_effect_failure_evidence(&variant).is_none(),
                "provider-observed or ambiguous failure must remain fail closed"
            );
        }
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
            None,
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

    fn callback_ack_fixture(
        root: &std::path::Path,
        effect_identity: CallbackEffectIdentity,
        intaken: bool,
    ) -> (SessionLifecycleStore, AcknowledgeChildCallbackRequest) {
        let (store, _) = durable_callback_fixture(root, TerminalState::Completed);
        let admission = ChildAdmissionRecord::new(
            "commander-callback",
            "a".repeat(64),
            Some("commander-thread-1".to_string()),
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
        store.admit_child(&admission).expect("child admission");
        let receipt = store
            .terminal_receipt("transaction-callback", "event-callback")
            .expect("terminal receipt");
        let mut callback = DurableCallbackRecord::new(
            &receipt,
            json!("child result"),
            json!({"kind": "gateway.callback"}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            effect_identity.clone(),
        )
        .expect("callback record");
        callback.commander_thread_id = Some("commander-thread-1".to_string());
        store.publish_callback(&callback).expect("callback publish");
        if intaken {
            store
                .mark_callback_intaken(
                    &callback.transaction_id,
                    &callback.event_id,
                    &callback.callback_payload_sha256,
                )
                .expect("callback intake");
        }
        let effect_identity = match effect_identity {
            CallbackEffectIdentity::Exact { effect_id } => {
                AcknowledgeChildCallbackEffectIdentity::Exact { effect_id }
            }
            CallbackEffectIdentity::ProvenZeroEffect {
                classification,
                evidence_sha256,
            } => AcknowledgeChildCallbackEffectIdentity::ProvenZeroEffect {
                classification,
                evidence_sha256,
            },
            CallbackEffectIdentity::UnsettledEffect {
                classification,
                evidence_sha256,
            } => AcknowledgeChildCallbackEffectIdentity::UnsettledEffect {
                classification,
                evidence_sha256,
            },
        };
        let request = AcknowledgeChildCallbackRequest {
            parent_session_id: "commander-callback".to_string(),
            parent_mission_revision_sha256: "a".repeat(64),
            commander_thread_id: "commander-thread-1".to_string(),
            child_session_id: "child-callback".to_string(),
            child_runtime_id: "runtime-callback".to_string(),
            child_lease_id: "lease-callback".to_string(),
            transaction_id: "transaction-callback".to_string(),
            event_id: "event-callback".to_string(),
            callback_payload_sha256: callback.callback_payload_sha256,
            effect_identity,
        };
        (store, request)
    }

    #[test]
    fn public_child_callback_ack_is_exactly_once_without_provider_continuation() {
        let root = tempfile::tempdir().expect("callback ACK root");
        let (store, request) = callback_ack_fixture(
            root.path(),
            CallbackEffectIdentity::Exact {
                effect_id: "runtime-callback.message".to_string(),
            },
            true,
        );
        let first: AcknowledgeChildCallbackResponse = serde_json::from_value(
            acknowledge_child_callback_from_store(&store, request.clone()).expect("first ACK"),
        )
        .expect("first ACK response");
        let replay: AcknowledgeChildCallbackResponse = serde_json::from_value(
            acknowledge_child_callback_from_store(&store, request).expect("ACK replay"),
        )
        .expect("ACK replay response");
        assert_eq!(first.outcome, AcknowledgeChildCallbackOutcome::Acknowledged);
        assert_eq!(
            replay.outcome,
            AcknowledgeChildCallbackOutcome::AlreadyAcknowledged
        );
        assert_eq!(
            store.readback().expect("readback").acknowledged_callbacks,
            1
        );
        assert!(store.callbacks_for_replay().expect("callbacks").is_empty());
        assert!(
            store
                .callback_continuations_for_replay()
                .expect("continuations")
                .is_empty()
        );
    }

    #[test]
    fn public_child_callback_ack_rejects_changed_bound_identities() {
        let root = tempfile::tempdir().expect("callback ACK root");
        let (store, request) = callback_ack_fixture(
            root.path(),
            CallbackEffectIdentity::Exact {
                effect_id: "runtime-callback.message".to_string(),
            },
            true,
        );
        let mut variants = Vec::new();
        let mut changed = request.clone();
        changed.parent_session_id = "other-parent".into();
        variants.push(changed);
        let mut changed = request.clone();
        changed.child_session_id = "other-child".into();
        variants.push(changed);
        let mut changed = request.clone();
        changed.child_runtime_id = "other-runtime".into();
        variants.push(changed);
        let mut changed = request.clone();
        changed.child_lease_id = "other-lease".into();
        variants.push(changed);
        let mut changed = request.clone();
        changed.commander_thread_id = "other-thread".into();
        variants.push(changed);
        let mut changed = request.clone();
        changed.parent_mission_revision_sha256 = "b".repeat(64);
        variants.push(changed);
        let mut changed = request.clone();
        changed.transaction_id = "other-transaction".into();
        variants.push(changed);
        let mut changed = request.clone();
        changed.event_id = "other-event".into();
        variants.push(changed);
        let mut changed = request.clone();
        changed.callback_payload_sha256 = "c".repeat(64);
        variants.push(changed);
        let mut changed = request;
        changed.effect_identity = AcknowledgeChildCallbackEffectIdentity::Exact {
            effect_id: "other-effect".into(),
        };
        variants.push(changed);
        for changed in variants {
            assert!(acknowledge_child_callback_from_store(&store, changed).is_err());
        }
        assert_eq!(
            store.readback().expect("readback").acknowledged_callbacks,
            0
        );
    }

    #[test]
    fn public_child_callback_ack_requires_intaken_and_settled_effect() {
        let pending_root = tempfile::tempdir().expect("pending callback root");
        let (pending_store, pending_request) = callback_ack_fixture(
            pending_root.path(),
            CallbackEffectIdentity::Exact {
                effect_id: "runtime-callback.message".to_string(),
            },
            false,
        );
        assert!(
            acknowledge_child_callback_from_store(&pending_store, pending_request)
                .expect_err("pending callback")
                .to_string()
                .contains("INTAKEN_CALLBACK_NOT_FOUND")
        );

        let unsettled_root = tempfile::tempdir().expect("unsettled callback root");
        let (unsettled_store, unsettled_request) = callback_ack_fixture(
            unsettled_root.path(),
            CallbackEffectIdentity::UnsettledEffect {
                classification: "effect_receipt_incomplete".to_string(),
                evidence_sha256: "d".repeat(64),
            },
            true,
        );
        assert!(
            acknowledge_child_callback_from_store(&unsettled_store, unsettled_request)
                .expect_err("unsettled effect")
                .to_string()
                .contains("CALLBACK_UNSETTLED_EFFECT_ACK_BLOCKED")
        );
        assert_eq!(
            unsettled_store
                .readback()
                .expect("readback")
                .acknowledged_callbacks,
            0
        );
    }

    #[test]
    fn public_child_zero_effect_callback_ack_replays_without_effect_execution() {
        let root = tempfile::tempdir().expect("zero-effect callback root");
        let (store, request) = callback_ack_fixture(
            root.path(),
            CallbackEffectIdentity::ProvenZeroEffect {
                classification: "pre_provider_zero_effect".to_string(),
                evidence_sha256: "e".repeat(64),
            },
            true,
        );
        acknowledge_child_callback_from_store(&store, request.clone()).expect("zero ACK");
        let replay: AcknowledgeChildCallbackResponse = serde_json::from_value(
            acknowledge_child_callback_from_store(&store, request).expect("zero ACK replay"),
        )
        .expect("zero replay response");
        assert_eq!(
            replay.outcome,
            AcknowledgeChildCallbackOutcome::AlreadyAcknowledged
        );
        assert!(
            store
                .callback_continuations_for_replay()
                .expect("continuations")
                .is_empty()
        );
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
        assert!(
            store
                .callbacks_for_replay()
                .expect("active child callbacks")
                .is_empty()
        );

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

        let callbacks = store
            .callbacks_for_replay()
            .expect("single callback replay");
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
        assert!(
            store
                .callbacks_for_replay()
                .expect("post-ack callbacks")
                .is_empty()
        );
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
    fn commander_convergence_runtime_output_must_bind_exact_continuation() {
        let receipt = callback_receipt(TerminalState::Completed);
        let mut callback = DurableCallbackRecord::new(
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
        callback.commander_thread_id = Some("commander-thread-1".to_string());
        let record =
            ContinuationDispatchRecord::from_callback(&callback).expect("target continuation");
        let final_content = json!("commander converged");
        let effect = serde_json::to_value(&record.effect_identity).expect("effect value");
        let proof = CommanderConvergenceProof {
            schema_version: runtime_contract::COMMANDER_CONVERGENCE_PROOF_SCHEMA_VERSION
                .to_string(),
            request_id: record.request_id.clone(),
            callback_payload_sha256: record.callback_payload_sha256.clone(),
            effect_identity_sha256: session_lifecycle::canonical_value_sha256(&effect),
            child_session_id: record.child_session_id.clone(),
            child_transaction_id: record.child_transaction_id.clone(),
            child_runtime_id: record.child_runtime_id.clone(),
            requested_action: record.requested_action.clone(),
            target_thread_id: record
                .commander_thread_id
                .clone()
                .expect("commander target"),
            pre_revision_sha256: record.parent_mission_revision_sha256.clone(),
            post_revision_sha256: "b".repeat(64),
            target_turn_id: "turn-commander-1".to_string(),
            final_assistant_sha256: session_lifecycle::canonical_value_sha256(&final_content),
        };
        let mut runtime = RuntimeAggregate::new(
            record.runtime_id.clone(),
            record.commander_session_id.clone(),
            "continuation-agent".to_string(),
            RuntimeProviderConfig {
                base: ProviderConfig {
                    tura_llm_name: "official_codex_app_server".to_string(),
                    default_model_tier: None,
                    current_model: Some("test-model".to_string()),
                    stream: true,
                    temperature: 0.0,
                    max_tokens: 256,
                    tool_choice: ToolChoice::Auto,
                    time_out_ms: 1_000,
                },
                thinking: false,
                provider_name: "official_codex_app_server".to_string(),
                model_name: "test-model".to_string(),
                provider_url_name: "local".to_string(),
                llm_provider_name: "openai".to_string(),
            },
            Utc::now(),
        );
        runtime
            .mark_called(runtime.created_at)
            .expect("runtime called");
        runtime
            .mark_waiting_first_token()
            .expect("runtime waiting first token");
        runtime
            .mark_first_token(runtime.created_at)
            .expect("runtime first token");
        runtime
            .set_output(json!({
                "content": final_content,
                "commander_convergence_proof": proof,
            }))
            .expect("capture provider output");
        runtime
            .finish_success(runtime.created_at, None)
            .expect("finish runtime");

        assert_eq!(
            commander_convergence_proof_from_runtime(&record, &runtime)
                .expect("exact proof replay"),
            proof
        );

        let mut fallback = runtime.clone();
        fallback.runtime_id = "callback-continuation-recovery-runtime-test".to_string();
        fallback.fallback_from_id = Some(record.runtime_id.clone());
        assert_eq!(
            commander_convergence_proof_from_runtime(&record, &fallback)
                .expect("exact fallback proof replay"),
            proof
        );
        let mut bounded_retry = fallback.clone();
        bounded_retry.runtime_id =
            "callback-continuation-recovery-runtime-bounded-retry".to_string();
        bounded_retry.fallback_from_id = Some(fallback.runtime_id.clone());
        assert_eq!(
            commander_convergence_proof_from_runtime(&record, &bounded_retry)
                .expect("bounded continuation retry proof replay"),
            proof
        );
        fallback.fallback_from_id = Some("unrelated-runtime".to_string());
        assert!(
            commander_convergence_proof_from_runtime(&record, &fallback)
                .expect_err("unbound fallback must fail")
                .to_string()
                .contains("COMMANDER_CONVERGENCE_RUNTIME_IDENTITY_MISMATCH")
        );

        let mut wrong_final = runtime;
        wrong_final
            .output
            .as_mut()
            .and_then(|output| output.get_mut("content"))
            .expect("final content")
            .clone_from(&json!("different final"));
        assert!(
            commander_convergence_proof_from_runtime(&record, &wrong_final)
                .expect_err("final answer hash mismatch must fail at Router replay")
                .to_string()
                .contains("COMMANDER_CONVERGENCE_FINAL_ASSISTANT_HASH_MISMATCH")
        );
    }

    #[test]
    fn startup_convergence_evidence_distinguishes_absence_from_loader_errors() {
        let receipt = callback_receipt(TerminalState::Completed);
        let mut callback = DurableCallbackRecord::new(
            &receipt,
            json!("child result"),
            json!({"kind": "gateway.callback"}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".to_string(),
            },
        )
        .expect("callback");
        callback.commander_thread_id = Some("commander-thread-1".to_string());
        let record = ContinuationDispatchRecord::from_callback(&callback).expect("continuation");
        let binding = commander_continuation_binding(&record).expect("binding");

        let absent = tempfile::tempdir().expect("absent ledger root");
        assert!(
            terminal_commander_convergence_recovery_evidence(
                absent.path(),
                &record.commander_session_id,
                &record.runtime_id,
                &binding,
            )
            .expect("true absence")
            .is_none()
        );

        let malformed = tempfile::tempdir().expect("malformed ledger root");
        let ledger_root = malformed.path().join(".tura/run/effect_ledgers");
        std::fs::create_dir_all(&ledger_root).expect("ledger directory");
        std::fs::write(ledger_root.join("malformed.json"), b"{").expect("malformed ledger");
        assert!(
            terminal_commander_convergence_recovery_evidence(
                malformed.path(),
                &record.commander_session_id,
                &record.runtime_id,
                &binding,
            )
            .is_err()
        );

        let overfull = tempfile::tempdir().expect("overfull ledger root");
        let ledger_root = overfull.path().join(".tura/run/effect_ledgers");
        std::fs::create_dir_all(&ledger_root).expect("ledger directory");
        for index in 0..257 {
            std::fs::write(ledger_root.join(format!("{index:03}.json")), b"{}").expect("ledger");
        }
        assert!(
            terminal_commander_convergence_recovery_evidence(
                overfull.path(),
                &record.commander_session_id,
                &record.runtime_id,
                &binding,
            )
            .is_err()
        );
    }

    struct RecoveryTestEnvGuard {
        session_db: crate::services::session_db::SessionDbService,
        previous_db: Option<std::ffi::OsString>,
        previous_project: Option<std::ffi::OsString>,
    }

    impl RecoveryTestEnvGuard {
        fn install(
            session_db: crate::services::session_db::SessionDbService,
            db_root: &std::path::Path,
            project_root: &std::path::Path,
        ) -> Self {
            let guard = Self {
                session_db,
                previous_db: std::env::var_os("SESSION_LOG_DB_ROOT"),
                previous_project: std::env::var_os("TURA_PROJECT_ROOT"),
            };
            #[allow(unsafe_code)]
            unsafe {
                std::env::set_var("SESSION_LOG_DB_ROOT", db_root);
                std::env::set_var("TURA_PROJECT_ROOT", project_root);
            }
            guard
        }
    }

    impl Drop for RecoveryTestEnvGuard {
        fn drop(&mut self) {
            self.session_db.shutdown();
            #[allow(unsafe_code)]
            unsafe {
                match self.previous_db.take() {
                    Some(value) => std::env::set_var("SESSION_LOG_DB_ROOT", value),
                    None => std::env::remove_var("SESSION_LOG_DB_ROOT"),
                }
                match self.previous_project.take() {
                    Some(value) => std::env::set_var("TURA_PROJECT_ROOT", value),
                    None => std::env::remove_var("TURA_PROJECT_ROOT"),
                }
            }
        }
    }

    #[test]
    fn recovery_test_env_guard_restores_shared_env_during_unwind() {
        let _lock = crate::services::ROUTER_TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_db = std::env::var_os("SESSION_LOG_DB_ROOT");
        let previous_project = std::env::var_os("TURA_PROJECT_ROOT");
        let root = tempfile::tempdir().expect("env guard root");
        let result = std::panic::catch_unwind(|| {
            let _env = RecoveryTestEnvGuard::install(
                crate::services::session_db::SessionDbService::new(),
                root.path(),
                root.path(),
            );
            panic!("injected assertion unwind");
        });
        assert!(result.is_err());
        assert_eq!(std::env::var_os("SESSION_LOG_DB_ROOT"), previous_db);
        assert_eq!(std::env::var_os("TURA_PROJECT_ROOT"), previous_project);
    }

    #[tokio::test]
    async fn malformed_convergence_ledger_callsite_has_zero_durable_mutation() {
        let _lock = crate::services::ROUTER_TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let root = tempfile::tempdir().expect("isolated recovery root");
        let project = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("project root");
        let state = build_state();
        let _env = RecoveryTestEnvGuard::install(state.session_db.clone(), root.path(), project);
        state.session_db.start().expect("isolated session db");
        let session_id = "commander-callback";
        let directory = root.path().join("workspace");
        std::fs::create_dir_all(&directory).expect("workspace");
        let created = session_log_contract::client::call_service(
            &session_log_contract::SessionLogCommand::CreateSession(Box::new(
                session_log_contract::CreateSessionRequest {
                    command_id: "create-callsite".into(),
                    session_id: session_id.into(),
                    creation_command: lifecycle::SessionCommand::CreateSession {
                        task_plan: lifecycle::TaskPlan::default(),
                    },
                    copy_context: false,
                    workspace: directory.display().to_string(),
                    session_directory: directory.display().to_string(),
                    name: "callsite".into(),
                    created_at: 1,
                    model: None,
                    agent: None,
                    session_type: "coding".into(),
                    kill_processes_on_start: false,
                    validator_enabled: false,
                    force_planning: false,
                    model_variant: None,
                    model_acceleration_enabled: false,
                    disable_permission_restrictions: true,
                    use_last_tool_call_response: false,
                    auto_session_name: false,
                    initial_task_plan_patch: None,
                },
            )),
        )
        .expect("create session");
        assert!(matches!(
            created,
            session_log_contract::SessionLogResponse::SessionCommandApplied { .. }
        ));

        let receipt = callback_receipt(TerminalState::Completed);
        let mut callback = DurableCallbackRecord::new(
            &receipt,
            json!("child result"),
            json!({"kind": "gateway.callback"}),
            "a".repeat(64),
            session_lifecycle::canonical_value_sha256(&json!("delegated prompt")),
            CallbackEffectIdentity::Exact {
                effect_id: "message-1".into(),
            },
        )
        .expect("callback");
        callback.commander_thread_id = Some("commander-thread-1".into());
        let mut continuation =
            ContinuationDispatchRecord::from_callback(&callback).expect("continuation");
        continuation.state = ContinuationDispatchState::Dispatched;
        let runtime_id = continuation.runtime_id.clone();
        let lease_id = continuation.lease_id.clone();
        let store = lifecycle_store(session_id).expect("lifecycle store");
        store.write_terminal_receipt(&receipt).expect("receipt");
        store
            .intake(&receipt.transaction_id, &receipt.event_id)
            .expect("receipt intake");
        store.publish_callback(&callback).expect("callback publish");
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("callback intake");
        store
            .prepare_callback_continuation(&continuation)
            .expect("prepare continuation");
        store
            .mark_callback_continuation_dispatched(&continuation)
            .expect("dispatch continuation");
        register_and_activate_runtime(
            session_id,
            &runtime_id,
            &lease_id,
            None,
            Some(RuntimeLifecycleIdentity {
                commander_session_id: session_id.into(),
                transaction_id: continuation.request_id.clone(),
                parent_mission_revision_sha256: Some(
                    continuation.parent_mission_revision_sha256.clone(),
                ),
                delegated_input_sha256: Some(continuation.delegated_input_sha256.clone()),
                task_id: None,
                goal_id: None,
                operator_override: false,
                dispatch_runtime_id: runtime_id.clone(),
                dispatch_lease_id: lease_id.clone(),
                receipt_event_seq: 0,
            }),
        )
        .expect("register runtime");
        let snapshot = match session_log_contract::client::call_service(
            &session_log_contract::SessionLogCommand::GetRuntimeLease(
                session_log_contract::GetRuntimeLeaseRequest {
                    runtime_id: runtime_id.clone(),
                    database_path: None,
                },
            ),
        )
        .expect("lease")
        {
            session_log_contract::SessionLogResponse::RuntimeLeaseRead {
                runtime: Some(runtime),
            } => runtime,
            other => panic!("unexpected lease {other:?}"),
        };
        let before_runtime = serde_json::to_value(
            session_log_contract::client::call_service(
                &session_log_contract::SessionLogCommand::ReplayRuntime(
                    session_log_contract::ReplayRuntimeRequest {
                        runtime_id: runtime_id.clone(),
                    },
                ),
            )
            .expect("runtime before"),
        )
        .expect("serialize runtime");
        let before_session = read_session_snapshot(session_id).expect("session before");
        let lifecycle_before = store.readback().expect("lifecycle before");
        let ledgers = directory.join(".tura/run/effect_ledgers");
        std::fs::create_dir_all(&ledgers).expect("ledgers");
        std::fs::write(ledgers.join("malformed.json"), b"{").expect("malformed ledger");
        let error = ExecutionService::new().recovery_close_runtime(&state, json!({
            "receipt_id": "callsite-recovery", "database_path": snapshot.database_path,
            "runtime_id": runtime_id.clone(), "session_id": session_id, "lease_id": lease_id,
            "expected_lease_active": true, "expected_revision": snapshot.revision,
            "expected_last_event_seq": snapshot.last_event_seq,
            "expected_session_event_seq": snapshot.session_event_seq,
            "expected_session_state": snapshot.session_state, "reason": "orphaned_runtime"
        })).await.expect_err("malformed evidence must fail closed");
        assert!(
            error.to_string().contains("invalid execution ledger"),
            "{error}"
        );
        let after_runtime = serde_json::to_value(
            session_log_contract::client::call_service(
                &session_log_contract::SessionLogCommand::ReplayRuntime(
                    session_log_contract::ReplayRuntimeRequest { runtime_id },
                ),
            )
            .expect("runtime after"),
        )
        .expect("serialize runtime");
        assert_eq!(before_runtime, after_runtime);
        assert_eq!(
            before_session,
            read_session_snapshot(session_id).expect("session after")
        );
        assert_eq!(lifecycle_before, store.readback().expect("lifecycle after"));
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

    fn child_pre_admission_request() -> RegisterChildSessionRequest {
        let prompt = "exact delegated prompt";
        RegisterChildSessionRequest {
            parent_session_id: "commander-pre-admission".to_string(),
            parent_mission_revision_sha256: "a".repeat(64),
            commander_thread_id: Some("thread-pre-admission".to_string()),
            child_session_id: "child-pre-admission".to_string(),
            child_runtime_id: "runtime-pre-admission".to_string(),
            child_transaction_id: "transaction-pre-admission".to_string(),
            child_lease_id: "lease-pre-admission".to_string(),
            callback_request_id: "transaction-pre-admission".to_string(),
            effect_id: "runtime-pre-admission.message".to_string(),
            delegated_input_sha256: session_lifecycle::canonical_value_sha256(&json!(prompt)),
            session_directory: "/tmp/child-pre-admission".to_string(),
            session_name: "child pre-admission".to_string(),
            created_at_ms: 1,
            execution_payload: json!({"prompt": prompt}),
        }
    }

    #[test]
    fn child_execution_contract_is_validated_before_durable_admission() {
        let valid = child_pre_admission_request();
        let payload = validated_child_execution_payload(&valid).expect("valid exact request");
        assert_eq!(payload["parent_session_id"], valid.parent_session_id);
        assert_eq!(
            payload["delegated_input_sha256"],
            valid.delegated_input_sha256
        );

        let mut malformed = Vec::new();

        let mut invalid_execution = valid.clone();
        invalid_execution.execution_payload = json!({"worker_env": "invalid"});
        malformed.push(invalid_execution);

        let mut invalid_digest = valid.clone();
        invalid_digest.delegated_input_sha256 = "b".repeat(64);
        malformed.push(invalid_digest);

        let mut invalid_capsule = valid.clone();
        invalid_capsule.execution_payload = json!({
            "prompt": "exact delegated prompt",
            "task_context_capsule": {"schema_version": "invalid"},
            "jspace_contract": {"semantic_sha256": "c".repeat(64)}
        });
        malformed.push(invalid_capsule);

        let mut missing_jspace = valid.clone();
        missing_jspace.execution_payload = json!({
            "prompt": "exact delegated prompt",
            "task_context_capsule": {"schema_version": "invalid"}
        });
        malformed.push(missing_jspace);

        let mut conflicting_context = valid.clone();
        conflicting_context.execution_payload = json!({
            "prompt": "exact delegated prompt",
            "runtime_context": "raw",
            "task_context_capsule": {"schema_version": "invalid"}
        });
        malformed.push(conflicting_context);

        for request in malformed {
            validated_child_execution_payload(&request)
                .expect_err("malformed request must fail before durable admission");
        }
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
            assert_eq!(
                store
                    .readback()
                    .expect("missing admission readback")
                    .intaken_callbacks,
                0
            );

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
        assert_eq!(
            store
                .readback()
                .expect("mismatch readback")
                .intaken_callbacks,
            0
        );
    }
}
