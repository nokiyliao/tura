use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};

use lifecycle::{RuntimeAggregate, RuntimeEvent, RuntimeState};
use runtime_contract::LifecycleExecutionContext;
use session_lifecycle::{
    LifecycleConfig, SessionLifecycleStore, TerminalReceipt, TerminalReceiptIdentity,
    TerminalState, commander_store_path,
};
use session_log_contract::{
    ActivateRuntimeLeaseRequest, AppendSessionFeedEventRequest, CommitRuntimeEventRequest,
    RegisterRuntimeRequest, RuntimeEventCommitOutcome, RuntimeLeaseOutcome,
    RuntimeLifecycleIdentity, RuntimeRegistrationOutcome, SessionFeedAppendOutcome,
    SessionFeedEvent, SessionLogCommand, SessionLogResponse,
};

use crate::session_log_client::SessionLogClient;

#[derive(Debug, Clone)]
struct RuntimeCursor {
    lease_id: String,
    revision: u64,
    next_event_seq: u64,
    pending_terminal: Option<RuntimeEvent>,
}

enum FeedCommand {
    Append(Box<AppendSessionFeedEventRequest>),
    Barrier {
        runtime_id: String,
        response: mpsc::Sender<Result<(), String>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeFeedPublisher {
    runtime_id: String,
    target_session_id: String,
    lease_id: String,
    state: Arc<Mutex<RuntimeFeedState>>,
    sender: mpsc::Sender<FeedCommand>,
}

#[derive(Debug)]
struct RuntimeFeedState {
    next_event_seq: u64,
    assistant_text: String,
}

impl RuntimeFeedPublisher {
    pub(crate) fn publish(&self, mut event: SessionFeedEvent) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous_text_len = state.assistant_text.len();
        match &mut event {
            SessionFeedEvent::AssistantTextDelta { delta, .. } => {
                state.assistant_text.push_str(delta);
            }
            SessionFeedEvent::AgentMessage { reply_message, .. }
                if !state.assistant_text.is_empty() =>
            {
                reply_message.clone_from(&state.assistant_text);
            }
            _ => {}
        }
        let event_seq = state.next_event_seq;
        if self
            .sender
            .send(FeedCommand::Append(Box::new(
                AppendSessionFeedEventRequest {
                    runtime_id: self.runtime_id.clone(),
                    target_session_id: self.target_session_id.clone(),
                    lease_id: self.lease_id.clone(),
                    event_id: format!("{}:feed:{event_seq}", self.runtime_id),
                    event,
                },
            )))
            .is_err()
        {
            state.assistant_text.truncate(previous_text_len);
            return Err(format!("runtime {} feed writer stopped", self.runtime_id));
        }
        state.next_event_seq += 1;
        Ok(())
    }
}

/// Synchronous ordered writer owned by one supervised runtime worker.
///
/// A worker may execute several provider runtimes sequentially. Each runtime
/// gets its own lease and cursor; events are acknowledged locally only after
/// the session service confirms that exact sequence position.
#[derive(Debug)]
pub(crate) struct RuntimeEventWriter {
    session_id: String,
    initial_runtime_id: String,
    initial_lease_id: String,
    cursors: HashMap<String, RuntimeCursor>,
    feed_states: HashMap<String, Arc<Mutex<RuntimeFeedState>>>,
    feed_sender: Option<mpsc::Sender<FeedCommand>>,
    feed_worker: Option<std::thread::JoinHandle<()>>,
    client: SessionLogClient,
    lifecycle: Option<LifecycleExecutionContext>,
    next_receipt_event_seq: u64,
}

impl RuntimeEventWriter {
    pub(crate) fn commander_continuation(
        &self,
    ) -> Option<&runtime_contract::CommanderContinuationBinding> {
        self.lifecycle
            .as_ref()
            .and_then(|context| context.commander_continuation.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn new(
        session_id: String,
        initial_runtime_id: String,
        initial_lease_id: String,
    ) -> Result<Self, String> {
        Self::new_with_lifecycle(session_id, initial_runtime_id, initial_lease_id, None)
    }

    pub(crate) fn new_with_lifecycle(
        session_id: String,
        initial_runtime_id: String,
        initial_lease_id: String,
        lifecycle: Option<LifecycleExecutionContext>,
    ) -> Result<Self, String> {
        if session_id.trim().is_empty()
            || initial_runtime_id.trim().is_empty()
            || initial_lease_id.trim().is_empty()
        {
            return Err("runtime event writer identifiers must be non-empty".to_string());
        }
        let client = SessionLogClient::discover()
            .map_err(|error| format!("failed to discover session service: {error}"))?;
        let (feed_sender, feed_receiver) = mpsc::channel();
        let feed_client = client.clone();
        let feed_worker = std::thread::spawn(move || run_feed_worker(feed_client, feed_receiver));
        Ok(Self {
            session_id,
            initial_runtime_id,
            initial_lease_id,
            cursors: HashMap::new(),
            feed_states: HashMap::new(),
            feed_sender: Some(feed_sender),
            feed_worker: Some(feed_worker),
            client,
            lifecycle,
            next_receipt_event_seq: 0,
        })
    }

    pub(crate) fn flush(&mut self, runtime: &mut RuntimeAggregate) -> Result<(), String> {
        if runtime.session_id != self.session_id {
            return Err(format!(
                "runtime {} belongs to session {}, not writer session {}",
                runtime.runtime_id, runtime.session_id, self.session_id
            ));
        }
        self.prepare_runtime(runtime)?;

        while let Some(event) = runtime.next_uncommitted_event().cloned() {
            if is_terminal_event(&event) {
                let cursor = self
                    .cursors
                    .get_mut(&runtime.runtime_id)
                    .ok_or_else(|| format!("runtime {} has no event cursor", runtime.runtime_id))?;
                if cursor.pending_terminal.is_some() {
                    return Err(format!(
                        "runtime {} produced more than one terminal event",
                        runtime.runtime_id
                    ));
                }
                cursor.pending_terminal = Some(event);
                runtime.acknowledge_uncommitted_event();
                continue;
            }
            let cursor = self
                .cursors
                .get(&runtime.runtime_id)
                .cloned()
                .ok_or_else(|| format!("runtime {} has no event cursor", runtime.runtime_id))?;
            let (revision, next_event_seq) =
                commit_runtime_event(&self.client, &runtime.runtime_id, &cursor, event)?;
            let stored_cursor = self
                .cursors
                .get_mut(&runtime.runtime_id)
                .expect("runtime cursor was prepared before commit");
            stored_cursor.revision = revision;
            stored_cursor.next_event_seq = next_event_seq;
            runtime.acknowledge_uncommitted_event();
        }
        Ok(())
    }

    pub(crate) fn feed_publisher(
        &mut self,
        runtime_id: &str,
        target_session_id: &str,
    ) -> Result<RuntimeFeedPublisher, String> {
        let cursor = self
            .cursors
            .get(runtime_id)
            .ok_or_else(|| format!("runtime {runtime_id} has no event cursor"))?;
        let state = Arc::clone(
            self.feed_states
                .entry(runtime_id.to_string())
                .or_insert_with(|| {
                    Arc::new(Mutex::new(RuntimeFeedState {
                        next_event_seq: 1,
                        assistant_text: String::new(),
                    }))
                }),
        );
        Ok(RuntimeFeedPublisher {
            runtime_id: runtime_id.to_string(),
            target_session_id: target_session_id.to_string(),
            lease_id: cursor.lease_id.clone(),
            state,
            sender: self
                .feed_sender
                .as_ref()
                .ok_or_else(|| "runtime feed writer is closed".to_string())?
                .clone(),
        })
    }

    pub(crate) fn seal_runtime(&mut self, runtime_id: &str) -> Result<(), String> {
        self.seal_runtime_with(runtime_id, commit_runtime_event)
    }

    fn seal_runtime_with(
        &mut self,
        runtime_id: &str,
        commit: impl FnOnce(
            &SessionLogClient,
            &str,
            &RuntimeCursor,
            RuntimeEvent,
        ) -> Result<(u64, u64), String>,
    ) -> Result<(), String> {
        let feed_error = self.feed_barrier(runtime_id).err();
        let cursor = self
            .cursors
            .get(runtime_id)
            .cloned()
            .ok_or_else(|| format!("runtime {runtime_id} has no event cursor"))?;
        let Some(event) = cursor.pending_terminal.clone() else {
            return Err(format!(
                "runtime {runtime_id} has no pending terminal event"
            ));
        };
        self.write_terminal_receipt(runtime_id, &cursor, &event)?;
        let (revision, next_event_seq) = commit(&self.client, runtime_id, &cursor, event)?;
        let cursor = self
            .cursors
            .get_mut(runtime_id)
            .expect("runtime cursor existed before terminal commit");
        cursor.revision = revision;
        cursor.next_event_seq = next_event_seq;
        cursor.pending_terminal = None;
        self.next_receipt_event_seq += 1;
        feed_error.map_or(Ok(()), Err)
    }

    fn write_terminal_receipt(
        &self,
        runtime_id: &str,
        cursor: &RuntimeCursor,
        event: &RuntimeEvent,
    ) -> Result<(), String> {
        let Some(lifecycle) = self.lifecycle.as_ref() else {
            return Ok(());
        };
        if lifecycle.transaction_id.trim().is_empty()
            || lifecycle.commander_session_id.trim().is_empty()
        {
            return Err("runtime lifecycle identifiers must be non-empty".to_string());
        }
        let (terminal_state, finished_at_ms, runtime_state) = terminal_receipt_state(event)?;
        let idempotency_key = format!("{runtime_id}:{}", cursor.next_event_seq);
        let event_id = format!("{idempotency_key}:session-projection");
        let mut receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                &lifecycle.transaction_id,
                event_id,
                self.next_receipt_event_seq,
                &lifecycle.commander_session_id,
                &self.session_id,
                runtime_id,
                &cursor.lease_id,
            ),
            terminal_state,
            finished_at_ms,
        );
        receipt.task_id.clone_from(&lifecycle.task_id);
        receipt.goal_id.clone_from(&lifecycle.goal_id);
        receipt.operator_override = lifecycle.operator_override;
        if let Some(value) = &lifecycle.parent_mission_revision_sha256 {
            receipt.audit_metadata.insert(
                "parent_mission_revision_sha256".to_string(),
                serde_json::json!(value),
            );
        }
        if let Some(value) = &lifecycle.delegated_input_sha256 {
            receipt.audit_metadata.insert(
                "delegated_input_sha256".to_string(),
                serde_json::json!(value),
            );
        }
        if let Some(binding) = lifecycle.commander_continuation.as_ref() {
            receipt.audit_metadata.insert(
                "commander_continuation_request_id".to_string(),
                serde_json::json!(binding.continuation_request_id),
            );
            receipt.audit_metadata.insert(
                "commander_continuation_target_thread_id".to_string(),
                serde_json::json!(binding.target_thread_id),
            );
            if let Some(digest) = binding
                .continuation_request_id
                .strip_prefix("callback-continuation-request-")
            {
                receipt.audit_metadata.insert(
                    "commander_continuation_origin_runtime_id".to_string(),
                    serde_json::json!(format!("callback-continuation-runtime-{digest}")),
                );
                receipt.audit_metadata.insert(
                    "commander_continuation_origin_lease_id".to_string(),
                    serde_json::json!(format!("callback-continuation-lease-{digest}")),
                );
            }
        }
        receipt.audit_metadata.insert(
            "runtime_event_seq".to_string(),
            serde_json::json!(cursor.next_event_seq),
        );
        receipt.audit_metadata.insert(
            "runtime_expected_revision".to_string(),
            serde_json::json!(cursor.revision),
        );
        receipt.audit_metadata.insert(
            "runtime_state".to_string(),
            serde_json::json!(runtime_state),
        );
        receipt.audit_metadata.insert(
            "dispatch_runtime_id".to_string(),
            serde_json::json!(self.initial_runtime_id),
        );
        receipt.audit_metadata.insert(
            "dispatch_lease_id".to_string(),
            serde_json::json!(self.initial_lease_id),
        );
        let base = session_log_contract::client::default_db_dir().join("session_lifecycle_v1");
        let root = commander_store_path(&base, &lifecycle.commander_session_id)
            .map_err(|error| error.to_string())?;
        let store = SessionLifecycleStore::open(
            root,
            &lifecycle.commander_session_id,
            LifecycleConfig::default(),
        )
        .map_err(|error| error.to_string())?;
        store
            .write_terminal_receipt(&receipt)
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn feed_barrier(&self, runtime_id: &str) -> Result<(), String> {
        let (response, receiver) = mpsc::channel();
        self.feed_sender
            .as_ref()
            .ok_or_else(|| "runtime feed writer is closed".to_string())?
            .send(FeedCommand::Barrier {
                runtime_id: runtime_id.to_string(),
                response,
            })
            .map_err(|_| format!("runtime {runtime_id} feed writer stopped"))?;
        receiver
            .recv()
            .map_err(|_| format!("runtime {runtime_id} feed barrier was dropped"))?
    }

    fn prepare_runtime(&mut self, runtime: &RuntimeAggregate) -> Result<(), String> {
        let runtime_id = runtime.runtime_id.as_str();
        if self.cursors.contains_key(runtime_id) {
            return Ok(());
        }
        let response =
            self.client
                .call_typed(SessionLogCommand::RegisterRuntime(RegisterRuntimeRequest {
                    runtime_id: runtime_id.to_string(),
                    session_id: self.session_id.clone(),
                    fallback_from_id: runtime.fallback_from_id.clone(),
                    lifecycle: self
                        .lifecycle
                        .as_ref()
                        .map(|lifecycle| RuntimeLifecycleIdentity {
                            commander_session_id: lifecycle.commander_session_id.clone(),
                            transaction_id: lifecycle.transaction_id.clone(),
                            parent_mission_revision_sha256: lifecycle
                                .parent_mission_revision_sha256
                                .clone(),
                            delegated_input_sha256: lifecycle.delegated_input_sha256.clone(),
                            task_id: lifecycle.task_id.clone(),
                            goal_id: lifecycle.goal_id.clone(),
                            operator_override: lifecycle.operator_override,
                            dispatch_runtime_id: self.initial_runtime_id.clone(),
                            dispatch_lease_id: self.initial_lease_id.clone(),
                            receipt_event_seq: self.next_receipt_event_seq,
                        }),
                }))?;
        let (revision, next_event_seq) = match response {
            SessionLogResponse::RuntimeRegistered {
                result:
                    RuntimeRegistrationOutcome::Registered {
                        revision,
                        next_event_seq,
                        ..
                    }
                    | RuntimeRegistrationOutcome::AlreadyRegistered {
                        revision,
                        next_event_seq,
                        ..
                    },
            } => (revision, next_event_seq),
            SessionLogResponse::RuntimeRegistered { result } => {
                return Err(format!(
                    "session service rejected runtime {runtime_id} registration: {result:?}"
                ));
            }
            SessionLogResponse::Error { error } => {
                return Err(format!(
                    "session service failed to register runtime {runtime_id}: {error}"
                ));
            }
            other => {
                return Err(format!(
                    "unexpected session service runtime registration response: {other:?}"
                ));
            }
        };
        self.seed_receipt_sequence_after_registration()?;
        let lease_id = if runtime_id == self.initial_runtime_id {
            self.initial_lease_id.clone()
        } else {
            format!("lease-{}", uuid::Uuid::new_v4())
        };
        let response = self
            .client
            .call_typed(SessionLogCommand::ActivateRuntimeLease(
                ActivateRuntimeLeaseRequest {
                    runtime_id: runtime_id.to_string(),
                    lease_id: lease_id.clone(),
                },
            ))?;
        match response {
            SessionLogResponse::RuntimeLeaseActivated {
                result: RuntimeLeaseOutcome::Activated | RuntimeLeaseOutcome::AlreadyActive,
            } => {}
            SessionLogResponse::RuntimeLeaseActivated { result } => {
                return Err(format!(
                    "session service rejected runtime {runtime_id} lease: {result:?}"
                ));
            }
            SessionLogResponse::Error { error } => {
                return Err(format!(
                    "session service failed to activate runtime {runtime_id}: {error}"
                ));
            }
            other => {
                return Err(format!(
                    "unexpected session service runtime lease response: {other:?}"
                ));
            }
        }

        self.cursors.insert(
            runtime_id.to_string(),
            RuntimeCursor {
                lease_id,
                revision,
                next_event_seq,
                pending_terminal: None,
            },
        );
        Ok(())
    }

    fn seed_receipt_sequence_after_registration(&mut self) -> Result<(), String> {
        if !self.cursors.is_empty() {
            return Ok(());
        }
        let Some(lifecycle) = self.lifecycle.as_ref() else {
            return Ok(());
        };
        let base = session_log_contract::client::default_db_dir().join("session_lifecycle_v1");
        let root = commander_store_path(&base, &lifecycle.commander_session_id)
            .map_err(|error| error.to_string())?;
        let store = SessionLifecycleStore::open(
            root,
            &lifecycle.commander_session_id,
            LifecycleConfig::default(),
        )
        .map_err(|error| error.to_string())?;
        self.next_receipt_event_seq = store
            .next_receipt_event_sequence(&lifecycle.transaction_id)
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl Drop for RuntimeEventWriter {
    fn drop(&mut self) {
        self.feed_sender.take();
        if let Some(worker) = self.feed_worker.take() {
            let _ = worker.join();
        }
    }
}

fn is_terminal_event(event: &RuntimeEvent) -> bool {
    matches!(
        event,
        RuntimeEvent::RuntimeFinished { .. } | RuntimeEvent::RuntimeFailed { .. }
    )
}

fn terminal_receipt_state(
    event: &RuntimeEvent,
) -> Result<(TerminalState, i64, RuntimeState), String> {
    match event {
        RuntimeEvent::RuntimeFinished { finished_at, .. } => Ok((
            TerminalState::Completed,
            finished_at.timestamp_millis(),
            RuntimeState::Finished,
        )),
        RuntimeEvent::RuntimeFailed {
            finished_at, state, ..
        } => {
            let terminal_state = if *state == RuntimeState::Cancelled {
                TerminalState::Cancelled
            } else {
                TerminalState::Failed
            };
            Ok((terminal_state, finished_at.timestamp_millis(), *state))
        }
        _ => Err("runtime terminal receipt requires a terminal event".to_string()),
    }
}

fn commit_runtime_event(
    client: &SessionLogClient,
    runtime_id: &str,
    cursor: &RuntimeCursor,
    event: RuntimeEvent,
) -> Result<(u64, u64), String> {
    let response = client.call_typed(SessionLogCommand::CommitRuntimeEvent(
        CommitRuntimeEventRequest {
            runtime_id: runtime_id.to_string(),
            event_seq: cursor.next_event_seq,
            expected_revision: cursor.revision,
            lease_id: cursor.lease_id.clone(),
            idempotency_key: format!("{runtime_id}:{}", cursor.next_event_seq),
            event,
        },
    ))?;
    match response {
        SessionLogResponse::RuntimeEventCommitted {
            result:
                RuntimeEventCommitOutcome::Applied {
                    revision,
                    next_event_seq,
                    ..
                }
                | RuntimeEventCommitOutcome::Duplicate {
                    revision,
                    next_event_seq,
                },
        } => Ok((revision, next_event_seq)),
        SessionLogResponse::RuntimeEventCommitted { result } => Err(format!(
            "session service rejected runtime {runtime_id} event {}: {result:?}",
            cursor.next_event_seq
        )),
        SessionLogResponse::Error { error } => Err(format!(
            "session service failed to commit runtime {runtime_id} event {}: {error}",
            cursor.next_event_seq
        )),
        other => Err(format!(
            "unexpected session service runtime event response: {other:?}"
        )),
    }
}

fn run_feed_worker(client: SessionLogClient, receiver: mpsc::Receiver<FeedCommand>) {
    let mut errors = HashMap::<String, String>::new();
    while let Ok(command) = receiver.recv() {
        match command {
            FeedCommand::Append(request) => {
                if errors.contains_key(&request.runtime_id) {
                    continue;
                }
                let runtime_id = request.runtime_id.clone();
                let live_only =
                    matches!(&request.event, SessionFeedEvent::AssistantTextDelta { .. });
                let result = client.call_typed(SessionLogCommand::AppendSessionFeedEvent(*request));
                let error = match result {
                    Ok(SessionLogResponse::SessionFeedEventAppended {
                        result:
                            SessionFeedAppendOutcome::Applied { .. }
                            | SessionFeedAppendOutcome::Duplicate { .. }
                            | SessionFeedAppendOutcome::PublishedLive,
                    }) => None,
                    Ok(SessionLogResponse::SessionFeedEventAppended { result }) => Some(format!(
                        "session service rejected runtime {runtime_id} feed event: {result:?}"
                    )),
                    Ok(SessionLogResponse::Error { error }) => Some(format!(
                        "session service failed to append runtime {runtime_id} feed event: {error}"
                    )),
                    Ok(other) => Some(format!(
                        "unexpected session service feed response for runtime {runtime_id}: {other:?}"
                    )),
                    Err(error) => Some(error),
                };
                if let Some(error) = error {
                    if live_only {
                        tracing::warn!(
                            runtime_id = %runtime_id,
                            error = %error,
                            "dropping transient assistant text delta"
                        );
                    } else {
                        errors.insert(runtime_id, error);
                    }
                }
            }
            FeedCommand::Barrier {
                runtime_id,
                response,
            } => {
                let result = errors.remove(&runtime_id).map_or(Ok(()), Err);
                let _ = response.send(result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use lifecycle::{
        ProviderConfig, RuntimeError, RuntimeProviderConfig, SessionCommand, TaskPlan, ToolChoice,
    };
    use session_log_contract::{
        CreateSessionRequest, GetRuntimeLeaseRequest, ReadSessionFeedRequest, ReplayRuntimeRequest,
        SessionFeedEvent,
    };
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn feed_publisher_keeps_completed_text_when_no_live_delta_was_sent() {
        let (sender, receiver) = mpsc::channel();
        let publisher = RuntimeFeedPublisher {
            runtime_id: "runtime-final-only".to_string(),
            target_session_id: "session-final-only".to_string(),
            lease_id: "lease-final-only".to_string(),
            state: Arc::new(Mutex::new(RuntimeFeedState {
                next_event_seq: 1,
                assistant_text: String::new(),
            })),
            sender,
        };

        publisher
            .publish(SessionFeedEvent::AgentMessage {
                message_id: "runtime-final-only.message".to_string(),
                part_id: "runtime-final-only.message".to_string(),
                reply_message: "fallback final text".to_string(),
                new_learning: String::new(),
                runtime_status: None,
                context_tokens: None,
                usage: None,
                created_at: 1,
                updated_at: 2,
            })
            .expect("queue final-only message");

        let FeedCommand::Append(request) = receiver.recv().expect("queued final-only event") else {
            panic!("expected an appended feed event");
        };
        assert!(matches!(
            request.event,
            SessionFeedEvent::AgentMessage { reply_message, .. }
                if reply_message == "fallback final text"
        ));
    }

    #[test]
    fn feed_publisher_rolls_back_live_text_and_sequence_when_queue_is_closed() {
        let (sender, receiver) = mpsc::channel();
        drop(receiver);
        let state = Arc::new(Mutex::new(RuntimeFeedState {
            next_event_seq: 7,
            assistant_text: "kept".to_string(),
        }));
        let publisher = RuntimeFeedPublisher {
            runtime_id: "runtime-closed-feed".to_string(),
            target_session_id: "session-closed-feed".to_string(),
            lease_id: "lease-closed-feed".to_string(),
            state: Arc::clone(&state),
            sender,
        };

        assert!(
            publisher
                .publish(SessionFeedEvent::AssistantTextDelta {
                    message_id: "runtime-closed-feed.message".to_string(),
                    part_id: "runtime-closed-feed.message".to_string(),
                    delta: " discarded".to_string(),
                    created_at: 1,
                    updated_at: 2,
                })
                .is_err()
        );

        let state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(state.next_event_seq, 7);
        assert_eq!(state.assistant_text, "kept");
    }

    #[test]
    fn terminal_receipt_is_durable_before_commit_and_reuses_identity_on_retry() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let root = tempfile::tempdir().expect("isolated lifecycle root");
        let previous_root = std::env::var_os("SESSION_LOG_DB_ROOT");
        // SAFETY: ENV_LOCK serializes process-environment mutation in this module.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("SESSION_LOG_DB_ROOT", root.path())
        };

        let lifecycle = LifecycleExecutionContext {
            transaction_id: "ordering-transaction".to_string(),
            commander_session_id: "ordering-commander".to_string(),
            parent_mission_revision_sha256: None,
            delegated_input_sha256: None,
            task_id: Some("ordering-task".to_string()),
            goal_id: Some("ordering-goal".to_string()),
            operator_override: true,
            commander_continuation: None,
        };
        let mut writer = RuntimeEventWriter::new_with_lifecycle(
            "ordering-child".to_string(),
            "ordering-runtime".to_string(),
            "ordering-lease".to_string(),
            Some(lifecycle),
        )
        .expect("runtime writer");
        writer.cursors.insert(
            "ordering-runtime".to_string(),
            RuntimeCursor {
                lease_id: "ordering-lease".to_string(),
                revision: 4,
                next_event_seq: 5,
                pending_terminal: Some(RuntimeEvent::RuntimeFinished {
                    finished_at: Utc::now(),
                    usage: None,
                }),
            },
        );

        let event_id = "ordering-runtime:5:session-projection";
        let error = writer
            .seal_runtime_with("ordering-runtime", |_, runtime_id, cursor, event| {
                assert_eq!(runtime_id, "ordering-runtime");
                assert_eq!(cursor.next_event_seq, 5);
                assert!(matches!(event, RuntimeEvent::RuntimeFinished { .. }));
                let base =
                    session_log_contract::client::default_db_dir().join("session_lifecycle_v1");
                let store = SessionLifecycleStore::open(
                    commander_store_path(&base, "ordering-commander").expect("store path"),
                    "ordering-commander",
                    LifecycleConfig::default(),
                )
                .expect("lifecycle store");
                let receipt = store
                    .terminal_receipt("ordering-transaction", event_id)
                    .expect("receipt must exist before terminal commit");
                assert_eq!(receipt.runtime_id, "ordering-runtime");
                assert_eq!(receipt.lease_id, "ordering-lease");
                assert_eq!(receipt.event_seq, 0);
                Err("SIMULATED_CRASH_BEFORE_TERMINAL_COMMIT".to_string())
            })
            .expect_err("injected terminal commit crash");
        assert_eq!(error, "SIMULATED_CRASH_BEFORE_TERMINAL_COMMIT");

        let base = session_log_contract::client::default_db_dir().join("session_lifecycle_v1");
        let store = SessionLifecycleStore::open(
            commander_store_path(&base, "ordering-commander").expect("store path"),
            "ordering-commander",
            LifecycleConfig::default(),
        )
        .expect("lifecycle store");
        let readback = store.readback().expect("receipt readback after crash");
        assert_eq!(readback.pending_receipts, 1);
        assert_eq!(readback.applied_receipts, 0);
        assert!(
            writer
                .cursors
                .get("ordering-runtime")
                .and_then(|cursor| cursor.pending_terminal.as_ref())
                .is_some()
        );
        assert_eq!(writer.next_receipt_event_seq, 0);

        writer
            .seal_runtime_with("ordering-runtime", |_, runtime_id, cursor, event| {
                assert_eq!(runtime_id, "ordering-runtime");
                assert_eq!(cursor.next_event_seq, 5);
                assert!(matches!(event, RuntimeEvent::RuntimeFinished { .. }));
                Ok((5, 6))
            })
            .expect("retry reuses the durable receipt and commits");
        let receipt = store
            .terminal_receipt("ordering-transaction", event_id)
            .expect("same receipt remains durable");
        assert_eq!(receipt.event_seq, 0);
        assert_eq!(writer.next_receipt_event_seq, 1);
        assert!(
            writer
                .cursors
                .get("ordering-runtime")
                .and_then(|cursor| cursor.pending_terminal.as_ref())
                .is_none()
        );

        match previous_root {
            Some(value) => {
                // SAFETY: ENV_LOCK serializes process-environment mutation in this module.
                #[allow(
                    unsafe_code,
                    reason = "Rust 2024 process-environment mutation audited at the caller"
                )]
                unsafe {
                    std::env::set_var("SESSION_LOG_DB_ROOT", value)
                }
            }
            None => {
                // SAFETY: ENV_LOCK serializes process-environment mutation in this module.
                #[allow(
                    unsafe_code,
                    reason = "Rust 2024 process-environment mutation audited at the caller"
                )]
                unsafe {
                    std::env::remove_var("SESSION_LOG_DB_ROOT")
                }
            }
        }
    }

    #[test]
    fn feed_barrier_persists_exact_live_text_before_terminal_runtime_commit() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let root = tempfile::tempdir().expect("session db root");
        let home = root.path().join("home");
        std::fs::create_dir_all(&home).expect("session db home");
        let previous_home = std::env::var_os("TURA_HOME");
        let previous_root = std::env::var_os("SESSION_LOG_DB_ROOT");
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_HOME", &home)
        };
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("SESSION_LOG_DB_ROOT", root.path())
        };
        let handle = std::thread::spawn(session_log::service::run_socket_service);
        let started = std::time::Instant::now();
        while !session_log_contract::client::service_is_running() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(10),
                "session service did not start"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }

        let session_id = "writer-feed-session".to_string();
        let runtime_id = "writer-feed-runtime".to_string();
        let workspace = root.path().join("workspace").to_string_lossy().to_string();
        let now = Utc::now();
        session_log_contract::client::call_service(&SessionLogCommand::CreateSession(Box::new(
            CreateSessionRequest {
                command_id: format!("create:{session_id}"),
                session_id: session_id.clone(),
                creation_command: SessionCommand::CreateSession {
                    task_plan: TaskPlan::default(),
                },
                copy_context: false,
                workspace: workspace.clone(),
                session_directory: workspace,
                name: "writer feed ordering".to_string(),
                created_at: now.timestamp_millis(),
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
            },
        )))
        .expect("create session");

        let provider = RuntimeProviderConfig {
            base: ProviderConfig {
                tura_llm_name: "test".to_string(),
                default_model_tier: None,
                current_model: None,
                stream: true,
                temperature: 0.0,
                max_tokens: 1024,
                tool_choice: ToolChoice::Auto,
                time_out_ms: 30_000,
            },
            thinking: false,
            provider_name: "test".to_string(),
            model_name: "test-model".to_string(),
            provider_url_name: "local".to_string(),
            llm_provider_name: "test".to_string(),
        };
        let mut runtime = RuntimeAggregate::new(
            runtime_id.clone(),
            session_id.clone(),
            "agent".to_string(),
            provider.clone(),
            now,
        );
        let mut writer = RuntimeEventWriter::new(
            session_id.clone(),
            runtime_id.clone(),
            "writer-feed-lease".to_string(),
        )
        .expect("writer");
        writer.flush(&mut runtime).expect("flush creation");
        let full_provider_input = serde_json::json!({
            "messages": [
                {"role": "system", "content": "FULL_IDENTITY_SENTINEL"},
                {"role": "system", "content": "FULL_PROMPT_STYLE_SENTINEL"},
                {"role": "system", "content": "FULL_OPERATION_MANUAL_SENTINEL"},
                {"role": "user", "content": "visible user"},
                {"role": "assistant", "content": "visible assistant"}
            ],
            "tools": [{"type": "function", "name": "command_run"}],
            "options": {
                "stream": true,
                "tool_choice": "auto"
            }
        });
        runtime
            .set_input(full_provider_input.clone())
            .expect("capture full provider input");
        writer
            .flush(&mut runtime)
            .expect("persist full provider input");
        let stream_publisher = writer
            .feed_publisher(&runtime_id, &session_id)
            .expect("stream publisher");
        stream_publisher
            .publish(SessionFeedEvent::AssistantTextDelta {
                message_id: format!("{runtime_id}.message"),
                part_id: format!("{runtime_id}.message"),
                delta: "first paragraph\n\n".to_string(),
                created_at: now.timestamp_millis(),
                updated_at: now.timestamp_millis(),
            })
            .expect("queue live text delta");
        stream_publisher
            .publish(SessionFeedEvent::AssistantTextDelta {
                message_id: format!("{runtime_id}.message"),
                part_id: format!("{runtime_id}.message"),
                delta: "second paragraph".to_string(),
                created_at: now.timestamp_millis(),
                updated_at: now.timestamp_millis(),
            })
            .expect("queue second live text delta");
        drop(stream_publisher);
        let final_publisher = writer
            .feed_publisher(&runtime_id, &session_id)
            .expect("final publisher");
        final_publisher
            .publish(SessionFeedEvent::AgentMessage {
                message_id: format!("{runtime_id}.message"),
                part_id: format!("{runtime_id}.message"),
                reply_message: "first paragraph\nsecond paragraph".to_string(),
                new_learning: String::new(),
                runtime_status: None,
                context_tokens: None,
                usage: None,
                created_at: now.timestamp_millis(),
                updated_at: now.timestamp_millis(),
            })
            .expect("queue completed text event");
        runtime
            .mark_called(now + chrono::Duration::milliseconds(1))
            .expect("mark called");
        runtime.mark_waiting_first_token().expect("mark waiting");
        runtime
            .mark_first_token(now + chrono::Duration::milliseconds(2))
            .expect("first token");
        runtime
            .finish_success(now + chrono::Duration::milliseconds(3), None)
            .expect("finish runtime");
        writer.flush(&mut runtime).expect("defer terminal event");

        let replay = session_log_contract::client::call_service(&SessionLogCommand::ReplayRuntime(
            ReplayRuntimeRequest {
                runtime_id: runtime_id.clone(),
            },
        ))
        .expect("replay before seal");
        let SessionLogResponse::RuntimeReplayed {
            runtime: Some(replay),
        } = replay
        else {
            panic!("runtime replay missing before seal");
        };
        assert!(!replay.aggregate.state.is_terminal());
        assert_eq!(
            replay.aggregate.input.as_ref(),
            Some(&full_provider_input),
            "identity, prompt style, operation manual, messages, tools and options must replay exactly from SQLite"
        );

        writer.seal_runtime(&runtime_id).expect("seal runtime");
        let response = session_log_contract::client::call_service(
            &SessionLogCommand::ReadSessionFeed(ReadSessionFeedRequest {
                session_id: session_id.clone(),
                after_cursor: 0,
                limit: 10,
            }),
        )
        .expect("read feed");
        let SessionLogResponse::SessionFeed {
            entries,
            next_cursor,
        } = response
        else {
            panic!("session feed response missing");
        };
        assert_eq!(next_cursor, 4);
        assert_eq!(
            entries.iter().map(|entry| entry.cursor).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert!(matches!(
            &entries[0].event,
            SessionFeedEvent::SessionSnapshotCreated { .. }
        ));
        assert!(matches!(
            &entries[1].event,
            SessionFeedEvent::SessionProjectionUpdated { projection, .. }
                if !projection.state.is_terminal()
        ));
        assert!(matches!(
            &entries[2].event,
            SessionFeedEvent::AgentMessage { reply_message, .. }
                if reply_message == "first paragraph\n\nsecond paragraph"
        ));
        assert!(
            entries
                .iter()
                .all(|entry| !matches!(entry.event, SessionFeedEvent::AssistantTextDelta { .. })),
            "assistant text deltas must not be persisted in the Session feed"
        );
        let serialized_feed = serde_json::to_string(&entries).expect("serialize Session feed");
        assert!(!serialized_feed.contains("FULL_IDENTITY_SENTINEL"));
        assert!(!serialized_feed.contains("FULL_PROMPT_STYLE_SENTINEL"));
        assert!(!serialized_feed.contains("FULL_OPERATION_MANUAL_SENTINEL"));
        assert!(matches!(
            &entries[3].event,
            SessionFeedEvent::SessionProjectionUpdated { projection, .. }
                if projection.state.is_terminal()
        ));
        let replay = session_log_contract::client::call_service(&SessionLogCommand::ReplayRuntime(
            ReplayRuntimeRequest { runtime_id },
        ))
        .expect("replay after seal");
        let SessionLogResponse::RuntimeReplayed {
            runtime: Some(replay),
        } = replay
        else {
            panic!("runtime replay missing after seal");
        };
        assert!(replay.aggregate.state.is_terminal());

        let rejected_runtime_id = "writer-rejected-feed-runtime".to_string();
        let mut rejected_runtime = RuntimeAggregate::new(
            rejected_runtime_id.clone(),
            session_id.clone(),
            "agent".to_string(),
            provider,
            now,
        );
        let mut rejected_writer = RuntimeEventWriter::new(
            session_id.clone(),
            rejected_runtime_id.clone(),
            "writer-rejected-feed-lease".to_string(),
        )
        .expect("rejected feed writer");
        rejected_writer
            .flush(&mut rejected_runtime)
            .expect("flush rejected runtime creation");
        let rejected_publisher = rejected_writer
            .feed_publisher(&rejected_runtime_id, "missing-parent-session")
            .expect("rejected feed publisher");
        rejected_publisher
            .publish(SessionFeedEvent::AgentMessage {
                message_id: format!("{rejected_runtime_id}.message"),
                part_id: format!("{rejected_runtime_id}.message"),
                reply_message: "visible failure".to_string(),
                new_learning: String::new(),
                runtime_status: None,
                context_tokens: None,
                usage: None,
                created_at: now.timestamp_millis(),
                updated_at: now.timestamp_millis(),
            })
            .expect("queue rejected feed event");
        rejected_runtime
            .finish_failure(
                now + chrono::Duration::milliseconds(4),
                RuntimeError {
                    error_code: Some("PRE_PROVIDER_EXECUTE_TURN_FAILED".to_string()),
                    error_text: Some("missing route".to_string()),
                    retry_allowed: false,
                    fallback_allowed: false,
                    fallback_to_id: None,
                },
                RuntimeState::Failed,
                None,
            )
            .expect("finish rejected runtime");
        rejected_writer
            .flush(&mut rejected_runtime)
            .expect("defer rejected runtime terminal");

        let feed_error = rejected_writer
            .seal_runtime(&rejected_runtime_id)
            .expect_err("rejected feed remains observable after terminal commit");
        assert!(feed_error.contains("TargetSessionNotFound"));
        let replay = session_log_contract::client::call_service(&SessionLogCommand::ReplayRuntime(
            ReplayRuntimeRequest {
                runtime_id: rejected_runtime_id.clone(),
            },
        ))
        .expect("replay rejected feed runtime after seal");
        let SessionLogResponse::RuntimeReplayed {
            runtime: Some(replay),
        } = replay
        else {
            panic!("rejected feed runtime replay missing after seal");
        };
        assert!(replay.aggregate.state.is_terminal());
        assert_eq!(replay.aggregate.state, RuntimeState::Failed);
        let lease = session_log_contract::client::call_service(
            &SessionLogCommand::GetRuntimeLease(GetRuntimeLeaseRequest {
                runtime_id: rejected_runtime_id,
                database_path: None,
            }),
        )
        .expect("read rejected feed runtime lease after seal");
        let SessionLogResponse::RuntimeLeaseRead {
            runtime: Some(lease),
        } = lease
        else {
            panic!("rejected feed runtime lease missing after seal");
        };
        assert!(lease.terminal);
        assert!(!lease.lease_active);

        let _ = session_log_contract::client::call_service(&SessionLogCommand::Shutdown);
        let _ = handle.join();
        match previous_home {
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            Some(value) => {
                #[allow(
                    unsafe_code,
                    reason = "Rust 2024 process-environment mutation audited at the caller"
                )]
                unsafe {
                    std::env::set_var("TURA_HOME", value)
                }
            }
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            None => {
                #[allow(
                    unsafe_code,
                    reason = "Rust 2024 process-environment mutation audited at the caller"
                )]
                unsafe {
                    std::env::remove_var("TURA_HOME")
                }
            }
        }
        match previous_root {
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            Some(value) => {
                #[allow(
                    unsafe_code,
                    reason = "Rust 2024 process-environment mutation audited at the caller"
                )]
                unsafe {
                    std::env::set_var("SESSION_LOG_DB_ROOT", value)
                }
            }
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            None => {
                #[allow(
                    unsafe_code,
                    reason = "Rust 2024 process-environment mutation audited at the caller"
                )]
                unsafe {
                    std::env::remove_var("SESSION_LOG_DB_ROOT")
                }
            }
        }
    }
}
