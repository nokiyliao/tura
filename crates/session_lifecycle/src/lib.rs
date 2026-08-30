//! Receipt-driven, same-identity Session lifecycle shared by runtime and router.
//!
//! This sidecar records convergence state only. It cannot create, fork, delete,
//! or reassign a Session; `tura_session_db` remains the Session authority.

use fs2::FileExt;
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
    event::{AccessKind, AccessMode, ModifyKind},
};
use runtime_contract::CommanderConvergenceProof;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::time::Duration;

const RECEIPT_SCHEMA: &str = "tura_terminal_receipt_v1";
const STORED_RECEIPT_SCHEMA: &str = "tura_stored_terminal_receipt_v1";
const ACK_SCHEMA: &str = "tura_terminal_receipt_ack_v1";
const CALLBACK_SCHEMA: &str = "tura_durable_terminal_callback_v1";
const CALLBACK_ACK_SCHEMA: &str = "tura_durable_terminal_callback_ack_v1";
const CONTINUATION_SCHEMA: &str = "tura_callback_continuation_dispatch_v1";
const CHILD_ADMISSION_SCHEMA: &str = "tura_child_session_admission_v1";
const RELEASE_SCHEMA: &str = "tura_terminal_slot_release_v1";
const BLOCKER_SCHEMA: &str = "tura_session_lifecycle_blocker_v1";
const RECONCILE_SCHEMA: &str = "tura_session_lifecycle_reconcile_v1";
const CHECKPOINT_EVIDENCE_SCHEMA: &str = "tura_native_checkpoint_evidence_v1";
pub const DEFAULT_WATCHDOG_INTERVAL_SECS: u64 = 1_800;
pub const MINIMUM_WATCHDOG_INTERVAL_SECS: u64 = 301;
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

pub fn commander_store_path(base: &Path, commander_session_id: &str) -> LifecycleResult<PathBuf> {
    require_identifier("commander_session_id", commander_session_id)?;
    let digest = Sha256::digest(commander_session_id.as_bytes());
    Ok(base.join(format!("{:x}", digest)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleBlocker {
    pub code: String,
    pub detail: String,
}

impl LifecycleBlocker {
    fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for LifecycleBlocker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.detail.is_empty() {
            formatter.write_str(&self.code)
        } else {
            write!(formatter, "{}:{}", self.code, self.detail)
        }
    }
}

impl std::error::Error for LifecycleBlocker {}

type LifecycleResult<T> = Result<T, LifecycleBlocker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalState {
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalReceiptIdentity {
    pub transaction_id: String,
    pub event_id: String,
    pub event_seq: u64,
    pub commander_session_id: String,
    pub child_session_id: String,
    pub runtime_id: String,
    pub lease_id: String,
}

impl TerminalReceiptIdentity {
    pub fn new(
        transaction_id: impl Into<String>,
        event_id: impl Into<String>,
        event_seq: u64,
        commander_session_id: impl Into<String>,
        child_session_id: impl Into<String>,
        runtime_id: impl Into<String>,
        lease_id: impl Into<String>,
    ) -> Self {
        Self {
            transaction_id: transaction_id.into(),
            event_id: event_id.into(),
            event_seq,
            commander_session_id: commander_session_id.into(),
            child_session_id: child_session_id.into(),
            runtime_id: runtime_id.into(),
            lease_id: lease_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalReceipt {
    pub schema_version: String,
    pub transaction_id: String,
    pub event_id: String,
    pub event_seq: u64,
    pub commander_session_id: String,
    pub child_session_id: String,
    pub runtime_id: String,
    pub lease_id: String,
    pub terminal_state: TerminalState,
    pub finished_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(default)]
    pub operator_override: bool,
    #[serde(default)]
    pub history_readable: bool,
    #[serde(default)]
    pub follow_up_capable: bool,
    #[serde(default)]
    pub audit_metadata: BTreeMap<String, Value>,
}

impl TerminalReceipt {
    pub fn new(
        identity: TerminalReceiptIdentity,
        terminal_state: TerminalState,
        finished_at_ms: i64,
    ) -> Self {
        Self {
            schema_version: RECEIPT_SCHEMA.to_string(),
            transaction_id: identity.transaction_id,
            event_id: identity.event_id,
            event_seq: identity.event_seq,
            commander_session_id: identity.commander_session_id,
            child_session_id: identity.child_session_id,
            runtime_id: identity.runtime_id,
            lease_id: identity.lease_id,
            terminal_state,
            finished_at_ms,
            task_id: None,
            goal_id: None,
            operator_override: false,
            history_readable: true,
            follow_up_capable: true,
            audit_metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredReceipt {
    schema_version: String,
    payload_sha256: String,
    receipt: TerminalReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptWriteOutcome {
    Written(PathBuf),
    AlreadyDurable(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntakeOutcome {
    Applied { event_seq: u64 },
    Duplicate { event_seq: u64 },
    Pending { blocker: LifecycleBlocker },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckOutcome {
    Acknowledged,
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallbackEffectIdentity {
    Exact {
        effect_id: String,
    },
    ProvenZeroEffect {
        classification: String,
        evidence_sha256: String,
    },
    UnsettledEffect {
        classification: String,
        evidence_sha256: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildAdmissionRecord {
    pub schema_version: String,
    pub parent_session_id: String,
    pub parent_mission_revision_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commander_thread_id: Option<String>,
    pub child_session_id: String,
    pub child_runtime_id: String,
    pub child_transaction_id: String,
    pub child_lease_id: String,
    pub callback_request_id: String,
    pub effect_id: String,
    pub delegated_input_sha256: String,
    pub execution_payload_sha256: String,
    pub session_directory: String,
    pub session_name: String,
    pub created_at_ms: i64,
}

impl ChildAdmissionRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_session_id: impl Into<String>,
        parent_mission_revision_sha256: impl Into<String>,
        commander_thread_id: Option<String>,
        child_session_id: impl Into<String>,
        child_runtime_id: impl Into<String>,
        child_transaction_id: impl Into<String>,
        child_lease_id: impl Into<String>,
        callback_request_id: impl Into<String>,
        effect_id: impl Into<String>,
        delegated_input_sha256: impl Into<String>,
        execution_payload_sha256: impl Into<String>,
        session_directory: impl Into<String>,
        session_name: impl Into<String>,
        created_at_ms: i64,
    ) -> Self {
        Self {
            schema_version: CHILD_ADMISSION_SCHEMA.to_string(),
            parent_session_id: parent_session_id.into(),
            parent_mission_revision_sha256: parent_mission_revision_sha256.into(),
            commander_thread_id,
            child_session_id: child_session_id.into(),
            child_runtime_id: child_runtime_id.into(),
            child_transaction_id: child_transaction_id.into(),
            child_lease_id: child_lease_id.into(),
            callback_request_id: callback_request_id.into(),
            effect_id: effect_id.into(),
            delegated_input_sha256: delegated_input_sha256.into(),
            execution_payload_sha256: execution_payload_sha256.into(),
            session_directory: session_directory.into(),
            session_name: session_name.into(),
            created_at_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildAdmissionOutcome {
    Admitted,
    AlreadyAdmitted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableCallbackRecord {
    pub schema_version: String,
    pub transaction_id: String,
    pub event_id: String,
    pub commander_session_id: String,
    pub child_session_id: String,
    pub runtime_id: String,
    pub lease_id: String,
    pub terminal_receipt_sha256: String,
    pub terminal_state: TerminalState,
    pub callback_payload: Value,
    pub callback_payload_sha256: String,
    pub transport_payload: Value,
    pub transport_payload_sha256: String,
    pub parent_mission_revision_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commander_thread_id: Option<String>,
    pub delegated_input_sha256: String,
    pub effect_identity: CallbackEffectIdentity,
}

impl DurableCallbackRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        receipt: &TerminalReceipt,
        callback_payload: Value,
        transport_payload: Value,
        parent_mission_revision_sha256: impl Into<String>,
        delegated_input_sha256: impl Into<String>,
        effect_identity: CallbackEffectIdentity,
    ) -> LifecycleResult<Self> {
        Ok(Self {
            schema_version: CALLBACK_SCHEMA.to_string(),
            transaction_id: receipt.transaction_id.clone(),
            event_id: receipt.event_id.clone(),
            commander_session_id: receipt.commander_session_id.clone(),
            child_session_id: receipt.child_session_id.clone(),
            runtime_id: receipt.runtime_id.clone(),
            lease_id: receipt.lease_id.clone(),
            terminal_receipt_sha256: terminal_receipt_sha256(receipt)?,
            terminal_state: receipt.terminal_state,
            callback_payload_sha256: canonical_value_sha256(&callback_payload),
            callback_payload,
            transport_payload_sha256: canonical_value_sha256(&transport_payload),
            transport_payload,
            parent_mission_revision_sha256: parent_mission_revision_sha256.into(),
            commander_thread_id: None,
            delegated_input_sha256: delegated_input_sha256.into(),
            effect_identity,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackWriteOutcome {
    Written(PathBuf),
    AlreadyDurable(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackIntakeOutcome {
    Intaken,
    AlreadyIntaken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationDispatchState {
    Prepared,
    Dispatched,
    Completed,
    Acknowledged,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationDispatchRecord {
    pub schema_version: String,
    pub request_id: String,
    pub runtime_id: String,
    pub lease_id: String,
    pub commander_session_id: String,
    pub child_session_id: String,
    pub child_transaction_id: String,
    pub child_event_id: String,
    pub child_runtime_id: String,
    pub callback_payload_sha256: String,
    pub parent_mission_revision_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commander_thread_id: Option<String>,
    pub delegated_input_sha256: String,
    pub effect_identity: CallbackEffectIdentity,
    pub parent_input: Value,
    pub requested_action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_proof: Option<CommanderConvergenceProof>,
    pub state: ContinuationDispatchState,
}

impl ContinuationDispatchRecord {
    pub fn from_callback(callback: &DurableCallbackRecord) -> LifecycleResult<Self> {
        if matches!(
            callback.effect_identity,
            CallbackEffectIdentity::UnsettledEffect { .. }
        ) {
            return Err(LifecycleBlocker::new(
                "CONTINUATION_UNSETTLED_EFFECT_BLOCKED",
                &callback.event_id,
            ));
        }
        let digest = continuation_identity_sha256(callback)?;
        let requested_action = if callback.terminal_state == TerminalState::Completed {
            "MISSION_VERIFICATION"
        } else {
            "ROUTE_SELECTION"
        };
        let parent_input = serde_json::json!({
            "schema_version": "tura_internal_callback_continuation_v1",
            "authority": "none",
            "requested_action": requested_action,
            "callback": {
                "child_session_id": &callback.child_session_id,
                "transaction_id": &callback.transaction_id,
                "event_id": &callback.event_id,
                "runtime_id": &callback.runtime_id,
                "terminal_state": callback.terminal_state,
                "result": &callback.callback_payload,
                "callback_payload_sha256": &callback.callback_payload_sha256,
                "parent_mission_revision_sha256": &callback.parent_mission_revision_sha256,
                "delegated_input_sha256": &callback.delegated_input_sha256,
                "effect_identity": &callback.effect_identity,
            }
        });
        Ok(Self {
            schema_version: CONTINUATION_SCHEMA.to_string(),
            request_id: format!("callback-continuation-request-{digest}"),
            runtime_id: format!("callback-continuation-runtime-{digest}"),
            lease_id: format!("callback-continuation-lease-{digest}"),
            commander_session_id: callback.commander_session_id.clone(),
            child_session_id: callback.child_session_id.clone(),
            child_transaction_id: callback.transaction_id.clone(),
            child_event_id: callback.event_id.clone(),
            child_runtime_id: callback.runtime_id.clone(),
            callback_payload_sha256: callback.callback_payload_sha256.clone(),
            parent_mission_revision_sha256: callback.parent_mission_revision_sha256.clone(),
            commander_thread_id: callback.commander_thread_id.clone(),
            delegated_input_sha256: callback.delegated_input_sha256.clone(),
            effect_identity: callback.effect_identity.clone(),
            parent_input,
            requested_action: requested_action.to_string(),
            convergence_proof: None,
            state: ContinuationDispatchState::Prepared,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationWriteOutcome {
    Prepared,
    AlreadyPrepared,
    AlreadyDispatched,
    AlreadyCompleted,
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallbackAcknowledgement {
    schema_version: String,
    record: DurableCallbackRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptAcknowledgement {
    schema_version: String,
    transaction_id: String,
    event_id: String,
    event_seq: u64,
    commander_session_id: String,
    delivery_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityFailureKind {
    CompactionFailed,
    TransportUncertain,
    CallbackPending,
    ReplayPending,
    RestartPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingIdentityFailure {
    pub commander_session_id: String,
    pub transaction_id: String,
    pub event_id: String,
    pub kind: IdentityFailureKind,
    pub blocker: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointEventKind {
    CloseWrite,
    Modify,
    WatchdogReconcile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointIdentity {
    pub commander_session_id: String,
    pub checkpoint_id: String,
    pub source_path: PathBuf,
    pub inode: u64,
    pub size: u64,
    pub modified_ns: u128,
    pub sha256: String,
    pub complete_write: bool,
    pub event_kind: CheckpointEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointEvidence {
    pub schema_version: String,
    pub commander_session_id: String,
    pub child_session_id: String,
    pub checkpoint_id: String,
    pub stage: String,
    pub next_sequence: u64,
    pub next_management_sequence: u64,
    pub persisted_delta_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfig {
    pub watchdog_interval_secs: u64,
    pub event_channel_capacity: usize,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            watchdog_interval_secs: DEFAULT_WATCHDOG_INTERVAL_SECS,
            event_channel_capacity: 64,
        }
    }
}

impl LifecycleConfig {
    pub fn validate(self) -> LifecycleResult<Self> {
        if self.watchdog_interval_secs < MINIMUM_WATCHDOG_INTERVAL_SECS {
            return Err(LifecycleBlocker::new(
                "WATCHDOG_INTERVAL_NOT_DEMOTED",
                self.watchdog_interval_secs.to_string(),
            ));
        }
        if self.event_channel_capacity == 0 {
            return Err(LifecycleBlocker::new(
                "NATIVE_EVENT_CHANNEL_UNBOUNDED_OR_EMPTY",
                "capacity must be positive",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiveEffectEvidence {
    pub runtime_worker_alive: bool,
    pub active_tool_calls: usize,
    pub active_command_runs: usize,
    pub live_effect_processes: usize,
    pub pending_init: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclaimOutcome {
    Released,
    AlreadyReleased,
    Retained { blocker: LifecycleBlocker },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SlotReleaseReceipt {
    schema_version: String,
    transaction_id: String,
    event_id: String,
    commander_session_id: String,
    child_session_id: String,
    runtime_id: String,
    lease_id: String,
    history_readable: bool,
    follow_up_capable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockerRecord {
    schema_version: String,
    sequence: u64,
    code: String,
    detail: String,
    commander_session_id: String,
    transaction_id: Option<String>,
    event_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconcileRecord {
    schema_version: String,
    source: CheckpointEventKind,
    commander_session_id: String,
    checkpoint_id: String,
    blocker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleReadback {
    pub commander_session_id: String,
    pub primary_trigger: String,
    pub watchdog_interval_secs: u64,
    pub pending_receipts: usize,
    pub applied_receipts: usize,
    pub acknowledged_receipts: usize,
    pub pending_callbacks: usize,
    pub intaken_callbacks: usize,
    pub acknowledged_callbacks: usize,
    pub released_slots: usize,
    pub last_reconcile: Option<Value>,
    pub last_blocker: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct SessionLifecycleStore {
    root: PathBuf,
    commander_session_id: String,
    config: LifecycleConfig,
}

impl SessionLifecycleStore {
    pub fn open(
        root: impl Into<PathBuf>,
        commander_session_id: impl Into<String>,
        config: LifecycleConfig,
    ) -> LifecycleResult<Self> {
        let commander_session_id = commander_session_id.into();
        require_identifier("commander_session_id", &commander_session_id)?;
        let config = config.validate()?;
        let store = Self {
            root: root.into(),
            commander_session_id,
            config,
        };
        store.ensure_layout()?;
        Ok(store)
    }

    pub fn write_terminal_receipt(
        &self,
        receipt: &TerminalReceipt,
    ) -> LifecycleResult<ReceiptWriteOutcome> {
        self.validate_receipt(receipt)?;
        self.with_lock(|| {
            let key = receipt_key(&receipt.transaction_id, &receipt.event_id);
            for directory in ["applied", "pending"] {
                let path = self.root.join("receipts").join(directory).join(&key);
                if path.exists() {
                    let existing = read_stored_receipt(&path)?;
                    if existing.receipt == *receipt {
                        return Ok(ReceiptWriteOutcome::AlreadyDurable(path));
                    }
                    return Err(LifecycleBlocker::new("RECEIPT_IDENTITY_CONFLICT", key));
                }
            }
            let stored = StoredReceipt {
                schema_version: STORED_RECEIPT_SCHEMA.to_string(),
                payload_sha256: payload_sha256(receipt)?,
                receipt: receipt.clone(),
            };
            let path = self.root.join("receipts/pending").join(key);
            durable_write_json(&path, &stored)?;
            Ok(ReceiptWriteOutcome::Written(path))
        })
    }

    pub fn admit_child(
        &self,
        record: &ChildAdmissionRecord,
    ) -> LifecycleResult<ChildAdmissionOutcome> {
        self.validate_child_admission(record)?;
        self.with_lock(|| {
            let path = self
                .root
                .join("admissions/children")
                .join(child_admission_key(&record.child_session_id));
            if path.exists() {
                let existing: ChildAdmissionRecord = read_json(&path)?;
                self.validate_child_admission(&existing)?;
                if existing == *record {
                    return Ok(ChildAdmissionOutcome::AlreadyAdmitted);
                }
                return Err(LifecycleBlocker::new(
                    "CHILD_ADMISSION_IDENTITY_CONFLICT",
                    &record.child_session_id,
                ));
            }
            durable_write_json(&path, record)?;
            Ok(ChildAdmissionOutcome::Admitted)
        })
    }

    pub fn child_admission(
        &self,
        child_session_id: &str,
    ) -> LifecycleResult<Option<ChildAdmissionRecord>> {
        require_identifier("child_session_id", child_session_id)?;
        self.with_lock(|| {
            let path = self
                .root
                .join("admissions/children")
                .join(child_admission_key(child_session_id));
            if !path.exists() {
                return Ok(None);
            }
            let record: ChildAdmissionRecord = read_json(&path)?;
            self.validate_child_admission(&record)?;
            Ok(Some(record))
        })
    }

    pub fn terminal_receipt(
        &self,
        transaction_id: &str,
        event_id: &str,
    ) -> LifecycleResult<TerminalReceipt> {
        require_identifier("transaction_id", transaction_id)?;
        require_identifier("event_id", event_id)?;
        self.with_lock(|| {
            let key = receipt_key(transaction_id, event_id);
            for directory in ["applied", "pending"] {
                let path = self.root.join("receipts").join(directory).join(&key);
                if path.exists() {
                    let stored = read_stored_receipt(&path)?;
                    self.validate_receipt(&stored.receipt)?;
                    return Ok(stored.receipt);
                }
            }
            Err(LifecycleBlocker::new("TERMINAL_RECEIPT_NOT_FOUND", key))
        })
    }

    pub fn intake(&self, transaction_id: &str, event_id: &str) -> LifecycleResult<IntakeOutcome> {
        require_identifier("transaction_id", transaction_id)?;
        require_identifier("event_id", event_id)?;
        self.with_lock(|| {
            let key = receipt_key(transaction_id, event_id);
            let applied_path = self.root.join("receipts/applied").join(&key);
            if applied_path.exists() {
                let stored = read_stored_receipt(&applied_path)?;
                let pending_path = self.root.join("receipts/pending").join(&key);
                if pending_path.exists() {
                    let pending = read_stored_receipt(&pending_path)?;
                    if pending != stored {
                        return Err(LifecycleBlocker::new("RECEIPT_REPLAY_CONFLICT", key));
                    }
                    remove_durable(&pending_path)?;
                }
                return Ok(IntakeOutcome::Duplicate {
                    event_seq: stored.receipt.event_seq,
                });
            }
            let pending_path = self.root.join("receipts/pending").join(&key);
            if !pending_path.exists() {
                return Err(LifecycleBlocker::new("PENDING_RECEIPT_NOT_FOUND", key));
            }
            let stored = read_stored_receipt(&pending_path)?;
            self.validate_receipt(&stored.receipt)?;
            let expected = self.next_receipt_event_sequence_unlocked(transaction_id)?;
            if stored.receipt.event_seq != expected {
                let code = if stored.receipt.event_seq > expected {
                    "RECEIPT_EVENT_OUT_OF_ORDER"
                } else {
                    "RECEIPT_EVENT_SEQUENCE_CONFLICT"
                };
                let blocker = LifecycleBlocker::new(
                    code,
                    format!("expected={expected},received={}", stored.receipt.event_seq),
                );
                self.write_blocker_unlocked(&blocker, Some(transaction_id), Some(event_id))?;
                return Ok(IntakeOutcome::Pending { blocker });
            }
            durable_write_json(&applied_path, &stored)?;
            remove_durable(&pending_path)?;
            Ok(IntakeOutcome::Applied {
                event_seq: stored.receipt.event_seq,
            })
        })
    }

    pub fn replay_pending(&self) -> LifecycleResult<Vec<(String, String, IntakeOutcome)>> {
        let mut identities = Vec::new();
        for path in json_files(&self.root.join("receipts/pending"))? {
            let stored = read_stored_receipt(&path)?;
            identities.push((
                stored.receipt.transaction_id,
                stored.receipt.event_id,
                stored.receipt.event_seq,
            ));
        }
        identities.sort_by_key(|(_, _, sequence)| *sequence);
        identities
            .into_iter()
            .map(|(transaction_id, event_id, _)| {
                self.intake(&transaction_id, &event_id)
                    .map(|outcome| (transaction_id, event_id, outcome))
            })
            .collect()
    }

    pub fn acknowledge(
        &self,
        transaction_id: &str,
        event_id: &str,
        delivery_id: &str,
    ) -> LifecycleResult<AckOutcome> {
        require_identifier("delivery_id", delivery_id)?;
        self.with_lock(|| {
            let key = receipt_key(transaction_id, event_id);
            let applied = read_stored_receipt(&self.root.join("receipts/applied").join(&key))?;
            let acknowledgement = ReceiptAcknowledgement {
                schema_version: ACK_SCHEMA.to_string(),
                transaction_id: transaction_id.to_string(),
                event_id: event_id.to_string(),
                event_seq: applied.receipt.event_seq,
                commander_session_id: self.commander_session_id.clone(),
                delivery_id: delivery_id.to_string(),
            };
            let path = self.root.join("receipts/acknowledged").join(key);
            if path.exists() {
                let existing: ReceiptAcknowledgement = read_json(&path)?;
                if existing == acknowledgement {
                    return Ok(AckOutcome::AlreadyAcknowledged);
                }
                return Err(LifecycleBlocker::new(
                    "RECEIPT_ACK_CONFLICT",
                    path.display().to_string(),
                ));
            }
            durable_write_json(&path, &acknowledgement)?;
            Ok(AckOutcome::Acknowledged)
        })
    }

    pub fn publish_callback(
        &self,
        record: &DurableCallbackRecord,
    ) -> LifecycleResult<CallbackWriteOutcome> {
        self.validate_callback(record)?;
        self.with_lock(|| {
            let key = receipt_key(&record.transaction_id, &record.event_id);
            for directory in ["acknowledged", "intaken", "pending"] {
                let path = self.root.join("callbacks").join(directory).join(&key);
                if path.exists() {
                    if directory == "acknowledged" {
                        let acknowledgement: CallbackAcknowledgement = read_json(&path)?;
                        if acknowledgement.record == *record {
                            return Ok(CallbackWriteOutcome::AlreadyDurable(path));
                        }
                    } else {
                        let existing: DurableCallbackRecord = read_json(&path)?;
                        if existing == *record {
                            return Ok(CallbackWriteOutcome::AlreadyDurable(path));
                        }
                    }
                    return Err(LifecycleBlocker::new("CALLBACK_IDENTITY_CONFLICT", key));
                }
            }
            let path = self.root.join("callbacks/pending").join(key);
            durable_write_json(&path, record)?;
            Ok(CallbackWriteOutcome::Written(path))
        })
    }

    pub fn callbacks_for_replay(&self) -> LifecycleResult<Vec<DurableCallbackRecord>> {
        let mut callbacks = BTreeMap::new();
        for directory in ["pending", "intaken"] {
            for path in json_files(&self.root.join("callbacks").join(directory))? {
                let callback: DurableCallbackRecord = read_json(&path)?;
                let key = receipt_key(&callback.transaction_id, &callback.event_id);
                if self.root.join("callbacks/acknowledged").join(key).exists() {
                    continue;
                }
                self.validate_callback(&callback)?;
                let identity = (callback.transaction_id.clone(), callback.event_id.clone());
                if let Some(existing) = callbacks.get(&identity) {
                    if existing == &callback {
                        continue;
                    }
                    return Err(LifecycleBlocker::new(
                        "CALLBACK_IDENTITY_CONFLICT",
                        path.display().to_string(),
                    ));
                }
                callbacks.insert(identity, callback);
            }
        }
        Ok(callbacks.into_values().collect())
    }

    pub fn intaken_callback(
        &self,
        transaction_id: &str,
        event_id: &str,
    ) -> LifecycleResult<Option<DurableCallbackRecord>> {
        require_identifier("transaction_id", transaction_id)?;
        require_identifier("event_id", event_id)?;
        let path = self
            .root
            .join("callbacks/intaken")
            .join(receipt_key(transaction_id, event_id));
        if !path.exists() {
            return Ok(None);
        }
        let record: DurableCallbackRecord = read_json(&path)?;
        self.validate_callback(&record)?;
        Ok(Some(record))
    }

    pub fn mark_callback_intaken(
        &self,
        transaction_id: &str,
        event_id: &str,
        callback_payload_sha256: &str,
    ) -> LifecycleResult<CallbackIntakeOutcome> {
        require_sha256("callback_payload_sha256", callback_payload_sha256)?;
        self.with_lock(|| {
            let key = receipt_key(transaction_id, event_id);
            let intaken = self.root.join("callbacks/intaken").join(&key);
            if intaken.exists() {
                let record: DurableCallbackRecord = read_json(&intaken)?;
                if record.callback_payload_sha256 == callback_payload_sha256 {
                    return Ok(CallbackIntakeOutcome::AlreadyIntaken);
                }
                return Err(LifecycleBlocker::new("CALLBACK_INTAKE_CONFLICT", key));
            }
            let pending = self.root.join("callbacks/pending").join(&key);
            let record: DurableCallbackRecord = read_json(&pending)?;
            if record.callback_payload_sha256 != callback_payload_sha256 {
                return Err(LifecycleBlocker::new("CALLBACK_INTAKE_CONFLICT", key));
            }
            durable_write_json(&intaken, &record)?;
            remove_durable(&pending)?;
            Ok(CallbackIntakeOutcome::Intaken)
        })
    }

    pub fn acknowledge_callback(
        &self,
        transaction_id: &str,
        event_id: &str,
        callback_payload_sha256: &str,
        effect_identity: &CallbackEffectIdentity,
    ) -> LifecycleResult<AckOutcome> {
        require_sha256("callback_payload_sha256", callback_payload_sha256)?;
        if matches!(
            effect_identity,
            CallbackEffectIdentity::UnsettledEffect { .. }
        ) {
            return Err(LifecycleBlocker::new(
                "CALLBACK_UNSETTLED_EFFECT_ACK_BLOCKED",
                event_id,
            ));
        }
        self.with_lock(|| {
            let key = receipt_key(transaction_id, event_id);
            let acknowledgement = CallbackAcknowledgement {
                schema_version: CALLBACK_ACK_SCHEMA.to_string(),
                record: {
                    let intaken = self.root.join("callbacks/intaken").join(&key);
                    let record: DurableCallbackRecord = read_json(&intaken)?;
                    if record.callback_payload_sha256 != callback_payload_sha256
                        || record.effect_identity != *effect_identity
                    {
                        return Err(LifecycleBlocker::new("CALLBACK_ACK_CONFLICT", key));
                    }
                    record
                },
            };
            let acknowledged = self.root.join("callbacks/acknowledged").join(&key);
            if acknowledged.exists() {
                let existing: CallbackAcknowledgement = read_json(&acknowledged)?;
                return if existing == acknowledgement {
                    Ok(AckOutcome::AlreadyAcknowledged)
                } else {
                    Err(LifecycleBlocker::new("CALLBACK_ACK_CONFLICT", key))
                };
            }
            durable_write_json(&acknowledged, &acknowledgement)?;
            Ok(AckOutcome::Acknowledged)
        })
    }

    pub fn prepare_callback_continuation(
        &self,
        record: &ContinuationDispatchRecord,
    ) -> LifecycleResult<ContinuationWriteOutcome> {
        self.validate_continuation(record)?;
        self.with_lock(|| {
            let path = self.continuation_path(&record.child_transaction_id, &record.child_event_id);
            if path.exists() {
                let existing: ContinuationDispatchRecord = read_json(&path)?;
                if !continuation_bound_fields_match(&existing, record) {
                    return Err(LifecycleBlocker::new(
                        "CONTINUATION_IDENTITY_CONFLICT",
                        path.display().to_string(),
                    ));
                }
                return Ok(match existing.state {
                    ContinuationDispatchState::Prepared => {
                        ContinuationWriteOutcome::AlreadyPrepared
                    }
                    ContinuationDispatchState::Dispatched => {
                        ContinuationWriteOutcome::AlreadyDispatched
                    }
                    ContinuationDispatchState::Completed => {
                        ContinuationWriteOutcome::AlreadyCompleted
                    }
                    ContinuationDispatchState::Acknowledged => {
                        ContinuationWriteOutcome::AlreadyAcknowledged
                    }
                });
            }
            durable_write_json(&path, record)?;
            Ok(ContinuationWriteOutcome::Prepared)
        })
    }

    pub fn mark_callback_continuation_dispatched(
        &self,
        expected: &ContinuationDispatchRecord,
    ) -> LifecycleResult<ContinuationWriteOutcome> {
        self.transition_callback_continuation(expected, ContinuationDispatchState::Dispatched)
    }

    pub fn bind_callback_continuation_convergence_proof(
        &self,
        expected: &ContinuationDispatchRecord,
        proof: &CommanderConvergenceProof,
    ) -> LifecycleResult<ContinuationWriteOutcome> {
        self.validate_continuation(expected)?;
        self.validate_convergence_proof(expected, proof)?;
        self.with_lock(|| {
            let path =
                self.continuation_path(&expected.child_transaction_id, &expected.child_event_id);
            let mut existing: ContinuationDispatchRecord = read_json(&path)?;
            if !continuation_bound_fields_match(&existing, expected) {
                return Err(LifecycleBlocker::new(
                    "CONTINUATION_IDENTITY_CONFLICT",
                    path.display().to_string(),
                ));
            }
            if let Some(bound) = existing.convergence_proof.as_ref() {
                return if bound == proof {
                    Ok(match existing.state {
                        ContinuationDispatchState::Prepared => {
                            ContinuationWriteOutcome::AlreadyPrepared
                        }
                        ContinuationDispatchState::Dispatched => {
                            ContinuationWriteOutcome::AlreadyDispatched
                        }
                        ContinuationDispatchState::Completed => {
                            ContinuationWriteOutcome::AlreadyCompleted
                        }
                        ContinuationDispatchState::Acknowledged => {
                            ContinuationWriteOutcome::AlreadyAcknowledged
                        }
                    })
                } else {
                    Err(LifecycleBlocker::new(
                        "COMMANDER_CONVERGENCE_PROOF_CONFLICT",
                        &existing.request_id,
                    ))
                };
            }
            existing.convergence_proof = Some(proof.clone());
            durable_write_json(&path, &existing)?;
            Ok(match existing.state {
                ContinuationDispatchState::Prepared => ContinuationWriteOutcome::AlreadyPrepared,
                ContinuationDispatchState::Dispatched => {
                    ContinuationWriteOutcome::AlreadyDispatched
                }
                ContinuationDispatchState::Completed => ContinuationWriteOutcome::AlreadyCompleted,
                ContinuationDispatchState::Acknowledged => {
                    ContinuationWriteOutcome::AlreadyAcknowledged
                }
            })
        })
    }

    pub fn mark_callback_continuation_completed(
        &self,
        expected: &ContinuationDispatchRecord,
    ) -> LifecycleResult<ContinuationWriteOutcome> {
        self.transition_callback_continuation(expected, ContinuationDispatchState::Completed)
    }

    pub fn mark_callback_continuation_acknowledged(
        &self,
        expected: &ContinuationDispatchRecord,
    ) -> LifecycleResult<ContinuationWriteOutcome> {
        self.transition_callback_continuation(expected, ContinuationDispatchState::Acknowledged)
    }

    pub fn callback_continuations_for_replay(
        &self,
    ) -> LifecycleResult<Vec<ContinuationDispatchRecord>> {
        let mut records = Vec::new();
        for path in json_files(&self.root.join("continuations"))? {
            let record: ContinuationDispatchRecord = read_json(&path)?;
            self.validate_continuation(&record)?;
            if record.state != ContinuationDispatchState::Acknowledged {
                records.push(record);
            }
        }
        Ok(records)
    }

    pub fn callback_continuation_completion_proven(
        &self,
        record: &ContinuationDispatchRecord,
    ) -> LifecycleResult<bool> {
        self.validate_continuation(record)?;
        if record.commander_thread_id.is_some() && record.convergence_proof.is_none() {
            return Ok(false);
        }
        self.with_lock(|| {
            for path in json_files(&self.root.join("receipts/applied"))? {
                let stored = read_stored_receipt(&path)?;
                let receipt = &stored.receipt;
                let normal_runtime_identity =
                    receipt.runtime_id == record.runtime_id && receipt.lease_id == record.lease_id;
                let fallback_runtime_identity =
                    record
                        .commander_thread_id
                        .as_ref()
                        .is_some_and(|target_thread_id| {
                            receipt.runtime_id != record.runtime_id
                                && receipt
                                    .audit_metadata
                                    .get("commander_continuation_request_id")
                                    == Some(&Value::String(record.request_id.clone()))
                                && receipt
                                    .audit_metadata
                                    .get("commander_continuation_target_thread_id")
                                    == Some(&Value::String(target_thread_id.clone()))
                                && receipt
                                    .audit_metadata
                                    .get("commander_continuation_origin_runtime_id")
                                    == Some(&Value::String(record.runtime_id.clone()))
                                && receipt
                                    .audit_metadata
                                    .get("commander_continuation_origin_lease_id")
                                    == Some(&Value::String(record.lease_id.clone()))
                        });
                if receipt.transaction_id != record.request_id
                    || receipt.commander_session_id != record.commander_session_id
                    || receipt.child_session_id != record.commander_session_id
                    || (!normal_runtime_identity && !fallback_runtime_identity)
                    || receipt.terminal_state != TerminalState::Completed
                {
                    continue;
                }
                self.validate_receipt(receipt)?;
                let key = receipt_key(&receipt.transaction_id, &receipt.event_id);
                let released = self.root.join("slots/released").join(key);
                if !released.exists() {
                    return Ok(false);
                }
                let release: SlotReleaseReceipt = read_json(&released)?;
                return Ok(release.transaction_id == receipt.transaction_id
                    && release.event_id == receipt.event_id
                    && release.commander_session_id == receipt.commander_session_id
                    && release.child_session_id == receipt.child_session_id
                    && release.runtime_id == receipt.runtime_id
                    && release.lease_id == receipt.lease_id);
            }
            Ok(false)
        })
    }

    fn transition_callback_continuation(
        &self,
        expected: &ContinuationDispatchRecord,
        target: ContinuationDispatchState,
    ) -> LifecycleResult<ContinuationWriteOutcome> {
        self.validate_continuation(expected)?;
        self.with_lock(|| {
            let path =
                self.continuation_path(&expected.child_transaction_id, &expected.child_event_id);
            let mut existing: ContinuationDispatchRecord = read_json(&path)?;
            if !continuation_bound_fields_match(&existing, expected) {
                return Err(LifecycleBlocker::new(
                    "CONTINUATION_IDENTITY_CONFLICT",
                    path.display().to_string(),
                ));
            }
            if target == ContinuationDispatchState::Completed
                && existing.commander_thread_id.is_some()
            {
                let expected_proof = expected.convergence_proof.as_ref().ok_or_else(|| {
                    LifecycleBlocker::new(
                        "COMMANDER_CONVERGENCE_PROOF_MISSING",
                        &expected.request_id,
                    )
                })?;
                let persisted_proof = existing.convergence_proof.as_ref().ok_or_else(|| {
                    LifecycleBlocker::new(
                        "COMMANDER_CONVERGENCE_PROOF_NOT_DURABLE",
                        &expected.request_id,
                    )
                })?;
                if persisted_proof != expected_proof {
                    return Err(LifecycleBlocker::new(
                        "COMMANDER_CONVERGENCE_PROOF_CONFLICT",
                        &expected.request_id,
                    ));
                }
            }
            if existing.state == ContinuationDispatchState::Acknowledged {
                return Ok(ContinuationWriteOutcome::AlreadyAcknowledged);
            }
            if target == ContinuationDispatchState::Acknowledged
                && existing.state != ContinuationDispatchState::Completed
            {
                return Err(LifecycleBlocker::new(
                    "CONTINUATION_ACK_BEFORE_COMPLETION_BLOCKED",
                    &existing.request_id,
                ));
            }
            let valid_transition = matches!(
                (existing.state, target),
                (
                    ContinuationDispatchState::Prepared,
                    ContinuationDispatchState::Dispatched
                ) | (
                    ContinuationDispatchState::Dispatched,
                    ContinuationDispatchState::Completed
                ) | (
                    ContinuationDispatchState::Completed,
                    ContinuationDispatchState::Acknowledged
                )
            );
            if existing.state == target {
                return Ok(match target {
                    ContinuationDispatchState::Prepared => {
                        ContinuationWriteOutcome::AlreadyPrepared
                    }
                    ContinuationDispatchState::Dispatched => {
                        ContinuationWriteOutcome::AlreadyDispatched
                    }
                    ContinuationDispatchState::Completed => {
                        ContinuationWriteOutcome::AlreadyCompleted
                    }
                    ContinuationDispatchState::Acknowledged => {
                        ContinuationWriteOutcome::AlreadyAcknowledged
                    }
                });
            }
            if !valid_transition {
                return Err(LifecycleBlocker::new(
                    "CONTINUATION_STATE_TRANSITION_BLOCKED",
                    format!("{}:{:?}:{target:?}", existing.request_id, existing.state),
                ));
            }
            existing.state = target;
            durable_write_json(&path, &existing)?;
            Ok(match target {
                ContinuationDispatchState::Prepared => ContinuationWriteOutcome::AlreadyPrepared,
                ContinuationDispatchState::Dispatched => {
                    ContinuationWriteOutcome::AlreadyDispatched
                }
                ContinuationDispatchState::Completed => ContinuationWriteOutcome::AlreadyCompleted,
                ContinuationDispatchState::Acknowledged => {
                    ContinuationWriteOutcome::AlreadyAcknowledged
                }
            })
        })
    }

    fn continuation_path(&self, transaction_id: &str, event_id: &str) -> PathBuf {
        self.root
            .join("continuations")
            .join(receipt_key(transaction_id, event_id))
    }

    pub fn record_identity_failure(&self, failure: PendingIdentityFailure) -> LifecycleResult<()> {
        if failure.commander_session_id != self.commander_session_id {
            return Err(LifecycleBlocker::new(
                "COMMANDER_IDENTITY_MISMATCH",
                failure.commander_session_id,
            ));
        }
        require_identifier("transaction_id", &failure.transaction_id)?;
        require_identifier("event_id", &failure.event_id)?;
        let key = receipt_key(&failure.transaction_id, &failure.event_id);
        self.with_lock(|| {
            durable_write_json(&self.root.join("identity_pending").join(key), &failure)
        })
    }

    pub fn publish_checkpoint_evidence(
        &self,
        child_session_id: &str,
        stage: &str,
        next_sequence: u64,
        next_management_sequence: u64,
        persisted_delta: &[u8],
    ) -> LifecycleResult<PathBuf> {
        require_identifier("child_session_id", child_session_id)?;
        require_identifier("checkpoint_stage", stage)?;
        let persisted_delta_sha256 = format!("{:x}", Sha256::digest(persisted_delta));
        let checkpoint_id = checkpoint_evidence_id(
            &self.commander_session_id,
            child_session_id,
            stage,
            next_sequence,
            next_management_sequence,
            &persisted_delta_sha256,
        );
        let evidence = CheckpointEvidence {
            schema_version: CHECKPOINT_EVIDENCE_SCHEMA.to_string(),
            commander_session_id: self.commander_session_id.clone(),
            child_session_id: child_session_id.to_string(),
            checkpoint_id: checkpoint_id.clone(),
            stage: stage.to_string(),
            next_sequence,
            next_management_sequence,
            persisted_delta_sha256,
        };
        self.with_lock(|| {
            let path = self
                .root
                .join("checkpoints")
                .join(format!("{checkpoint_id}.json"));
            if path.exists() {
                let existing: CheckpointEvidence = read_json(&path)?;
                if existing == evidence {
                    return Ok(path);
                }
                return Err(LifecycleBlocker::new(
                    "CHECKPOINT_EVIDENCE_IDENTITY_CONFLICT",
                    checkpoint_id,
                ));
            }
            durable_write_json(&path, &evidence)?;
            Ok(path)
        })
    }

    pub fn admit_checkpoint(
        &self,
        first: &CheckpointIdentity,
        stable: &CheckpointIdentity,
    ) -> LifecycleResult<CheckpointIdentity> {
        self.validate_checkpoint_pair(first, stable, false)
    }

    pub fn reconcile_checkpoint(
        &self,
        first: &CheckpointIdentity,
        stable: &CheckpointIdentity,
    ) -> LifecycleResult<CheckpointIdentity> {
        let result = self.validate_checkpoint_pair(first, stable, true);
        let record = ReconcileRecord {
            schema_version: RECONCILE_SCHEMA.to_string(),
            source: CheckpointEventKind::WatchdogReconcile,
            commander_session_id: self.commander_session_id.clone(),
            checkpoint_id: stable.checkpoint_id.clone(),
            blocker: result.as_ref().err().map(|error| error.code.clone()),
        };
        durable_write_json(&self.root.join("readback/last_reconcile.json"), &record)?;
        result
    }

    pub fn reclaim_terminal_slot(
        &self,
        transaction_id: &str,
        event_id: &str,
        evidence: LiveEffectEvidence,
    ) -> LifecycleResult<ReclaimOutcome> {
        self.with_lock(|| {
            let key = receipt_key(transaction_id, event_id);
            let stored = read_stored_receipt(&self.root.join("receipts/applied").join(&key))?;
            let blocker = reclaim_blocker(evidence);
            if let Some(blocker) = blocker {
                self.write_blocker_unlocked(&blocker, Some(transaction_id), Some(event_id))?;
                return Ok(ReclaimOutcome::Retained { blocker });
            }
            let release = SlotReleaseReceipt {
                schema_version: RELEASE_SCHEMA.to_string(),
                transaction_id: transaction_id.to_string(),
                event_id: event_id.to_string(),
                commander_session_id: stored.receipt.commander_session_id,
                child_session_id: stored.receipt.child_session_id,
                runtime_id: stored.receipt.runtime_id,
                lease_id: stored.receipt.lease_id,
                history_readable: stored.receipt.history_readable,
                follow_up_capable: stored.receipt.follow_up_capable,
            };
            let path = self.root.join("slots/released").join(key);
            if path.exists() {
                let existing: SlotReleaseReceipt = read_json(&path)?;
                if existing == release {
                    return Ok(ReclaimOutcome::AlreadyReleased);
                }
                return Err(LifecycleBlocker::new(
                    "SLOT_RELEASE_IDENTITY_CONFLICT",
                    path.display().to_string(),
                ));
            }
            durable_write_json(&path, &release)?;
            Ok(ReclaimOutcome::Released)
        })
    }

    pub fn readback(&self) -> LifecycleResult<LifecycleReadback> {
        Ok(LifecycleReadback {
            commander_session_id: self.commander_session_id.clone(),
            primary_trigger: "native_checkpoint_event".to_string(),
            watchdog_interval_secs: self.config.watchdog_interval_secs,
            pending_receipts: json_files(&self.root.join("receipts/pending"))?.len(),
            applied_receipts: json_files(&self.root.join("receipts/applied"))?.len(),
            acknowledged_receipts: json_files(&self.root.join("receipts/acknowledged"))?.len(),
            pending_callbacks: json_files(&self.root.join("callbacks/pending"))?.len(),
            intaken_callbacks: json_files(&self.root.join("callbacks/intaken"))?.len(),
            acknowledged_callbacks: json_files(&self.root.join("callbacks/acknowledged"))?.len(),
            released_slots: json_files(&self.root.join("slots/released"))?.len(),
            last_reconcile: read_optional_value(&self.root.join("readback/last_reconcile.json"))?,
            last_blocker: read_optional_value(&self.root.join("readback/last_blocker.json"))?,
        })
    }

    fn ensure_layout(&self) -> LifecycleResult<()> {
        for relative in [
            "admissions/children",
            "receipts/pending",
            "receipts/applied",
            "receipts/acknowledged",
            "callbacks/pending",
            "callbacks/intaken",
            "callbacks/acknowledged",
            "continuations",
            "identity_pending",
            "checkpoints",
            "slots/released",
            "blockers",
            "readback",
        ] {
            fs::create_dir_all(self.root.join(relative)).map_err(io_blocker)?;
        }
        Ok(())
    }

    fn validate_child_admission(&self, record: &ChildAdmissionRecord) -> LifecycleResult<()> {
        if record.schema_version != CHILD_ADMISSION_SCHEMA {
            return Err(LifecycleBlocker::new(
                "CHILD_ADMISSION_SCHEMA_UNSUPPORTED",
                &record.schema_version,
            ));
        }
        for (name, value) in [
            ("parent_session_id", record.parent_session_id.as_str()),
            ("child_session_id", record.child_session_id.as_str()),
            ("child_runtime_id", record.child_runtime_id.as_str()),
            ("child_transaction_id", record.child_transaction_id.as_str()),
            ("child_lease_id", record.child_lease_id.as_str()),
            ("callback_request_id", record.callback_request_id.as_str()),
            ("effect_id", record.effect_id.as_str()),
            ("session_directory", record.session_directory.as_str()),
            ("session_name", record.session_name.as_str()),
        ] {
            require_identifier(name, value)?;
        }
        for (name, value) in [
            (
                "parent_mission_revision_sha256",
                record.parent_mission_revision_sha256.as_str(),
            ),
            (
                "delegated_input_sha256",
                record.delegated_input_sha256.as_str(),
            ),
            (
                "execution_payload_sha256",
                record.execution_payload_sha256.as_str(),
            ),
        ] {
            require_sha256(name, value)?;
        }
        if record.parent_session_id != self.commander_session_id {
            return Err(LifecycleBlocker::new(
                "CHILD_ADMISSION_PARENT_IDENTITY_MISMATCH",
                &record.parent_session_id,
            ));
        }
        if record.parent_session_id == record.child_session_id {
            return Err(LifecycleBlocker::new(
                "CHILD_ADMISSION_IDENTITY_CONFLICT",
                "parent_equals_child",
            ));
        }
        if record.callback_request_id != record.child_transaction_id {
            return Err(LifecycleBlocker::new(
                "CHILD_ADMISSION_CALLBACK_IDENTITY_CONFLICT",
                &record.callback_request_id,
            ));
        }
        let canonical_effect_id = format!("{}.message", record.child_runtime_id);
        if record.effect_id != canonical_effect_id {
            return Err(LifecycleBlocker::new(
                "CHILD_ADMISSION_EFFECT_IDENTITY_CONFLICT",
                format!("expected={canonical_effect_id},actual={}", record.effect_id),
            ));
        }
        if record.created_at_ms <= 0 {
            return Err(LifecycleBlocker::new(
                "CHILD_ADMISSION_IDENTITY_INVALID",
                "created_at_ms",
            ));
        }
        Ok(())
    }

    fn validate_receipt(&self, receipt: &TerminalReceipt) -> LifecycleResult<()> {
        if receipt.schema_version != RECEIPT_SCHEMA {
            return Err(LifecycleBlocker::new(
                "RECEIPT_SCHEMA_UNSUPPORTED",
                &receipt.schema_version,
            ));
        }
        for (name, value) in [
            ("transaction_id", receipt.transaction_id.as_str()),
            ("event_id", receipt.event_id.as_str()),
            (
                "commander_session_id",
                receipt.commander_session_id.as_str(),
            ),
            ("child_session_id", receipt.child_session_id.as_str()),
            ("runtime_id", receipt.runtime_id.as_str()),
            ("lease_id", receipt.lease_id.as_str()),
        ] {
            require_identifier(name, value)?;
        }
        if receipt.commander_session_id != self.commander_session_id {
            return Err(LifecycleBlocker::new(
                "COMMANDER_IDENTITY_MISMATCH",
                &receipt.commander_session_id,
            ));
        }
        if !receipt.history_readable || !receipt.follow_up_capable {
            return Err(LifecycleBlocker::new(
                "HISTORY_OR_FOLLOW_UP_NOT_PRESERVED",
                &receipt.event_id,
            ));
        }
        Ok(())
    }

    fn validate_callback(&self, record: &DurableCallbackRecord) -> LifecycleResult<()> {
        if record.schema_version != CALLBACK_SCHEMA {
            return Err(LifecycleBlocker::new(
                "CALLBACK_SCHEMA_UNSUPPORTED",
                &record.schema_version,
            ));
        }
        for (name, value) in [
            ("transaction_id", record.transaction_id.as_str()),
            ("event_id", record.event_id.as_str()),
            ("commander_session_id", record.commander_session_id.as_str()),
            ("child_session_id", record.child_session_id.as_str()),
            ("runtime_id", record.runtime_id.as_str()),
            ("lease_id", record.lease_id.as_str()),
        ] {
            require_identifier(name, value)?;
        }
        for (name, value) in [
            (
                "terminal_receipt_sha256",
                record.terminal_receipt_sha256.as_str(),
            ),
            (
                "callback_payload_sha256",
                record.callback_payload_sha256.as_str(),
            ),
            (
                "transport_payload_sha256",
                record.transport_payload_sha256.as_str(),
            ),
            (
                "parent_mission_revision_sha256",
                record.parent_mission_revision_sha256.as_str(),
            ),
            (
                "delegated_input_sha256",
                record.delegated_input_sha256.as_str(),
            ),
        ] {
            require_sha256(name, value)?;
        }
        if record.commander_session_id != self.commander_session_id {
            return Err(LifecycleBlocker::new(
                "COMMANDER_IDENTITY_MISMATCH",
                &record.commander_session_id,
            ));
        }
        if canonical_value_sha256(&record.callback_payload) != record.callback_payload_sha256
            || canonical_value_sha256(&record.transport_payload) != record.transport_payload_sha256
        {
            return Err(LifecycleBlocker::new(
                "CALLBACK_PAYLOAD_DIGEST_MISMATCH",
                &record.event_id,
            ));
        }
        if record.callback_payload_sha256 == record.delegated_input_sha256 {
            return Err(LifecycleBlocker::new(
                "PROMPT_ECHO_REJECTED",
                &record.event_id,
            ));
        }
        match &record.effect_identity {
            CallbackEffectIdentity::Exact { effect_id } => {
                require_identifier("effect_id", effect_id)?
            }
            CallbackEffectIdentity::ProvenZeroEffect {
                classification,
                evidence_sha256,
            }
            | CallbackEffectIdentity::UnsettledEffect {
                classification,
                evidence_sha256,
            } => {
                require_identifier("effect_classification", classification)?;
                require_sha256("effect_evidence_sha256", evidence_sha256)?;
            }
        }
        let receipt = self.terminal_receipt(&record.transaction_id, &record.event_id)?;
        if receipt.commander_session_id != record.commander_session_id
            || receipt.child_session_id != record.child_session_id
            || receipt.runtime_id != record.runtime_id
            || receipt.lease_id != record.lease_id
            || receipt.terminal_state != record.terminal_state
            || terminal_receipt_sha256(&receipt)? != record.terminal_receipt_sha256
        {
            return Err(LifecycleBlocker::new(
                "CALLBACK_TERMINAL_RECEIPT_IDENTITY_CONFLICT",
                &record.event_id,
            ));
        }
        Ok(())
    }

    fn validate_continuation(&self, record: &ContinuationDispatchRecord) -> LifecycleResult<()> {
        if record.schema_version != CONTINUATION_SCHEMA {
            return Err(LifecycleBlocker::new(
                "CONTINUATION_SCHEMA_UNSUPPORTED",
                &record.schema_version,
            ));
        }
        for (name, value) in [
            ("request_id", record.request_id.as_str()),
            ("runtime_id", record.runtime_id.as_str()),
            ("lease_id", record.lease_id.as_str()),
            ("commander_session_id", record.commander_session_id.as_str()),
            ("child_session_id", record.child_session_id.as_str()),
            ("child_transaction_id", record.child_transaction_id.as_str()),
            ("child_event_id", record.child_event_id.as_str()),
            ("child_runtime_id", record.child_runtime_id.as_str()),
        ] {
            require_identifier(name, value)?;
        }
        for (name, value) in [
            (
                "callback_payload_sha256",
                record.callback_payload_sha256.as_str(),
            ),
            (
                "parent_mission_revision_sha256",
                record.parent_mission_revision_sha256.as_str(),
            ),
            (
                "delegated_input_sha256",
                record.delegated_input_sha256.as_str(),
            ),
        ] {
            require_sha256(name, value)?;
        }
        if record.commander_session_id != self.commander_session_id {
            return Err(LifecycleBlocker::new(
                "COMMANDER_IDENTITY_MISMATCH",
                &record.commander_session_id,
            ));
        }
        if record.requested_action
            != record
                .parent_input
                .get("requested_action")
                .and_then(Value::as_str)
                .unwrap_or_default()
            || !matches!(
                record.requested_action.as_str(),
                "MISSION_VERIFICATION" | "ROUTE_SELECTION"
            )
        {
            return Err(LifecycleBlocker::new(
                "CONTINUATION_REQUESTED_ACTION_INVALID",
                &record.request_id,
            ));
        }
        if record
            .commander_thread_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(LifecycleBlocker::new(
                "COMMANDER_THREAD_IDENTITY_INVALID",
                &record.request_id,
            ));
        }
        if let Some(proof) = record.convergence_proof.as_ref() {
            self.validate_convergence_proof(record, proof)?;
        }
        if matches!(
            record.effect_identity,
            CallbackEffectIdentity::UnsettledEffect { .. }
        ) {
            return Err(LifecycleBlocker::new(
                "CONTINUATION_UNSETTLED_EFFECT_BLOCKED",
                &record.child_event_id,
            ));
        }
        let callback_result = record
            .parent_input
            .pointer("/callback/result")
            .ok_or_else(|| {
                LifecycleBlocker::new(
                    "CONTINUATION_CALLBACK_RESULT_MISSING",
                    &record.child_event_id,
                )
            })?;
        if canonical_value_sha256(callback_result) != record.callback_payload_sha256 {
            return Err(LifecycleBlocker::new(
                "CONTINUATION_CALLBACK_RESULT_HASH_MISMATCH",
                &record.child_event_id,
            ));
        }
        let callback_key = receipt_key(&record.child_transaction_id, &record.child_event_id);
        let intaken_path = self.root.join("callbacks/intaken").join(callback_key);
        if !intaken_path.exists() {
            return Err(LifecycleBlocker::new(
                "CONTINUATION_INTAKEN_CALLBACK_NOT_FOUND",
                &record.child_event_id,
            ));
        }
        let callback: DurableCallbackRecord = read_json(&intaken_path)?;
        self.validate_callback(&callback)?;
        let expected = ContinuationDispatchRecord::from_callback(&callback)?;
        if !continuation_bound_fields_match(&expected, record) {
            return Err(LifecycleBlocker::new(
                "CONTINUATION_CALLBACK_BINDING_CONFLICT",
                &record.child_event_id,
            ));
        }
        let digest = continuation_bound_identity_sha256(record)?;
        for (name, actual, expected) in [
            (
                "request_id",
                record.request_id.as_str(),
                format!("callback-continuation-request-{digest}"),
            ),
            (
                "runtime_id",
                record.runtime_id.as_str(),
                format!("callback-continuation-runtime-{digest}"),
            ),
            (
                "lease_id",
                record.lease_id.as_str(),
                format!("callback-continuation-lease-{digest}"),
            ),
        ] {
            if actual != expected {
                return Err(LifecycleBlocker::new(
                    "CONTINUATION_DERIVED_IDENTITY_MISMATCH",
                    name,
                ));
            }
        }
        Ok(())
    }

    fn validate_convergence_proof(
        &self,
        record: &ContinuationDispatchRecord,
        proof: &CommanderConvergenceProof,
    ) -> LifecycleResult<()> {
        proof
            .validate_shape()
            .map_err(|error| LifecycleBlocker::new("COMMANDER_CONVERGENCE_PROOF_INVALID", error))?;
        let commander_thread_id = record.commander_thread_id.as_deref().ok_or_else(|| {
            LifecycleBlocker::new(
                "COMMANDER_CONVERGENCE_PROOF_WITHOUT_TARGET",
                &record.request_id,
            )
        })?;
        let effect = serde_json::to_value(&record.effect_identity).map_err(json_blocker)?;
        let effect_identity_sha256 = canonical_value_sha256(&effect);
        if proof.request_id != record.request_id
            || proof.callback_payload_sha256 != record.callback_payload_sha256
            || proof.effect_identity_sha256 != effect_identity_sha256
            || proof.child_session_id != record.child_session_id
            || proof.child_transaction_id != record.child_transaction_id
            || proof.child_runtime_id != record.child_runtime_id
            || proof.requested_action != record.requested_action
            || proof.target_thread_id != commander_thread_id
            || proof.pre_revision_sha256 != record.parent_mission_revision_sha256
        {
            return Err(LifecycleBlocker::new(
                "COMMANDER_CONVERGENCE_PROOF_BINDING_MISMATCH",
                &record.request_id,
            ));
        }
        Ok(())
    }

    fn validate_checkpoint_pair(
        &self,
        first: &CheckpointIdentity,
        stable: &CheckpointIdentity,
        watchdog: bool,
    ) -> LifecycleResult<CheckpointIdentity> {
        if first.commander_session_id != self.commander_session_id
            || stable.commander_session_id != self.commander_session_id
        {
            return Err(LifecycleBlocker::new(
                "CHECKPOINT_SESSION_IDENTITY_MISMATCH",
                &stable.commander_session_id,
            ));
        }
        if first.checkpoint_id.trim().is_empty() || stable.checkpoint_id.trim().is_empty() {
            return Err(LifecycleBlocker::new(
                "CHECKPOINT_IDENTITY_MISSING",
                "checkpoint_id",
            ));
        }
        if !watchdog
            && !matches!(
                first.event_kind,
                CheckpointEventKind::CloseWrite | CheckpointEventKind::Modify
            )
        {
            return Err(LifecycleBlocker::new(
                "CHECKPOINT_NATIVE_EVENT_REQUIRED",
                format!("{:?}", first.event_kind),
            ));
        }
        if !first.complete_write || !stable.complete_write {
            return Err(LifecycleBlocker::new(
                "CHECKPOINT_WRITE_INCOMPLETE",
                &stable.checkpoint_id,
            ));
        }
        if first.checkpoint_id != stable.checkpoint_id
            || first.source_path != stable.source_path
            || first.inode != stable.inode
            || first.size != stable.size
            || first.modified_ns != stable.modified_ns
            || first.sha256 != stable.sha256
        {
            return Err(LifecycleBlocker::new(
                "CHECKPOINT_SOURCE_NOT_STABLE",
                &stable.checkpoint_id,
            ));
        }
        if stable.sha256.len() != 64 {
            return Err(LifecycleBlocker::new(
                "CHECKPOINT_SHA256_INVALID",
                &stable.sha256,
            ));
        }
        Ok(stable.clone())
    }

    pub fn next_receipt_event_sequence(&self, transaction_id: &str) -> LifecycleResult<u64> {
        require_identifier("transaction_id", transaction_id)?;
        self.with_lock(|| self.next_receipt_event_sequence_unlocked(transaction_id))
    }

    fn next_receipt_event_sequence_unlocked(&self, transaction_id: &str) -> LifecycleResult<u64> {
        let mut sequences = BTreeSet::new();
        for path in json_files(&self.root.join("receipts/applied"))? {
            let stored = read_stored_receipt(&path)?;
            if stored.receipt.transaction_id == transaction_id
                && !sequences.insert(stored.receipt.event_seq)
            {
                return Err(LifecycleBlocker::new(
                    "APPLIED_EVENT_SEQUENCE_DUPLICATE",
                    stored.receipt.event_seq.to_string(),
                ));
            }
        }
        let mut expected = 0;
        for sequence in sequences {
            if sequence != expected {
                return Err(LifecycleBlocker::new(
                    "APPLIED_EVENT_SEQUENCE_GAP",
                    format!("expected={expected},received={sequence}"),
                ));
            }
            expected += 1;
        }
        Ok(expected)
    }

    fn write_blocker_unlocked(
        &self,
        blocker: &LifecycleBlocker,
        transaction_id: Option<&str>,
        event_id: Option<&str>,
    ) -> LifecycleResult<()> {
        let blocker_directory = self.root.join("blockers");
        let last_path = self.root.join("readback/last_blocker.json");
        let last = if last_path.exists() {
            let last: BlockerRecord = read_json(&last_path)?;
            let durable_path = blocker_directory.join(format!("{:020}.json", last.sequence));
            let durable: BlockerRecord = read_json(&durable_path).map_err(|error| {
                LifecycleBlocker::new(
                    "BLOCKER_HIGH_WATER_MISMATCH",
                    format!("{}:{error}", durable_path.display()),
                )
            })?;
            if durable != last {
                return Err(LifecycleBlocker::new(
                    "BLOCKER_HIGH_WATER_MISMATCH",
                    durable_path.display().to_string(),
                ));
            }
            Some(last)
        } else {
            let mut entries = fs::read_dir(&blocker_directory).map_err(io_blocker)?;
            if entries.next().is_some() {
                return Err(LifecycleBlocker::new(
                    "BLOCKER_HIGH_WATER_MISSING",
                    blocker_directory.display().to_string(),
                ));
            }
            None
        };

        if last.as_ref().is_some_and(|record| {
            record.code == blocker.code
                && record.detail == blocker.detail
                && record.commander_session_id == self.commander_session_id
                && record.transaction_id.as_deref() == transaction_id
                && record.event_id.as_deref() == event_id
        }) {
            return Ok(());
        }

        let sequence = match last.as_ref() {
            Some(record) => record
                .sequence
                .checked_add(1)
                .ok_or_else(|| LifecycleBlocker::new("BLOCKER_SEQUENCE_EXHAUSTED", "u64"))?,
            None => 0,
        };
        let record = BlockerRecord {
            schema_version: BLOCKER_SCHEMA.to_string(),
            sequence,
            code: blocker.code.clone(),
            detail: blocker.detail.clone(),
            commander_session_id: self.commander_session_id.clone(),
            transaction_id: transaction_id.map(str::to_string),
            event_id: event_id.map(str::to_string),
        };
        let record_path = blocker_directory.join(format!("{sequence:020}.json"));
        if record_path.exists() {
            let existing: BlockerRecord = read_json(&record_path)?;
            if existing != record {
                return Err(LifecycleBlocker::new(
                    "BLOCKER_SEQUENCE_CONFLICT",
                    record_path.display().to_string(),
                ));
            }
        } else {
            durable_write_json(&record_path, &record)?;
        }
        durable_write_json(&last_path, &record)
    }

    fn with_lock<T>(&self, operation: impl FnOnce() -> LifecycleResult<T>) -> LifecycleResult<T> {
        let lock_path = self.root.join("lifecycle.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(io_blocker)?;
        lock.lock_exclusive().map_err(io_blocker)?;
        let result = operation();
        FileExt::unlock(&lock).map_err(io_blocker)?;
        result
    }
}

pub fn checkpoint_identity_from_path(
    path: &Path,
    event_kind: CheckpointEventKind,
) -> LifecycleResult<CheckpointIdentity> {
    let evidence: CheckpointEvidence = read_json(path)?;
    if evidence.schema_version != CHECKPOINT_EVIDENCE_SCHEMA {
        return Err(LifecycleBlocker::new(
            "CHECKPOINT_EVIDENCE_SCHEMA_UNSUPPORTED",
            evidence.schema_version,
        ));
    }
    let expected_id = checkpoint_evidence_id(
        &evidence.commander_session_id,
        &evidence.child_session_id,
        &evidence.stage,
        evidence.next_sequence,
        evidence.next_management_sequence,
        &evidence.persisted_delta_sha256,
    );
    if evidence.checkpoint_id != expected_id {
        return Err(LifecycleBlocker::new(
            "CHECKPOINT_EVIDENCE_DIGEST_MISMATCH",
            evidence.checkpoint_id,
        ));
    }
    let bytes = fs::read(path).map_err(io_blocker)?;
    let metadata = fs::metadata(path).map_err(io_blocker)?;
    let modified_ns = metadata
        .modified()
        .map_err(io_blocker)?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| LifecycleBlocker::new("CHECKPOINT_MTIME_INVALID", error.to_string()))?
        .as_nanos();
    Ok(CheckpointIdentity {
        commander_session_id: evidence.commander_session_id,
        checkpoint_id: expected_id,
        source_path: path.to_path_buf(),
        inode: checkpoint_inode(&metadata),
        size: metadata.len(),
        modified_ns,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        complete_write: true,
        event_kind,
    })
}

fn continuation_identity_sha256(callback: &DurableCallbackRecord) -> LifecycleResult<String> {
    continuation_identity_sha256_from_fields(
        &callback.commander_session_id,
        &callback.child_session_id,
        &callback.transaction_id,
        &callback.event_id,
        &callback.runtime_id,
        &callback.callback_payload_sha256,
        &callback.parent_mission_revision_sha256,
        &callback.delegated_input_sha256,
        &callback.effect_identity,
    )
}

fn continuation_bound_identity_sha256(
    record: &ContinuationDispatchRecord,
) -> LifecycleResult<String> {
    continuation_identity_sha256_from_fields(
        &record.commander_session_id,
        &record.child_session_id,
        &record.child_transaction_id,
        &record.child_event_id,
        &record.child_runtime_id,
        &record.callback_payload_sha256,
        &record.parent_mission_revision_sha256,
        &record.delegated_input_sha256,
        &record.effect_identity,
    )
}

#[allow(clippy::too_many_arguments)]
fn continuation_identity_sha256_from_fields(
    commander_session_id: &str,
    child_session_id: &str,
    transaction_id: &str,
    event_id: &str,
    child_runtime_id: &str,
    callback_payload_sha256: &str,
    parent_mission_revision_sha256: &str,
    delegated_input_sha256: &str,
    effect_identity: &CallbackEffectIdentity,
) -> LifecycleResult<String> {
    let effect = serde_json::to_value(effect_identity).map_err(json_blocker)?;
    let effect_sha256 = canonical_value_sha256(&effect);
    let mut hasher = Sha256::new();
    for value in [
        commander_session_id,
        child_session_id,
        transaction_id,
        event_id,
        child_runtime_id,
        callback_payload_sha256,
        parent_mission_revision_sha256,
        delegated_input_sha256,
        effect_sha256.as_str(),
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn continuation_bound_fields_match(
    existing: &ContinuationDispatchRecord,
    expected: &ContinuationDispatchRecord,
) -> bool {
    let mut existing = existing.clone();
    let mut expected = expected.clone();
    existing.state = ContinuationDispatchState::Prepared;
    expected.state = ContinuationDispatchState::Prepared;
    existing.convergence_proof = None;
    expected.convergence_proof = None;
    existing == expected
}

fn checkpoint_evidence_id(
    commander_session_id: &str,
    child_session_id: &str,
    stage: &str,
    next_sequence: u64,
    next_management_sequence: u64,
    persisted_delta_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    let next_sequence = next_sequence.to_string();
    let next_management_sequence = next_management_sequence.to_string();
    for value in [
        commander_session_id,
        child_session_id,
        stage,
        &next_sequence,
        &next_management_sequence,
        persisted_delta_sha256,
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(unix)]
fn checkpoint_inode(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.ino()
}

#[cfg(not(unix))]
fn checkpoint_inode(_metadata: &fs::Metadata) -> u64 {
    0
}

pub struct NativeCheckpointEventAdapter {
    _watcher: RecommendedWatcher,
    receiver: Receiver<Result<Event, notify::Error>>,
}

#[derive(Debug)]
pub enum NativeEventRead {
    Event(Event),
    Ignored,
    ChannelFull,
    Disconnected,
}

impl NativeCheckpointEventAdapter {
    pub fn watch(path: &Path, capacity: usize) -> LifecycleResult<Self> {
        if capacity == 0 {
            return Err(LifecycleBlocker::new(
                "NATIVE_EVENT_CHANNEL_UNBOUNDED_OR_EMPTY",
                "capacity must be positive",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let mut watcher = notify::recommended_watcher(move |event| {
            send_native_event(&sender, event);
        })
        .map_err(notify_blocker)?;
        watcher
            .watch(path, RecursiveMode::NonRecursive)
            .map_err(notify_blocker)?;
        Ok(Self {
            _watcher: watcher,
            receiver,
        })
    }

    pub fn receive(&self, wait: Duration) -> NativeEventRead {
        match self.receiver.recv_timeout(wait) {
            Ok(Ok(event)) if is_checkpoint_write_event(&event) => NativeEventRead::Event(event),
            Ok(Ok(_)) | Ok(Err(_)) => NativeEventRead::Ignored,
            Err(RecvTimeoutError::Timeout) => NativeEventRead::Ignored,
            Err(RecvTimeoutError::Disconnected) => NativeEventRead::Disconnected,
        }
    }
}

fn send_native_event(
    sender: &SyncSender<Result<Event, notify::Error>>,
    event: Result<Event, notify::Error>,
) {
    match sender.try_send(event) {
        Ok(()) | Err(TrySendError::Disconnected(_)) => {}
        Err(TrySendError::Full(_)) => {
            // Event loss is intentionally reconciled by the watchdog; the
            // callback never blocks the native FSEvents thread.
        }
    }
}

pub fn is_checkpoint_write_event(event: &Event) -> bool {
    matches!(
        event.kind,
        EventKind::Access(AccessKind::Close(AccessMode::Write))
            | EventKind::Modify(ModifyKind::Data(_))
            | EventKind::Modify(ModifyKind::Any)
    )
}

fn reclaim_blocker(evidence: LiveEffectEvidence) -> Option<LifecycleBlocker> {
    if evidence.runtime_worker_alive {
        Some(LifecycleBlocker::new(
            "LIVE_RUNTIME_WORKER_REMAINS",
            "runtime_worker_alive=true",
        ))
    } else if evidence.active_tool_calls > 0 {
        Some(LifecycleBlocker::new(
            "LIVE_TOOL_CALL_REMAINS",
            evidence.active_tool_calls.to_string(),
        ))
    } else if evidence.active_command_runs > 0 {
        Some(LifecycleBlocker::new(
            "LIVE_COMMAND_RUN_REMAINS",
            evidence.active_command_runs.to_string(),
        ))
    } else if evidence.live_effect_processes > 0 {
        Some(LifecycleBlocker::new(
            "LIVE_EFFECT_PROCESS_REMAINS",
            evidence.live_effect_processes.to_string(),
        ))
    } else if evidence.pending_init {
        Some(LifecycleBlocker::new(
            "PENDING_INIT_REMAINS",
            "pending_init=true",
        ))
    } else {
        None
    }
}

fn require_identifier(name: &str, value: &str) -> LifecycleResult<()> {
    if value.trim().is_empty() {
        Err(LifecycleBlocker::new("LIFECYCLE_IDENTITY_MISSING", name))
    } else {
        Ok(())
    }
}

fn require_sha256(name: &str, value: &str) -> LifecycleResult<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(LifecycleBlocker::new("LIFECYCLE_SHA256_INVALID", name))
    }
}

fn receipt_key(transaction_id: &str, event_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(transaction_id.as_bytes());
    hasher.update([0]);
    hasher.update(event_id.as_bytes());
    format!("{:x}.json", hasher.finalize())
}

fn child_admission_key(child_session_id: &str) -> String {
    format!("{:x}.json", Sha256::digest(child_session_id.as_bytes()))
}

fn payload_sha256(receipt: &TerminalReceipt) -> LifecycleResult<String> {
    let bytes = serde_json::to_vec(receipt).map_err(json_blocker)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub fn terminal_receipt_sha256(receipt: &TerminalReceipt) -> LifecycleResult<String> {
    payload_sha256(receipt)
}

pub fn canonical_value_sha256(value: &Value) -> String {
    fn canonical(value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::String(value) => serde_json::to_string(value).expect("JSON string is encodable"),
            Value::Array(values) => format!(
                "[{}]",
                values.iter().map(canonical).collect::<Vec<_>>().join(",")
            ),
            Value::Object(values) => {
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort();
                let fields = keys
                    .into_iter()
                    .map(|key| {
                        format!(
                            "{}:{}",
                            serde_json::to_string(key).expect("JSON key is encodable"),
                            canonical(&values[key])
                        )
                    })
                    .collect::<Vec<_>>();
                format!("{{{}}}", fields.join(","))
            }
        }
    }
    format!("{:x}", Sha256::digest(canonical(value).as_bytes()))
}

fn read_stored_receipt(path: &Path) -> LifecycleResult<StoredReceipt> {
    let stored: StoredReceipt = read_json(path)?;
    if stored.schema_version != STORED_RECEIPT_SCHEMA {
        return Err(LifecycleBlocker::new(
            "STORED_RECEIPT_SCHEMA_UNSUPPORTED",
            &stored.schema_version,
        ));
    }
    if payload_sha256(&stored.receipt)? != stored.payload_sha256 {
        return Err(LifecycleBlocker::new(
            "RECEIPT_CORRUPT",
            path.display().to_string(),
        ));
    }
    Ok(stored)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> LifecycleResult<T> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(io_blocker)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        LifecycleBlocker::new(
            "LIFECYCLE_RECORD_CORRUPT",
            format!("{}:{error}", path.display()),
        )
    })
}

fn read_optional_value(path: &Path) -> LifecycleResult<Option<Value>> {
    if path.exists() {
        read_json(path).map(Some)
    } else {
        Ok(None)
    }
}

fn durable_write_json(path: &Path, value: &impl Serialize) -> LifecycleResult<()> {
    let mut bytes = serde_json::to_vec(value).map_err(json_blocker)?;
    bytes.push(b'\n');
    durable_write(path, &bytes)
}

fn durable_write(path: &Path, bytes: &[u8]) -> LifecycleResult<()> {
    let parent = path.parent().ok_or_else(|| {
        LifecycleBlocker::new("LIFECYCLE_PATH_PARENT_MISSING", path.display().to_string())
    })?;
    fs::create_dir_all(parent).map_err(io_blocker)?;
    let temporary = parent.join(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("record"),
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(io_blocker)?;
    file.write_all(bytes).map_err(io_blocker)?;
    file.sync_all().map_err(io_blocker)?;
    drop(file);
    fs::rename(&temporary, path).map_err(io_blocker)?;
    sync_directory(parent)?;
    Ok(())
}

fn remove_durable(path: &Path) -> LifecycleResult<()> {
    if path.exists() {
        fs::remove_file(path).map_err(io_blocker)?;
        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }
    }
    Ok(())
}

#[cfg(not(windows))]
fn sync_directory(path: &Path) -> LifecycleResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_blocker)
}

#[cfg(windows)]
fn sync_directory(_path: &Path) -> LifecycleResult<()> {
    Ok(())
}

fn json_files(directory: &Path) -> LifecycleResult<Vec<PathBuf>> {
    let mut paths = fs::read_dir(directory)
        .map_err(io_blocker)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn io_blocker(error: std::io::Error) -> LifecycleBlocker {
    LifecycleBlocker::new("LIFECYCLE_IO_FAILED", error.to_string())
}

fn json_blocker(error: serde_json::Error) -> LifecycleBlocker {
    LifecycleBlocker::new("LIFECYCLE_JSON_FAILED", error.to_string())
}

fn notify_blocker(error: notify::Error) -> LifecycleBlocker {
    LifecycleBlocker::new("NATIVE_EVENT_ADAPTER_FAILED", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(root: &Path) -> SessionLifecycleStore {
        SessionLifecycleStore::open(root, "commander-1", LifecycleConfig::default())
            .expect("open lifecycle store")
    }

    fn receipt(event_id: &str, event_seq: u64) -> TerminalReceipt {
        let mut receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                "transaction-1",
                event_id,
                event_seq,
                "commander-1",
                "child-1",
                format!("runtime-{event_seq}"),
                format!("lease-{event_seq}"),
            ),
            TerminalState::Completed,
            1_786_845_600_000 + event_seq as i64,
        );
        receipt.task_id = Some("task-1".to_string());
        receipt.goal_id = Some("goal-1".to_string());
        receipt.operator_override = true;
        receipt
    }

    fn callback(receipt: &TerminalReceipt, text: &str) -> DurableCallbackRecord {
        DurableCallbackRecord::new(
            receipt,
            Value::String(text.to_string()),
            serde_json::json!({
                "kind": "gateway.callback",
                "payload": {
                    "session_id": receipt.child_session_id,
                    "runtime_id": receipt.runtime_id,
                    "body": {"item": {"id": "runtime-0.message", "text": text}}
                }
            }),
            "69edd74f732aa5bed571d652e7f91874a16881116b454218a508f413a33fcd70",
            canonical_value_sha256(&Value::String("delegated prompt".to_string())),
            CallbackEffectIdentity::Exact {
                effect_id: "runtime-0.message".to_string(),
            },
        )
        .expect("callback record")
    }

    fn publish_callback_fixture(
        store: &SessionLifecycleStore,
        receipt: &TerminalReceipt,
        callback: &DurableCallbackRecord,
    ) {
        store
            .write_terminal_receipt(receipt)
            .expect("terminal receipt");
        store
            .intake(&receipt.transaction_id, &receipt.event_id)
            .expect("receipt intake");
        store.publish_callback(callback).expect("publish callback");
    }

    fn child_admission() -> ChildAdmissionRecord {
        ChildAdmissionRecord::new(
            "commander-1",
            "69edd74f732aa5bed571d652e7f91874a16881116b454218a508f413a33fcd70",
            None,
            "child-1",
            "runtime-0",
            "transaction-1",
            "lease-0",
            "transaction-1",
            "runtime-0.message",
            &canonical_value_sha256(&Value::String("delegated prompt".to_string())),
            &canonical_value_sha256(&serde_json::json!({"prompt": "delegated prompt"})),
            "/tmp/child-1",
            "delegated child",
            1_786_845_600_000,
        )
    }

    #[test]
    fn child_admission_is_idempotent_and_reuses_callback_ack_continuation_store() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let admission = child_admission();
        assert_eq!(
            store.admit_child(&admission).expect("admit child"),
            ChildAdmissionOutcome::Admitted
        );
        assert_eq!(
            store.admit_child(&admission).expect("replay admission"),
            ChildAdmissionOutcome::AlreadyAdmitted
        );
        assert_eq!(
            store
                .child_admission(&admission.child_session_id)
                .expect("admission readback"),
            Some(admission.clone())
        );

        let mut changed = admission.clone();
        changed.effect_id = "foreign.message".to_string();
        assert_eq!(
            store.admit_child(&changed).unwrap_err().code,
            "CHILD_ADMISSION_EFFECT_IDENTITY_CONFLICT"
        );

        let terminal = receipt("child-terminal", 0);
        let callback = callback(&terminal, "child result");
        publish_callback_fixture(&store, &terminal, &callback);
        assert!(matches!(
            store.publish_callback(&callback).expect("callback replay"),
            CallbackWriteOutcome::AlreadyDurable(_)
        ));
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("callback intake");
        let continuation =
            ContinuationDispatchRecord::from_callback(&callback).expect("continuation identity");
        assert_eq!(
            store
                .prepare_callback_continuation(&continuation)
                .expect("prepare continuation"),
            ContinuationWriteOutcome::Prepared
        );
        assert_eq!(
            store
                .acknowledge_callback(
                    &callback.transaction_id,
                    &callback.event_id,
                    &callback.callback_payload_sha256,
                    &callback.effect_identity,
                )
                .expect("callback ack"),
            AckOutcome::Acknowledged
        );
        assert_eq!(
            store
                .acknowledge_callback(
                    &callback.transaction_id,
                    &callback.event_id,
                    &callback.callback_payload_sha256,
                    &callback.effect_identity,
                )
                .expect("callback ack replay"),
            AckOutcome::AlreadyAcknowledged
        );
        assert_eq!(
            store
                .callback_continuations_for_replay()
                .expect("continuation eligibility"),
            vec![continuation]
        );
    }

    #[test]
    fn child_admission_rejects_missing_parent_identity() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let mut admission = child_admission();
        admission.parent_session_id.clear();
        assert_eq!(
            store.admit_child(&admission).unwrap_err().code,
            "LIFECYCLE_IDENTITY_MISSING"
        );
    }

    #[test]
    fn callback_continuation_identity_is_deterministic_and_restart_safe() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let lifecycle = store(root.path());
        let terminal_receipt = receipt("continuation-event", 0);
        let callback = callback(&terminal_receipt, "child result");
        publish_callback_fixture(&lifecycle, &terminal_receipt, &callback);
        lifecycle
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake callback");
        let first = ContinuationDispatchRecord::from_callback(&callback)
            .expect("derive continuation identity");
        let second = ContinuationDispatchRecord::from_callback(&callback)
            .expect("rederive continuation identity");
        assert_eq!(first, second);
        assert_ne!(first.runtime_id, callback.runtime_id);
        assert_eq!(first.parent_input["authority"], "none");
        assert_eq!(
            first.parent_input["requested_action"],
            "MISSION_VERIFICATION"
        );
        assert_eq!(
            first.parent_input["callback"]["result"],
            callback.callback_payload
        );
        assert!(!first.parent_input.to_string().contains("delegated prompt"));
        assert_eq!(
            lifecycle
                .prepare_callback_continuation(&first)
                .expect("prepare continuation"),
            ContinuationWriteOutcome::Prepared
        );

        let reopened = store(root.path());
        assert_eq!(
            reopened
                .prepare_callback_continuation(&second)
                .expect("restart replay"),
            ContinuationWriteOutcome::AlreadyPrepared
        );
        assert_eq!(
            reopened
                .callback_continuations_for_replay()
                .expect("continuation replay readback"),
            vec![first]
        );
    }

    #[test]
    fn callback_continuation_cas_conflicts_and_unsettled_effect_stays_unacked() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let terminal_receipt = receipt("continuation-conflict", 0);
        let callback = callback(&terminal_receipt, "child result");
        publish_callback_fixture(&store, &terminal_receipt, &callback);
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake callback");
        let record = ContinuationDispatchRecord::from_callback(&callback)
            .expect("derive continuation identity");
        store
            .prepare_callback_continuation(&record)
            .expect("prepare continuation");
        let mut changed = record.clone();
        changed.parent_input["requested_action"] = Value::String("ROUTE_SELECTION".to_string());
        changed.requested_action = "ROUTE_SELECTION".to_string();
        assert_eq!(
            store
                .prepare_callback_continuation(&changed)
                .expect_err("changed binding must conflict")
                .code,
            "CONTINUATION_CALLBACK_BINDING_CONFLICT"
        );
        assert_eq!(
            store
                .mark_callback_continuation_dispatched(&record)
                .expect("durable parent dispatch"),
            ContinuationWriteOutcome::AlreadyDispatched
        );
        assert_eq!(
            store
                .mark_callback_continuation_acknowledged(&record)
                .expect_err("dispatched continuation cannot be acknowledged")
                .code,
            "CONTINUATION_ACK_BEFORE_COMPLETION_BLOCKED"
        );
        assert_eq!(
            store
                .callback_continuations_for_replay()
                .expect("dispatched continuation readback")[0]
                .state,
            ContinuationDispatchState::Dispatched
        );

        let mut unsettled = callback;
        unsettled.effect_identity = CallbackEffectIdentity::UnsettledEffect {
            classification: "ambiguous".to_string(),
            evidence_sha256: "b".repeat(64),
        };
        assert_eq!(
            ContinuationDispatchRecord::from_callback(&unsettled)
                .expect_err("unsettled effect must not dispatch")
                .code,
            "CONTINUATION_UNSETTLED_EFFECT_BLOCKED"
        );
    }

    #[test]
    fn commander_target_continuation_requires_exact_bound_proof_before_completion() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let terminal_receipt = receipt("commander-convergence", 0);
        let mut targeted_callback = callback(&terminal_receipt, "child result");
        targeted_callback.commander_thread_id = Some("commander-thread-1".to_string());
        publish_callback_fixture(&store, &terminal_receipt, &targeted_callback);
        store
            .mark_callback_intaken(
                &targeted_callback.transaction_id,
                &targeted_callback.event_id,
                &targeted_callback.callback_payload_sha256,
            )
            .expect("intake callback");
        let record = ContinuationDispatchRecord::from_callback(&targeted_callback)
            .expect("target continuation");
        store
            .prepare_callback_continuation(&record)
            .expect("prepare target continuation");
        store
            .mark_callback_continuation_dispatched(&record)
            .expect("dispatch target continuation");
        assert_eq!(
            store
                .mark_callback_continuation_completed(&record)
                .expect_err("target continuation cannot complete without proof")
                .code,
            "COMMANDER_CONVERGENCE_PROOF_MISSING"
        );
        let effect = serde_json::to_value(&record.effect_identity).expect("effect value");
        let mut proof = CommanderConvergenceProof {
            schema_version: runtime_contract::COMMANDER_CONVERGENCE_PROOF_SCHEMA_VERSION
                .to_string(),
            request_id: record.request_id.clone(),
            callback_payload_sha256: record.callback_payload_sha256.clone(),
            effect_identity_sha256: canonical_value_sha256(&effect),
            child_session_id: record.child_session_id.clone(),
            child_transaction_id: record.child_transaction_id.clone(),
            child_runtime_id: record.child_runtime_id.clone(),
            requested_action: record.requested_action.clone(),
            target_thread_id: "wrong-thread".to_string(),
            pre_revision_sha256: record.parent_mission_revision_sha256.clone(),
            post_revision_sha256: "b".repeat(64),
            target_turn_id: "turn-1".to_string(),
            final_assistant_sha256: "c".repeat(64),
        };
        assert_eq!(
            store
                .bind_callback_continuation_convergence_proof(&record, &proof)
                .expect_err("wrong target proof must not bind")
                .code,
            "COMMANDER_CONVERGENCE_PROOF_BINDING_MISMATCH"
        );
        proof.target_thread_id = "commander-thread-1".to_string();
        proof.requested_action = "ROUTE_SELECTION".to_string();
        assert_eq!(
            store
                .bind_callback_continuation_convergence_proof(&record, &proof)
                .expect_err("wrong action proof must not bind")
                .code,
            "COMMANDER_CONVERGENCE_PROOF_BINDING_MISMATCH"
        );
        proof.requested_action = record.requested_action.clone();
        let mut caller_only = record.clone();
        caller_only.convergence_proof = Some(proof.clone());
        assert_eq!(
            store
                .mark_callback_continuation_completed(&caller_only)
                .expect_err("caller proof cannot bypass durable proof binding")
                .code,
            "COMMANDER_CONVERGENCE_PROOF_NOT_DURABLE"
        );
        store
            .bind_callback_continuation_convergence_proof(&record, &proof)
            .expect("bind exact proof");
        assert_eq!(
            store
                .bind_callback_continuation_convergence_proof(&record, &proof)
                .expect("identical proof replay is idempotent"),
            ContinuationWriteOutcome::AlreadyDispatched
        );
        let mut bound = record.clone();
        bound.convergence_proof = Some(proof);
        store
            .mark_callback_continuation_completed(&bound)
            .expect("complete with exact proof");
        assert_eq!(
            store
                .callback_continuations_for_replay()
                .expect("proof readback")[0]
                .convergence_proof,
            bound.convergence_proof
        );

        let legacy = ContinuationDispatchRecord::from_callback(&callback(
            &terminal_receipt,
            "legacy result",
        ))
        .expect("legacy continuation");
        assert!(legacy.commander_thread_id.is_none());
        assert!(legacy.convergence_proof.is_none());
    }

    #[test]
    fn continuation_requires_exact_intaken_callback_and_canonical_result_hash() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let terminal_receipt = receipt("continuation-binding", 0);
        let callback = callback(&terminal_receipt, "child result");
        let continuation =
            ContinuationDispatchRecord::from_callback(&callback).expect("continuation");

        publish_callback_fixture(&store, &terminal_receipt, &callback);
        assert_eq!(
            store
                .prepare_callback_continuation(&continuation)
                .expect_err("pending callback must not prepare")
                .code,
            "CONTINUATION_INTAKEN_CALLBACK_NOT_FOUND"
        );
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake callback");
        drop(store);
        let reopened =
            SessionLifecycleStore::open(root.path(), "commander-1", LifecycleConfig::default())
                .expect("restart lifecycle store");

        let mut tampered_result = continuation.clone();
        tampered_result.parent_input["callback"]["result"] = Value::String("changed".into());
        assert_eq!(
            reopened
                .prepare_callback_continuation(&tampered_result)
                .expect_err("tampered result must fail closed")
                .code,
            "CONTINUATION_CALLBACK_RESULT_HASH_MISMATCH"
        );

        let mut mismatched_sha = continuation.clone();
        mismatched_sha.callback_payload_sha256 = "b".repeat(64);
        assert_eq!(
            reopened
                .prepare_callback_continuation(&mismatched_sha)
                .expect_err("mismatched payload sha must fail closed")
                .code,
            "CONTINUATION_CALLBACK_RESULT_HASH_MISMATCH"
        );
        reopened
            .prepare_callback_continuation(&continuation)
            .expect("restart after intake prepares exact callback");

        let intaken_path = root
            .path()
            .join("callbacks/intaken")
            .join(receipt_key(&callback.transaction_id, &callback.event_id));
        let mut changed_callback = callback.clone();
        changed_callback.callback_payload = Value::String("changed".into());
        changed_callback.callback_payload_sha256 =
            canonical_value_sha256(&changed_callback.callback_payload);
        durable_write_json(&intaken_path, &changed_callback)
            .expect("simulate changed durable callback after restart");
        assert_eq!(
            reopened
                .callback_continuations_for_replay()
                .expect_err("changed callback must fail restart")
                .code,
            "CONTINUATION_CALLBACK_BINDING_CONFLICT"
        );

        durable_write_json(&intaken_path, &callback).expect("restore exact callback");
        remove_durable(&intaken_path).expect("simulate missing durable callback after restart");
        assert_eq!(
            reopened
                .callback_continuations_for_replay()
                .expect_err("missing callback must fail restart")
                .code,
            "CONTINUATION_INTAKEN_CALLBACK_NOT_FOUND"
        );
    }

    #[test]
    fn dispatched_continuation_requires_successful_terminal_and_reclaim_evidence() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let terminal_receipt = receipt("child-terminal", 0);
        let callback = callback(&terminal_receipt, "child result");
        publish_callback_fixture(&store, &terminal_receipt, &callback);
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake callback");
        let continuation = ContinuationDispatchRecord::from_callback(&callback)
            .expect("derive continuation identity");
        store
            .prepare_callback_continuation(&continuation)
            .expect("prepare continuation");
        store
            .mark_callback_continuation_dispatched(&continuation)
            .expect("dispatch continuation");
        assert!(
            !store
                .callback_continuation_completion_proven(&continuation)
                .expect("no terminal evidence")
        );

        let parent_receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                &continuation.request_id,
                "parent-terminal",
                0,
                &continuation.commander_session_id,
                &continuation.commander_session_id,
                &continuation.runtime_id,
                &continuation.lease_id,
            ),
            TerminalState::Completed,
            1_786_845_600_100,
        );
        store
            .write_terminal_receipt(&parent_receipt)
            .expect("durable parent terminal receipt");
        store
            .intake(&continuation.request_id, "parent-terminal")
            .expect("apply parent terminal receipt");
        assert!(
            !store
                .callback_continuation_completion_proven(&continuation)
                .expect("terminal without reclaim")
        );
        store
            .reclaim_terminal_slot(
                &continuation.request_id,
                "parent-terminal",
                LiveEffectEvidence::default(),
            )
            .expect("reclaim parent runtime");
        assert!(
            store
                .callback_continuation_completion_proven(&continuation)
                .expect("terminal and reclaim prove completion")
        );
    }

    #[test]
    fn commander_target_accepts_only_exact_fallback_terminal_identity() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let child_receipt = receipt("commander-fallback-child", 0);
        let mut callback = callback(&child_receipt, "child result");
        callback.commander_thread_id = Some("commander-thread-1".to_string());
        publish_callback_fixture(&store, &child_receipt, &callback);
        store
            .mark_callback_intaken(
                &callback.transaction_id,
                &callback.event_id,
                &callback.callback_payload_sha256,
            )
            .expect("intake targeted callback");
        let record = ContinuationDispatchRecord::from_callback(&callback)
            .expect("derive targeted continuation");
        store
            .prepare_callback_continuation(&record)
            .expect("prepare targeted continuation");
        store
            .mark_callback_continuation_dispatched(&record)
            .expect("dispatch targeted continuation");
        let effect = serde_json::to_value(&record.effect_identity).expect("effect value");
        let proof = CommanderConvergenceProof {
            schema_version: runtime_contract::COMMANDER_CONVERGENCE_PROOF_SCHEMA_VERSION
                .to_string(),
            request_id: record.request_id.clone(),
            callback_payload_sha256: record.callback_payload_sha256.clone(),
            effect_identity_sha256: canonical_value_sha256(&effect),
            child_session_id: record.child_session_id.clone(),
            child_transaction_id: record.child_transaction_id.clone(),
            child_runtime_id: record.child_runtime_id.clone(),
            requested_action: record.requested_action.clone(),
            target_thread_id: "commander-thread-1".to_string(),
            pre_revision_sha256: record.parent_mission_revision_sha256.clone(),
            post_revision_sha256: "b".repeat(64),
            target_turn_id: "turn-fallback".to_string(),
            final_assistant_sha256: "c".repeat(64),
        };
        store
            .bind_callback_continuation_convergence_proof(&record, &proof)
            .expect("bind fallback proof");
        let mut bound = record.clone();
        bound.convergence_proof = Some(proof);

        let mut fallback_receipt = TerminalReceipt::new(
            TerminalReceiptIdentity::new(
                &record.request_id,
                "fallback-terminal",
                0,
                &record.commander_session_id,
                &record.commander_session_id,
                "fallback-runtime",
                "fallback-lease",
            ),
            TerminalState::Completed,
            1_786_845_600_200,
        );
        fallback_receipt.audit_metadata.insert(
            "commander_continuation_request_id".to_string(),
            Value::String(record.request_id.clone()),
        );
        fallback_receipt.audit_metadata.insert(
            "commander_continuation_target_thread_id".to_string(),
            Value::String("commander-thread-1".to_string()),
        );
        fallback_receipt.audit_metadata.insert(
            "commander_continuation_origin_runtime_id".to_string(),
            Value::String(record.runtime_id.clone()),
        );
        fallback_receipt.audit_metadata.insert(
            "commander_continuation_origin_lease_id".to_string(),
            Value::String(record.lease_id.clone()),
        );
        store
            .write_terminal_receipt(&fallback_receipt)
            .expect("write fallback terminal receipt");
        store
            .intake(&record.request_id, "fallback-terminal")
            .expect("apply fallback terminal receipt");
        assert!(
            !store
                .callback_continuation_completion_proven(&bound)
                .expect("fallback without release is incomplete")
        );
        store
            .reclaim_terminal_slot(
                &record.request_id,
                "fallback-terminal",
                LiveEffectEvidence::default(),
            )
            .expect("reclaim fallback terminal slot");
        assert!(
            store
                .callback_continuation_completion_proven(&bound)
                .expect("exact fallback identity proves completion")
        );
    }

    #[test]
    fn durable_callback_success_and_duplicate_replay_are_exactly_once() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let receipt = receipt("callback-success", 0);
        store
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        store
            .intake("transaction-1", "callback-success")
            .expect("receipt intake");
        let record = callback(&receipt, "child result");

        assert!(matches!(
            store.publish_callback(&record).expect("publish callback"),
            CallbackWriteOutcome::Written(_)
        ));
        assert!(matches!(
            store
                .publish_callback(&record)
                .expect("duplicate publication"),
            CallbackWriteOutcome::AlreadyDurable(_)
        ));
        assert_eq!(
            store.callbacks_for_replay().expect("pending replay"),
            vec![record.clone()]
        );
        assert_eq!(
            store
                .mark_callback_intaken(
                    &record.transaction_id,
                    &record.event_id,
                    &record.callback_payload_sha256,
                )
                .expect("intake callback"),
            CallbackIntakeOutcome::Intaken
        );
        assert_eq!(
            store
                .acknowledge_callback(
                    &record.transaction_id,
                    &record.event_id,
                    &record.callback_payload_sha256,
                    &record.effect_identity,
                )
                .expect("ack callback"),
            AckOutcome::Acknowledged
        );
        assert_eq!(
            store
                .acknowledge_callback(
                    &record.transaction_id,
                    &record.event_id,
                    &record.callback_payload_sha256,
                    &record.effect_identity,
                )
                .expect("duplicate ack"),
            AckOutcome::AlreadyAcknowledged
        );
        assert!(
            store
                .callbacks_for_replay()
                .expect("acked replay")
                .is_empty()
        );
        let readback = store.readback().expect("callback readback");
        assert_eq!(readback.pending_callbacks, 0);
        assert_eq!(readback.intaken_callbacks, 1);
        assert_eq!(readback.acknowledged_callbacks, 1);
    }

    #[test]
    fn durable_callback_changed_payload_or_revision_conflicts() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let receipt = receipt("callback-conflict", 0);
        store
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        store
            .intake("transaction-1", "callback-conflict")
            .expect("receipt intake");
        let record = callback(&receipt, "original result");
        store.publish_callback(&record).expect("original callback");

        let changed_payload = callback(&receipt, "changed result");
        assert_eq!(
            store.publish_callback(&changed_payload).unwrap_err().code,
            "CALLBACK_IDENTITY_CONFLICT"
        );
        let mut changed_revision = record;
        changed_revision.parent_mission_revision_sha256 = "7".repeat(64);
        assert_eq!(
            store.publish_callback(&changed_revision).unwrap_err().code,
            "CALLBACK_IDENTITY_CONFLICT"
        );
    }

    #[test]
    fn unsettled_effect_callback_cannot_be_acknowledged() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let mut receipt = receipt("callback-unsettled", 0);
        receipt.terminal_state = TerminalState::Failed;
        store
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        store
            .intake("transaction-1", "callback-unsettled")
            .expect("receipt intake");
        let mut record = callback(&receipt, "typed failure");
        record.effect_identity = CallbackEffectIdentity::UnsettledEffect {
            classification: "terminal_receipt_without_settled_effect_evidence".to_string(),
            evidence_sha256: record.terminal_receipt_sha256.clone(),
        };
        store.publish_callback(&record).expect("publish callback");
        store
            .mark_callback_intaken(
                &record.transaction_id,
                &record.event_id,
                &record.callback_payload_sha256,
            )
            .expect("durable intake");

        let error = store
            .acknowledge_callback(
                &record.transaction_id,
                &record.event_id,
                &record.callback_payload_sha256,
                &record.effect_identity,
            )
            .expect_err("unsettled effect must stay unacknowledged");
        assert_eq!(error.code, "CALLBACK_UNSETTLED_EFFECT_ACK_BLOCKED");
        let readback = store.readback().expect("readback");
        assert_eq!(readback.intaken_callbacks, 1);
        assert_eq!(readback.acknowledged_callbacks, 0);
        assert_eq!(readback.acknowledged_receipts, 0);
    }

    #[test]
    fn callback_transport_failure_remains_pending_and_restart_replays_same_record() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let first = store(root.path());
        let receipt = receipt("callback-restart", 0);
        first
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        first
            .intake("transaction-1", "callback-restart")
            .expect("receipt intake");
        let record = callback(&receipt, "restart result");
        first
            .publish_callback(&record)
            .expect("durable before transport");
        drop(first);

        let restarted = store(root.path());
        assert_eq!(
            restarted.callbacks_for_replay().expect("restart replay"),
            vec![record]
        );
        let readback = restarted.readback().expect("restart readback");
        assert_eq!(readback.pending_callbacks, 1);
        assert_eq!(readback.acknowledged_callbacks, 0);
    }

    #[test]
    fn intake_crash_window_replays_one_identical_record() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let receipt = receipt("callback-crash-window", 0);
        store
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        store
            .intake("transaction-1", "callback-crash-window")
            .expect("receipt intake");
        let record = callback(&receipt, "crash-window result");
        store.publish_callback(&record).expect("pending callback");
        let key = receipt_key(&record.transaction_id, &record.event_id);
        durable_write_json(&root.path().join("callbacks/intaken").join(key), &record)
            .expect("simulate durable intake before pending removal");

        assert_eq!(
            store.callbacks_for_replay().expect("deduplicated replay"),
            vec![record]
        );
    }

    #[test]
    fn callback_prompt_echo_is_rejected_before_publication_or_ack() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let receipt = receipt("callback-echo", 0);
        store
            .write_terminal_receipt(&receipt)
            .expect("terminal receipt");
        store
            .intake("transaction-1", "callback-echo")
            .expect("receipt intake");
        let echo = Value::String("delegated prompt".to_string());
        let record = DurableCallbackRecord::new(
            &receipt,
            echo.clone(),
            serde_json::json!({"payload": {"body": {"item": {"text": echo}}}}),
            "69edd74f732aa5bed571d652e7f91874a16881116b454218a508f413a33fcd70",
            canonical_value_sha256(&Value::String("delegated prompt".to_string())),
            CallbackEffectIdentity::Exact {
                effect_id: "assistant-message-echo".to_string(),
            },
        )
        .expect("echo record");
        assert_eq!(
            store.publish_callback(&record).unwrap_err().code,
            "PROMPT_ECHO_REJECTED"
        );
        let readback = store.readback().expect("echo readback");
        assert_eq!(readback.pending_callbacks, 0);
        assert_eq!(readback.acknowledged_callbacks, 0);
    }

    #[test]
    fn durable_receipt_intake_and_ack_are_exactly_once() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let receipt = receipt("event-0", 0);

        assert!(matches!(
            store
                .write_terminal_receipt(&receipt)
                .expect("write receipt"),
            ReceiptWriteOutcome::Written(_)
        ));
        assert_eq!(
            store.intake("transaction-1", "event-0").expect("intake"),
            IntakeOutcome::Applied { event_seq: 0 }
        );
        assert_eq!(
            store
                .intake("transaction-1", "event-0")
                .expect("duplicate intake"),
            IntakeOutcome::Duplicate { event_seq: 0 }
        );
        assert_eq!(
            store
                .acknowledge("transaction-1", "event-0", "delivery-1")
                .expect("ack"),
            AckOutcome::Acknowledged
        );
        assert_eq!(
            store
                .acknowledge("transaction-1", "event-0", "delivery-1")
                .expect("duplicate ack"),
            AckOutcome::AlreadyAcknowledged
        );

        let readback = store.readback().expect("readback");
        assert_eq!(readback.pending_receipts, 0);
        assert_eq!(readback.applied_receipts, 1);
        assert_eq!(readback.acknowledged_receipts, 1);
    }

    #[test]
    fn crash_and_replay_matrix_converges_without_replacement_identity() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let first = store(root.path());

        let before_receipt = first
            .intake("transaction-1", "event-0")
            .expect_err("crash before receipt has no terminal claim");
        assert_eq!(before_receipt.code, "PENDING_RECEIPT_NOT_FOUND");

        first
            .write_terminal_receipt(&receipt("event-0", 0))
            .expect("crash after durable receipt");
        drop(first);

        let restarted = store(root.path());
        let replay = restarted.replay_pending().expect("restart replay");
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].2, IntakeOutcome::Applied { event_seq: 0 });

        drop(restarted);
        let after_intake = store(root.path());
        assert_eq!(
            after_intake
                .intake("transaction-1", "event-0")
                .expect("crash after intake before ack"),
            IntakeOutcome::Duplicate { event_seq: 0 }
        );
        let readback = after_intake.readback().expect("pending callback readback");
        assert_eq!(readback.commander_session_id, "commander-1");
        assert_eq!(readback.acknowledged_receipts, 0);
    }

    #[test]
    fn next_receipt_sequence_uses_only_applied_same_transaction() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        store
            .write_terminal_receipt(&receipt("event-0", 0))
            .expect("write seq0");
        assert_eq!(store.next_receipt_event_sequence("transaction-1").unwrap(), 0);
        assert_eq!(
            store.intake("transaction-1", "event-0").unwrap(),
            IntakeOutcome::Applied { event_seq: 0 }
        );
        assert_eq!(store.next_receipt_event_sequence("transaction-1").unwrap(), 1);
        let mut other = receipt("other-event-0", 0);
        other.transaction_id = "transaction-2".to_string();
        store.write_terminal_receipt(&other).expect("write other pending");
        assert_eq!(store.next_receipt_event_sequence("transaction-1").unwrap(), 1);
    }

    #[test]
    fn duplicate_and_out_of_order_events_do_not_progress_twice() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        store
            .write_terminal_receipt(&receipt("event-1", 1))
            .expect("write future receipt");
        let pending = store
            .intake("transaction-1", "event-1")
            .expect("future receipt remains pending");
        assert!(matches!(
            pending,
            IntakeOutcome::Pending { ref blocker }
                if blocker.code == "RECEIPT_EVENT_OUT_OF_ORDER"
        ));
        store
            .write_terminal_receipt(&receipt("event-0", 0))
            .expect("write first receipt");
        assert_eq!(
            store
                .intake("transaction-1", "event-0")
                .expect("first intake"),
            IntakeOutcome::Applied { event_seq: 0 }
        );
        assert_eq!(
            store
                .intake("transaction-1", "event-1")
                .expect("second intake"),
            IntakeOutcome::Applied { event_seq: 1 }
        );
        assert_eq!(
            store.intake("transaction-1", "event-1").expect("duplicate"),
            IntakeOutcome::Duplicate { event_seq: 1 }
        );
        assert_eq!(store.readback().expect("readback").applied_receipts, 2);
    }

    #[test]
    fn replay_cleans_matching_pending_copy_after_applied_write_crash() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        store
            .write_terminal_receipt(&receipt("event-0", 0))
            .expect("write pending receipt");
        let key = receipt_key("transaction-1", "event-0");
        let pending = root.path().join("receipts/pending").join(&key);
        let applied = root.path().join("receipts/applied").join(&key);
        fs::copy(&pending, &applied).expect("simulate crash after applied write");

        assert_eq!(
            store
                .intake("transaction-1", "event-0")
                .expect("reconcile duplicate receipt"),
            IntakeOutcome::Duplicate { event_seq: 0 }
        );
        let readback = store.readback().expect("reconciled readback");
        assert_eq!(readback.pending_receipts, 0);
        assert_eq!(readback.applied_receipts, 1);
    }

    #[test]
    fn corrupt_receipt_fails_closed_and_remains_pending() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let outcome = store
            .write_terminal_receipt(&receipt("event-corrupt", 0))
            .expect("write receipt");
        let path = match outcome {
            ReceiptWriteOutcome::Written(path) => path,
            ReceiptWriteOutcome::AlreadyDurable(_) => panic!("new receipt should be written"),
        };
        let mut value: Value = read_json(&path).expect("read stored receipt");
        value["receipt"]["runtime_id"] = Value::String("tampered-runtime".to_string());
        durable_write_json(&path, &value).expect("tamper receipt fixture");

        let error = store
            .intake("transaction-1", "event-corrupt")
            .expect_err("corrupt receipt must fail closed");
        assert_eq!(error.code, "RECEIPT_CORRUPT");
        assert_eq!(store.readback().expect("readback").pending_receipts, 1);
    }

    #[test]
    fn all_identity_failures_remain_attached_to_commander() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        for (index, kind) in [
            IdentityFailureKind::CompactionFailed,
            IdentityFailureKind::TransportUncertain,
            IdentityFailureKind::CallbackPending,
            IdentityFailureKind::ReplayPending,
            IdentityFailureKind::RestartPending,
        ]
        .into_iter()
        .enumerate()
        {
            store
                .record_identity_failure(PendingIdentityFailure {
                    commander_session_id: "commander-1".to_string(),
                    transaction_id: format!("failure-{index}"),
                    event_id: format!("failure-event-{index}"),
                    kind,
                    blocker: "OUTCOME_UNKNOWN_PENDING_READBACK".to_string(),
                })
                .expect("persist same-identity failure");
        }
        let failures = json_files(&root.path().join("identity_pending")).expect("failure files");
        assert_eq!(failures.len(), 5);
        for path in failures {
            let failure: PendingIdentityFailure = read_json(&path).expect("failure record");
            assert_eq!(failure.commander_session_id, "commander-1");
        }
    }

    #[test]
    fn checkpoint_requires_native_event_stable_close_and_exact_identity() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let checkpoint = CheckpointIdentity {
            commander_session_id: "commander-1".to_string(),
            checkpoint_id: "checkpoint-7".to_string(),
            source_path: root.path().join("checkpoint.jsonl"),
            inode: 42,
            size: 4096,
            modified_ns: 99,
            sha256: "a".repeat(64),
            complete_write: true,
            event_kind: CheckpointEventKind::CloseWrite,
        };
        assert_eq!(
            store
                .admit_checkpoint(&checkpoint, &checkpoint)
                .expect("stable native checkpoint"),
            checkpoint
        );
        let mut changed = checkpoint.clone();
        changed.size += 1;
        assert_eq!(
            store
                .admit_checkpoint(&checkpoint, &changed)
                .expect_err("unstable write must block")
                .code,
            "CHECKPOINT_SOURCE_NOT_STABLE"
        );
        let mut wrong_id = checkpoint.clone();
        wrong_id.commander_session_id = "replacement-session".to_string();
        assert_eq!(
            store
                .admit_checkpoint(&checkpoint, &wrong_id)
                .expect_err("identity drift must block")
                .code,
            "CHECKPOINT_SESSION_IDENTITY_MISMATCH"
        );
    }

    #[test]
    fn durable_checkpoint_evidence_round_trips_to_native_identity() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let path = store
            .publish_checkpoint_evidence(
                "child-1",
                "auto_compact_context",
                44,
                8,
                br#"{"delta":"exact"}"#,
            )
            .expect("publish checkpoint evidence");
        let duplicate = store
            .publish_checkpoint_evidence(
                "child-1",
                "auto_compact_context",
                44,
                8,
                br#"{"delta":"exact"}"#,
            )
            .expect("idempotent checkpoint evidence");
        assert_eq!(path, duplicate);

        let identity = checkpoint_identity_from_path(&path, CheckpointEventKind::CloseWrite)
            .expect("native checkpoint identity");
        assert_eq!(identity.commander_session_id, "commander-1");
        assert_eq!(identity.source_path, path);
        assert_eq!(identity.sha256.len(), 64);
        assert!(identity.complete_write);
        store
            .admit_checkpoint(&identity, &identity)
            .expect("published evidence is admissible");
    }

    #[test]
    fn terminal_slot_release_fails_closed_on_each_live_evidence_type() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        store
            .write_terminal_receipt(&receipt("event-0", 0))
            .expect("receipt");
        store.intake("transaction-1", "event-0").expect("intake");

        let cases = [
            (
                LiveEffectEvidence {
                    runtime_worker_alive: true,
                    ..Default::default()
                },
                "LIVE_RUNTIME_WORKER_REMAINS",
            ),
            (
                LiveEffectEvidence {
                    active_tool_calls: 1,
                    ..Default::default()
                },
                "LIVE_TOOL_CALL_REMAINS",
            ),
            (
                LiveEffectEvidence {
                    active_command_runs: 1,
                    ..Default::default()
                },
                "LIVE_COMMAND_RUN_REMAINS",
            ),
            (
                LiveEffectEvidence {
                    live_effect_processes: 1,
                    ..Default::default()
                },
                "LIVE_EFFECT_PROCESS_REMAINS",
            ),
            (
                LiveEffectEvidence {
                    pending_init: true,
                    ..Default::default()
                },
                "PENDING_INIT_REMAINS",
            ),
        ];
        for (evidence, code) in cases {
            let result = store
                .reclaim_terminal_slot("transaction-1", "event-0", evidence)
                .expect("typed retain result");
            assert!(matches!(
                result,
                ReclaimOutcome::Retained { blocker } if blocker.code == code
            ));
        }
        assert_eq!(
            store
                .reclaim_terminal_slot("transaction-1", "event-0", LiveEffectEvidence::default(),)
                .expect("release"),
            ReclaimOutcome::Released
        );
        assert_eq!(
            store
                .reclaim_terminal_slot("transaction-1", "event-0", LiveEffectEvidence::default(),)
                .expect("idempotent release"),
            ReclaimOutcome::AlreadyReleased
        );
        let readback = store.readback().expect("readback");
        assert_eq!(readback.released_slots, 1);
    }

    #[test]
    fn repeated_identical_blocker_observations_are_coalesced() {
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        store
            .write_terminal_receipt(&receipt("event-0", 0))
            .expect("receipt");
        store.intake("transaction-1", "event-0").expect("intake");

        for _ in 0..10 {
            let outcome = store
                .reclaim_terminal_slot(
                    "transaction-1",
                    "event-0",
                    LiveEffectEvidence {
                        live_effect_processes: 1,
                        ..Default::default()
                    },
                )
                .expect("repeated blocker observation");
            assert!(matches!(
                outcome,
                ReclaimOutcome::Retained { blocker }
                    if blocker.code == "LIVE_EFFECT_PROCESS_REMAINS"
            ));
        }
        let blocker_directory = root.path().join("blockers");
        assert_eq!(json_files(&blocker_directory).expect("blockers").len(), 1);

        store
            .reclaim_terminal_slot(
                "transaction-1",
                "event-0",
                LiveEffectEvidence {
                    live_effect_processes: 2,
                    ..Default::default()
                },
            )
            .expect("changed blocker observation");
        let blockers = json_files(&blocker_directory).expect("changed blockers");
        assert_eq!(blockers.len(), 2);
        let last: BlockerRecord =
            read_json(&root.path().join("readback/last_blocker.json")).expect("last blocker");
        assert_eq!(last.sequence, 1);
        assert_eq!(last.detail, "2");
    }

    #[test]
    fn watchdog_is_demoted_and_never_used_as_elapsed_failure_evidence() {
        assert!(DEFAULT_WATCHDOG_INTERVAL_SECS > 300);
        assert_eq!(
            LifecycleConfig {
                watchdog_interval_secs: 300,
                event_channel_capacity: 1,
            }
            .validate()
            .expect_err("300 second primary scan must be rejected")
            .code,
            "WATCHDOG_INTERVAL_NOT_DEMOTED"
        );
        let root = tempfile::tempdir().expect("temp lifecycle root");
        let store = store(root.path());
        let checkpoint = CheckpointIdentity {
            commander_session_id: "commander-1".to_string(),
            checkpoint_id: "watchdog-checkpoint".to_string(),
            source_path: root.path().join("checkpoint.jsonl"),
            inode: 1,
            size: 2,
            modified_ns: 3,
            sha256: "b".repeat(64),
            complete_write: true,
            event_kind: CheckpointEventKind::WatchdogReconcile,
        };
        store
            .reconcile_checkpoint(&checkpoint, &checkpoint)
            .expect("watchdog reconciles event loss");
        let readback = store.readback().expect("readback");
        assert_eq!(readback.primary_trigger, "native_checkpoint_event");
        assert!(readback.last_reconcile.is_some());
    }
}
