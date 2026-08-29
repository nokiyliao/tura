use runtime_contract::{
    COMMANDER_CONVERGENCE_PROOF_SCHEMA_VERSION, CommanderContinuationBinding,
    CommanderConvergenceProof,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{ChildStdout, Command};

const ASSOCIATION_FILE: &str = "official-codex-app-server-association.json";
const ASSOCIATION_DIRECTORY: &str = "official_codex_associations";
const ASSOCIATION_SCHEMA_VERSION: u32 = 2;
const CHILD_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodexAppServerExecutable {
    pub path: PathBuf,
    pub prefix_args: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexAppServerExecutableIdentity {
    pub canonical_path: PathBuf,
    pub version: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexTurnSubmissionState {
    Prepared,
    Acknowledged,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexTurnAttempt {
    pub attempt_id: String,
    pub input_sha256: String,
    pub authoritative_turn_ids_before_submit: Vec<String>,
    pub state: CodexTurnSubmissionState,
    pub turn_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexMissionSnapshot {
    pub input_sha256: String,
    pub thread_start_params: Value,
    pub turn_input: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexObservedToolEffectState {
    Observed,
    Reconciled,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexCommandReceiptReference {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexObservedCommandAccess {
    ReadOnly,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexReadOnlyCommandObservation {
    pub access: CodexObservedCommandAccess,
    pub command_type: String,
    pub command_line: String,
    pub enumerated_index: usize,
    pub effective_step: u64,
    pub binding_id: Option<String>,
    pub claim_identity: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexReadOnlyEffectObservation {
    pub original_runtime_id: String,
    pub tool_call_id: String,
    pub execution_id: String,
    pub commands: Vec<CodexReadOnlyCommandObservation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexCommandRunCommandObservation {
    pub command_type: String,
    pub enumerated_index: usize,
    pub effective_step: u64,
    pub binding_id: Option<String>,
    pub claim_identity: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexCommandRunEffectObservation {
    pub runtime_id: String,
    pub tool_call_id: String,
    pub execution_id: String,
    pub commands: Vec<CodexCommandRunCommandObservation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexObservedToolEffect {
    pub request_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_params: Option<Value>,
    pub original_request_id: Value,
    pub state: CodexObservedToolEffectState,
    pub response: Option<Value>,
    pub command_receipts: Vec<CodexCommandReceiptReference>,
    pub replay_request_id: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_observation: Option<CodexReadOnlyEffectObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_run_observation: Option<CodexCommandRunEffectObservation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexInterruptedRecoveryState {
    Prepared,
    ThreadAcknowledged,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexInterruptedRecoveryAttempt {
    pub interrupted_turn_id: String,
    pub replay_effect_count: usize,
    pub state: CodexInterruptedRecoveryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_thread_id: Option<String>,
}

pub const CODEX_EXECUTION_LEDGER_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexExecutionLedger {
    pub schema_version: u32,
    pub tura_session_id: String,
    pub canonical_input_sha256: String,
    pub runtime_ids: Vec<String>,
    pub effects: Vec<CodexObservedToolEffect>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupted_recovery: Option<CodexInterruptedRecoveryAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commander_convergence_proof: Option<CommanderConvergenceProof>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commander_convergence_final_assistant: Option<String>,
}

impl CodexExecutionLedger {
    pub fn new(request: &OfficialCodexTurnRequest, canonical_input_sha256: String) -> Self {
        Self {
            schema_version: CODEX_EXECUTION_LEDGER_SCHEMA_VERSION,
            tura_session_id: request.tura_session_id.clone(),
            canonical_input_sha256,
            runtime_ids: vec![request.runtime_id.clone()],
            effects: Vec::new(),
            interrupted_recovery: None,
            terminal_status: None,
            commander_convergence_proof: None,
            commander_convergence_final_assistant: None,
        }
    }

    fn validate_for_request(
        &mut self,
        request: &OfficialCodexTurnRequest,
        canonical_input_sha256: &str,
    ) -> Result<(), String> {
        if self.schema_version != CODEX_EXECUTION_LEDGER_SCHEMA_VERSION {
            return Err(format!(
                "unsupported execution ledger schema version {}",
                self.schema_version
            ));
        }
        if self.tura_session_id != request.tura_session_id {
            return Err(format!(
                "execution ledger belongs to Tura session {}, not {}",
                self.tura_session_id, request.tura_session_id
            ));
        }
        if self.canonical_input_sha256 != canonical_input_sha256 {
            return Err("execution ledger canonical input digest changed".to_string());
        }
        if self.terminal_status.as_deref() == Some("completed") {
            let binding = request
                .commander_continuation
                .as_ref()
                .ok_or_else(|| "execution ledger is already terminal completed".to_string())?;
            let proof = self
                .commander_convergence_proof
                .as_ref()
                .ok_or_else(|| "terminal Commander convergence proof is missing".to_string())?;
            let final_assistant = self
                .commander_convergence_final_assistant
                .as_ref()
                .ok_or_else(|| {
                    "terminal Commander convergence final assistant is missing".to_string()
                })?;
            proof.validate_shape()?;
            if proof.request_id != binding.continuation_request_id
                || proof.callback_payload_sha256 != binding.callback_payload_sha256
                || proof.effect_identity_sha256 != binding.effect_identity_sha256
                || proof.child_session_id != binding.child_session_id
                || proof.child_transaction_id != binding.child_transaction_id
                || proof.child_runtime_id != binding.child_runtime_id
                || proof.requested_action != binding.requested_action
                || proof.target_thread_id != binding.target_thread_id
                || proof.pre_revision_sha256 != binding.pre_revision_sha256
                || proof.final_assistant_sha256
                    != sha256_canonical_json(Value::String(final_assistant.clone()))
                        .map_err(|error| error.to_string())?
            {
                return Err(
                    "terminal Commander convergence proof does not match request identity"
                        .to_string(),
                );
            }
            if !self.runtime_ids.iter().any(|id| id == &request.runtime_id) {
                let fallback_from_id = request.fallback_from_id.as_deref().ok_or_else(|| {
                    "terminal Commander convergence replay omitted fallback_from_id".to_string()
                })?;
                if !self.runtime_ids.iter().any(|id| id == fallback_from_id) {
                    return Err(
                        "terminal Commander convergence fallback is not bound to the durable runtime lineage"
                            .to_string(),
                    );
                }
                self.runtime_ids.push(request.runtime_id.clone());
            }
            return Ok(());
        }
        if !self.runtime_ids.iter().any(|id| id == &request.runtime_id) {
            let fallback_from_id = request.fallback_from_id.as_deref().ok_or_else(|| {
                "execution ledger runtime changed without fallback_from_id".to_string()
            })?;
            if !self.runtime_ids.iter().any(|id| id == fallback_from_id) {
                return Err("execution ledger fallback source is not durable".to_string());
            }
            self.runtime_ids.push(request.runtime_id.clone());
        }
        Ok(())
    }
}

pub fn load_terminal_commander_convergence_ledger(
    session_directory: &Path,
    tura_session_id: &str,
    original_runtime_id: &str,
    binding: &CommanderContinuationBinding,
) -> Result<Option<CodexExecutionLedger>, String> {
    binding.validate()?;
    let directory = session_directory
        .join(".tura")
        .join("run")
        .join("effect_ledgers");
    let metadata = match std::fs::symlink_metadata(&directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("execution ledger root is not a regular directory".to_string());
    }
    let mut paths = std::fs::read_dir(&directory)
        .map_err(|error| error.to_string())?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    paths.sort();
    if paths.len() > 256 {
        return Err("execution ledger scan exceeded bounded population".to_string());
    }
    let mut matched = None;
    for path in paths {
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "execution ledger member is not a regular file: {}",
                path.display()
            ));
        }
        let ledger: CodexExecutionLedger =
            serde_json::from_slice(&std::fs::read(&path).map_err(|error| error.to_string())?)
                .map_err(|error| format!("invalid execution ledger {}: {error}", path.display()))?;
        if ledger.tura_session_id != tura_session_id
            || !ledger
                .runtime_ids
                .iter()
                .any(|runtime_id| runtime_id == original_runtime_id)
            || ledger.terminal_status.as_deref() != Some("completed")
        {
            continue;
        }
        let file_digest = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if file_digest != ledger.canonical_input_sha256 {
            return Err("execution ledger filename/input digest mismatch".to_string());
        }
        let proof = ledger
            .commander_convergence_proof
            .as_ref()
            .ok_or_else(|| "terminal Commander convergence proof is missing".to_string())?;
        let final_assistant = ledger
            .commander_convergence_final_assistant
            .as_ref()
            .ok_or_else(|| {
                "terminal Commander convergence final assistant is missing".to_string()
            })?;
        proof.validate_shape()?;
        if proof.request_id != binding.continuation_request_id
            || proof.callback_payload_sha256 != binding.callback_payload_sha256
            || proof.effect_identity_sha256 != binding.effect_identity_sha256
            || proof.child_session_id != binding.child_session_id
            || proof.child_transaction_id != binding.child_transaction_id
            || proof.child_runtime_id != binding.child_runtime_id
            || proof.requested_action != binding.requested_action
            || proof.target_thread_id != binding.target_thread_id
            || proof.pre_revision_sha256 != binding.pre_revision_sha256
            || proof.final_assistant_sha256
                != sha256_canonical_json(Value::String(final_assistant.clone()))
                    .map_err(|error| error.to_string())?
        {
            return Err(
                "terminal Commander convergence ledger does not match the requested binding"
                    .to_string(),
            );
        }
        if matched.replace(ledger).is_some() {
            return Err("multiple terminal Commander convergence ledgers matched".to_string());
        }
    }
    Ok(matched)
}

#[derive(Clone, Debug)]
pub struct OfficialCodexTurnRequest {
    pub tura_session_id: String,
    pub runtime_id: String,
    pub fallback_from_id: Option<String>,
    pub session_directory: PathBuf,
    pub model: String,
    pub messages: Vec<Value>,
    pub turn_context: Option<String>,
    pub executable: CodexAppServerExecutable,
    pub dynamic_tools: Vec<Value>,
    pub allowed_command_run_commands: Option<BTreeSet<String>>,
    pub disable_permission_restrictions: bool,
    pub commander_continuation: Option<CommanderContinuationBinding>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CodexThreadAssociation {
    pub schema_version: u32,
    pub tura_session_id: String,
    pub thread_id: String,
    pub codex_session_id: String,
    pub executable_identity: CodexAppServerExecutableIdentity,
    pub active_turn_id: Option<String>,
    pub turn_attempt: Option<CodexTurnAttempt>,
    #[serde(default, skip_serializing, rename = "mission_snapshot")]
    pub mission_snapshot: Option<CodexMissionSnapshot>,
    #[serde(default, skip_serializing, rename = "observed_tool_effects")]
    pub observed_tool_effects: Vec<CodexObservedToolEffect>,
    #[serde(default, skip_serializing, rename = "interrupted_recovery")]
    pub interrupted_recovery: Option<CodexInterruptedRecoveryAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub association_scope_id: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct OfficialCodexUsage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub model_context_window: Option<u64>,
    pub monetary_cost_authority: &'static str,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OfficialCodexAuthoritativeEvent {
    pub sequence: u64,
    pub method: String,
    pub params: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OfficialCodexTurnResponse {
    pub content: Value,
    pub association: CodexThreadAssociation,
    pub usage: Option<OfficialCodexUsage>,
    pub authoritative_events: Vec<OfficialCodexAuthoritativeEvent>,
    pub commander_convergence_proof: Option<CommanderConvergenceProof>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OfficialCodexServerRequest {
    pub method: String,
    pub params: Value,
}

pub type OfficialCodexServerRequestFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

pub trait OfficialCodexServerRequestHandler: Send {
    fn handle<'a>(
        &'a mut self,
        request: OfficialCodexServerRequest,
    ) -> OfficialCodexServerRequestFuture<'a>;

    fn observe_read_only_effect(
        &mut self,
        _request: &OfficialCodexServerRequest,
    ) -> Result<Option<CodexReadOnlyEffectObservation>, String> {
        Ok(None)
    }

    fn observe_command_run_effect(
        &mut self,
        _request: &OfficialCodexServerRequest,
        _runtime_id: &str,
    ) -> Result<Option<CodexCommandRunEffectObservation>, String> {
        Ok(None)
    }

    fn verify_never_claimed_read_only_effect(
        &mut self,
        _observation: &CodexReadOnlyEffectObservation,
    ) -> Result<(), String> {
        Err("runtime read-only reconciliation is unavailable".to_string())
    }

    fn verify_completed_read_only_effect(
        &mut self,
        _observation: &CodexReadOnlyEffectObservation,
    ) -> Result<(), String> {
        Err("runtime completed read-only reconciliation is unavailable".to_string())
    }

    fn load_execution_ledger(
        &mut self,
        _canonical_input_sha256: &str,
    ) -> Result<Option<CodexExecutionLedger>, String> {
        Ok(None)
    }

    fn persist_execution_ledger(&mut self, _ledger: &CodexExecutionLedger) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum OfficialCodexAppServerError {
    #[error("failed to create Codex association directory {path}: {source}")]
    CreateAssociationDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to read Codex association {path}: {source}")]
    ReadAssociation {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid Codex association {path}: {source}")]
    DecodeAssociation {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("Codex association belongs to Tura session {actual}, not {expected}")]
    AssociationSessionMismatch { expected: String, actual: String },
    #[error("unsupported Codex association schema version {0}")]
    UnsupportedAssociationVersion(u32),
    #[error("Tura session {0} has an unreconciled active Codex turn")]
    UnreconciledActiveTurn(String),
    #[error("the pending official Codex turn for Tura session {0} belongs to different input")]
    TurnAttemptInputMismatch(String),
    #[error(
        "authoritative thread state contains {candidate_count} new turns for attempt {attempt_id}"
    )]
    AmbiguousTurnReconciliation {
        attempt_id: String,
        candidate_count: usize,
    },
    #[error("failed to encode Codex association: {0}")]
    EncodeAssociation(serde_json::Error),
    #[error("failed to persist Codex association {path}: {source}")]
    PersistAssociation {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to start official Codex App Server {path}: {source}")]
    Spawn {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to canonicalize official Codex executable {path}: {source}")]
    CanonicalizeExecutable {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to read official Codex executable {path}: {source}")]
    ReadExecutable {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to query official Codex executable version at {path}: {source}")]
    QueryExecutableVersion {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("official Codex executable version query failed ({status}): {stderr}")]
    ExecutableVersionFailed {
        status: std::process::ExitStatus,
        stderr: String,
    },
    #[error("official Codex executable at {0} returned an empty version")]
    EmptyExecutableVersion(PathBuf),
    #[error("official Codex executable identity changed for Tura session {0}")]
    ExecutableIdentityMismatch(String),
    #[error("official Codex App Server did not expose its {0} pipe")]
    MissingPipe(&'static str),
    #[error("failed to write official Codex App Server protocol: {0}")]
    ProtocolWrite(std::io::Error),
    #[error("failed to read official Codex App Server protocol: {0}")]
    ProtocolRead(std::io::Error),
    #[error("official Codex App Server closed stdout before turn completion")]
    UnexpectedEof,
    #[error("official Codex App Server emitted invalid JSON: {source}; line={line}")]
    InvalidJson {
        line: String,
        source: serde_json::Error,
    },
    #[error("official Codex App Server returned an error for {method}: {error}")]
    RpcError { method: String, error: Value },
    #[error("invalid official Codex App Server response: {0}")]
    InvalidResponse(String),
    #[error("COMMANDER_CONTINUATION_BINDING_INVALID: {0}")]
    CommanderContinuationBindingInvalid(String),
    #[error(
        "COMMANDER_TARGET_PREIMAGE_MISMATCH: thread {thread_id} expected {expected}, observed {actual}"
    )]
    CommanderTargetPreimageMismatch {
        thread_id: String,
        expected: String,
        actual: String,
    },
    #[error("COMMANDER_CONVERGENCE_PROOF_INVALID: {0}")]
    CommanderConvergenceProofInvalid(String),
    #[error("official Codex App Server requested unsupported client method {0}")]
    UnsupportedServerRequest(String),
    #[error("governed handler rejected official Codex App Server method {method}: {reason}")]
    ServerRequestRejected { method: String, reason: String },
    #[error("official Codex turn {turn_id} ended with status {status}")]
    TurnNotCompleted { turn_id: String, status: String },
    #[error(
        "OFFICIAL_CODEX_INTERRUPTED_RECOVERY_MISSING_MISSION_SNAPSHOT: turn {turn_id} has no canonical mission snapshot"
    )]
    MissingMissionSnapshot { turn_id: String },
    #[error(
        "OFFICIAL_CODEX_INTERRUPTED_RECOVERY_CONFLICTING_ATTEMPT: recovery already exists for interrupted turn {turn_id}"
    )]
    ConflictingInterruptedRecovery { turn_id: String },
    #[error(
        "OFFICIAL_CODEX_INTERRUPTED_RECOVERY_UNCERTAIN_EFFECT: effect {effect_index} is not durably reconciled: {reason}"
    )]
    UncertainToolEffect { effect_index: usize, reason: String },
    #[error(
        "OFFICIAL_CODEX_INTERRUPTED_RECOVERY_CONFLICTING_EFFECT: expected effect {effect_index} digest {expected}, observed {actual}"
    )]
    ConflictingRecoveryToolEffect {
        effect_index: usize,
        expected: String,
        actual: String,
    },
    #[error(
        "OFFICIAL_CODEX_INTERRUPTED_RECOVERY_UNCONSUMED_EFFECTS: recovered turn {turn_id} consumed {consumed} of {expected} reconciled effects"
    )]
    UnconsumedRecoveryEffects {
        turn_id: String,
        consumed: usize,
        expected: usize,
    },
    #[error("official Codex turn completed without an authoritative assistant message")]
    MissingFinalAnswer,
    #[error("failed to wait for official Codex App Server: {0}")]
    ChildWait(std::io::Error),
    #[error("official Codex App Server exited unsuccessfully ({status}): {stderr}")]
    ChildExit {
        status: std::process::ExitStatus,
        stderr: String,
    },
}

impl OfficialCodexAppServerError {
    pub fn allows_exact_input_retry(&self) -> bool {
        matches!(
            self,
            Self::ProtocolWrite(_)
                | Self::ProtocolRead(_)
                | Self::UnexpectedEof
                | Self::ChildWait(_)
                | Self::ChildExit { .. }
        )
    }
}

pub async fn run_official_codex_turn(
    request: OfficialCodexTurnRequest,
    mut request_handler: Option<&mut dyn OfficialCodexServerRequestHandler>,
) -> Result<OfficialCodexTurnResponse, OfficialCodexAppServerError> {
    if let Some(binding) = request.commander_continuation.as_ref() {
        binding
            .validate()
            .map_err(OfficialCodexAppServerError::CommanderContinuationBindingInvalid)?;
        let expected_original_runtime_id = binding
            .continuation_request_id
            .strip_prefix("callback-continuation-request-")
            .map(|digest| format!("callback-continuation-runtime-{digest}"))
            .unwrap_or_default();
        let runtime_is_original = request.runtime_id == expected_original_runtime_id;
        let runtime_is_bound_fallback =
            request.fallback_from_id.as_deref() == Some(expected_original_runtime_id.as_str());
        if !runtime_is_original && !runtime_is_bound_fallback {
            return Err(
                OfficialCodexAppServerError::CommanderContinuationBindingInvalid(
                    "runtime/request identity mismatch".to_string(),
                ),
            );
        }
    }
    let executable_identity = identify_executable(&request.executable).await?;
    let association_scope_id = association_scope_id(&request);
    let prior = load_thread_association_scoped(
        &request.session_directory,
        &request.tura_session_id,
        &association_scope_id,
    )?;
    if prior
        .as_ref()
        .is_some_and(|association| association.executable_identity != executable_identity)
    {
        return Err(OfficialCodexAppServerError::ExecutableIdentityMismatch(
            request.tura_session_id,
        ));
    }
    let current_snapshot = canonical_mission_snapshot(&request, &executable_identity)?;
    let execution_ledger = load_execution_ledger(
        &request,
        prior.as_ref(),
        &current_snapshot,
        &mut request_handler,
    )?;
    if execution_ledger.terminal_status.as_deref() == Some("completed") {
        let mut association = prior.ok_or_else(|| {
            OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                "terminal convergence ledger omitted target association".to_string(),
            )
        })?;
        association.active_turn_id = None;
        association.turn_attempt = None;
        persist_thread_association(&request.session_directory, &association)?;
        return terminal_commander_convergence_response(&execution_ledger, association);
    }
    if prior.as_ref().is_some_and(|association| {
        association.active_turn_id.is_some() && association.turn_attempt.is_none()
    }) {
        return Err(OfficialCodexAppServerError::UnreconciledActiveTurn(
            request.tura_session_id,
        ));
    }

    let mut command = Command::new(&request.executable.path);
    command
        .args(&request.executable.prefix_args)
        .arg("app-server")
        .arg("--listen")
        .arg("stdio://")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|source| OfficialCodexAppServerError::Spawn {
            path: request.executable.path.clone(),
            source,
        })?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or(OfficialCodexAppServerError::MissingPipe("stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or(OfficialCodexAppServerError::MissingPipe("stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or(OfficialCodexAppServerError::MissingPipe("stderr"))?;
    let stderr_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let result = stderr.read_to_end(&mut bytes).await;
        (result, bytes)
    });

    let mut lines = BufReader::new(stdout).lines();
    let protocol_result = run_protocol(
        &request,
        prior,
        executable_identity,
        current_snapshot,
        execution_ledger,
        &mut stdin,
        &mut lines,
        &mut request_handler,
    )
    .await;
    drop(stdin);

    let status = match tokio::time::timeout(CHILD_EXIT_TIMEOUT, child.wait()).await {
        Ok(result) => result.map_err(OfficialCodexAppServerError::ChildWait)?,
        Err(_) => {
            let _ = child.start_kill();
            child
                .wait()
                .await
                .map_err(OfficialCodexAppServerError::ChildWait)?
        }
    };
    let stderr_bytes = match stderr_task.await {
        Ok((Ok(_), bytes)) => bytes,
        Ok((Err(_), bytes)) => bytes,
        Err(_) => Vec::new(),
    };
    let stderr_text = String::from_utf8_lossy(&stderr_bytes).trim().to_string();

    match protocol_result {
        Err(error) => Err(error),
        Ok(response) if status.success() => Ok(response),
        Ok(_) => Err(OfficialCodexAppServerError::ChildExit {
            status,
            stderr: stderr_text,
        }),
    }
}

async fn identify_executable(
    executable: &CodexAppServerExecutable,
) -> Result<CodexAppServerExecutableIdentity, OfficialCodexAppServerError> {
    let canonical_path = std::fs::canonicalize(&executable.path).map_err(|source| {
        OfficialCodexAppServerError::CanonicalizeExecutable {
            path: executable.path.clone(),
            source,
        }
    })?;
    let bytes = std::fs::read(&canonical_path).map_err(|source| {
        OfficialCodexAppServerError::ReadExecutable {
            path: canonical_path.clone(),
            source,
        }
    })?;
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    let output = Command::new(&canonical_path)
        .args(&executable.prefix_args)
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(
            |source| OfficialCodexAppServerError::QueryExecutableVersion {
                path: canonical_path.clone(),
                source,
            },
        )?;
    if !output.status.success() {
        return Err(OfficialCodexAppServerError::ExecutableVersionFailed {
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if version.is_empty() {
        return Err(OfficialCodexAppServerError::EmptyExecutableVersion(
            canonical_path,
        ));
    }
    Ok(CodexAppServerExecutableIdentity {
        canonical_path,
        version,
        sha256,
    })
}

struct TurnRequestContext<'a> {
    session_directory: &'a Path,
    runtime_id: &'a str,
    execution_ledger: &'a mut CodexExecutionLedger,
}

fn execution_ledger_error(
    effect_index: usize,
    reason: impl Into<String>,
) -> OfficialCodexAppServerError {
    OfficialCodexAppServerError::UncertainToolEffect {
        effect_index,
        reason: format!("runtime execution ledger: {}", reason.into()),
    }
}

fn persist_execution_ledger(
    request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
    ledger: &CodexExecutionLedger,
    effect_index: usize,
) -> Result<(), OfficialCodexAppServerError> {
    let Some(handler) = request_handler.as_deref_mut() else {
        if ledger.effects.is_empty() {
            return Ok(());
        }
        return Err(execution_ledger_error(
            effect_index,
            "persistence handler is unavailable",
        ));
    };
    handler
        .persist_execution_ledger(ledger)
        .map_err(|reason| execution_ledger_error(effect_index, reason))
}

fn load_execution_ledger(
    request: &OfficialCodexTurnRequest,
    prior: Option<&CodexThreadAssociation>,
    current_snapshot: &CodexMissionSnapshot,
    request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
) -> Result<CodexExecutionLedger, OfficialCodexAppServerError> {
    let loaded = match request_handler.as_deref_mut() {
        Some(handler) => handler
            .load_execution_ledger(&current_snapshot.input_sha256)
            .map_err(|reason| execution_ledger_error(0, reason))?,
        None => None,
    };
    let mut ledger = loaded.unwrap_or_else(|| {
        CodexExecutionLedger::new(request, current_snapshot.input_sha256.clone())
    });
    ledger
        .validate_for_request(request, &current_snapshot.input_sha256)
        .map_err(|reason| execution_ledger_error(0, reason))?;

    if let Some(prior) = prior {
        let has_legacy_state = prior.mission_snapshot.is_some()
            || !prior.observed_tool_effects.is_empty()
            || prior.interrupted_recovery.is_some();
        if has_legacy_state {
            let snapshot = prior.mission_snapshot.as_ref().ok_or_else(|| {
                execution_ledger_error(0, "legacy recovery state omitted mission snapshot")
            })?;
            if snapshot.input_sha256 != current_snapshot.input_sha256 {
                return Err(execution_ledger_error(
                    0,
                    "legacy mission snapshot does not match canonical runtime input",
                ));
            }
            if !ledger.effects.is_empty() && ledger.effects != prior.observed_tool_effects {
                return Err(execution_ledger_error(
                    0,
                    "legacy and canonical effect journals disagree",
                ));
            }
            if ledger.effects.is_empty() {
                ledger.effects.clone_from(&prior.observed_tool_effects);
                ledger.interrupted_recovery = prior.interrupted_recovery.clone();
            }
        }
    }
    persist_execution_ledger(request_handler, &ledger, 0)?;
    Ok(ledger)
}

fn terminal_commander_convergence_response(
    execution_ledger: &CodexExecutionLedger,
    association: CodexThreadAssociation,
) -> Result<OfficialCodexTurnResponse, OfficialCodexAppServerError> {
    let final_assistant = execution_ledger
        .commander_convergence_final_assistant
        .clone()
        .ok_or_else(|| {
            OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                "terminal convergence ledger omitted final assistant".to_string(),
            )
        })?;
    let proof = execution_ledger
        .commander_convergence_proof
        .clone()
        .ok_or_else(|| {
            OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                "terminal convergence ledger omitted proof".to_string(),
            )
        })?;
    Ok(OfficialCodexTurnResponse {
        content: Value::String(final_assistant),
        association,
        usage: None,
        authoritative_events: Vec::new(),
        commander_convergence_proof: Some(proof),
    })
}

async fn run_protocol(
    request: &OfficialCodexTurnRequest,
    prior: Option<CodexThreadAssociation>,
    executable_identity: CodexAppServerExecutableIdentity,
    current_snapshot: CodexMissionSnapshot,
    mut execution_ledger: CodexExecutionLedger,
    stdin: &mut tokio::process::ChildStdin,
    lines: &mut Lines<BufReader<ChildStdout>>,
    request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
) -> Result<OfficialCodexTurnResponse, OfficialCodexAppServerError> {
    let mut next_id = 0_u64;
    let mut pending = Vec::new();
    let mut authoritative_events = Vec::new();
    let mut seen_authoritative_events = HashSet::new();
    let initialize = json!({
        "clientInfo": {
            "name": "tura",
            "title": "Tura",
            "version": env!("CARGO_PKG_VERSION")
        },
        "capabilities": {
            "experimentalApi": true
        }
    });
    rpc_request(
        "initialize",
        initialize,
        &mut next_id,
        stdin,
        lines,
        request_handler,
        &mut pending,
        None,
    )
    .await?;
    write_message(stdin, &json!({"method": "initialized", "params": {}})).await?;

    let recovery_thread_id = execution_ledger
        .interrupted_recovery
        .as_ref()
        .filter(|recovery| recovery.state == CodexInterruptedRecoveryState::ThreadAcknowledged)
        .and_then(|recovery| recovery.recovery_thread_id.as_deref());
    let resume_thread_id = request
        .commander_continuation
        .as_ref()
        .map(|binding| binding.target_thread_id.as_str())
        .or(recovery_thread_id)
        .or_else(|| {
            prior
                .as_ref()
                .map(|association| association.thread_id.as_str())
        });
    let thread_response = if let Some(thread_id) = resume_thread_id {
        rpc_request(
            "thread/resume",
            json!({"threadId": thread_id}),
            &mut next_id,
            stdin,
            lines,
            request_handler,
            &mut pending,
            None,
        )
        .await?
    } else {
        rpc_request(
            "thread/start",
            current_snapshot.thread_start_params.clone(),
            &mut next_id,
            stdin,
            lines,
            request_handler,
            &mut pending,
            None,
        )
        .await?
    };

    let thread = thread_response
        .get("thread")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            OfficialCodexAppServerError::InvalidResponse(
                "thread start/resume result omitted thread".to_string(),
            )
        })?;
    let thread_id = required_string(thread, "id", "thread.id")?;
    if let Some(expected_thread_id) = resume_thread_id {
        if expected_thread_id != thread_id {
            return Err(OfficialCodexAppServerError::InvalidResponse(format!(
                "thread/resume returned {}, expected {}",
                thread_id, expected_thread_id
            )));
        }
    }

    async fn start_turn(
        request: &OfficialCodexTurnRequest,
        association: &mut CodexThreadAssociation,
        execution_ledger: &mut CodexExecutionLedger,
        snapshot: &CodexMissionSnapshot,
        authoritative_turn_ids_before_submit: Vec<String>,
        next_id: &mut u64,
        stdin: &mut tokio::process::ChildStdin,
        lines: &mut Lines<BufReader<ChildStdout>>,
        request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
        pending: &mut Vec<Value>,
    ) -> Result<(String, Option<Value>), OfficialCodexAppServerError> {
        association.turn_attempt = Some(CodexTurnAttempt {
            attempt_id: uuid::Uuid::new_v4().to_string(),
            input_sha256: snapshot.input_sha256.clone(),
            authoritative_turn_ids_before_submit,
            state: CodexTurnSubmissionState::Prepared,
            turn_id: None,
        });
        persist_thread_association(&request.session_directory, association)?;
        let turn_start_params = json!({
            "threadId": association.thread_id,
            "input": snapshot.turn_input,
        });
        let turn_response = {
            let mut context = TurnRequestContext {
                session_directory: &request.session_directory,
                runtime_id: &request.runtime_id,
                execution_ledger,
            };
            rpc_request(
                "turn/start",
                turn_start_params,
                next_id,
                stdin,
                lines,
                request_handler,
                pending,
                Some(&mut context),
            )
            .await?
        };
        let turn_id = turn_response
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                OfficialCodexAppServerError::InvalidResponse(
                    "turn/start result omitted turn.id".to_string(),
                )
            })?
            .to_string();
        if let Some(attempt) = association.turn_attempt.as_mut() {
            attempt.state = CodexTurnSubmissionState::Acknowledged;
            attempt.turn_id = Some(turn_id.clone());
        }
        association.active_turn_id = Some(turn_id.clone());
        persist_thread_association(&request.session_directory, association)?;
        Ok((turn_id, turn_response.get("turn").cloned()))
    }

    async fn start_interrupted_recovery(
        request: &OfficialCodexTurnRequest,
        association: &mut CodexThreadAssociation,
        execution_ledger: &mut CodexExecutionLedger,
        current_snapshot: &CodexMissionSnapshot,
        interrupted_turn_id: &str,
        next_id: &mut u64,
        stdin: &mut tokio::process::ChildStdin,
        lines: &mut Lines<BufReader<ChildStdout>>,
        request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
        pending: &mut Vec<Value>,
    ) -> Result<(String, Option<Value>), OfficialCodexAppServerError> {
        match execution_ledger.interrupted_recovery.as_ref() {
            Some(recovery)
                if recovery.state == CodexInterruptedRecoveryState::Prepared
                    && recovery.interrupted_turn_id == interrupted_turn_id => {}
            Some(recovery)
                if recovery.state == CodexInterruptedRecoveryState::ThreadAcknowledged
                    && recovery.interrupted_turn_id != interrupted_turn_id =>
            {
                for effect in &mut execution_ledger.effects {
                    effect.replay_request_id = None;
                }
                execution_ledger.interrupted_recovery = None;
            }
            Some(_) => {
                return Err(
                    OfficialCodexAppServerError::ConflictingInterruptedRecovery {
                        turn_id: interrupted_turn_id.to_string(),
                    },
                );
            }
            None => {}
        }
        if execution_ledger.canonical_input_sha256 != current_snapshot.input_sha256 {
            return Err(OfficialCodexAppServerError::TurnAttemptInputMismatch(
                request.tura_session_id.clone(),
            ));
        }
        reconcile_never_claimed_read_only_effects(
            &request.session_directory,
            execution_ledger,
            request_handler,
        )?;
        reconcile_durable_command_run_effects(
            &request.session_directory,
            execution_ledger,
            request_handler,
        )?;
        for (effect_index, effect) in execution_ledger.effects.iter().enumerate() {
            validate_reconciled_tool_effect(&request.session_directory, effect_index, effect)?;
        }

        let interrupted_thread_id = association.thread_id.clone();
        association.active_turn_id = None;
        association.turn_attempt = None;
        if execution_ledger.interrupted_recovery.is_none() {
            execution_ledger.interrupted_recovery = Some(CodexInterruptedRecoveryAttempt {
                interrupted_turn_id: interrupted_turn_id.to_string(),
                replay_effect_count: execution_ledger.effects.len(),
                state: CodexInterruptedRecoveryState::Prepared,
                recovery_thread_id: None,
            });
        }
        persist_execution_ledger(request_handler, execution_ledger, 0)?;
        persist_thread_association(&request.session_directory, association)?;

        pending.clear();
        let thread_response = rpc_request(
            "thread/start",
            current_snapshot.thread_start_params.clone(),
            next_id,
            stdin,
            lines,
            request_handler,
            pending,
            None,
        )
        .await?;
        let thread = thread_response
            .get("thread")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                OfficialCodexAppServerError::InvalidResponse(
                    "recovery thread/start result omitted thread".to_string(),
                )
            })?;
        let thread_id = required_string(thread, "id", "recovery thread.id")?;
        if thread_id == interrupted_thread_id {
            return Err(OfficialCodexAppServerError::InvalidResponse(
                "interrupted recovery did not create a fresh official Codex thread".to_string(),
            ));
        }
        let codex_session_id = required_string(thread, "sessionId", "recovery thread.sessionId")?;
        let (recovery_items, replay_request_ids) =
            interrupted_recovery_items(current_snapshot, execution_ledger)?;
        rpc_request(
            "thread/inject_items",
            json!({"threadId": thread_id, "items": recovery_items}),
            next_id,
            stdin,
            lines,
            request_handler,
            pending,
            None,
        )
        .await?;
        for (effect, replay_request_id) in
            execution_ledger.effects.iter_mut().zip(replay_request_ids)
        {
            effect.replay_request_id = Some(replay_request_id);
        }
        if let Some(recovery) = execution_ledger.interrupted_recovery.as_mut() {
            recovery.state = CodexInterruptedRecoveryState::ThreadAcknowledged;
            recovery.recovery_thread_id = Some(thread_id.clone());
        }
        persist_execution_ledger(request_handler, execution_ledger, 0)?;
        association.thread_id = thread_id.clone();
        association.codex_session_id = codex_session_id;
        persist_thread_association(&request.session_directory, association)?;
        let mut recovery_snapshot = current_snapshot.clone();
        recovery_snapshot.turn_input.clear();
        let started = start_turn(
            request,
            association,
            execution_ledger,
            &recovery_snapshot,
            Vec::new(),
            next_id,
            stdin,
            lines,
            request_handler,
            pending,
        )
        .await?;
        if started.0 == interrupted_turn_id {
            return Err(OfficialCodexAppServerError::InvalidResponse(
                "interrupted recovery did not create a fresh official Codex turn".to_string(),
            ));
        }
        Ok(started)
    }

    fn finish_successful_turn(
        request: &OfficialCodexTurnRequest,
        mut association: CodexThreadAssociation,
        execution_ledger: &mut CodexExecutionLedger,
        request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
        turn_id: &str,
        final_answer: Option<String>,
        usage: Option<OfficialCodexUsage>,
        authoritative_events: Vec<OfficialCodexAuthoritativeEvent>,
    ) -> Result<OfficialCodexTurnResponse, OfficialCodexAppServerError> {
        if let Some(recovery) = execution_ledger.interrupted_recovery.as_ref() {
            let consumed = execution_ledger
                .effects
                .iter()
                .take(recovery.replay_effect_count)
                .filter(|effect| effect.replay_request_id.is_some())
                .count();
            if consumed != recovery.replay_effect_count {
                return Err(OfficialCodexAppServerError::UnconsumedRecoveryEffects {
                    turn_id: turn_id.to_string(),
                    consumed,
                    expected: recovery.replay_effect_count,
                });
            }
        }
        execution_ledger.terminal_status = Some("completed".to_string());
        execution_ledger.interrupted_recovery = None;
        persist_execution_ledger(request_handler, execution_ledger, 0)?;
        association.active_turn_id = None;
        association.turn_attempt = None;
        persist_thread_association(&request.session_directory, &association)?;
        Ok(OfficialCodexTurnResponse {
            content: Value::String(
                final_answer.ok_or(OfficialCodexAppServerError::MissingFinalAnswer)?,
            ),
            association,
            usage,
            authoritative_events,
            commander_convergence_proof: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_successful_turn_with_convergence(
        request: &OfficialCodexTurnRequest,
        association: CodexThreadAssociation,
        execution_ledger: &mut CodexExecutionLedger,
        request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
        turn_id: &str,
        final_answer: Option<String>,
        usage: Option<OfficialCodexUsage>,
        authoritative_events: Vec<OfficialCodexAuthoritativeEvent>,
        commander_pre_turn_ids: Option<&[String]>,
        next_id: &mut u64,
        stdin: &mut tokio::process::ChildStdin,
        lines: &mut Lines<BufReader<ChildStdout>>,
        pending: &mut Vec<Value>,
    ) -> Result<OfficialCodexTurnResponse, OfficialCodexAppServerError> {
        let convergence_proof = if let Some(binding) = request.commander_continuation.as_ref() {
            let final_answer = final_answer
                .as_deref()
                .ok_or(OfficialCodexAppServerError::MissingFinalAnswer)?;
            if final_answer.trim() == latest_user_input(&request.messages)?.trim() {
                return Err(
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                        "final assistant is a prompt echo".to_string(),
                    ),
                );
            }
            let pre_turn_ids = commander_pre_turn_ids.ok_or_else(|| {
                OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                    "pre-submit turn identity set is missing".to_string(),
                )
            })?;
            let thread_read = rpc_request(
                "thread/read",
                json!({"threadId": binding.target_thread_id, "includeTurns": true}),
                next_id,
                stdin,
                lines,
                request_handler,
                pending,
                None,
            )
            .await?;
            let thread = thread_read
                .get("thread")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    OfficialCodexAppServerError::InvalidResponse(
                        "post-submit thread/read omitted thread".to_string(),
                    )
                })?;
            let post_thread_id = required_string(thread, "id", "post-submit thread.id")?;
            if post_thread_id != binding.target_thread_id {
                return Err(
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(format!(
                        "post-submit thread/read returned {post_thread_id}, expected {}",
                        binding.target_thread_id
                    )),
                );
            }
            let turns = thread
                .get("turns")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let post_turn_ids = ordered_turn_ids(&turns)?;
            if post_turn_ids.len() != pre_turn_ids.len() + 1
                || post_turn_ids[..pre_turn_ids.len()] != *pre_turn_ids
                || post_turn_ids.last().map(String::as_str) != Some(turn_id)
            {
                return Err(
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                        "target thread did not advance by the exact completed turn".to_string(),
                    ),
                );
            }
            let target_turn = turns.last().ok_or_else(|| {
                OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                    "post-submit target turn is missing".to_string(),
                )
            })?;
            if target_turn.get("status").and_then(Value::as_str) != Some("completed")
                || authoritative_turn_answer(target_turn).as_deref() != Some(final_answer)
            {
                return Err(
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                        "post-submit target turn/final assistant does not match completion"
                            .to_string(),
                    ),
                );
            }
            let proof = CommanderConvergenceProof {
                schema_version: COMMANDER_CONVERGENCE_PROOF_SCHEMA_VERSION.to_string(),
                request_id: binding.continuation_request_id.clone(),
                callback_payload_sha256: binding.callback_payload_sha256.clone(),
                effect_identity_sha256: binding.effect_identity_sha256.clone(),
                child_session_id: binding.child_session_id.clone(),
                child_transaction_id: binding.child_transaction_id.clone(),
                child_runtime_id: binding.child_runtime_id.clone(),
                requested_action: binding.requested_action.clone(),
                target_thread_id: binding.target_thread_id.clone(),
                pre_revision_sha256: binding.pre_revision_sha256.clone(),
                post_revision_sha256: canonical_thread_revision_sha256_from_ids(
                    &binding.target_thread_id,
                    &post_turn_ids,
                ),
                target_turn_id: turn_id.to_string(),
                final_assistant_sha256: sha256_canonical_json(Value::String(
                    final_answer.to_string(),
                ))?,
            };
            proof
                .validate_shape()
                .map_err(OfficialCodexAppServerError::CommanderConvergenceProofInvalid)?;
            Some(proof)
        } else {
            None
        };
        if let Some(proof) = convergence_proof.as_ref() {
            execution_ledger.commander_convergence_proof = Some(proof.clone());
            execution_ledger.commander_convergence_final_assistant = final_answer.clone();
        }
        let mut response = finish_successful_turn(
            request,
            association,
            execution_ledger,
            request_handler,
            turn_id,
            final_answer,
            usage,
            authoritative_events,
        )?;
        response.commander_convergence_proof = convergence_proof;
        Ok(response)
    }
    let codex_session_id = thread
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| prior.as_ref().map(|value| value.codex_session_id.clone()))
        .ok_or_else(|| {
            OfficialCodexAppServerError::InvalidResponse(
                "thread result omitted thread.sessionId".to_string(),
            )
        })?;
    let mut association = CodexThreadAssociation {
        schema_version: ASSOCIATION_SCHEMA_VERSION,
        tura_session_id: request.tura_session_id.clone(),
        thread_id: thread_id.clone(),
        codex_session_id,
        executable_identity,
        active_turn_id: None,
        turn_attempt: prior.as_ref().and_then(|value| value.turn_attempt.clone()),
        mission_snapshot: None,
        observed_tool_effects: Vec::new(),
        interrupted_recovery: None,
        association_scope_id: (association_scope_id(request) != request.tura_session_id)
            .then(|| association_scope_id(request)),
    };
    persist_thread_association(&request.session_directory, &association)?;
    let authoritative_turns = if resume_thread_id.is_some() {
        let thread_read = rpc_request(
            "thread/read",
            json!({"threadId": thread_id, "includeTurns": true}),
            &mut next_id,
            stdin,
            lines,
            request_handler,
            &mut pending,
            None,
        )
        .await?;
        if let Some(binding) = request.commander_continuation.as_ref() {
            let pre_thread_id = thread_read
                .pointer("/thread/id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    OfficialCodexAppServerError::InvalidResponse(
                        "pre-submit thread/read omitted thread.id".to_string(),
                    )
                })?;
            if pre_thread_id != binding.target_thread_id {
                return Err(OfficialCodexAppServerError::InvalidResponse(format!(
                    "pre-submit thread/read returned {pre_thread_id}, expected {}",
                    binding.target_thread_id
                )));
            }
        }
        thread_read
            .pointer("/thread/turns")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let commander_pre_turn_ids = if let Some(binding) = request.commander_continuation.as_ref() {
        let pre_turn_ids = association
            .turn_attempt
            .as_ref()
            .map(|attempt| attempt.authoritative_turn_ids_before_submit.clone())
            .map(Ok)
            .unwrap_or_else(|| ordered_turn_ids(&authoritative_turns))?;
        let actual =
            canonical_thread_revision_sha256_from_ids(&binding.target_thread_id, &pre_turn_ids);
        if actual != binding.pre_revision_sha256 {
            return Err(
                OfficialCodexAppServerError::CommanderTargetPreimageMismatch {
                    thread_id: binding.target_thread_id.clone(),
                    expected: binding.pre_revision_sha256.clone(),
                    actual,
                },
            );
        }
        Some(pre_turn_ids)
    } else {
        None
    };

    let mut recovered_turn = None;
    if let Some(attempt) = association.turn_attempt.as_ref() {
        if attempt.input_sha256 != current_snapshot.input_sha256 {
            return Err(OfficialCodexAppServerError::TurnAttemptInputMismatch(
                request.tura_session_id.clone(),
            ));
        }
        if request.commander_continuation.is_some()
            && attempt.state == CodexTurnSubmissionState::Prepared
        {
            return Err(
                OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                    "target turn/start delivery is uncertain; blind reconciliation is forbidden"
                        .to_string(),
                ),
            );
        }
        let candidates = authoritative_turns
            .iter()
            .filter(|turn| {
                turn.get("id").and_then(Value::as_str).is_some_and(|id| {
                    !attempt
                        .authoritative_turn_ids_before_submit
                        .iter()
                        .any(|known| known == id)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if request.commander_continuation.is_some() {
            let expected_turn_id = attempt.turn_id.as_deref().ok_or_else(|| {
                OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                    "acknowledged target turn omitted durable turn id".to_string(),
                )
            })?;
            if candidates.len() != 1
                || candidates[0].get("id").and_then(Value::as_str) != Some(expected_turn_id)
            {
                return Err(
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                        "target turn cannot be reconciled to its exact durable turn id".to_string(),
                    ),
                );
            }
            recovered_turn = candidates.first().cloned();
        } else {
            match candidates.as_slice() {
                [] => {
                    association.active_turn_id = None;
                    association.turn_attempt = None;
                    persist_thread_association(&request.session_directory, &association)?;
                }
                [turn] => recovered_turn = Some(turn.clone()),
                _ => {
                    return Err(OfficialCodexAppServerError::AmbiguousTurnReconciliation {
                        attempt_id: attempt.attempt_id.clone(),
                        candidate_count: candidates.len(),
                    });
                }
            }
        }
    }

    let (mut turn_id, mut authoritative_turn) = if let Some(turn) = recovered_turn {
        let turn_id = turn
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                OfficialCodexAppServerError::InvalidResponse(
                    "thread/read turn omitted id".to_string(),
                )
            })?
            .to_string();
        if let Some(attempt) = association.turn_attempt.as_mut() {
            attempt.state = CodexTurnSubmissionState::Acknowledged;
            attempt.turn_id = Some(turn_id.clone());
        }
        association.active_turn_id = Some(turn_id.clone());
        persist_thread_association(&request.session_directory, &association)?;
        (turn_id, Some(turn))
    } else {
        let snapshot = if let Some(recovery) = execution_ledger.interrupted_recovery.as_ref() {
            if recovery.state != CodexInterruptedRecoveryState::ThreadAcknowledged {
                return Err(
                    OfficialCodexAppServerError::ConflictingInterruptedRecovery {
                        turn_id: recovery.interrupted_turn_id.clone(),
                    },
                );
            }
            if execution_ledger.canonical_input_sha256 != current_snapshot.input_sha256 {
                return Err(OfficialCodexAppServerError::TurnAttemptInputMismatch(
                    request.tura_session_id.clone(),
                ));
            }
            let mut snapshot = current_snapshot.clone();
            snapshot.turn_input.clear();
            snapshot
        } else {
            current_snapshot.clone()
        };
        let authoritative_turn_ids_before_submit = authoritative_turns
            .iter()
            .filter_map(|turn| turn.get("id").and_then(Value::as_str).map(str::to_string))
            .collect();
        start_turn(
            request,
            &mut association,
            &mut execution_ledger,
            &snapshot,
            authoritative_turn_ids_before_submit,
            &mut next_id,
            stdin,
            lines,
            request_handler,
            &mut pending,
        )
        .await?
    };

    let mut final_answer = authoritative_turn
        .as_ref()
        .and_then(authoritative_turn_answer);
    let mut usage = None;
    if let Some(turn) = authoritative_turn.as_ref() {
        let status = turn
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if status == "completed" {
            return finish_successful_turn_with_convergence(
                request,
                association,
                &mut execution_ledger,
                request_handler,
                &turn_id,
                final_answer,
                usage,
                authoritative_events,
                commander_pre_turn_ids.as_deref(),
                &mut next_id,
                stdin,
                lines,
                &mut pending,
            )
            .await;
        }
        if status == "interrupted" {
            if request.commander_continuation.is_some() {
                return Err(
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                        "target Commander turn interrupted; delivery remains unsettled".to_string(),
                    ),
                );
            }
            let started = start_interrupted_recovery(
                request,
                &mut association,
                &mut execution_ledger,
                &current_snapshot,
                &turn_id,
                &mut next_id,
                stdin,
                lines,
                request_handler,
                &mut pending,
            )
            .await?;
            turn_id = started.0;
            authoritative_turn = started.1;
            final_answer = authoritative_turn
                .as_ref()
                .and_then(authoritative_turn_answer);
        } else if !matches!(status, "inProgress" | "pending") {
            execution_ledger.terminal_status = Some(status.to_string());
            persist_execution_ledger(request_handler, &execution_ledger, 0)?;
            association.active_turn_id = None;
            association.turn_attempt = None;
            persist_thread_association(&request.session_directory, &association)?;
            return Err(OfficialCodexAppServerError::TurnNotCompleted {
                turn_id,
                status: status.to_string(),
            });
        }
    }
    let mut queue = VecDeque::from(std::mem::take(&mut pending));
    loop {
        let message = if let Some(message) = queue.pop_front() {
            message
        } else {
            read_message(lines).await?
        };
        if message.get("method").is_none() {
            continue;
        }
        if message.get("id").is_some() {
            let mut context = TurnRequestContext {
                session_directory: &request.session_directory,
                runtime_id: &request.runtime_id,
                execution_ledger: &mut execution_ledger,
            };
            handle_server_request(&message, stdin, request_handler, Some(&mut context)).await?;
            continue;
        }
        let method = message.get("method").and_then(Value::as_str);
        if method.is_some_and(is_authoritative_event) {
            let event_key = serde_json::to_string(&canonical_json(message.clone()))
                .map_err(OfficialCodexAppServerError::EncodeAssociation)?;
            if seen_authoritative_events.insert(event_key) {
                let event = OfficialCodexAuthoritativeEvent {
                    sequence: authoritative_events.len() as u64,
                    method: method.unwrap_or_default().to_string(),
                    params: message.get("params").cloned().unwrap_or(Value::Null),
                };
                authoritative_events.push(event);
            }
        }
        match method {
            Some("item/completed") => {
                if let Some(text) = final_agent_text(message.pointer("/params/item")) {
                    final_answer = Some(text);
                }
            }
            Some("thread/tokenUsage/updated") => {
                usage = parse_usage(message.get("params"));
            }
            Some("turn/completed") => {
                let completed_turn = message.pointer("/params/turn").ok_or_else(|| {
                    OfficialCodexAppServerError::InvalidResponse(
                        "turn/completed omitted params.turn".to_string(),
                    )
                })?;
                let completed_id = completed_turn
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if completed_id != turn_id {
                    return Err(OfficialCodexAppServerError::InvalidResponse(format!(
                        "turn/completed identified {completed_id}, expected {turn_id}"
                    )));
                }
                let status = completed_turn
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                if status == "interrupted" {
                    if request.commander_continuation.is_some() {
                        return Err(
                            OfficialCodexAppServerError::CommanderConvergenceProofInvalid(
                                "target Commander turn interrupted; delivery remains unsettled"
                                    .to_string(),
                            ),
                        );
                    }
                    let started = start_interrupted_recovery(
                        request,
                        &mut association,
                        &mut execution_ledger,
                        &current_snapshot,
                        &turn_id,
                        &mut next_id,
                        stdin,
                        lines,
                        request_handler,
                        &mut pending,
                    )
                    .await?;
                    turn_id = started.0;
                    authoritative_turn = started.1;
                    final_answer = authoritative_turn
                        .as_ref()
                        .and_then(authoritative_turn_answer);
                    queue.extend(std::mem::take(&mut pending));
                    continue;
                }
                if status != "completed" {
                    execution_ledger.terminal_status = Some(status.to_string());
                    persist_execution_ledger(request_handler, &execution_ledger, 0)?;
                    return Err(OfficialCodexAppServerError::TurnNotCompleted {
                        turn_id: turn_id.clone(),
                        status: status.to_string(),
                    });
                }
                if let Some(items) = completed_turn.get("items").and_then(Value::as_array) {
                    for item in items {
                        if let Some(text) = final_agent_text(Some(item)) {
                            final_answer = Some(text);
                        }
                    }
                }
                return finish_successful_turn_with_convergence(
                    request,
                    association,
                    &mut execution_ledger,
                    request_handler,
                    &turn_id,
                    final_answer,
                    usage,
                    authoritative_events,
                    commander_pre_turn_ids.as_deref(),
                    &mut next_id,
                    stdin,
                    lines,
                    &mut pending,
                )
                .await;
            }
            _ => {}
        }
    }
}

async fn rpc_request(
    method: &str,
    params: Value,
    next_id: &mut u64,
    stdin: &mut tokio::process::ChildStdin,
    lines: &mut Lines<BufReader<ChildStdout>>,
    request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
    pending: &mut Vec<Value>,
    mut turn_context: Option<&mut TurnRequestContext<'_>>,
) -> Result<Value, OfficialCodexAppServerError> {
    let id = *next_id;
    *next_id += 1;
    write_message(
        stdin,
        &json!({"id": id, "method": method, "params": params}),
    )
    .await?;
    loop {
        let message = read_message(lines).await?;
        if message.get("id").and_then(Value::as_u64) == Some(id) && message.get("method").is_none()
        {
            if let Some(error) = message.get("error") {
                return Err(OfficialCodexAppServerError::RpcError {
                    method: method.to_string(),
                    error: error.clone(),
                });
            }
            return message.get("result").cloned().ok_or_else(|| {
                OfficialCodexAppServerError::InvalidResponse(format!(
                    "{method} response omitted result"
                ))
            });
        }
        if message.get("method").is_some() && message.get("id").is_some() {
            handle_server_request(
                &message,
                stdin,
                request_handler,
                turn_context.as_deref_mut(),
            )
            .await?;
        } else {
            pending.push(message);
        }
    }
}

async fn handle_server_request(
    message: &Value,
    stdin: &mut tokio::process::ChildStdin,
    request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
    mut turn_context: Option<&mut TurnRequestContext<'_>>,
) -> Result<(), OfficialCodexAppServerError> {
    let id = message.get("id").cloned().ok_or_else(|| {
        OfficialCodexAppServerError::InvalidResponse("server request omitted id".to_string())
    })?;
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            OfficialCodexAppServerError::InvalidResponse(
                "server request omitted method".to_string(),
            )
        })?
        .to_string();
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let effect_identity = server_effect_identity(&params, &id);
    let command_run_observation = if method == "item/tool/call" {
        let Some(context) = turn_context.as_deref() else {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index: 0,
                reason: "command_run effect omitted its runtime context".to_string(),
            });
        };
        match request_handler.as_deref_mut() {
            Some(handler) => handler
                .observe_command_run_effect(
                    &OfficialCodexServerRequest {
                        method: method.clone(),
                        params: params.clone(),
                    },
                    context.runtime_id,
                )
                .map_err(|reason| OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index: context.execution_ledger.effects.len(),
                    reason: format!("runtime command identity normalization failed: {reason}"),
                })?,
            None => None,
        }
    } else {
        None
    };
    let read_only_observation = if method == "item/tool/call"
        && turn_context
            .as_deref()
            .is_some_and(|context| context.execution_ledger.interrupted_recovery.is_none())
    {
        let effect_index = turn_context
            .as_deref()
            .map(|context| context.execution_ledger.effects.len())
            .unwrap_or(0);
        if let Some(handler) = request_handler.as_deref_mut() {
            handler
                .observe_read_only_effect(&OfficialCodexServerRequest {
                    method: method.clone(),
                    params: params.clone(),
                })
                .map_err(|reason| OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: format!("runtime access classification failed: {reason}"),
                })?
        } else {
            None
        }
    } else {
        None
    };
    let effect_disposition = if let Some(context) = turn_context.as_deref_mut() {
        prepare_tool_effect(
            context,
            &method,
            &params,
            &effect_identity,
            read_only_observation,
            command_run_observation,
        )?
    } else {
        ToolEffectDisposition::Execute(None)
    };
    if method == "item/tool/call"
        && let Some(context) = turn_context.as_deref_mut()
    {
        let effect_index = match &effect_disposition {
            ToolEffectDisposition::Execute(index) => index.unwrap_or(0),
            ToolEffectDisposition::Replay(_) => 0,
        };
        persist_execution_ledger(request_handler, context.execution_ledger, effect_index)?;
    }
    if let ToolEffectDisposition::Replay(result) = effect_disposition {
        return write_message(stdin, &json!({"id": id, "result": result})).await;
    }
    let effect_index = match effect_disposition {
        ToolEffectDisposition::Execute(effect_index) => effect_index,
        ToolEffectDisposition::Replay(_) => unreachable!(),
    };
    let handled = match request_handler.as_deref_mut() {
        Some(handler) => {
            handler
                .handle(OfficialCodexServerRequest {
                    method: method.clone(),
                    params,
                })
                .await
        }
        None => {
            write_message(
                stdin,
                &json!({
                    "id": id,
                    "error": {"code": -32601, "message": "Tura has no governed handler for this request"}
                }),
            )
            .await?;
            return Err(OfficialCodexAppServerError::UnsupportedServerRequest(
                method,
            ));
        }
    };
    return match handled {
        Ok(result) => {
            if let (Some(effect_index), Some(context)) = (effect_index, turn_context.as_deref_mut())
            {
                let command_evidence = extract_command_receipt_references(
                    context.session_directory,
                    effect_index,
                    &result,
                )?;
                let effect = context
                    .execution_ledger
                    .effects
                    .get_mut(effect_index)
                    .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: "durable effect journal entry disappeared".to_string(),
                    })?;
                effect.response = Some(result.clone());
                effect.command_receipts = command_evidence.references;
                if command_evidence.replayable {
                    effect.state = CodexObservedToolEffectState::Reconciled;
                }
                persist_execution_ledger(request_handler, context.execution_ledger, effect_index)?;
            }
            write_message(stdin, &json!({"id": id, "result": result})).await
        }
        Err(reason) => {
            write_message(
                stdin,
                &json!({
                    "id": id,
                    "error": {"code": -32000, "message": reason}
                }),
            )
            .await?;
            Err(OfficialCodexAppServerError::ServerRequestRejected { method, reason })
        }
    };
}

enum ToolEffectDisposition {
    Replay(Value),
    Execute(Option<usize>),
}

fn prepare_tool_effect(
    context: &mut TurnRequestContext<'_>,
    method: &str,
    params: &Value,
    effect_identity: &Value,
    read_only_observation: Option<CodexReadOnlyEffectObservation>,
    command_run_observation: Option<CodexCommandRunEffectObservation>,
) -> Result<ToolEffectDisposition, OfficialCodexAppServerError> {
    if method != "item/tool/call" {
        return Ok(ToolEffectDisposition::Execute(None));
    }
    let request_sha256 = server_request_semantic_sha256(method, params)?;
    if let Some(recovery) = context.execution_ledger.interrupted_recovery.as_ref() {
        if let Some((effect_index, effect)) = context
            .execution_ledger
            .effects
            .iter()
            .take(recovery.replay_effect_count)
            .enumerate()
            .find(|(_, effect)| effect.replay_request_id.as_ref() == Some(effect_identity))
        {
            if effect.request_sha256 != request_sha256 {
                return Err(OfficialCodexAppServerError::ConflictingRecoveryToolEffect {
                    effect_index,
                    expected: effect.request_sha256.clone(),
                    actual: request_sha256,
                });
            }
            validate_reconciled_tool_effect(context.session_directory, effect_index, effect)?;
            return Ok(ToolEffectDisposition::Replay(
                effect.response.clone().ok_or_else(|| {
                    OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: "reconciled effect omitted its response".to_string(),
                    }
                })?,
            ));
        }
        if let Some(effect_index) = context
            .execution_ledger
            .effects
            .iter()
            .take(recovery.replay_effect_count)
            .position(|effect| effect.replay_request_id.is_none())
        {
            let effect = &context.execution_ledger.effects[effect_index];
            if effect.request_sha256 != request_sha256 {
                return Err(OfficialCodexAppServerError::ConflictingRecoveryToolEffect {
                    effect_index,
                    expected: effect.request_sha256.clone(),
                    actual: request_sha256,
                });
            }
            validate_reconciled_tool_effect(context.session_directory, effect_index, effect)?;
            let result = effect.response.clone().ok_or_else(|| {
                OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: "reconciled effect omitted its response".to_string(),
                }
            })?;
            context.execution_ledger.effects[effect_index].replay_request_id =
                Some(effect_identity.clone());
            return Ok(ToolEffectDisposition::Replay(result));
        }
        if let Some((effect_index, effect)) = context
            .execution_ledger
            .effects
            .iter()
            .take(recovery.replay_effect_count)
            .enumerate()
            .find(|(_, effect)| effect.request_sha256 == request_sha256)
        {
            validate_reconciled_tool_effect(context.session_directory, effect_index, effect)?;
            return Ok(ToolEffectDisposition::Replay(
                effect.response.clone().ok_or_else(|| {
                    OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: "reconciled effect omitted its response".to_string(),
                    }
                })?,
            ));
        }
    }
    if let Some((effect_index, effect)) = context
        .execution_ledger
        .effects
        .iter()
        .enumerate()
        .find(|(_, effect)| effect.original_request_id == *effect_identity)
    {
        if effect.request_sha256 != request_sha256 {
            return Err(OfficialCodexAppServerError::ConflictingRecoveryToolEffect {
                effect_index,
                expected: effect.request_sha256.clone(),
                actual: request_sha256,
            });
        }
        validate_reconciled_tool_effect(context.session_directory, effect_index, effect)?;
        return Ok(ToolEffectDisposition::Replay(
            effect.response.clone().ok_or_else(|| {
                OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: "reconciled effect omitted its response".to_string(),
                }
            })?,
        ));
    }

    let effect_index = context.execution_ledger.effects.len();
    context
        .execution_ledger
        .effects
        .push(CodexObservedToolEffect {
            request_sha256,
            runtime_id: Some(context.runtime_id.to_string()),
            request_params: Some(params.clone()),
            original_request_id: effect_identity.clone(),
            state: CodexObservedToolEffectState::Observed,
            response: None,
            command_receipts: Vec::new(),
            replay_request_id: None,
            read_only_observation,
            command_run_observation,
        });
    Ok(ToolEffectDisposition::Execute(Some(effect_index)))
}

const MAX_SYNTHETIC_READ_ONLY_COMMANDS: usize = 32;
const MAX_OBSERVED_IDENTITY_BYTES: usize = 512;
const MAX_OBSERVED_COMMAND_LINE_BYTES: usize = 16 * 1024;

fn command_claim_identity(
    execution_id: &str,
    binding_id: Option<&str>,
    effective_step: u64,
    enumerated_index: usize,
) -> String {
    if let Some(binding_id) = binding_id {
        if binding_id == execution_id
            || binding_id
                .strip_prefix(execution_id)
                .is_some_and(|suffix| suffix.starts_with(':'))
        {
            binding_id.to_string()
        } else {
            format!("{execution_id}:{binding_id}")
        }
    } else {
        format!("{execution_id}:step:{effective_step}:index:{enumerated_index}")
    }
}

fn validate_command_run_observation(
    effect_index: usize,
    observation: &CodexCommandRunEffectObservation,
) -> Result<(), OfficialCodexAppServerError> {
    let expected_execution_id = format!("{}:{}", observation.runtime_id, observation.tool_call_id);
    if observation.runtime_id.is_empty()
        || observation.runtime_id.len() > MAX_OBSERVED_IDENTITY_BYTES
        || observation.tool_call_id.is_empty()
        || observation.tool_call_id.len() > MAX_OBSERVED_IDENTITY_BYTES
        || observation.execution_id != expected_execution_id
        || observation.execution_id.len() > MAX_OBSERVED_IDENTITY_BYTES
        || observation.commands.is_empty()
        || observation.commands.len() > MAX_SYNTHETIC_READ_ONLY_COMMANDS
    {
        return Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "persisted command_run observation is malformed or oversized".to_string(),
        });
    }

    let mut claim_identities = HashSet::new();
    let mut encoded_identities = HashSet::new();
    for (enumerated_index, command) in observation.commands.iter().enumerate() {
        let expected_claim_identity = command_claim_identity(
            &observation.execution_id,
            command.binding_id.as_deref(),
            command.effective_step,
            enumerated_index,
        );
        if command.command_type.is_empty()
            || command.command_type.len() > MAX_OBSERVED_IDENTITY_BYTES
            || command.enumerated_index != enumerated_index
            || command.effective_step == 0
            || command.binding_id.as_ref().is_some_and(|binding_id| {
                binding_id.is_empty() || binding_id.len() > MAX_OBSERVED_IDENTITY_BYTES
            })
            || command.claim_identity != expected_claim_identity
            || command.claim_identity.len() > MAX_OBSERVED_IDENTITY_BYTES
            || !claim_identities.insert(command.claim_identity.as_str())
            || !encoded_identities.insert(encode_command_receipt_identity(&command.claim_identity))
        {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "persisted command_run command or exact receipt identity is invalid"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn validate_read_only_observation(
    effect_index: usize,
    observation: &CodexReadOnlyEffectObservation,
) -> Result<(), OfficialCodexAppServerError> {
    let expected_execution_id = format!(
        "{}:{}",
        observation.original_runtime_id, observation.tool_call_id
    );
    if observation.original_runtime_id.is_empty()
        || observation.original_runtime_id.len() > MAX_OBSERVED_IDENTITY_BYTES
        || observation.tool_call_id.is_empty()
        || observation.tool_call_id.len() > MAX_OBSERVED_IDENTITY_BYTES
        || observation.execution_id != expected_execution_id
        || observation.execution_id.len() > MAX_OBSERVED_IDENTITY_BYTES
        || observation.commands.is_empty()
        || observation.commands.len() > MAX_SYNTHETIC_READ_ONLY_COMMANDS
    {
        return Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "persisted read-only observation is malformed or oversized".to_string(),
        });
    }

    let mut claim_identities = HashSet::new();
    for (enumerated_index, command) in observation.commands.iter().enumerate() {
        let expected_claim_identity = command_claim_identity(
            &observation.execution_id,
            command.binding_id.as_deref(),
            command.effective_step,
            enumerated_index,
        );
        if command.access != CodexObservedCommandAccess::ReadOnly
            || command.command_type != "zsh"
            || command.command_line.is_empty()
            || command.command_line.len() > MAX_OBSERVED_COMMAND_LINE_BYTES
            || command.enumerated_index != enumerated_index
            || command.binding_id.as_ref().is_some_and(|binding_id| {
                binding_id.is_empty() || binding_id.len() > MAX_OBSERVED_IDENTITY_BYTES
            })
            || command.claim_identity != expected_claim_identity
            || command.claim_identity.len() > MAX_OBSERVED_IDENTITY_BYTES
            || !claim_identities.insert(command.claim_identity.as_str())
        {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "persisted read-only command or exact claim identity is invalid"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn synthesize_never_claimed_read_only_response(
    effect_index: usize,
    observation: &CodexReadOnlyEffectObservation,
) -> Result<Value, OfficialCodexAppServerError> {
    validate_read_only_observation(effect_index, observation)?;
    let results: Vec<Value> = observation
        .commands
        .iter()
        .map(synthesize_never_claimed_read_only_command)
        .collect();
    let output = json!({"results": results});
    Ok(json!({
        "contentItems": [{"type": "inputText", "text": output.to_string()}],
        "success": true
    }))
}

fn synthesize_never_claimed_read_only_command(command: &CodexReadOnlyCommandObservation) -> Value {
    json!({
        "command_type": command.command_type,
        "command_id": command.claim_identity,
        "step": command.effective_step,
        "success": true,
        "output": {
            "schema_version": "official_codex_unclaimed_read_only_terminal_v1",
            "terminal_state": "not_started",
            "outcome": "known_zero_mutation",
            "claim_state": "absent",
            "claim_identity": command.claim_identity,
            "process_state": "never_started",
            "mutation_count": 0,
            "authoritative_publication": "none",
            "delivery_state": "synthetic_replay"
        }
    })
}

fn encode_command_receipt_identity(identity: &str) -> String {
    let mut encoded = String::with_capacity(identity.len());
    for character in identity.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
            encoded.push(character);
        } else {
            encoded.push_str(&format!("_x{:x}_", character as u32));
        }
    }
    if encoded.is_empty() {
        "command_run".to_string()
    } else {
        encoded
    }
}

fn read_only_recovery_artifact_exists(
    effect_index: usize,
    path: &Path,
) -> Result<bool, OfficialCodexAppServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: format!(
                "read-only recovery artifact state is unavailable at {}: {error}",
                path.display()
            ),
        }),
    }
}

fn synthesize_completed_read_only_command(
    session_directory: &Path,
    effect_index: usize,
    command: &CodexReadOnlyCommandObservation,
) -> Result<Value, OfficialCodexAppServerError> {
    let receipt_directory = session_directory
        .join(".tura")
        .join("run")
        .join("command_receipts");
    let receipt_path = receipt_directory.join(format!(
        "{}.json",
        encode_command_receipt_identity(&command.claim_identity)
    ));
    let bytes = std::fs::read(&receipt_path).map_err(|error| {
        OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: format!(
                "completed read-only terminal receipt is unavailable at {}: {error}",
                receipt_path.display()
            ),
        }
    })?;
    let receipt: Value = serde_json::from_slice(&bytes).map_err(|error| {
        OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: format!("completed read-only terminal receipt is invalid: {error}"),
        }
    })?;
    if receipt.get("call_id").and_then(Value::as_str) != Some(command.claim_identity.as_str()) {
        return Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "completed read-only terminal receipt identity changed".to_string(),
        });
    }
    validate_terminal_receipt(effect_index, &receipt, true)?;
    let absolute_path = std::fs::canonicalize(&receipt_path).map_err(|error| {
        OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: format!(
                "completed read-only terminal receipt cannot be canonicalized: {error}"
            ),
        }
    })?;
    Ok(json!({
        "command_type": command.command_type,
        "command_id": command.claim_identity,
        "step": command.effective_step,
        "success": true,
        "output": {
            "message": "read-only command completed before transport interruption; stdout was not delivered",
            "terminal_receipt": receipt,
            "terminal_receipt_path": absolute_path,
            "delivery_state": "reconstructed_from_durable_terminal_receipt"
        }
    }))
}

fn synthesize_reconciled_read_only_response(
    session_directory: &Path,
    effect_index: usize,
    observation: &CodexReadOnlyEffectObservation,
) -> Result<Value, OfficialCodexAppServerError> {
    validate_read_only_observation(effect_index, observation)?;
    let receipt_directory = session_directory
        .join(".tura")
        .join("run")
        .join("command_receipts");
    let mut results = Vec::with_capacity(observation.commands.len());
    for command in &observation.commands {
        let encoded_identity = encode_command_receipt_identity(&command.claim_identity);
        let claim_path = receipt_directory.join(format!("{encoded_identity}.claim.json"));
        let receipt_path = receipt_directory.join(format!("{encoded_identity}.json"));
        let claim_exists = read_only_recovery_artifact_exists(effect_index, &claim_path)?;
        let receipt_exists = read_only_recovery_artifact_exists(effect_index, &receipt_path)?;

        if receipt_exists {
            results.push(synthesize_completed_read_only_command(
                session_directory,
                effect_index,
                command,
            )?);
        } else if claim_exists {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: format!(
                    "read-only command {} was claimed without a terminal receipt",
                    command.claim_identity
                ),
            });
        } else {
            results.push(synthesize_never_claimed_read_only_command(command));
        }
    }
    let output = json!({"results": results});
    Ok(json!({
        "contentItems": [{"type": "inputText", "text": output.to_string()}],
        "success": true
    }))
}

fn reconcile_never_claimed_read_only_effects(
    session_directory: &Path,
    execution_ledger: &mut CodexExecutionLedger,
    request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
) -> Result<(), OfficialCodexAppServerError> {
    for effect_index in 0..execution_ledger.effects.len() {
        let observation = {
            let effect = &execution_ledger.effects[effect_index];
            let Some(observation) = effect.read_only_observation.as_ref() else {
                continue;
            };
            if effect.state != CodexObservedToolEffectState::Observed
                || effect.response.is_some()
                || !effect.command_receipts.is_empty()
                || effect.replay_request_id.is_some()
            {
                return Err(OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: "observed read-only effect carried additional or terminal state"
                        .to_string(),
                });
            }
            observation.clone()
        };

        validate_read_only_observation(effect_index, &observation)?;
        let handler = request_handler.as_deref_mut().ok_or_else(|| {
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "runtime read-only claim verifier is unavailable".to_string(),
            }
        })?;
        let (response, command_receipts) = match handler
            .verify_never_claimed_read_only_effect(&observation)
        {
            Ok(()) => (
                synthesize_never_claimed_read_only_response(effect_index, &observation)?,
                Vec::new(),
            ),
            Err(unclaimed_reason) => {
                handler
                        .verify_completed_read_only_effect(&observation)
                        .map_err(|completed_reason| {
                            OfficialCodexAppServerError::UncertainToolEffect {
                                effect_index,
                                reason: format!(
                                    "read-only effect was neither unclaimed nor durably completed: unclaimed={unclaimed_reason}; completed={completed_reason}"
                                ),
                            }
                        })?;
                let response = synthesize_reconciled_read_only_response(
                    session_directory,
                    effect_index,
                    &observation,
                )?;
                let evidence =
                    extract_command_receipt_references(session_directory, effect_index, &response)?;
                if !evidence.replayable {
                    return Err(OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: "completed read-only effect was not replayable".to_string(),
                    });
                }
                (response, evidence.references)
            }
        };
        let effect = &mut execution_ledger.effects[effect_index];
        effect.response = Some(response);
        effect.command_receipts = command_receipts;
        effect.state = CodexObservedToolEffectState::Reconciled;
        persist_execution_ledger(request_handler, execution_ledger, effect_index)?;
    }
    Ok(())
}

fn reconcile_durable_command_run_effects(
    session_directory: &Path,
    execution_ledger: &mut CodexExecutionLedger,
    request_handler: &mut Option<&mut dyn OfficialCodexServerRequestHandler>,
) -> Result<(), OfficialCodexAppServerError> {
    for effect_index in 0..execution_ledger.effects.len() {
        let (runtime_id, params, persisted_observation) = {
            let effect = &execution_ledger.effects[effect_index];
            if effect.state != CodexObservedToolEffectState::Observed
                || effect.response.is_some()
                || !effect.command_receipts.is_empty()
                || effect.replay_request_id.is_some()
            {
                continue;
            }
            let Some(params) = effect.request_params.as_ref() else {
                continue;
            };
            if params.get("tool").and_then(Value::as_str) != Some("command_run") {
                continue;
            }
            let runtime_id = match effect.runtime_id.as_deref() {
                Some(runtime_id)
                    if execution_ledger
                        .runtime_ids
                        .iter()
                        .any(|candidate| candidate == runtime_id) =>
                {
                    runtime_id.to_string()
                }
                Some(_) => {
                    return Err(OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: "effect runtime identity is not owned by its execution ledger"
                            .to_string(),
                    });
                }
                None if execution_ledger.runtime_ids.len() == 1 => {
                    execution_ledger.runtime_ids[0].clone()
                }
                None => {
                    return Err(OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: "legacy effect runtime identity is ambiguous".to_string(),
                    });
                }
            };
            (
                runtime_id,
                params.clone(),
                effect.command_run_observation.clone(),
            )
        };

        let observation = match persisted_observation {
            Some(observation) => observation,
            None => request_handler
                .as_deref_mut()
                .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: "runtime command identity normalizer is unavailable".to_string(),
                })?
                .observe_command_run_effect(
                    &OfficialCodexServerRequest {
                        method: "item/tool/call".to_string(),
                        params: params.clone(),
                    },
                    &runtime_id,
                )
                .map_err(|reason| OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: format!("runtime command identity normalization failed: {reason}"),
                })?
                .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: "runtime did not produce a command_run identity observation"
                        .to_string(),
                })?,
        };
        validate_command_run_observation(effect_index, &observation)?;
        let raw_call_id = params
            .get("callId")
            .or_else(|| params.get("call_id"))
            .or_else(|| params.get("id"))
            .and_then(Value::as_str)
            .filter(|call_id| !call_id.is_empty())
            .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "observed command_run omitted its tool call identity".to_string(),
            })?;
        if observation.runtime_id != runtime_id || observation.tool_call_id != raw_call_id {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "command_run observation changed its effect or tool call identity"
                    .to_string(),
            });
        }

        let mut results = Vec::with_capacity(observation.commands.len());
        let mut canonical_paths = HashSet::new();
        let mut receipt_digests = HashSet::new();
        for command in &observation.commands {
            let claim_identity = &command.claim_identity;
            let receipt_path = session_directory
                .join(".tura")
                .join("run")
                .join("command_receipts")
                .join(format!(
                    "{}.json",
                    encode_command_receipt_identity(&claim_identity)
                ));
            let absolute_path = std::fs::canonicalize(&receipt_path).map_err(|error| {
                OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: format!(
                        "terminal receipt for command {claim_identity} cannot be canonicalized: {error}"
                    ),
                }
            })?;
            if !canonical_paths.insert(absolute_path.clone()) {
                return Err(OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: format!(
                        "multiple commands resolved to terminal receipt {}",
                        absolute_path.display()
                    ),
                });
            }
            let bytes = std::fs::read(&absolute_path).map_err(|error| {
                OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: format!(
                        "terminal receipt for command {claim_identity} is unavailable: {error}"
                    ),
                }
            })?;
            let receipt_sha256 = format!("{:x}", Sha256::digest(&bytes));
            if !receipt_digests.insert(receipt_sha256) {
                return Err(OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: "multiple command outcomes shared one terminal receipt digest"
                        .to_string(),
                });
            }
            let receipt: Value = serde_json::from_slice(&bytes).map_err(|error| {
                OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: format!(
                        "terminal receipt for command {claim_identity} is invalid: {error}"
                    ),
                }
            })?;
            if receipt.get("call_id").and_then(Value::as_str) != Some(claim_identity.as_str()) {
                return Err(OfficialCodexAppServerError::UncertainToolEffect {
                    effect_index,
                    reason: format!(
                        "terminal receipt identity changed for command {claim_identity}"
                    ),
                });
            }
            let command_succeeded = receipt.get("exit_code").and_then(Value::as_i64) == Some(0);
            validate_terminal_receipt(effect_index, &receipt, command_succeeded)?;
            results.push(json!({
                "command_type": command.command_type,
                "command_id": claim_identity,
                "step": command.effective_step,
                "success": command_succeeded,
                "output": {
                    "exit_code": receipt.get("exit_code").cloned().unwrap_or(Value::Null),
                    "terminal_receipt": receipt,
                    "terminal_receipt_path": absolute_path,
                    "delivery_state": "reconstructed_from_durable_terminal_receipt"
                }
            }));
        }

        let output = json!({"results": results});
        let response = json!({
            "contentItems": [{"type": "inputText", "text": output.to_string()}],
            "success": true
        });
        let evidence =
            extract_command_receipt_references(session_directory, effect_index, &response)?;
        if !evidence.replayable || evidence.references.len() != observation.commands.len() {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "durable command_run result was incomplete or unsettled".to_string(),
            });
        }
        let effect = &mut execution_ledger.effects[effect_index];
        effect.runtime_id = Some(runtime_id);
        effect.command_run_observation = Some(observation);
        effect.response = Some(response);
        effect.command_receipts = evidence.references;
        effect.state = CodexObservedToolEffectState::Reconciled;
        persist_execution_ledger(request_handler, execution_ledger, effect_index)?;
    }
    Ok(())
}

fn server_effect_identity(params: &Value, rpc_id: &Value) -> Value {
    params
        .get("callId")
        .or_else(|| params.get("call_id"))
        .or_else(|| params.get("id"))
        .cloned()
        .unwrap_or_else(|| rpc_id.clone())
}

fn server_request_semantic_sha256(
    method: &str,
    params: &Value,
) -> Result<String, OfficialCodexAppServerError> {
    let tool = params
        .get("tool")
        .or_else(|| params.get("name"))
        .or_else(|| params.get("toolName"))
        .cloned()
        .unwrap_or_else(|| Value::String("command_run".to_string()));
    let arguments = params
        .get("arguments")
        .or_else(|| params.get("input"))
        .cloned()
        .map(|value| match value {
            Value::String(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
            other => other,
        })
        .unwrap_or_else(|| json!({}));
    sha256_canonical_json(json!({
        "method": method,
        "tool": tool,
        "arguments": arguments,
    }))
}

fn interrupted_recovery_items(
    snapshot: &CodexMissionSnapshot,
    execution_ledger: &CodexExecutionLedger,
) -> Result<(Vec<Value>, Vec<Value>), OfficialCodexAppServerError> {
    let user_text = snapshot
        .turn_input
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            OfficialCodexAppServerError::InvalidResponse(
                "canonical recovery snapshot omitted its text input".to_string(),
            )
        })?;
    let mut items = vec![json!({
        "type": "message",
        "role": "user",
        "content": [{"type": "input_text", "text": user_text}],
    })];
    let mut replay_request_ids = Vec::with_capacity(execution_ledger.effects.len());
    for (effect_index, effect) in execution_ledger.effects.iter().enumerate() {
        let params = effect.request_params.as_ref().ok_or_else(|| {
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "reconciled effect omitted its canonical tool request".to_string(),
            }
        })?;
        let request_sha256 = server_request_semantic_sha256("item/tool/call", params)?;
        if request_sha256 != effect.request_sha256 {
            return Err(OfficialCodexAppServerError::ConflictingRecoveryToolEffect {
                effect_index,
                expected: effect.request_sha256.clone(),
                actual: request_sha256,
            });
        }
        let tool = params
            .get("tool")
            .or_else(|| params.get("name"))
            .or_else(|| params.get("toolName"))
            .and_then(Value::as_str)
            .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "reconciled effect omitted its tool name".to_string(),
            })?;
        let arguments = params
            .get("arguments")
            .or_else(|| params.get("input"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let arguments = match arguments {
            Value::String(text) => text,
            value => serde_json::to_string(&value)
                .map_err(OfficialCodexAppServerError::EncodeAssociation)?,
        };
        let call_id = format!(
            "tura_recovery_{effect_index}_{}",
            &effect.request_sha256[..16]
        );
        let response = effect.response.as_ref().ok_or_else(|| {
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "reconciled effect omitted its response".to_string(),
            }
        })?;
        let output = match response
            .get("contentItems")
            .and_then(Value::as_array)
            .and_then(|content_items| {
                content_items
                    .iter()
                    .find_map(|item| item.get("text").and_then(Value::as_str))
            }) {
            Some(text) => text.to_string(),
            None => serde_json::to_string(response)
                .map_err(OfficialCodexAppServerError::EncodeAssociation)?,
        };
        items.push(json!({
            "type": "function_call",
            "name": tool,
            "arguments": arguments,
            "call_id": call_id,
        }));
        items.push(json!({
            "type": "function_call_output",
            "call_id": call_id,
            "output": output,
        }));
        replay_request_ids.push(Value::String(call_id));
    }
    Ok((items, replay_request_ids))
}

struct CommandReceiptEvidence {
    references: Vec<CodexCommandReceiptReference>,
    replayable: bool,
}

fn extract_command_receipt_references(
    session_directory: &Path,
    effect_index: usize,
    result: &Value,
) -> Result<CommandReceiptEvidence, OfficialCodexAppServerError> {
    let text = result
        .get("contentItems")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().find_map(|item| {
                item.get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
            })
        })
        .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "command_run response omitted contentItems text".to_string(),
        })?;
    let output: Value = serde_json::from_str(text).map_err(|error| {
        OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: format!("command_run response was not JSON: {error}"),
        }
    })?;
    let results = output
        .get("results")
        .and_then(Value::as_array)
        .filter(|results| !results.is_empty())
        .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "command_run response contained no command results".to_string(),
        })?;
    let mut references = Vec::with_capacity(results.len());
    let replayable = true;
    let mut receipt_root = None;
    for command_result in results {
        let command_succeeded = command_result.get("success").and_then(Value::as_bool);
        let command_succeeded =
            command_succeeded.ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "command result omitted success classification".to_string(),
            })?;
        let output = command_result.get("output").unwrap_or(command_result);
        let Some(receipt) = output.get("terminal_receipt") else {
            if command_succeeded
                && command_result.get("command_type").and_then(Value::as_str) == Some("task_status")
            {
                continue;
            }
            if deterministic_zero_effect_policy_denial(command_result) {
                continue;
            }
            if deterministic_zero_effect_unclaimed_read_only(command_result) {
                continue;
            }
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: format!(
                    "command result for {} omitted terminal_receipt",
                    command_result
                        .get("command_type")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown command")
                ),
            });
        };
        validate_terminal_receipt(effect_index, receipt, command_succeeded)?;
        let receipt_root = match receipt_root.as_ref() {
            Some(path) => path,
            None => {
                let session_root = std::fs::canonicalize(session_directory).map_err(|error| {
                    OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: format!("failed to canonicalize session directory: {error}"),
                    }
                })?;
                let path = std::fs::canonicalize(
                    session_root
                        .join(".tura")
                        .join("run")
                        .join("command_receipts"),
                )
                .map_err(|error| {
                    OfficialCodexAppServerError::UncertainToolEffect {
                        effect_index,
                        reason: format!("command receipt directory is unavailable: {error}"),
                    }
                })?;
                receipt_root.insert(path)
            }
        };
        let path = output
            .get("terminal_receipt_path")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .ok_or_else(|| OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "command result omitted an absolute terminal_receipt_path".to_string(),
            })?;
        let canonical_path = std::fs::canonicalize(&path).map_err(|error| {
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: format!(
                    "terminal receipt is unavailable at {}: {error}",
                    path.display()
                ),
            }
        })?;
        if !canonical_path.starts_with(&receipt_root) {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: format!(
                    "terminal receipt {} is outside the session receipt directory",
                    canonical_path.display()
                ),
            });
        }
        let bytes = std::fs::read(&canonical_path).map_err(|error| {
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: format!("failed to read terminal receipt: {error}"),
            }
        })?;
        let durable_receipt: Value = serde_json::from_slice(&bytes).map_err(|error| {
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: format!("durable terminal receipt is invalid JSON: {error}"),
            }
        })?;
        if durable_receipt != *receipt {
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "embedded and durable terminal receipts differ".to_string(),
            });
        }
        references.push(CodexCommandReceiptReference {
            path: canonical_path,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        });
    }
    Ok(CommandReceiptEvidence {
        references,
        replayable,
    })
}

fn deterministic_zero_effect_policy_denial(command_result: &Value) -> bool {
    command_result.get("success").and_then(Value::as_bool) == Some(false)
        && command_result.get("command_type").and_then(Value::as_str) == Some("jspace")
        && command_result
            .get("jspace_error_code")
            .and_then(Value::as_str)
            .is_some_and(|code| code.starts_with("JSPACE_"))
        && command_result.get("effect_state").and_then(Value::as_str) == Some("not_started")
        && command_result.get("mutation_count").and_then(Value::as_u64) == Some(0)
        && command_result
            .get("authority_effect")
            .and_then(Value::as_str)
            == Some("none")
        && command_result.get("delivery_state").and_then(Value::as_str)
            == Some("deterministic_policy_denial")
        && command_result.get("replayable").and_then(Value::as_bool) == Some(true)
}

fn deterministic_zero_effect_unclaimed_read_only(command_result: &Value) -> bool {
    let output = command_result.get("output").unwrap_or(command_result);
    command_result.get("success").and_then(Value::as_bool) == Some(true)
        && command_result.get("command_type").and_then(Value::as_str) == Some("zsh")
        && command_result
            .get("command_id")
            .and_then(Value::as_str)
            .is_some_and(|command_id| {
                !command_id.is_empty()
                    && output.get("claim_identity").and_then(Value::as_str) == Some(command_id)
            })
        && output.get("schema_version").and_then(Value::as_str)
            == Some("official_codex_unclaimed_read_only_terminal_v1")
        && output.get("terminal_state").and_then(Value::as_str) == Some("not_started")
        && output.get("outcome").and_then(Value::as_str) == Some("known_zero_mutation")
        && output.get("claim_state").and_then(Value::as_str) == Some("absent")
        && output.get("process_state").and_then(Value::as_str) == Some("never_started")
        && output.get("mutation_count").and_then(Value::as_u64) == Some(0)
        && output
            .get("authoritative_publication")
            .and_then(Value::as_str)
            == Some("none")
        && output.get("delivery_state").and_then(Value::as_str) == Some("synthetic_replay")
}

fn validate_terminal_receipt(
    effect_index: usize,
    receipt: &Value,
    command_succeeded: bool,
) -> Result<(), OfficialCodexAppServerError> {
    let common_valid = receipt.get("schema_version").and_then(Value::as_str)
        == Some("tura_command_terminal_receipt_v1")
        && receipt.get("outcome").and_then(Value::as_str) == Some("known")
        && receipt.get("process_reaped").and_then(Value::as_bool) == Some(true)
        && receipt.get("process_group_empty").and_then(Value::as_bool) == Some(true)
        && receipt.get("termination_proven").and_then(Value::as_bool) == Some(true)
        && receipt.get("authority_effect").and_then(Value::as_str) == Some("none")
        && receipt.get("staging_authority").and_then(Value::as_str) == Some("none");
    let terminal_valid = if command_succeeded {
        receipt.get("terminal_state").and_then(Value::as_str) == Some("completed")
            && receipt.get("failure_class").and_then(Value::as_str) == Some("none")
            && receipt.get("exit_code").and_then(Value::as_i64) == Some(0)
            && receipt.get("reconcile_required").and_then(Value::as_bool) == Some(false)
    } else if receipt.get("terminal_state").and_then(Value::as_str) == Some("not_started") {
        receipt.get("failure_class").and_then(Value::as_str) == Some("pre_execution_zero_effect")
            && receipt.get("termination_origin").and_then(Value::as_str)
                == Some("command_run_pre_execution")
            && receipt.get("pid").is_some_and(Value::is_null)
            && receipt
                .get("exit_code")
                .and_then(Value::as_i64)
                .is_some_and(|exit_code| exit_code != 0)
            && receipt.get("reconcile_required").and_then(Value::as_bool) == Some(false)
            && receipt.get("retry_safe").and_then(Value::as_bool) == Some(false)
            && receipt.get("auto_retry_allowed").and_then(Value::as_bool) == Some(false)
    } else {
        receipt.get("terminal_state").and_then(Value::as_str) == Some("failed")
            && receipt.get("failure_class").and_then(Value::as_str) == Some("workload_exit_nonzero")
            && receipt
                .get("exit_code")
                .and_then(Value::as_i64)
                .is_some_and(|exit_code| exit_code != 0)
            && receipt.get("reconcile_required").and_then(Value::as_bool) == Some(true)
    };
    let valid = common_valid && terminal_valid;
    if valid {
        Ok(())
    } else {
        Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "terminal receipt does not prove a known, zero-effect, reaped command outcome"
                .to_string(),
        })
    }
}

fn validate_reconciled_tool_effect(
    session_directory: &Path,
    effect_index: usize,
    effect: &CodexObservedToolEffect,
) -> Result<(), OfficialCodexAppServerError> {
    if effect.state != CodexObservedToolEffectState::Reconciled {
        return Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "tool effect remained observed without a reconciled result".to_string(),
        });
    }
    let response = effect.response.as_ref().ok_or_else(|| {
        OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "reconciled tool effect omitted its response".to_string(),
        }
    })?;
    if let Some(observation) = effect.read_only_observation.as_ref() {
        let synthesized = synthesize_never_claimed_read_only_response(effect_index, observation)?;
        if response == &synthesized {
            if effect.command_receipts.is_empty() {
                return Ok(());
            }
            return Err(OfficialCodexAppServerError::UncertainToolEffect {
                effect_index,
                reason: "synthetic never-claimed response carried command receipts".to_string(),
            });
        }
    }
    let evidence = extract_command_receipt_references(session_directory, effect_index, response)?;
    if !evidence.replayable {
        return Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "failed command result is deliverable but not replayable".to_string(),
        });
    }
    if evidence.references != effect.command_receipts {
        return Err(OfficialCodexAppServerError::UncertainToolEffect {
            effect_index,
            reason: "durable command receipt identity changed".to_string(),
        });
    }
    Ok(())
}

async fn write_message(
    stdin: &mut tokio::process::ChildStdin,
    value: &Value,
) -> Result<(), OfficialCodexAppServerError> {
    let mut bytes =
        serde_json::to_vec(value).map_err(OfficialCodexAppServerError::EncodeAssociation)?;
    bytes.push(b'\n');
    stdin
        .write_all(&bytes)
        .await
        .map_err(OfficialCodexAppServerError::ProtocolWrite)?;
    stdin
        .flush()
        .await
        .map_err(OfficialCodexAppServerError::ProtocolWrite)
}

async fn read_message(
    lines: &mut Lines<BufReader<ChildStdout>>,
) -> Result<Value, OfficialCodexAppServerError> {
    loop {
        let line = lines
            .next_line()
            .await
            .map_err(OfficialCodexAppServerError::ProtocolRead)?
            .ok_or(OfficialCodexAppServerError::UnexpectedEof)?;
        if line.trim().is_empty() {
            continue;
        }
        return serde_json::from_str(&line)
            .map_err(|source| OfficialCodexAppServerError::InvalidJson { line, source });
    }
}

fn developer_instructions(messages: &[Value]) -> Option<String> {
    let instructions = messages
        .iter()
        .filter(|message| {
            matches!(
                message.get("role").and_then(Value::as_str),
                Some("system" | "developer")
            )
        })
        .filter_map(message_text)
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>();
    (!instructions.is_empty()).then(|| instructions.join("\n\n"))
}

fn latest_user_input(messages: &[Value]) -> Result<String, OfficialCodexAppServerError> {
    messages
        .iter()
        .rev()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        .and_then(message_text)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| {
            OfficialCodexAppServerError::InvalidResponse(
                "turn request has no textual user input".to_string(),
            )
        })
}

fn message_text(message: &Value) -> Option<String> {
    match message.get("content")? {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| {
                    let kind = part.get("type").and_then(Value::as_str)?;
                    if matches!(kind, "text" | "input_text" | "output_text") {
                        part.get("text").and_then(Value::as_str)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

fn final_agent_text(item: Option<&Value>) -> Option<String> {
    let item = item?;
    (item.get("type").and_then(Value::as_str) == Some("agentMessage"))
        .then(|| item.get("text").and_then(Value::as_str).map(str::to_string))
        .flatten()
}

fn authoritative_turn_answer(turn: &Value) -> Option<String> {
    turn.get("items")
        .and_then(Value::as_array)
        .and_then(|items| {
            items
                .iter()
                .rev()
                .find_map(|item| final_agent_text(Some(item)))
        })
}

fn ordered_turn_ids(turns: &[Value]) -> Result<Vec<String>, OfficialCodexAppServerError> {
    let mut seen = HashSet::new();
    turns
        .iter()
        .enumerate()
        .map(|(index, turn)| {
            let id = turn
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(format!(
                        "thread/read turn {index} omitted id"
                    ))
                })?
                .to_string();
            if !seen.insert(id.clone()) {
                return Err(
                    OfficialCodexAppServerError::CommanderConvergenceProofInvalid(format!(
                        "thread/read repeated turn id {id}"
                    )),
                );
            }
            Ok(id)
        })
        .collect()
}

fn canonical_thread_revision_sha256_from_ids(thread_id: &str, turn_ids: &[String]) -> String {
    let value = canonical_json(json!({
        "thread_id": thread_id,
        "ordered_turn_ids": turn_ids,
    }));
    let bytes = serde_json::to_vec(&value).expect("canonical thread revision is serializable");
    format!("{:x}", Sha256::digest(bytes))
}

pub fn canonical_thread_revision_sha256(
    thread_id: &str,
    turns: &[Value],
) -> Result<String, OfficialCodexAppServerError> {
    Ok(canonical_thread_revision_sha256_from_ids(
        thread_id,
        &ordered_turn_ids(turns)?,
    ))
}

fn is_authoritative_event(method: &str) -> bool {
    method.starts_with("item/")
        || method.starts_with("turn/")
        || method == "thread/tokenUsage/updated"
}

fn canonical_mission_snapshot(
    request: &OfficialCodexTurnRequest,
    executable_identity: &CodexAppServerExecutableIdentity,
) -> Result<CodexMissionSnapshot, OfficialCodexAppServerError> {
    let mut thread_start_params = Map::new();
    thread_start_params.insert("model".to_string(), Value::String(request.model.clone()));
    thread_start_params.insert(
        "cwd".to_string(),
        Value::String(request.session_directory.display().to_string()),
    );
    let (approval_policy, sandbox) = if request.disable_permission_restrictions {
        ("never", "danger-full-access")
    } else {
        ("on-request", "workspace-write")
    };
    thread_start_params.insert(
        "approvalPolicy".to_string(),
        Value::String(approval_policy.to_string()),
    );
    thread_start_params.insert("sandbox".to_string(), Value::String(sandbox.to_string()));
    if let Some(instructions) = developer_instructions(&request.messages) {
        thread_start_params.insert(
            "developerInstructions".to_string(),
            Value::String(instructions),
        );
    }
    if !request.dynamic_tools.is_empty() {
        thread_start_params.insert(
            "dynamicTools".to_string(),
            Value::Array(request.dynamic_tools.clone()),
        );
    }
    let thread_start_params = Value::Object(thread_start_params);
    let mut turn_input = request
        .turn_context
        .as_deref()
        .map(str::trim)
        .filter(|context| !context.is_empty())
        .map(|context| vec![json!({"type": "text", "text": context})])
        .unwrap_or_default();
    turn_input.push(json!({
        "type": "text",
        "text": latest_user_input(&request.messages)?,
    }));
    let payload = json!({
        "turaSessionId": request.tura_session_id,
        "messages": request.messages,
        "threadStart": thread_start_params,
        "turnStart": {"input": turn_input},
        "executable": {
            "identity": executable_identity,
            "prefixArgs": request.executable.prefix_args,
        },
        "permissionSemantics": {
            "allowedCommandRunCommands": request.allowed_command_run_commands,
            "disablePermissionRestrictions": request.disable_permission_restrictions,
        },
        "commanderContinuation": request.commander_continuation,
    });
    Ok(CodexMissionSnapshot {
        input_sha256: sha256_canonical_json(payload)?,
        thread_start_params,
        turn_input,
    })
}

fn sha256_canonical_json(value: Value) -> Result<String, OfficialCodexAppServerError> {
    let bytes = serde_json::to_vec(&canonical_json(value))
        .map_err(OfficialCodexAppServerError::EncodeAssociation)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        other => other,
    }
}

fn parse_usage(params: Option<&Value>) -> Option<OfficialCodexUsage> {
    let params = params?;
    let last = params.pointer("/tokenUsage/last")?;
    Some(OfficialCodexUsage {
        input_tokens: last.get("inputTokens").and_then(Value::as_u64),
        cached_input_tokens: last.get("cachedInputTokens").and_then(Value::as_u64),
        output_tokens: last.get("outputTokens").and_then(Value::as_u64),
        reasoning_output_tokens: last.get("reasoningOutputTokens").and_then(Value::as_u64),
        total_tokens: last.get("totalTokens").and_then(Value::as_u64),
        model_context_window: params
            .pointer("/tokenUsage/modelContextWindow")
            .and_then(Value::as_u64),
        monetary_cost_authority: "unknown",
    })
}

fn required_string(
    object: &Map<String, Value>,
    key: &str,
    label: &str,
) -> Result<String, OfficialCodexAppServerError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            OfficialCodexAppServerError::InvalidResponse(format!("response omitted {label}"))
        })
}

pub fn load_thread_association(
    session_directory: &Path,
    tura_session_id: &str,
) -> Result<Option<CodexThreadAssociation>, OfficialCodexAppServerError> {
    load_thread_association_scoped(session_directory, tura_session_id, tura_session_id)
}

fn load_thread_association_scoped(
    session_directory: &Path,
    tura_session_id: &str,
    association_scope_id: &str,
) -> Result<Option<CodexThreadAssociation>, OfficialCodexAppServerError> {
    let path = association_path(session_directory, association_scope_id);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if association_scope_id != tura_session_id {
                return Ok(None);
            }
            let legacy_path = session_directory.join(ASSOCIATION_FILE);
            let legacy_bytes = match std::fs::read(&legacy_path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(source) => {
                    return Err(OfficialCodexAppServerError::ReadAssociation {
                        path: legacy_path,
                        source,
                    });
                }
            };
            let legacy: CodexThreadAssociation =
                serde_json::from_slice(&legacy_bytes).map_err(|source| {
                    OfficialCodexAppServerError::DecodeAssociation {
                        path: legacy_path.clone(),
                        source,
                    }
                })?;
            if legacy.schema_version != ASSOCIATION_SCHEMA_VERSION {
                return Err(OfficialCodexAppServerError::UnsupportedAssociationVersion(
                    legacy.schema_version,
                ));
            }
            return Ok((legacy.tura_session_id == tura_session_id).then_some(legacy));
        }
        Err(source) => return Err(OfficialCodexAppServerError::ReadAssociation { path, source }),
    };
    let association: CodexThreadAssociation = serde_json::from_slice(&bytes).map_err(|source| {
        OfficialCodexAppServerError::DecodeAssociation {
            path: path.clone(),
            source,
        }
    })?;
    if association.schema_version != ASSOCIATION_SCHEMA_VERSION {
        return Err(OfficialCodexAppServerError::UnsupportedAssociationVersion(
            association.schema_version,
        ));
    }
    if association.tura_session_id != tura_session_id {
        return Err(OfficialCodexAppServerError::AssociationSessionMismatch {
            expected: tura_session_id.to_string(),
            actual: association.tura_session_id,
        });
    }
    if association_scope_id != tura_session_id
        && association.association_scope_id.as_deref() != Some(association_scope_id)
    {
        return Err(OfficialCodexAppServerError::AssociationSessionMismatch {
            expected: association_scope_id.to_string(),
            actual: association
                .association_scope_id
                .unwrap_or(association.tura_session_id),
        });
    }
    Ok(Some(association))
}

fn persist_thread_association(
    session_directory: &Path,
    association: &CodexThreadAssociation,
) -> Result<(), OfficialCodexAppServerError> {
    let path = association_path(
        session_directory,
        association
            .association_scope_id
            .as_deref()
            .unwrap_or(&association.tura_session_id),
    );
    let association_directory = path.parent().unwrap_or(session_directory);
    std::fs::create_dir_all(association_directory).map_err(|source| {
        OfficialCodexAppServerError::CreateAssociationDirectory {
            path: association_directory.to_path_buf(),
            source,
        }
    })?;
    let temporary =
        association_directory.join(format!(".{ASSOCIATION_FILE}.{}.tmp", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(association)
        .map_err(OfficialCodexAppServerError::EncodeAssociation)?;
    let result = (|| -> Result<(), std::io::Error> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        std::io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &path)?;
        std::fs::File::open(association_directory)?.sync_all()?;
        Ok(())
    })();
    if let Err(source) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(OfficialCodexAppServerError::PersistAssociation { path, source });
    }
    Ok(())
}

fn association_path(session_directory: &Path, tura_session_id: &str) -> PathBuf {
    let session_digest = format!("{:x}", Sha256::digest(tura_session_id.as_bytes()));
    session_directory
        .join(".tura")
        .join("run")
        .join(ASSOCIATION_DIRECTORY)
        .join(format!("{session_digest}.json"))
}

fn association_scope_id(request: &OfficialCodexTurnRequest) -> String {
    request
        .commander_continuation
        .as_ref()
        .map(|binding| {
            let original_runtime_id = binding
                .continuation_request_id
                .strip_prefix("callback-continuation-request-")
                .map(|digest| format!("callback-continuation-runtime-{digest}"))
                .unwrap_or_else(|| request.runtime_id.clone());
            format!(
                "{}:commander-continuation:{}",
                request.tura_session_id, original_runtime_id
            )
        })
        .unwrap_or_else(|| request.tura_session_id.clone())
}

#[cfg(test)]
mod interrupted_read_only_reconciliation_tests {
    use super::*;

    #[derive(Default)]
    struct IdentityOnlyVerifier {
        handle_calls: usize,
    }

    impl OfficialCodexServerRequestHandler for IdentityOnlyVerifier {
        fn handle<'a>(
            &'a mut self,
            _request: OfficialCodexServerRequest,
        ) -> OfficialCodexServerRequestFuture<'a> {
            self.handle_calls += 1;
            Box::pin(async { Err("unexpected command replay".to_string()) })
        }

        fn verify_never_claimed_read_only_effect(
            &mut self,
            _observation: &CodexReadOnlyEffectObservation,
        ) -> Result<(), String> {
            Err("at least one command has a durable claim".to_string())
        }

        fn verify_completed_read_only_effect(
            &mut self,
            observation: &CodexReadOnlyEffectObservation,
        ) -> Result<(), String> {
            observation
                .commands
                .iter()
                .all(|command| command.access == CodexObservedCommandAccess::ReadOnly)
                .then_some(())
                .ok_or_else(|| "non-read-only command entered reconciliation".to_string())
        }
    }

    fn read_only_command(index: usize) -> CodexReadOnlyCommandObservation {
        let execution_id = "runtime-test:call-test";
        let effective_step = 1;
        CodexReadOnlyCommandObservation {
            access: CodexObservedCommandAccess::ReadOnly,
            command_type: "zsh".to_string(),
            command_line: format!("rg -n test-{index} crates/provider"),
            enumerated_index: index,
            effective_step,
            binding_id: None,
            claim_identity: command_claim_identity(execution_id, None, effective_step, index),
        }
    }

    fn read_only_observation() -> CodexReadOnlyEffectObservation {
        CodexReadOnlyEffectObservation {
            original_runtime_id: "runtime-test".to_string(),
            tool_call_id: "call-test".to_string(),
            execution_id: "runtime-test:call-test".to_string(),
            commands: vec![read_only_command(0), read_only_command(1)],
        }
    }

    fn execution_ledger(observation: CodexReadOnlyEffectObservation) -> CodexExecutionLedger {
        CodexExecutionLedger {
            schema_version: CODEX_EXECUTION_LEDGER_SCHEMA_VERSION,
            tura_session_id: "tura-test".to_string(),
            canonical_input_sha256: "a".repeat(64),
            runtime_ids: vec!["runtime-test".to_string()],
            effects: vec![CodexObservedToolEffect {
                request_sha256: "request-sha".to_string(),
                runtime_id: Some("runtime-test".to_string()),
                request_params: None,
                original_request_id: json!("call-test"),
                state: CodexObservedToolEffectState::Observed,
                response: None,
                command_receipts: Vec::new(),
                replay_request_id: None,
                read_only_observation: Some(observation),
                command_run_observation: None,
            }],
            interrupted_recovery: None,
            terminal_status: None,
            commander_convergence_proof: None,
            commander_convergence_final_assistant: None,
        }
    }

    #[test]
    fn thread_association_does_not_serialize_legacy_execution_truth() {
        let association = CodexThreadAssociation {
            schema_version: ASSOCIATION_SCHEMA_VERSION,
            tura_session_id: "tura-test".to_string(),
            thread_id: "thread-test".to_string(),
            codex_session_id: "codex-session-test".to_string(),
            executable_identity: CodexAppServerExecutableIdentity {
                canonical_path: PathBuf::from("/tmp/codex-test"),
                version: "codex-test".to_string(),
                sha256: "test-sha".to_string(),
            },
            active_turn_id: None,
            turn_attempt: None,
            mission_snapshot: Some(CodexMissionSnapshot {
                input_sha256: "a".repeat(64),
                thread_start_params: json!({}),
                turn_input: Vec::new(),
            }),
            observed_tool_effects: execution_ledger(read_only_observation()).effects,
            interrupted_recovery: None,
            association_scope_id: None,
        };

        let value = serde_json::to_value(association).expect("serialize association");
        assert!(value.get("mission_snapshot").is_none());
        assert!(value.get("observed_tool_effects").is_none());
        assert!(value.get("interrupted_recovery").is_none());
    }

    fn completed_terminal_receipt(call_id: &str) -> Value {
        json!({
            "schema_version": "tura_command_terminal_receipt_v1",
            "call_id": call_id,
            "pid": 4242,
            "terminal_state": "completed",
            "failure_class": "none",
            "termination_origin": "workload",
            "exit_code": 0,
            "wall_time_ms": 10,
            "wall_timeout_ms": 300000,
            "stall_timeout_ms": null,
            "outcome": "known",
            "process_reaped": true,
            "process_group_empty": true,
            "termination_proven": true,
            "authority_effect": "none",
            "authoritative_publication": "unproven",
            "staging_authority": "none",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": false,
            "replay_semantics": "durable_read_only_reconstruction"
        })
    }

    fn failed_terminal_receipt(call_id: &str, reconcile_required: bool) -> Value {
        json!({
            "schema_version": "tura_command_terminal_receipt_v1",
            "call_id": call_id,
            "pid": 4242,
            "terminal_state": "failed",
            "failure_class": "workload_exit_nonzero",
            "termination_origin": "workload",
            "exit_code": 2,
            "wall_time_ms": 10,
            "wall_timeout_ms": 300000,
            "stall_timeout_ms": null,
            "outcome": "known",
            "process_reaped": true,
            "process_group_empty": true,
            "termination_proven": true,
            "authority_effect": "none",
            "authoritative_publication": "unproven",
            "staging_authority": "none",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": reconcile_required,
            "replay_semantics": "durable_failed_command_reconstruction"
        })
    }

    fn pre_execution_terminal_receipt(call_id: &str) -> Value {
        json!({
            "schema_version": "tura_command_terminal_receipt_v1",
            "call_id": call_id,
            "pid": null,
            "terminal_state": "not_started",
            "failure_class": "pre_execution_zero_effect",
            "termination_origin": "command_run_pre_execution",
            "exit_code": 126,
            "wall_time_ms": 0,
            "wall_timeout_ms": 300000,
            "stall_timeout_ms": null,
            "outcome": "known",
            "process_reaped": true,
            "process_group_empty": true,
            "termination_proven": true,
            "authority_effect": "none",
            "authoritative_publication": "unproven",
            "staging_authority": "none",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": false,
            "replay_semantics": "diagnosed_replay_only_after_no_authoritative_publication_or_idempotent_cas_proof"
        })
    }

    #[test]
    fn failed_terminal_receipt_requires_explicit_reconciliation() {
        validate_terminal_receipt(
            0,
            &failed_terminal_receipt("runtime-test:call-test", true),
            false,
        )
        .expect("known failed receipt with explicit reconciliation is deliverable");

        let error = validate_terminal_receipt(
            0,
            &failed_terminal_receipt("runtime-test:call-test", false),
            false,
        )
        .expect_err("failed receipt without reconciliation must remain fail closed");
        assert!(matches!(
            error,
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index: 0,
                ..
            }
        ));
    }

    #[test]
    fn pre_execution_zero_effect_receipt_is_accepted_without_retry_authority() {
        validate_terminal_receipt(
            0,
            &pre_execution_terminal_receipt("runtime-test:pre-execution"),
            false,
        )
        .expect("exact not-started receipt should prove a known zero effect");

        let mut ambiguous = pre_execution_terminal_receipt("runtime-test:pre-execution");
        ambiguous["pid"] = json!(4242);
        let error = validate_terminal_receipt(0, &ambiguous, false)
            .expect_err("not-started receipt with a pid must remain fail closed");
        assert!(matches!(
            error,
            OfficialCodexAppServerError::UncertainToolEffect {
                effect_index: 0,
                ..
            }
        ));
    }

    #[test]
    fn legacy_command_effect_with_multiple_runtime_candidates_fails_closed() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut execution_ledger = CodexExecutionLedger {
            schema_version: CODEX_EXECUTION_LEDGER_SCHEMA_VERSION,
            tura_session_id: "tura-test".to_string(),
            canonical_input_sha256: "a".repeat(64),
            runtime_ids: vec!["runtime-first".to_string(), "runtime-second".to_string()],
            effects: vec![CodexObservedToolEffect {
                request_sha256: "request-sha".to_string(),
                runtime_id: None,
                request_params: Some(json!({
                    "callId": "call-test",
                    "tool": "command_run",
                    "arguments": {
                        "commands": [{"command_type": "zsh", "command_line": "pwd"}]
                    }
                })),
                original_request_id: json!("call-test"),
                state: CodexObservedToolEffectState::Observed,
                response: None,
                command_receipts: Vec::new(),
                replay_request_id: None,
                read_only_observation: None,
                command_run_observation: None,
            }],
            interrupted_recovery: None,
            terminal_status: None,
            commander_convergence_proof: None,
            commander_convergence_final_assistant: None,
        };
        let mut handler = IdentityOnlyVerifier::default();
        let mut request_handler = Some(&mut handler as &mut dyn OfficialCodexServerRequestHandler);
        let error = reconcile_durable_command_run_effects(
            root.path(),
            &mut execution_ledger,
            &mut request_handler,
        )
        .expect_err("legacy multi-runtime effect must not guess its receipt owner");
        assert!(
            error
                .to_string()
                .contains("legacy effect runtime identity is ambiguous")
        );
    }

    fn receipt_directory(session_directory: &Path) -> PathBuf {
        session_directory
            .join(".tura")
            .join("run")
            .join("command_receipts")
    }

    #[test]
    fn mixed_read_only_batch_reconciles_per_command_without_reexecution() {
        let root = tempfile::tempdir().expect("tempdir");
        let observation = read_only_observation();
        let completed_command = &observation.commands[1];
        let receipt_directory = receipt_directory(root.path());
        std::fs::create_dir_all(&receipt_directory).expect("receipt directory");
        let encoded = encode_command_receipt_identity(&completed_command.claim_identity);
        std::fs::write(
            receipt_directory.join(format!("{encoded}.claim.json")),
            serde_json::to_vec(&json!({
                "schema_version": "tura_command_execution_claim_v1",
                "call_id": completed_command.claim_identity,
                "authority_effect": "none"
            }))
            .expect("claim encode"),
        )
        .expect("claim write");
        std::fs::write(
            receipt_directory.join(format!("{encoded}.json")),
            serde_json::to_vec(&completed_terminal_receipt(
                &completed_command.claim_identity,
            ))
            .expect("terminal receipt encode"),
        )
        .expect("terminal receipt write");

        let mut execution_ledger = execution_ledger(observation);
        let mut handler = IdentityOnlyVerifier::default();
        {
            let mut request_handler =
                Some(&mut handler as &mut dyn OfficialCodexServerRequestHandler);
            reconcile_never_claimed_read_only_effects(
                root.path(),
                &mut execution_ledger,
                &mut request_handler,
            )
            .expect("mixed read-only reconciliation");
        }

        assert_eq!(handler.handle_calls, 0, "recovery replayed the tool call");
        let effect = &execution_ledger.effects[0];
        assert_eq!(effect.state, CodexObservedToolEffectState::Reconciled);
        assert_eq!(effect.command_receipts.len(), 1);
        validate_reconciled_tool_effect(root.path(), 0, effect)
            .expect("mixed response must remain independently replayable");
        let response = effect.response.as_ref().expect("reconciled response");
        let text = response["contentItems"][0]["text"]
            .as_str()
            .expect("response text");
        let output: Value = serde_json::from_str(text).expect("response JSON");
        assert_eq!(
            output["results"][0]["output"]["terminal_state"],
            "not_started"
        );
        assert_eq!(
            output["results"][1]["output"]["delivery_state"],
            "reconstructed_from_durable_terminal_receipt"
        );
    }

    #[test]
    fn claimed_read_only_command_without_terminal_receipt_fails_closed() {
        let root = tempfile::tempdir().expect("tempdir");
        let observation = read_only_observation();
        let claimed_command = &observation.commands[0];
        let receipt_directory = receipt_directory(root.path());
        std::fs::create_dir_all(&receipt_directory).expect("receipt directory");
        let encoded = encode_command_receipt_identity(&claimed_command.claim_identity);
        std::fs::write(
            receipt_directory.join(format!("{encoded}.claim.json")),
            "{}",
        )
        .expect("claim write");

        let mut execution_ledger = execution_ledger(observation);
        let mut handler = IdentityOnlyVerifier::default();
        let error = {
            let mut request_handler =
                Some(&mut handler as &mut dyn OfficialCodexServerRequestHandler);
            reconcile_never_claimed_read_only_effects(
                root.path(),
                &mut execution_ledger,
                &mut request_handler,
            )
            .expect_err("claimed command without terminal receipt must stay uncertain")
        };

        assert_eq!(
            handler.handle_calls, 0,
            "uncertain recovery replayed the tool call"
        );
        assert!(
            error
                .to_string()
                .contains("was claimed without a terminal receipt")
        );
        assert_eq!(
            execution_ledger.effects[0].state,
            CodexObservedToolEffectState::Observed
        );
    }
}
