//! Router-owned `command_run` execution.
//!
//! Runtime workers orchestrate turns, but shell/tool child processes are owned
//! here so aborting a runtime worker does not orphan process-tree cleanup.

use anyhow::{Context, Result, anyhow};
use code_tools::runtime::tool::CancellationToken;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tura_path::jspace::{JSpaceAdmissionCache, JSpaceError, JSpaceMatcher};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRunRequest {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub runtime_id: Option<String>,
    pub session_directory: PathBuf,
    pub arguments: Value,
    #[serde(default)]
    pub allowed_commands: Option<BTreeSet<String>>,
    #[serde(default)]
    pub command_env: BTreeMap<String, String>,
    #[serde(default)]
    pub sandbox: bool,
    #[serde(default)]
    pub jspace_contract: Option<Value>,
}

#[cfg(test)]
const TEST_PANIC_AFTER_RUNNING_CLAIM: &str = "TURA_TEST_PANIC_AFTER_RUNNING_CLAIM";

#[cfg(test)]
#[derive(Debug, Clone)]
struct PanicCleanupGate {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl PanicCleanupGate {
    fn new() -> Self {
        Self {
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommandRunService {
    active: Arc<AtomicUsize>,
    active_by_session: Arc<Mutex<HashMap<String, usize>>>,
    cancellations: Arc<Mutex<HashMap<String, HashMap<u64, CancellationToken>>>>,
    next_cancellation_id: Arc<AtomicU64>,
    idle: Arc<tokio::sync::Notify>,
    jspace: JSpaceAdmissionCache,
    panic_cleanup_quarantine: Arc<Mutex<Vec<PendingPanicCleanup>>>,
    #[cfg(test)]
    panic_cleanup_gate: Arc<Mutex<Option<PanicCleanupGate>>>,
}

impl CommandRunService {
    pub fn new() -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            active_by_session: Arc::new(Mutex::new(HashMap::new())),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            next_cancellation_id: Arc::new(AtomicU64::new(1)),
            idle: Arc::new(tokio::sync::Notify::new()),
            jspace: JSpaceAdmissionCache::default(),
            panic_cleanup_quarantine: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            panic_cleanup_gate: Arc::new(Mutex::new(None)),
        }
    }

    #[allow(
        dead_code,
        reason = "legacy in-process callers use the request-id path"
    )]
    pub async fn execute(&self, input: Value) -> Result<Value> {
        self.execute_with_request_id(input, None).await
    }

    pub async fn execute_with_request_id(
        &self,
        input: Value,
        request_id: Option<&str>,
    ) -> Result<Value> {
        self.execute_with_reservation(input, request_id, None).await
    }

    pub(crate) fn reserve_for_session(&self, session_id: Option<&str>) -> ActiveCommandRunGuard {
        ActiveCommandRunGuard::new(
            Arc::clone(&self.active),
            Arc::clone(&self.active_by_session),
            Arc::clone(&self.cancellations),
            Arc::clone(&self.next_cancellation_id),
            Arc::clone(&self.idle),
            session_id,
        )
    }

    pub(crate) async fn execute_with_reserved_session(
        &self,
        input: Value,
        request_id: Option<&str>,
        reservation: ActiveCommandRunGuard,
    ) -> Result<Value> {
        self.execute_with_reservation(input, request_id, Some(reservation))
            .await
    }

    async fn execute_with_reservation(
        &self,
        input: Value,
        request_id: Option<&str>,
        reservation: Option<ActiveCommandRunGuard>,
    ) -> Result<Value> {
        let request: CommandRunRequest =
            serde_json::from_value(input).context("invalid command_run router payload")?;
        let active =
            reservation.unwrap_or_else(|| self.reserve_for_session(request.session_id.as_deref()));
        if request.session_directory.as_os_str().is_empty() {
            return Err(anyhow!("command_run session_directory is required"));
        }
        let session_id = request.session_id.clone();
        let jspace_matcher = self
            .jspace
            .admit(
                session_id.as_deref().unwrap_or_default(),
                &request.session_directory,
                request.jspace_contract.as_ref(),
            )
            .map_err(|error| anyhow!(error.to_string()))?;
        if let Some(matcher) = jspace_matcher.as_deref()
            && let Err(error) = validate_jspace_arguments(matcher, &request.arguments)
        {
            return Ok(json!({
                "status": "finished",
                "owner": "router",
                "session_id": session_id,
                "runtime_id": request.runtime_id,
                "execution_id": request_id.unwrap_or("command-run-legacy"),
                "result": jspace_error_result(error),
            }));
        }
        let execution_id = request_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                request
                    .arguments
                    .get("execution_id")
                    .and_then(Value::as_str)
                    .unwrap_or("command-run-legacy")
            })
            .to_string();
        let mut arguments = request.arguments;
        if let Some(object) = arguments.as_object_mut() {
            object
                .entry("execution_id".to_string())
                .or_insert_with(|| Value::String(execution_id.clone()));
        }
        let (batch_execution_id, batch_call_ids) =
            code_tools::command_run::command_run_batch_identity(&arguments)
                .map_err(|error| anyhow!("COMMAND_RUN_BATCH_IDENTITY_INVALID:{error}"))?;
        let session_directory = request.session_directory;
        let cleanup_directory = session_directory.clone();
        #[cfg(test)]
        let worker_session_directory = session_directory.clone();
        let worker_session_id = session_id.clone();
        #[cfg(test)]
        let worker_call_ids = batch_call_ids.clone();
        let runtime_id = request.runtime_id;
        code_tools::shell_executor::begin_command_run_batch(
            &session_directory,
            &batch_execution_id,
            &batch_call_ids,
        )
        .map_err(|error| anyhow!("COMMAND_RUN_BATCH_ADMISSION_FAILED:{error}"))?;
        let panic_cleanup_quarantine = Arc::clone(&self.panic_cleanup_quarantine);
        #[allow(unused_mut)]
        let mut command_env = request.command_env;
        #[cfg(test)]
        let panic_after_running_claim =
            command_env.remove(TEST_PANIC_AFTER_RUNNING_CLAIM).is_some();
        #[cfg(test)]
        let panic_cleanup_gate = self.panic_cleanup_gate.lock().clone();
        let supervisor = tokio::spawn(async move {
            let cancellation = active.cancellation_token();
            let worker_cancellation = cancellation.clone();
            let worker = tokio::spawn(async move {
                let execution = code_tools::registry::with_command_environment(
                    command_env,
                    code_tools::command_run::execute_async_value_with_allowed_lock_scope_sandbox_and_cancellation(
                        arguments,
                        session_directory,
                        request.allowed_commands,
                        worker_session_id,
                        request.sandbox,
                        worker_cancellation,
                    ),
                );
                #[cfg(test)]
                let output = if panic_after_running_claim {
                    let execution = execution;
                    tokio::pin!(execution);
                    tokio::select! {
                        output = &mut execution => output,
                        _ = Self::wait_for_running_command_claim(
                            &worker_session_directory,
                            &worker_call_ids,
                        ) => panic!("injected command worker panic after running claim"),
                    }
                } else {
                    execution.await
                };
                #[cfg(not(test))]
                let output = execution.await;
                output
            });

            match worker.await {
                Ok(output) => {
                    code_tools::shell_executor::complete_command_run_batch(
                        &cleanup_directory,
                        &batch_execution_id,
                        &batch_call_ids,
                    )
                    .map_err(|error| anyhow!("COMMAND_RUN_BATCH_TERMINALIZATION_FAILED:{error}"))?;
                    drop(active);
                    Ok(json!({
                        "status": "finished",
                        "owner": "router",
                        "session_id": session_id,
                        "runtime_id": runtime_id,
                        "execution_id": execution_id,
                        "result": output,
                    }))
                }
                Err(worker_error) => {
                    cancellation.cancel();
                    #[cfg(test)]
                    if let Some(gate) = panic_cleanup_gate {
                        gate.entered.add_permits(1);
                        gate.release
                            .acquire()
                            .await
                            .expect("panic cleanup test gate")
                            .forget();
                    }
                    match code_tools::shell_executor::terminalize_interrupted_command_run_claims(
                        &cleanup_directory,
                        &batch_execution_id,
                        &batch_call_ids,
                    )
                    .await
                    {
                        Ok(_) => {
                            drop(active);
                            Err(anyhow!("ROUTER_COMMAND_RUN_WORKER_FAILED:{worker_error}"))
                        }
                        Err(cleanup_error) => {
                            panic_cleanup_quarantine.lock().push(PendingPanicCleanup {
                                session_directory: cleanup_directory,
                                batch_execution_id,
                                batch_call_ids,
                                active,
                            });
                            Err(anyhow!(
                                "ROUTER_COMMAND_RUN_PANIC_CLEANUP_INCOMPLETE:{worker_error}:{cleanup_error}"
                            ))
                        }
                    }
                }
            }
        });
        supervisor
            .await
            .map_err(|error| anyhow!("ROUTER_COMMAND_RUN_SUPERVISOR_FAILED:{error}"))?
    }

    pub fn active_count(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub fn active_count_for_session(&self, session_id: &str) -> usize {
        self.active_by_session
            .lock()
            .get(session_id)
            .copied()
            .unwrap_or(0)
    }

    pub async fn wait_for_session_idle(&self, session_id: &str) {
        loop {
            let notified = self.idle.notified();
            self.retry_panic_cleanups_for_session(session_id).await;
            if self.active_count_for_session(session_id) == 0 {
                return;
            }
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            }
        }
    }

    async fn retry_panic_cleanups_for_session(&self, session_id: &str) {
        let pending = {
            let mut quarantine = self.panic_cleanup_quarantine.lock();
            let mut pending = Vec::new();
            let mut index = 0;
            while index < quarantine.len() {
                if quarantine[index].active.session_id.as_deref() == Some(session_id) {
                    pending.push(quarantine.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            pending
        };
        let mut failed = Vec::new();
        for pending_cleanup in pending {
            if code_tools::shell_executor::terminalize_interrupted_command_run_claims(
                &pending_cleanup.session_directory,
                &pending_cleanup.batch_execution_id,
                &pending_cleanup.batch_call_ids,
            )
            .await
            .is_err()
            {
                failed.push(pending_cleanup);
            }
        }
        self.panic_cleanup_quarantine.lock().extend(failed);
    }

    pub fn cancel_session(&self, session_id: &str) -> usize {
        let tokens = self
            .cancellations
            .lock()
            .get(session_id)
            .map(|tokens| tokens.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for token in &tokens {
            token.cancel();
        }
        tokens.len()
    }

    #[cfg(test)]
    fn install_panic_cleanup_gate(&self) -> PanicCleanupGate {
        let gate = PanicCleanupGate::new();
        *self.panic_cleanup_gate.lock() = Some(gate.clone());
        gate
    }

    #[cfg(test)]
    fn jspace_admissions(&self) -> usize {
        self.jspace.admissions()
    }

    #[cfg(test)]
    async fn wait_for_running_command_claim(session_directory: &Path, call_ids: &[String]) {
        let directory = session_directory.join(".tura/run/command_receipts");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let running = std::fs::read_dir(&directory)
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(Result::ok)
                    .filter_map(|entry| std::fs::read(entry.path()).ok())
                    .filter_map(|raw| serde_json::from_slice::<Value>(&raw).ok())
                    .any(|claim| {
                        claim
                            .get("call_id")
                            .and_then(Value::as_str)
                            .is_some_and(|call_id| {
                                call_ids.iter().any(|expected| expected == call_id)
                            })
                            && claim.get("state").and_then(Value::as_str) == Some("running")
                    });
                if running {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("test command claim did not reach running state");
    }
}

fn validate_jspace_arguments(
    matcher: &JSpaceMatcher,
    arguments: &Value,
) -> Result<(), JSpaceError> {
    let Some(commands) = arguments.get("commands").and_then(Value::as_array) else {
        return Err(JSpaceError::new(
            "JSPACE_COMMAND_PAYLOAD_INVALID",
            "command",
            "",
            "command_run arguments must contain a commands array",
        ));
    };
    for command in commands {
        let command_type = command
            .get("command")
            .or_else(|| command.get("command_type"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let command_line = command
            .get("command_line")
            .and_then(Value::as_str)
            .unwrap_or_default();
        matcher.check_command(command_type, command_line)?;
        if code_tools::commands::canonical_command(command_type) == "apply_patch" {
            let changes = code_tools::commands::apply_patch::jspace_changes(command_line)
                .map_err(|error| JSpaceError::new("JSPACE_PATCH_MALFORMED", "modify", "", error))?;
            for (kind, path, move_path) in changes {
                let operation = match kind.as_str() {
                    "add" => "create",
                    "update" => "modify",
                    "delete" => "delete",
                    _ => {
                        return Err(JSpaceError::new(
                            "JSPACE_UNKNOWN_OPERATION",
                            "modify",
                            &path,
                            format!("unknown apply_patch change kind {kind}"),
                        ));
                    }
                };
                matcher.check_path(operation, Path::new(&path))?;
                if let Some(move_path) = move_path {
                    matcher.check_path("create", Path::new(&move_path))?;
                }
            }
        } else if let Some(workdir) = command
            .get("workdir")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            matcher.ensure_in_root(Path::new(workdir), "command")?;
        }
    }
    Ok(())
}

fn jspace_error_result(error: JSpaceError) -> Value {
    json!({
        "results": [{
            "success": false,
            "command_type": "jspace",
            "error": error.to_string(),
            "jspace_error_code": error.code(),
            "operation": error.operation(),
            "target": error.target(),
            "effect_state": "not_started",
            "mutation_count": 0,
            "authority_effect": "none",
            "delivery_state": "deterministic_policy_denial",
            "replayable": true,
        }]
    })
}

#[derive(Debug)]
pub(crate) struct ActiveCommandRunGuard {
    active: Arc<AtomicUsize>,
    active_by_session: Arc<Mutex<HashMap<String, usize>>>,
    cancellations: Arc<Mutex<HashMap<String, HashMap<u64, CancellationToken>>>>,
    cancellation_id: Option<u64>,
    cancellation: CancellationToken,
    idle: Arc<tokio::sync::Notify>,
    session_id: Option<String>,
}

#[derive(Debug)]
struct PendingPanicCleanup {
    session_directory: PathBuf,
    batch_execution_id: String,
    batch_call_ids: Vec<String>,
    active: ActiveCommandRunGuard,
}

impl ActiveCommandRunGuard {
    fn new(
        active: Arc<AtomicUsize>,
        active_by_session: Arc<Mutex<HashMap<String, usize>>>,
        cancellations: Arc<Mutex<HashMap<String, HashMap<u64, CancellationToken>>>>,
        next_cancellation_id: Arc<AtomicU64>,
        idle: Arc<tokio::sync::Notify>,
        session_id: Option<&str>,
    ) -> Self {
        active.fetch_add(1, Ordering::SeqCst);
        let session_id = session_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let cancellation = CancellationToken::new();
        let cancellation_id = session_id.as_ref().map(|session_id| {
            *active_by_session
                .lock()
                .entry(session_id.clone())
                .or_insert(0) += 1;
            let cancellation_id = next_cancellation_id.fetch_add(1, Ordering::SeqCst);
            cancellations
                .lock()
                .entry(session_id.clone())
                .or_default()
                .insert(cancellation_id, cancellation.clone());
            cancellation_id
        });
        Self {
            active,
            active_by_session,
            cancellations,
            cancellation_id,
            cancellation,
            idle,
            session_id,
        }
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl Drop for ActiveCommandRunGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        if let Some(session_id) = self.session_id.as_ref() {
            let mut active = self.active_by_session.lock();
            if let Some(count) = active.get_mut(session_id) {
                *count -= 1;
                if *count == 0 {
                    active.remove(session_id);
                }
            }
            drop(active);
            if let Some(cancellation_id) = self.cancellation_id {
                let mut cancellations = self.cancellations.lock();
                if let Some(tokens) = cancellations.get_mut(session_id) {
                    tokens.remove(&cancellation_id);
                    if tokens.is_empty() {
                        cancellations.remove(session_id);
                    }
                }
            }
        }
        self.idle.notify_waiters();
    }
}

impl Default for CommandRunService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandRunRequest, CommandRunService};
    use serde_json::{Value, json};
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::time::{Duration, Instant};
    use tura_path::jspace::{authorization_semantic_sha256, semantic_sha256};

    const ACTIVE_FIXTURE_DELAY_MS: u64 = 1200;
    const CONCURRENT_FIXTURE_DELAY_MS: u64 = 3000;
    const READ_ONLY_FIXTURE_TIMEOUT_MS: u64 = 30000;

    fn jspace_contract(root: &Path) -> Value {
        let mut contract = json!({
            "schema_version": "jspace_contract_v2",
            "repo_root": root,
            "dcf_generation": {
                "repo_root": root,
                "generation_id": "generation-test",
                "required_domain_bindings": {
                    "surface-map": {
                        "required_domains": ["surface"],
                        "source_fingerprints": {"surface": "surface-a"}
                    }
                }
            },
            "provenance": {"matched_surface_ids": ["surface-test"]},
            "matched_surface_ids": ["surface-test"],
            "read_scopes": ["src/**"],
            "write_scopes": ["src/**"],
            "allowed_operations": ["read", "create", "modify", "command"],
            "denied_operations": ["network", "install", "system_mutation"],
            "command_templates": [{
                "argv": ["git", "status", "--short"],
                "effects": ["read"],
                "targets": []
            }],
            "focused_verifiers": [],
            "declared_targets": ["src/main.rs"],
            "expansion": {
                "mode": "exact_target_only",
                "error_code": "JSPACE_EXPANSION_REQUIRED",
                "mutation_on_expansion": false
            }
        });
        contract["authorization_semantic_sha256"] =
            Value::String(authorization_semantic_sha256(&contract).expect("authorization digest"));
        contract["content_sha256"] = Value::String(semantic_sha256(&contract));
        contract
    }

    fn reseal_jspace(mut contract: Value) -> Value {
        contract
            .as_object_mut()
            .expect("contract object")
            .remove("content_sha256");
        contract["authorization_semantic_sha256"] =
            Value::String(authorization_semantic_sha256(&contract).expect("authorization digest"));
        contract["content_sha256"] = Value::String(semantic_sha256(&contract));
        contract
    }

    #[tokio::test]
    async fn command_run_service_executes_inside_requested_workspace() {
        let workspace = tempfile::tempdir().expect("workspace");
        let command_line = json!({
            "status": "done",
            "task_group": "订单清结算微服务"
        })
        .to_string();
        let response = CommandRunService::new()
            .execute(json!({
                "session_id": "session-1",
                "runtime_id": "runtime-1",
                "session_directory": workspace.path().display().to_string(),
                "arguments": {
                    "commands": [{
                        "command": "task_status",
                        "command_line": command_line
                    }]
                },
                "allowed_commands": ["task_status"]
            }))
            .await
            .expect("router command_run should execute");

        assert_eq!(response["owner"], "router");
        assert_eq!(response["session_id"], "session-1");
        assert_eq!(response["runtime_id"], "runtime-1");
        assert_eq!(
            response["result"]["results"][0]["command_type"],
            "task_status"
        );
        assert_eq!(response["result"]["results"][0]["success"], true);
        assert_eq!(CommandRunService::new().active_count(), 0);
    }

    #[tokio::test]
    async fn command_run_service_tracks_active_requests() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        assert_eq!(service.active_count(), 0);

        let request = json!({
            "session_id": "session-active",
            "runtime_id": "runtime-active",
            "session_directory": workspace.path().display().to_string(),
            "arguments": {
                "commands": [{
                    "command": "shell_command",
                    "command_line": json!({
                        "command": delayed_read_only_command("active", ACTIVE_FIXTURE_DELAY_MS),
                        "timeout_ms": READ_ONLY_FIXTURE_TIMEOUT_MS
                    }).to_string()
                }]
            }
        });
        let running = {
            let service = service.clone();
            tokio::spawn(async move { service.execute(request).await })
        };

        let started = Instant::now();
        while service.active_count() == 0 && started.elapsed().as_secs() < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(service.active_count(), 1);
        running
            .await
            .expect("command_run task should join")
            .expect("command_run should finish");
        assert_eq!(service.active_count(), 0);
    }

    #[tokio::test]
    async fn command_run_service_cancels_only_the_exact_session() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        let request = |session_id: &str, label: &str| {
            json!({
                "session_id": session_id,
                "runtime_id": format!("runtime-{session_id}"),
                "session_directory": workspace.path().display().to_string(),
                "arguments": {
                    "commands": [{
                        "command": "shell_command",
                        "command_line": json!({
                            "command": delayed_read_only_command(label, 5_000),
                            "timeout_ms": READ_ONLY_FIXTURE_TIMEOUT_MS
                        }).to_string()
                    }]
                }
            })
        };
        let first = {
            let service = service.clone();
            let request = request("cancel-first", "first");
            tokio::spawn(async move {
                service
                    .execute_with_request_id(request, Some("cancel-first-execution"))
                    .await
            })
        };
        let second = {
            let service = service.clone();
            let request = request("keep-second", "second");
            tokio::spawn(async move {
                service
                    .execute_with_request_id(request, Some("keep-second-execution"))
                    .await
            })
        };

        let started = Instant::now();
        while (service.active_count_for_session("cancel-first") == 0
            || service.active_count_for_session("keep-second") == 0)
            && started.elapsed() < Duration::from_secs(2)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(service.cancel_session("cancel-first"), 1);
        tokio::time::timeout(
            Duration::from_secs(2),
            service.wait_for_session_idle("cancel-first"),
        )
        .await
        .expect("cancelled session should drain promptly");
        assert_eq!(service.active_count_for_session("cancel-first"), 0);
        assert_eq!(service.active_count_for_session("keep-second"), 1);

        assert_eq!(service.cancel_session("keep-second"), 1);
        first
            .await
            .expect("first command task should join")
            .expect("first command response should remain deterministic");
        second
            .await
            .expect("second command task should join")
            .expect("second command response should remain deterministic");
        assert_eq!(service.active_count(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn outer_abort_preserves_router_ownership_until_seven_command_batch_terminalizes() {
        let workspace = tempfile::tempdir().expect("workspace");
        let receipt_directory = workspace.path().join(".tura/run/command_receipts");
        let first_gate = workspace.path().join("first-step.fifo");
        let cancellation_gate = workspace.path().join("cancel-step.fifo");
        for gate in [&first_gate, &cancellation_gate] {
            let status = std::process::Command::new("mkfifo")
                .arg(gate)
                .status()
                .expect("create FIFO");
            assert!(status.success(), "mkfifo failed for {}", gate.display());
        }
        let shell_item = |command: String, step: u64| {
            json!({
                "command": "shell_command",
                "command_line": json!({
                    "command": command,
                    "timeout_ms": 10_000
                }).to_string(),
                "step": step
            })
        };
        let mut commands = (0..4)
            .map(|index| shell_item(format!("printf 'ok-{index}\\n'"), 1))
            .collect::<Vec<_>>();
        commands.push(shell_item(format!("cat {}", first_gate.display()), 1));
        commands.push(shell_item("/bin/sh -c 'exit 2'".to_string(), 2));
        commands.push(shell_item(
            format!("cat {}", cancellation_gate.display()),
            3,
        ));

        let service = CommandRunService::new();
        let request = json!({
            "session_id": "outer-abort-session",
            "runtime_id": "outer-abort-runtime",
            "session_directory": workspace.path(),
            "arguments": {"commands": commands}
        });
        let outer = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .execute_with_request_id(request, Some("outer-abort-seven"))
                    .await
            })
        };

        wait_for_command_record_count(&receipt_directory, true, 5).await;
        assert!(!command_record_path(&receipt_directory, "outer-abort-seven", 5, true).exists());
        assert!(!command_record_path(&receipt_directory, "outer-abort-seven", 6, true).exists());
        outer.abort();
        assert!(
            outer
                .await
                .expect_err("outer request must be aborted")
                .is_cancelled()
        );
        assert_eq!(service.active_count_for_session("outer-abort-session"), 1);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                service.wait_for_session_idle("outer-abort-session")
            )
            .await
            .is_err(),
            "idle must remain false while Router-owned work is active"
        );

        let release = std::thread::spawn({
            let first_gate = first_gate.clone();
            move || std::fs::write(first_gate, b"released\n").expect("release first step")
        });
        release.join().expect("FIFO writer");
        wait_for_command_record_count(&receipt_directory, true, 7).await;
        assert_eq!(service.cancel_session("outer-abort-session"), 1);
        tokio::time::timeout(
            Duration::from_secs(5),
            service.wait_for_session_idle("outer-abort-session"),
        )
        .await
        .expect("cancelled supervised batch must terminalize");
        wait_for_command_record_count(&receipt_directory, false, 7).await;
        assert_eq!(service.active_count_for_session("outer-abort-session"), 0);

        for index in 0..7 {
            let claim = read_command_record(&receipt_directory, "outer-abort-seven", index, true);
            let receipt =
                read_command_record(&receipt_directory, "outer-abort-seven", index, false);
            assert_eq!(claim["execution_count"], 1, "{claim}");
            assert_eq!(
                claim["state"], receipt["terminal_state"],
                "{claim} {receipt}"
            );
            match index {
                0..=4 => {
                    assert_eq!(receipt["outcome"], "known", "{receipt}");
                    assert_eq!(receipt["terminal_state"], "completed", "{receipt}");
                    assert_eq!(receipt["exit_code"], 0, "{receipt}");
                }
                5 => {
                    assert_eq!(receipt["outcome"], "known", "{receipt}");
                    assert_eq!(receipt["terminal_state"], "failed", "{receipt}");
                    assert_eq!(receipt["exit_code"], 2, "{receipt}");
                }
                6 => {
                    assert!(
                        matches!(
                            receipt["terminal_state"].as_str(),
                            Some("cancelled" | "terminated" | "not_started")
                        ),
                        "{receipt}"
                    );
                    assert_eq!(receipt["outcome"], "unknown", "{receipt}");
                    assert_eq!(receipt["reconcile_required"], true, "{receipt}");
                }
                _ => unreachable!(),
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_panic_reaps_claimed_process_before_releasing_router_guard() {
        let workspace = tempfile::tempdir().expect("workspace");
        let receipt_directory = workspace.path().join(".tura/run/command_receipts");
        let late_effect = workspace.path().join("late-effect.txt");
        let service = CommandRunService::new();
        let cleanup_gate = service.install_panic_cleanup_gate();
        let request = json!({
            "session_id": "panic-cleanup-session",
            "runtime_id": "panic-cleanup-runtime",
            "session_directory": workspace.path(),
            "command_env": {super::TEST_PANIC_AFTER_RUNNING_CLAIM: "1"},
            "arguments": {
                "commands": [{
                    "command": "shell_command",
                    "id": "panic-cleanup",
                    "command_line": json!({
                        "command": format!(
                            "sleep 1; printf late > '{}'",
                            late_effect.display()
                        ),
                        "timeout_ms": 10_000
                    }).to_string(),
                    "step": 1
                }]
            }
        });
        let running = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .execute_with_request_id(request, Some("panic-cleanup"))
                    .await
            })
        };

        tokio::time::timeout(Duration::from_secs(5), cleanup_gate.entered.acquire())
            .await
            .expect("panic cleanup must reach the deterministic gate")
            .expect("panic cleanup gate")
            .forget();
        let claim: Value = serde_json::from_slice(
            &std::fs::read(receipt_directory.join("panic-cleanup.claim.json"))
                .expect("explicit-id claim"),
        )
        .expect("explicit-id claim JSON");
        let pid = claim["pid"].as_u64().expect("spawned command pid") as u32;
        assert_eq!(claim["state"], "running", "{claim}");
        assert_eq!(service.active_count_for_session("panic-cleanup-session"), 1);
        assert_eq!(service.cancel_session("panic-cleanup-session"), 1);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                service.wait_for_session_idle("panic-cleanup-session")
            )
            .await
            .is_err(),
            "Router must remain active while panic cleanup is pending"
        );
        assert!(!receipt_directory.join("panic-cleanup.json").exists());

        cleanup_gate.release.add_permits(1);
        let error = running
            .await
            .expect("outer Router task")
            .expect_err("worker panic must remain a Router error");
        assert!(
            error
                .to_string()
                .contains("ROUTER_COMMAND_RUN_WORKER_FAILED"),
            "{error:#}"
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            service.wait_for_session_idle("panic-cleanup-session"),
        )
        .await
        .expect("Router may become idle only after panic cleanup");
        assert_eq!(service.active_count_for_session("panic-cleanup-session"), 0);

        let claim: Value = serde_json::from_slice(
            &std::fs::read(receipt_directory.join("panic-cleanup.claim.json"))
                .expect("terminal explicit-id claim"),
        )
        .expect("terminal explicit-id claim JSON");
        let receipt: Value = serde_json::from_slice(
            &std::fs::read(receipt_directory.join("panic-cleanup.json"))
                .expect("explicit-id receipt"),
        )
        .expect("explicit-id receipt JSON");
        assert_eq!(claim["execution_count"], 1, "{claim}");
        assert_eq!(claim["state"], "interrupted", "{claim}");
        assert_eq!(receipt["terminal_state"], "interrupted", "{receipt}");
        assert_eq!(receipt["failure_class"], "worker_panic", "{receipt}");
        assert_eq!(receipt["outcome"], "unknown", "{receipt}");
        assert_eq!(receipt["retry_safe"], false, "{receipt}");
        assert_eq!(receipt["reconcile_required"], true, "{receipt}");
        assert_eq!(receipt["process_reaped"], true, "{receipt}");
        assert_eq!(receipt["process_group_empty"], true, "{receipt}");
        assert!(
            !std::process::Command::new("/bin/kill")
                .arg("-0")
                .arg(pid.to_string())
                .status()
                .expect("probe command pid")
                .success(),
            "panic-cleaned command pid {pid} must not remain alive"
        );
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert!(
            !late_effect.exists(),
            "command effect continued after panic cleanup"
        );
    }

    #[tokio::test]
    async fn malformed_request_has_zero_router_admission_and_zero_command_effect() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();

        service
            .execute_with_request_id(
                json!({
                    "session_id": 42,
                    "session_directory": workspace.path(),
                    "arguments": {"commands": []}
                }),
                Some("zero-admission"),
            )
            .await
            .expect_err("malformed payload must fail before Router admission");

        assert_eq!(service.active_count(), 0);
        assert!(!workspace.path().join(".tura/run/command_receipts").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn same_execution_recovery_does_not_duplicate_command_effect() {
        let workspace = tempfile::tempdir().expect("workspace");
        let marker = workspace.path().join("effect.txt");
        let service = CommandRunService::new();
        let request = json!({
            "session_id": "same-execution-session",
            "session_directory": workspace.path(),
            "arguments": {
                "commands": [{
                    "command": "shell_command",
                    "command_line": json!({
                        "command": format!("printf x >> {}", marker.display()),
                        "timeout_ms": 3_000
                    }).to_string(),
                    "step": 1
                }]
            }
        });

        let first = service
            .execute_with_request_id(request.clone(), Some("same-execution"))
            .await
            .expect("first execution");
        let recovery = service
            .execute_with_request_id(request, Some("same-execution"))
            .await
            .expect("recovery must return a fail-closed command result");

        assert_eq!(first["result"]["results"][0]["success"], true, "{first}");
        assert_eq!(
            recovery["result"]["results"][0]["success"], false,
            "{recovery}"
        );
        assert_eq!(std::fs::read_to_string(&marker).expect("marker"), "x");
        let claim = read_command_record(
            &workspace.path().join(".tura/run/command_receipts"),
            "same-execution",
            0,
            true,
        );
        assert_eq!(claim["execution_count"], 1, "{claim}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn out_of_order_completion_returns_deterministic_command_order() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        let commands = [300, 0, 150]
            .into_iter()
            .enumerate()
            .map(|(index, delay_ms)| {
                json!({
                    "command": "shell_command",
                    "command_line": json!({
                        "command": delayed_read_only_command(&format!("ordered-{index}"), delay_ms),
                        "timeout_ms": 3_000
                    }).to_string(),
                    "step": 1
                })
            })
            .collect::<Vec<_>>();

        let response = service
            .execute_with_request_id(
                json!({
                    "session_id": "ordered-session",
                    "session_directory": workspace.path(),
                    "arguments": {"commands": commands}
                }),
                Some("ordered-execution"),
            )
            .await
            .expect("ordered execution");
        let results = response["result"]["results"]
            .as_array()
            .expect("command results");
        assert_eq!(results.len(), 3, "{response}");
        for (index, result) in results.iter().enumerate() {
            assert_eq!(
                result["output"]["terminal_receipt"]["call_id"],
                format!("ordered-execution:step:1:index:{index}"),
                "{response}"
            );
            assert!(
                result["output"]["stdout"]
                    .as_str()
                    .is_some_and(|stdout| stdout.contains(&format!("ordered-{index}"))),
                "{response}"
            );
        }
    }

    #[tokio::test]
    async fn reserved_session_decode_failure_releases_command_admission() {
        let service = CommandRunService::new();
        let reservation = service.reserve_for_session(Some("reserved-session"));
        assert_eq!(service.active_count_for_session("reserved-session"), 1);

        service
            .execute_with_reserved_session(json!({"invalid": true}), None, reservation)
            .await
            .expect_err("invalid reserved command payload must fail");

        assert_eq!(service.active_count(), 0);
        assert_eq!(service.active_count_for_session("reserved-session"), 0);
    }

    #[test]
    fn command_run_payload_deserializes_allowed_commands_as_set() {
        let request: CommandRunRequest = serde_json::from_value(json!({
            "session_directory": ".",
            "arguments": { "commands": [] },
            "allowed_commands": ["shell_command", "shell_command", "task_status"]
        }))
        .expect("payload shape");

        assert_eq!(
            request.allowed_commands,
            Some(BTreeSet::from([
                "shell_command".to_string(),
                "task_status".to_string()
            ]))
        );
        assert!(request.command_env.is_empty());
        assert!(!request.sandbox);
    }

    #[test]
    fn command_run_payload_deserializes_task_scoped_command_environment() {
        let request: CommandRunRequest = serde_json::from_value(json!({
            "session_directory": ".",
            "arguments": { "commands": [] },
            "command_env": {
                "TURA_FORCED_CAPABILITY_DIRECTORIES": "[\"C:/commands/mcp\"]",
                "TURA_MCP_SERVER_NAME": "tura_filesystem"
            }
        }))
        .expect("payload shape");

        assert_eq!(
            request
                .command_env
                .get("TURA_MCP_SERVER_NAME")
                .map(String::as_str),
            Some("tura_filesystem")
        );
    }

    #[tokio::test]
    async fn command_run_service_handles_read_only_requests_concurrently() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        let request = |label: &str| {
            json!({
                "session_id": format!("session-{label}"),
                "runtime_id": format!("runtime-{label}"),
                "session_directory": workspace.path().display().to_string(),
                "arguments": {
                    "commands": [{
                        "step": 1,
                        "command": "shell_command",
                        "command_line": json!({
                            "command": delayed_read_only_command(label, CONCURRENT_FIXTURE_DELAY_MS),
                            "timeout_ms": READ_ONLY_FIXTURE_TIMEOUT_MS
                        }).to_string()
                    }]
                }
            })
        };

        let sequential_started = Instant::now();
        let seq_first = service
            .execute_with_request_id(request("seq-first"), Some("seq-first-execution"))
            .await
            .expect("sequential first command_run should finish");
        let seq_second = service
            .execute_with_request_id(request("seq-second"), Some("seq-second-execution"))
            .await
            .expect("sequential second command_run should finish");
        let sequential_elapsed = sequential_started.elapsed();
        assert_eq!(
            seq_first["result"]["results"][0]["success"], true,
            "sequential first command_run should succeed: {seq_first}"
        );
        assert_eq!(
            seq_second["result"]["results"][0]["success"], true,
            "sequential second command_run should succeed: {seq_second}"
        );

        let concurrent_started = Instant::now();
        let (first, second) = tokio::join!(
            service.execute_with_request_id(request("first"), Some("first-execution")),
            service.execute_with_request_id(request("second"), Some("second-execution"))
        );
        let concurrent_elapsed = concurrent_started.elapsed();

        let first = first.expect("first command_run should finish");
        let second = second.expect("second command_run should finish");
        assert_eq!(
            first["result"]["results"][0]["success"], true,
            "first concurrent command_run should succeed: {first}"
        );
        assert_eq!(
            second["result"]["results"][0]["success"], true,
            "second concurrent command_run should succeed: {second}"
        );
        let overlap_margin = Duration::from_millis(CONCURRENT_FIXTURE_DELAY_MS / 2);
        assert!(
            concurrent_elapsed + overlap_margin < sequential_elapsed,
            "read-only command_run requests should overlap instead of serializing; sequential_elapsed={sequential_elapsed:?}; concurrent_elapsed={concurrent_elapsed:?}"
        );
    }

    #[tokio::test]
    async fn jspace_admission_reuses_same_digest_and_rejects_changed_digest() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        let contract = jspace_contract(workspace.path());
        let request = |contract: Value| {
            json!({
                "session_id": "jspace-session",
                "runtime_id": "jspace-runtime",
                "session_directory": workspace.path().display().to_string(),
                "jspace_contract": contract,
                "arguments": {
                    "commands": [{
                        "command": "task_status",
                        "command_line": "{\"status\":\"done\"}"
                    }]
                }
            })
        };

        let first = service
            .execute(request(contract.clone()))
            .await
            .expect("first request");
        let second = service
            .execute(request(contract.clone()))
            .await
            .expect("second request");
        assert_eq!(first["result"]["results"][0]["success"], true);
        assert_eq!(second["result"]["results"][0]["success"], true);
        assert_eq!(service.jspace_admissions(), 1);

        let mut changed = contract.clone();
        changed["read_scopes"] = json!(["other/**"]);
        let changed = reseal_jspace(changed);
        let error = service
            .execute(request(changed))
            .await
            .expect_err("changed contract must be rejected");
        assert!(error.to_string().contains("JSPACE_CONTRACT_CHANGED"));
    }

    #[tokio::test]
    async fn jspace_expansion_is_reported_before_apply_patch_mutation() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        let contract = jspace_contract(workspace.path());
        let response = service
            .execute(json!({
                "session_id": "jspace-expansion-session",
                "runtime_id": "jspace-expansion-runtime",
                "session_directory": workspace.path().display().to_string(),
                "jspace_contract": contract,
                "arguments": {
                    "commands": [{
                        "command": "apply_patch",
                        "command_line": "*** Begin Patch\n*** Add File: outside.txt\n+must-not-exist\n*** End Patch"
                    }]
                }
            }))
            .await
            .expect("expansion is a command result, not an IPC failure");

        assert_eq!(
            response["result"]["results"][0]["jspace_error_code"],
            "JSPACE_EXPANSION_REQUIRED"
        );
        assert!(!workspace.path().join("outside.txt").exists());
    }

    #[tokio::test]
    async fn jspace_denies_unadmitted_exact_shell_command_without_running_it() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        let contract = jspace_contract(workspace.path());
        let marker = workspace.path().join("marker.txt");
        let response = service
            .execute(json!({
                "session_id": "jspace-command-session",
                "runtime_id": "jspace-command-runtime",
                "session_directory": workspace.path().display().to_string(),
                "jspace_contract": contract,
                "arguments": {
                    "commands": [{
                        "command": "shell_command",
                        "command_line": serde_json::to_string(&json!({
                            "command": format!("touch {}", marker.display())
                        })).expect("shell args")
                    }]
                }
            }))
            .await
            .expect("denial is a command result");

        assert_eq!(
            response["result"]["results"][0]["jspace_error_code"],
            "JSPACE_COMMAND_DENIED"
        );
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn jspace_rejects_shell_suffix_injection_before_execution() {
        let workspace = tempfile::tempdir().expect("workspace");
        let service = CommandRunService::new();
        let marker = workspace.path().join("injected.txt");
        let response = service
            .execute(json!({
                "session_id": "jspace-injection-session",
                "runtime_id": "jspace-injection-runtime",
                "session_directory": workspace.path().display().to_string(),
                "jspace_contract": jspace_contract(workspace.path()),
                "arguments": {
                    "commands": [{
                        "command": "shell_command",
                        "command_line": serde_json::to_string(&json!({
                            "command": format!(
                                "git status --short; touch {}",
                                marker.display()
                            )
                        })).expect("shell args")
                    }]
                }
            }))
            .await
            .expect("denial is a command result");

        assert_eq!(
            response["result"]["results"][0]["jspace_error_code"],
            "JSPACE_COMMAND_SYNTAX_UNSAFE"
        );
        assert!(!marker.exists());
    }

    fn delayed_read_only_command(label: &str, delay_ms: u64) -> String {
        if cfg!(windows) {
            format!("Test-Path .; Start-Sleep -Milliseconds {delay_ms}; Write-Output {label}")
        } else {
            format!(
                "find . -maxdepth 0; sleep {}.{:03}; printf {label}",
                delay_ms / 1000,
                delay_ms % 1000
            )
        }
    }

    async fn wait_for_command_record_count(directory: &Path, claims: bool, expected: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let count = std::fs::read_dir(directory)
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(Result::ok)
                    .filter(|entry| {
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        if claims {
                            name.ends_with(".claim.json")
                        } else {
                            name.ends_with(".json") && !name.ends_with(".claim.json")
                        }
                    })
                    .count();
                if count == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("command record count did not converge");
    }

    fn command_record_path(
        directory: &Path,
        execution_id: &str,
        index: usize,
        claim: bool,
    ) -> std::path::PathBuf {
        let identity = format!(
            "{execution_id}:step:{}:index:{index}",
            if index < 5 { 1 } else { index - 3 }
        );
        let encoded = identity.replace(':', "_x3a_");
        let suffix = if claim { ".claim.json" } else { ".json" };
        directory.join(format!("{encoded}{suffix}"))
    }

    fn read_command_record(
        directory: &Path,
        execution_id: &str,
        index: usize,
        claim: bool,
    ) -> Value {
        let path = command_record_path(directory, execution_id, index, claim);
        serde_json::from_slice(&std::fs::read(&path).expect("command record"))
            .expect("command record JSON")
    }
}
