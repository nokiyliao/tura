use serde_json::{Value, json};

use crate::app::AppState;
use crate::process_info::current_process_start_time;
use crate::services::managed_process::repo_root;
use crate::services::manager::{ServiceManager, WorkerAlreadyRunning};
use crate::services::models::WorkerSpec;
use crate::services::runtime_workers::runtime_worker_limit;
use runtime_contract::{CallContext, LifecycleExecutionContext, RunAgentRequest};
use tura_router::registry::{binary_target_diagnostics, resolve_binary_target};

const PROJECT_ROOT_ENV: &str = "TURA_PROJECT_ROOT";

/// Maximum recursion depth for child sub-sessions (fork-bomb guard, T5.4).
const MAX_PLANNING_DEPTH: usize = 3;
/// Pure-logic core of run_agent: resolve agent spec, spawn the runtime-
/// worker subprocess (CLI/NDJSON), forward the call, and stream the result
/// back. Gateway and child runtime dispatch use the `run-agent` CLI
/// subcommand, never router HTTP.
pub(crate) async fn dispatch_run_agent(
    state: &AppState,
    req: RunAgentRequest,
    ipc_request_id: String,
) -> (u16, Value) {
    dispatch_run_agent_inner(state, req, ipc_request_id, false).await
}

pub(crate) async fn dispatch_run_agent_with_runtime_slot(
    state: &AppState,
    req: RunAgentRequest,
    ipc_request_id: String,
) -> (u16, Value) {
    dispatch_run_agent_inner(state, req, ipc_request_id, true).await
}

async fn dispatch_run_agent_inner(
    state: &AppState,
    mut req: RunAgentRequest,
    ipc_request_id: String,
    runtime_slot_acquired: bool,
) -> (u16, Value) {
    if let Err(error) = req.validate_delegated_identity() {
        return (
            400,
            json!({
                "ok": false,
                "code": error.split(':').next().unwrap_or("DELEGATED_LIFECYCLE_IDENTITY_INVALID"),
                "error": error,
            }),
        );
    }
    let Some(session_id) = req
        .session_id
        .clone()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return (
            400,
            json!({
                "ok": false,
                "code": "LIFECYCLE_IDENTITY_MISSING",
                "error": "session_id is required; replacement Session creation is forbidden"
            }),
        );
    };
    let request_id = if ipc_request_id.trim().is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        ipc_request_id
    };
    if req.lifecycle.is_none() {
        req.lifecycle = Some(LifecycleExecutionContext {
            transaction_id: request_id.clone(),
            commander_session_id: req
                .parent_session_id
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| session_id.clone()),
            parent_mission_revision_sha256: req.parent_mission_revision_sha256.clone(),
            delegated_input_sha256: req.delegated_input_sha256.clone(),
            task_id: None,
            goal_id: None,
            operator_override: false,
        });
    }
    if router_debug_enabled() {
        eprintln!(
            "router debug: dispatch_run_agent start session_id={} agent={:?} model={:?}",
            session_id, req.agent, req.model
        );
    }

    let prompt = req
        .prompt
        .clone()
        .or_else(|| req.message.clone())
        .or_else(|| {
            req.input
                .as_ref()
                .and_then(|value| value.as_str().map(str::to_string))
        });
    let Some(prompt) = prompt.filter(|value| !value.trim().is_empty()) else {
        return (
            200,
            json!({"ok": true, "session_id": session_id, "message": "session ready; no prompt provided"}),
        );
    };

    // T5.4 recursion-depth and concurrency cap: prevent child-session fork
    // bombs; on breach, reject and report back to the parent session.
    let child_depth = req.depth.unwrap_or(0);
    if child_depth > MAX_PLANNING_DEPTH {
        return (
            429,
            json!({
                "ok": false,
                "session_id": session_id,
                "error": format!(
                    "planning depth {child_depth} exceeds limit {MAX_PLANNING_DEPTH}"
                )
            }),
        );
    }
    let runtime_worker_key = format!("runtime_worker:{session_id}");
    if state.manager.worker_alive_by_key(&runtime_worker_key).await {
        return (
            409,
            json!({
                "ok": false,
                "session_id": session_id,
                "error": format!("session {session_id} already has a running runtime")
            }),
        );
    }
    let active_workers = state.manager.count_workers_with_prefix("runtime_worker:");
    let maximum_parallel_runtime_workers =
        runtime_worker_limit(req.maximum_parallel_runtime_workers);
    if !runtime_slot_acquired && active_workers >= maximum_parallel_runtime_workers {
        return (
            429,
            json!({
                "ok": false,
                "session_id": session_id,
                "error": format!(
                    "runtime worker concurrency limit reached ({active_workers}/{maximum_parallel_runtime_workers})"
                )
            }),
        );
    }

    let request_project_root = match request_project_root(&req) {
        Ok(root) => root,
        Err(error) => {
            return (
                400,
                json!({
                    "ok": false,
                    "code": "TURA_PROJECT_ROOT_INVALID",
                    "error": error,
                    "session_id": session_id,
                }),
            );
        }
    };
    let agent_spec = match state.registry.agents.resolve_for_project(
        req.agent.as_deref(),
        req.session_type.as_deref(),
        request_project_root.as_deref(),
    ) {
        Ok(spec) => spec,
        Err(error) => {
            return (
                400,
                json!({
                    "ok": false,
                    "code": "TURA_AGENT_SELECTION_REJECTED",
                    "error": error,
                    "session_id": session_id,
                }),
            );
        }
    };

    if let Err(error) = state.session_db.start() {
        return (
            503,
            json!({
                "ok": false,
                "session_id": session_id,
                "error": format!("session_db service is not ready for runtime dispatch: {error}")
            }),
        );
    }

    let root = repo_root();
    let worker_binary = match resolve_runtime_worker_binary(&root) {
        Some(path) => path,
        None => {
            return (
                500,
                json!({
                    "ok": false,
                    "session_id": session_id,
                    "error": format!(
                        "runtime worker binary (tura_runtime) not found; checked {}",
                        binary_target_diagnostics(&root, "tura_runtime")
                    )
                }),
            );
        }
    };

    let router_pid = std::process::id();
    let mut env = vec![
        ("TURA_ROLE".to_string(), "runtime_worker".to_string()),
        ("TURA_RUNTIME_WORKER".to_string(), "1".to_string()),
        ("TURA_WORKER_MODE".to_string(), "one-shot".to_string()),
        ("TURA_RUNTIME_ONESHOT".to_string(), "1".to_string()),
        ("TURA_ROUTER_PARENT_PID".to_string(), router_pid.to_string()),
    ];
    if let Some(start_time) = current_process_start_time(router_pid) {
        env.push((
            "TURA_ROUTER_PARENT_START_TIME".to_string(),
            start_time.to_string(),
        ));
    }
    if let Some(model) = req
        .model
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        env.push(("TURA_SESSION_MODEL_OVERRIDE".to_string(), model.to_string()));
    }
    if let Some(parent) = req
        .parent_session_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        env.push(("TURA_PARENT_SESSION_ID".to_string(), parent.to_string()));
        env.push((
            "TURA_PLANNING_DEPTH".to_string(),
            req.depth.unwrap_or(1).to_string(),
        ));
    }
    // Pass through the gateway-supplied env contract (planning,
    // reasoning, stall-guard, ...) verbatim.
    for (key, value) in &req.worker_env {
        if key == PROJECT_ROOT_ENV {
            continue;
        }
        env.push((key.clone(), value.clone()));
    }
    push_router_owned_runtime_env(&mut env, request_project_root.as_deref());
    if let Ok(addr) = std::env::var("TURA_ROUTER_ADDR")
        && !addr.trim().is_empty()
    {
        env.push(("TURA_ROUTER_ADDR".to_string(), addr));
    }

    let spec = WorkerSpec {
        key: runtime_worker_key,
        service_name: "runtime_worker".to_string(),
        executable: worker_binary,
        args: Vec::new(),
        env,
    };

    let worker = match state.manager.ensure_exclusive_worker(spec).await {
        Ok(worker) => worker,
        Err(error) => {
            if error.downcast_ref::<WorkerAlreadyRunning>().is_some() {
                return (
                    409,
                    json!({
                        "ok": false,
                        "session_id": session_id,
                        "error": format!("session {session_id} already has a running runtime")
                    }),
                );
            }
            return (
                502,
                json!({
                    "ok": false,
                    "session_id": session_id,
                    "error": format!("failed to start runtime worker: {error}")
                }),
            );
        }
    };
    if router_debug_enabled() {
        eprintln!(
            "router debug: dispatch_run_agent worker ready session_id={} worker_id={}",
            session_id, worker.worker_id
        );
    }

    let call_input = runtime_worker_call_input(&req, &session_id, &agent_spec, &prompt);
    let ctx = CallContext {
        request_id,
        method: "POST".to_string(),
        path: format!("/runtime_worker/{session_id}"),
        input: call_input,
    };

    let worker_id = worker.worker_id.clone();
    let worker_cleanup = RuntimeWorkerCleanupGuard::new(state.manager.clone(), worker_id.clone());
    if router_debug_enabled() {
        eprintln!(
            "router debug: dispatch_run_agent calling worker session_id={session_id} worker_id={worker_id}"
        );
    }
    let call_result = state.manager.call_worker(&worker_id, ctx).await;
    worker_cleanup.stop_now().await;
    if router_debug_enabled() {
        eprintln!(
            "router debug: dispatch_run_agent worker returned session_id={} worker_id={} ok={}",
            session_id,
            worker_id,
            call_result.is_ok()
        );
    }
    match call_result {
        Ok(result) => {
            let ok = result.get("ok").and_then(Value::as_bool).unwrap_or(false);
            let status = if ok { 200 } else { 502 };
            (
                status,
                json!({
                    "ok": ok,
                    "session_id": session_id,
                    "worker_id": worker_id,
                    "agent": agent_spec.agent_name,
                    "result": result,
                }),
            )
        }
        Err(error) => (
            502,
            json!({
                "ok": false,
                "session_id": session_id,
                "worker_id": worker_id,
                "error": format!("runtime worker invocation failed: {error}")
            }),
        ),
    }
}

fn runtime_worker_call_input(
    req: &RunAgentRequest,
    session_id: &str,
    agent_spec: &tura_router::registry::agent::AgentSpec,
    prompt: &str,
) -> Value {
    json!({
        "runtime_id": req.runtime_id,
        "lease_id": req.lease_id,
        "fallback_from_id": req.fallback_from_id,
        "lifecycle": req.lifecycle,
        "session_id": session_id,
        "directory": req.directory,
        "model": req.model,
        "agent": agent_spec.agent_name,
        "agent_spec": agent_spec,
        "prompt": prompt,
        "runtime_context": req.runtime_context,
        "planning_mode_override": req.planning_mode_override,
        "jspace_contract": req.jspace_contract,
        "task_context_capsule": req.task_context_capsule,
        "no_op_manual": req.no_op_manual,
        "return_log": req.return_log,
    })
}

fn resolve_runtime_worker_binary(root: &std::path::Path) -> Option<std::path::PathBuf> {
    resolve_binary_target(root, "tura_runtime")
}

fn request_project_root(req: &RunAgentRequest) -> Result<Option<std::path::PathBuf>, String> {
    let Some(value) = req
        .worker_env
        .get(PROJECT_ROOT_ENV)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let path = std::path::PathBuf::from(value);
    if path.is_dir() {
        return Ok(Some(path));
    }
    Err(format!("{PROJECT_ROOT_ENV} is not a directory: {value}"))
}

fn push_router_owned_runtime_env(
    env: &mut Vec<(String, String)>,
    request_project_root: Option<&std::path::Path>,
) {
    env.push((
        "TURA_HOME".to_string(),
        tura_path::instance_home().display().to_string(),
    ));
    let project_root = request_project_root
        .map(std::path::Path::to_path_buf)
        .or_else(|| std::env::var_os(PROJECT_ROOT_ENV).map(std::path::PathBuf::from))
        .unwrap_or_else(tura_path::canonical_root);
    env.push((
        PROJECT_ROOT_ENV.to_string(),
        project_root.display().to_string(),
    ));
    for key in ["SESSION_LOG_DB_ROOT", "TURA_DB_ROOT"] {
        if let Some(value) = std::env::var_os(key).filter(|value| !value.is_empty()) {
            env.push((key.to_string(), value.to_string_lossy().to_string()));
        }
    }
}

struct RuntimeWorkerCleanupGuard {
    manager: ServiceManager,
    worker_id: String,
    active: std::sync::atomic::AtomicBool,
}

impl RuntimeWorkerCleanupGuard {
    fn new(manager: ServiceManager, worker_id: String) -> Self {
        Self {
            manager,
            worker_id,
            active: std::sync::atomic::AtomicBool::new(true),
        }
    }

    async fn stop_now(&self) {
        self.active
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.manager.stop_worker(&self.worker_id).await;
    }
}

impl Drop for RuntimeWorkerCleanupGuard {
    fn drop(&mut self) {
        if !self.active.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let manager = self.manager.clone();
        let worker_id = self.worker_id.clone();
        tokio::spawn(async move {
            manager.stop_worker(&worker_id).await;
        });
    }
}

fn router_debug_enabled() -> bool {
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
    use super::*;
    use crate::{build_state, runtime_utils::tokio_runtime};

    #[test]
    fn runtime_worker_call_input_preserves_runtime_and_lease_identity() -> anyhow::Result<()> {
        let state = build_state();
        let request: RunAgentRequest = serde_json::from_value(json!({
            "runtime_id": "runtime-lease-regression",
            "lease_id": "lease-regression",
            "fallback_from_id": "runtime-failed",
            "session_id": "session-lease-regression",
            "prompt": "exercise the runtime worker envelope",
            "jspace_contract": {
                "schema_version": "jspace_contract_v1",
                "semantic_sha256": "jspace-digest"
            },
            "task_context_capsule": {
                "schema_version": "task_context_capsule_v1",
                "semantic_sha256": "capsule-digest"
            }
        }))?;
        let agent_spec = state
            .registry
            .agents
            .resolve(Some("direct-text-only"), Some("coding"));

        let input = runtime_worker_call_input(
            &request,
            "session-lease-regression",
            &agent_spec,
            "exercise the runtime worker envelope",
        );

        assert_eq!(input["runtime_id"], "runtime-lease-regression");
        assert_eq!(input["lease_id"], "lease-regression");
        assert_eq!(input["fallback_from_id"], "runtime-failed");
        assert_eq!(input["session_id"], "session-lease-regression");
        assert_eq!(input["prompt"], "exercise the runtime worker envelope");
        assert_eq!(input["jspace_contract"]["semantic_sha256"], "jspace-digest");
        assert_eq!(
            input["task_context_capsule"]["semantic_sha256"],
            "capsule-digest"
        );
        assert_eq!(
            input["task_context_capsule"],
            request
                .task_context_capsule
                .clone()
                .expect("request capsule")
        );
        Ok(())
    }

    #[test]
    fn request_scoped_project_root_controls_agent_spec_and_worker_env() -> anyhow::Result<()> {
        let state = build_state();
        let project = tempfile::tempdir()?;
        let config = tura_agents::store::AgentConfig {
            agent_name: "direct".to_string(),
            description: Some("request-scoped direct agent".to_string()),
            aliases: vec![],
            icon_emoji: None,
            agent_directory: "agents/src/direct".into(),
            parent_agent_id: None,
            report_to_user: true,
            default_config: false,
            reflection: false,
            op_manual: false,
            self_reflection: false,
            provider: serde_json::json!({
                "current_model": "missing-route",
                "default_model_tier": "thinking",
                "tura_llm_name": "fast",
                "tool_choice": "Auto"
            }),
            agent_prompt: vec![],
            agent_capabilities: vec![],
            validator: serde_json::json!({
                "need_validator": false,
                "validator_name": null
            }),
        };
        tura_agents::store::save_dynamic_agent(project.path(), &config, None)
            .map_err(anyhow::Error::msg)?;
        let request: RunAgentRequest = serde_json::from_value(json!({
            "runtime_id": "runtime-project-root",
            "lease_id": "lease-project-root",
            "session_id": "session-project-root",
            "agent": "direct",
            "prompt": "do not reach a provider",
            "worker_env": {
                "TURA_PROJECT_ROOT": project.path().to_string_lossy()
            }
        }))?;

        let project_root = request_project_root(&request)
            .map_err(anyhow::Error::msg)?
            .expect("project root");
        let spec = state
            .registry
            .agents
            .resolve_for_project(
                request.agent.as_deref(),
                request.session_type.as_deref(),
                Some(&project_root),
            )
            .expect("request-scoped agent");
        let current_model = spec
            .config
            .as_ref()
            .and_then(|config| config.provider.get("current_model"))
            .and_then(Value::as_str);
        assert_eq!(current_model, Some("missing-route"));

        let mut env = request
            .worker_env
            .iter()
            .filter(|(key, _)| key.as_str() != PROJECT_ROOT_ENV)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        push_router_owned_runtime_env(&mut env, Some(&project_root));
        let project_roots = env
            .iter()
            .filter(|(key, _)| key == PROJECT_ROOT_ENV)
            .collect::<Vec<_>>();
        assert_eq!(project_roots.len(), 1);
        assert_eq!(project_roots[0].1, project.path().to_string_lossy());
        Ok(())
    }

    #[test]
    fn dispatch_rejects_explicit_unknown_agent_before_worker_start() -> anyhow::Result<()> {
        let state = build_state();
        let runtime = tokio_runtime()?;

        runtime.block_on(async {
            let request = serde_json::from_value(json!({
                "runtime_id": "runtime-missing-agent",
                "lease_id": "lease-missing-agent",
                "session_id": "session-missing-agent",
                "agent": "missing-executor",
                "prompt": "must fail before runtime worker start"
            }))?;

            let (status, body) =
                dispatch_run_agent(&state, request, "missing-agent-request".to_string()).await;

            assert_eq!(status, 400);
            assert_eq!(body["ok"], false);
            assert_eq!(body["code"], "TURA_AGENT_SELECTION_REJECTED");
            assert_eq!(body["session_id"], "session-missing-agent");
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("unknown explicit agent"))
            );
            assert_eq!(
                state.manager.count_workers_with_prefix("runtime_worker:"),
                0,
                "selection rejection must not start a runtime worker"
            );
            Ok::<_, anyhow::Error>(())
        })?;
        Ok(())
    }

    #[test]
    fn dispatch_run_agent_rejects_requests_over_runtime_worker_limit() -> anyhow::Result<()> {
        let state = build_state();
        let runtime = tokio_runtime()?;

        runtime.block_on(async {
            let configured_limit = 6;
            for index in 0..configured_limit {
                state
                    .manager
                    .ensure_exclusive_worker(WorkerSpec {
                        key: format!("runtime_worker:limit-fill-{index}"),
                        service_name: "runtime".to_string(),
                        executable: std::path::PathBuf::from(
                            "definitely-missing-runtime-worker-for-limit-test",
                        ),
                        args: vec!["--serve".to_string()],
                        env: vec![
                            ("TURA_WORKER_MODE".to_string(), "one-shot".to_string()),
                            ("TURA_DEBUG_RUNTIME".to_string(), "0".to_string()),
                        ],
                    })
                    .await?;
            }

            assert_eq!(
                state.manager.count_workers_with_prefix("runtime_worker:"),
                configured_limit
            );

            let request = serde_json::from_value(json!({
                "runtime_id": "runtime-over-limit",
                "lease_id": "lease-over-limit",
                "session_id": "over-limit-session",
                "maximum_parallel_runtime_workers": configured_limit,
                "prompt": "this request should be rejected before another runtime worker starts"
            }))?;
            let (status, body) =
                dispatch_run_agent(&state, request, "test-request".to_string()).await;

            assert_eq!(status, 429);
            assert_eq!(body["ok"], false);
            assert_eq!(body["session_id"], "over-limit-session");
            assert!(
                body["error"].as_str().is_some_and(
                    |error| error.contains("runtime worker concurrency limit reached (6/6)")
                ),
                "unexpected limit error body: {body}"
            );
            assert_eq!(
                state.manager.count_workers_with_prefix("runtime_worker:"),
                configured_limit,
                "rejected dispatch must not create another worker"
            );

            let stopped = state
                .manager
                .stop_workers_with_prefix("runtime_worker:")
                .await;
            assert_eq!(stopped, configured_limit);
            Ok::<_, anyhow::Error>(())
        })?;
        Ok(())
    }

    #[test]
    fn dispatch_run_agent_rejects_duplicate_running_runtime_for_session() -> anyhow::Result<()> {
        let state = build_state();
        let runtime = tokio_runtime()?;

        runtime.block_on(async {
            let handle = state
                .manager
                .ensure_exclusive_worker(WorkerSpec {
                    key: "runtime_worker:duplicate-session".to_string(),
                    service_name: "runtime_worker".to_string(),
                    executable: std::path::PathBuf::from(
                        "definitely-missing-runtime-worker-for-duplicate-session-test",
                    ),
                    args: Vec::new(),
                    env: vec![("TURA_WORKER_MODE".to_string(), "one-shot".to_string())],
                })
                .await?;
            assert_eq!(
                state.manager.count_workers_with_prefix("runtime_worker:"),
                1
            );

            let request = serde_json::from_value(json!({
                "runtime_id": "runtime-duplicate",
                "lease_id": "lease-duplicate",
                "session_id": "duplicate-session",
                "prompt": "a second runtime for this session must be rejected"
            }))?;
            let (status, body) =
                dispatch_run_agent(&state, request, "duplicate-request".to_string()).await;

            assert_eq!(status, 409);
            assert_eq!(body["ok"], false);
            assert_eq!(body["session_id"], "duplicate-session");
            assert!(
                body["error"].as_str().is_some_and(|error| error
                    .contains("session duplicate-session already has a running runtime")),
                "unexpected duplicate-session error body: {body}"
            );
            assert_eq!(
                state.manager.count_workers_with_prefix("runtime_worker:"),
                1,
                "duplicate dispatch must not create or replace the running worker"
            );
            assert!(state.manager.stop_worker(&handle.worker_id).await);
            Ok::<_, anyhow::Error>(())
        })?;
        Ok(())
    }

    #[test]
    fn runtime_worker_cleanup_guard_drop_stops_registered_worker() -> anyhow::Result<()> {
        let state = build_state();
        let runtime = tokio_runtime()?;

        runtime.block_on(async {
            let handle = state
                .manager
                .ensure_exclusive_worker(WorkerSpec {
                    key: "runtime_worker:drop-cleanup".to_string(),
                    service_name: "runtime_worker".to_string(),
                    executable: std::path::PathBuf::from(
                        "definitely-missing-runtime-worker-for-drop-cleanup",
                    ),
                    args: Vec::new(),
                    env: vec![("TURA_WORKER_MODE".to_string(), "one-shot".to_string())],
                })
                .await?;
            assert_eq!(
                state.manager.count_workers_with_prefix("runtime_worker:"),
                1
            );

            let guard =
                RuntimeWorkerCleanupGuard::new(state.manager.clone(), handle.worker_id.clone());
            drop(guard);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            assert_eq!(
                state.manager.count_workers_with_prefix("runtime_worker:"),
                0,
                "dropping the dispatch cleanup guard should remove the worker"
            );
            assert!(!state.manager.stop_worker(&handle.worker_id).await);
            Ok::<_, anyhow::Error>(())
        })?;
        Ok(())
    }
}
