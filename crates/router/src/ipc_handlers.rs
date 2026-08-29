use serde_json::{json, Value};
#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::app::AppState;
use crate::process_info::current_process_start_time;
use crate::services;
use crate::shutdown::mark_router_shutting_down;
use router_contract::{
    ExecuteCommandRequest, GetToolConfigResponse, GetToolResponse, IpcRequest, IpcResponse,
    ListCommandsRequest, ListCommandsResponse, ListToolsResponse, PatchToolConfigRequest,
    PatchToolRequest, ToolRegistryRequest, ToolRequest, METHOD_ENQUEUE_TURN,
    METHOD_EXECUTE_COMMAND, METHOD_GET_TOOL, METHOD_GET_TOOL_CONFIG, METHOD_HEALTH_CHECK,
    METHOD_LIST_COMMANDS, METHOD_LIST_TOOLS, METHOD_PATCH_TOOL, METHOD_PATCH_TOOL_CONFIG,
    METHOD_REGISTER_CHILD_SESSION,
};
use tura_router::registry::ToolRegistry;

pub(crate) async fn handle_ipc_request(state: &AppState, request: IpcRequest) -> IpcResponse {
    let result = match request.method.as_str() {
        "" | METHOD_HEALTH_CHECK
            if request.kind == "health_check" || request.method == METHOD_HEALTH_CHECK =>
        {
            crate::process_info::current_executable_sha256().map(|binary_sha256| {
                let session_db = state.session_db.start().unwrap_or_else(|error| {
                    json!({
                        "status": "error",
                        "error": error.to_string()
                    })
                });
                json!({
                    "status": "ok",
                    "pid": std::process::id(),
                    "process_start_time": current_process_start_time(std::process::id()),
                    "binary_sha256": binary_sha256,
                    "session_db": session_db,
                    "runtime_policy": {
                        "max_active_runtime_workers": services::runtime_workers::MAX_ACTIVE_RUNTIME_WORKERS,
                        "runtime_worker_idle_ttl_secs": services::runtime_workers::RUNTIME_WORKER_IDLE_TTL_SECS,
                        "max_idle_runtime_workers": services::runtime_workers::MAX_IDLE_RUNTIME_WORKERS
                    }
                })
            })
        }
        "session_db.lifecycle.start" => state.session_db.start(),
        "session_db.lifecycle.status" => Ok(state.session_db.status()),
        "session_db.lifecycle.restart" => state.session_db.restart(),
        "lifecycle.front_heartbeat" => state.lifecycle.heartbeat(&request.payload),
        "lifecycle.status" => Ok(state.lifecycle.snapshot()),
        METHOD_ENQUEUE_TURN => {
            state
                .execution
                .enqueue_turn_request(state, request.payload, &request.request_id)
                .await
        }
        METHOD_REGISTER_CHILD_SESSION => {
            state
                .execution
                .register_child_session_request(state, request.payload)
                .await
        }
        "execution.command_run" => {
            state
                .execution
                .command_run_request(state, request.payload, &request.request_id)
                .await
        }
        "execution.cancel_turn" => Ok(state.execution.cancel_turn(state, request.payload).await),
        "execution.probe_sessions" => state.execution.probe_sessions(state, request.payload).await,
        "execution.get_status" => Ok(state.execution.status(state).await),
        "execution.get_runtime_lease" => {
            state
                .execution
                .get_runtime_lease(state, request.payload)
                .await
        }
        "execution.recovery_close_runtime" => {
            state
                .execution
                .recovery_close_runtime(state, request.payload)
                .await
        }
        "session.take_user_commands" => services::user_commands::take(&request.payload),
        "execution.kill_session_workers" => Ok(state
            .execution
            .kill_session_workers(state, request.payload)
            .await),
        METHOD_LIST_COMMANDS
        | METHOD_EXECUTE_COMMAND
        | METHOD_LIST_TOOLS
        | METHOD_GET_TOOL
        | METHOD_PATCH_TOOL
        | METHOD_GET_TOOL_CONFIG
        | METHOD_PATCH_TOOL_CONFIG => {
            handle_registry_request(state, request.method.as_str(), request.payload)
        }
        "execution.shutdown" => {
            let stopped = state
                .manager
                .stop_workers_with_prefix("runtime_worker:")
                .await;
            state.session_db.shutdown();
            let background_process_scopes_terminated = mark_router_shutting_down(state);
            Ok(json!({
                "status": "shutting_down",
                "runtime_workers_stopped": stopped,
                "background_process_scopes_terminated": background_process_scopes_terminated
            }))
        }
        other => Err(anyhow::anyhow!("unknown router method: {other}")),
    };
    match result {
        Ok(payload) => IpcResponse::ok(request.request_id, payload),
        Err(error) => IpcResponse::error(request.request_id, error.to_string()),
    }
}

fn handle_registry_request(
    state: &AppState,
    method: &str,
    payload: Value,
) -> anyhow::Result<Value> {
    match method {
        METHOD_LIST_COMMANDS => {
            let request: ListCommandsRequest = decode_payload(payload)?;
            encode_payload(ListCommandsResponse {
                commands: state.registry.commands.list(request.directory.as_deref()),
            })
        }
        METHOD_EXECUTE_COMMAND => {
            let request: ExecuteCommandRequest = decode_payload(payload)?;
            encode_payload(state.registry.commands.execute(request))
        }
        METHOD_LIST_TOOLS => {
            let request: ToolRegistryRequest = decode_payload(payload)?;
            encode_payload(ListToolsResponse {
                tools: ToolRegistry::discover(request.repo_root).list(),
            })
        }
        METHOD_GET_TOOL => {
            let request: ToolRequest = decode_payload(payload)?;
            encode_payload(GetToolResponse {
                tool: ToolRegistry::discover(request.repo_root).get(&request.tool_id),
            })
        }
        METHOD_PATCH_TOOL => {
            let request: PatchToolRequest = decode_payload(payload)?;
            let tool = ToolRegistry::discover(request.repo_root)
                .patch_tool(&request.tool_id, request.patch)
                .map_err(anyhow::Error::msg)?;
            encode_payload(GetToolResponse { tool: Some(tool) })
        }
        METHOD_GET_TOOL_CONFIG => {
            let request: ToolRequest = decode_payload(payload)?;
            encode_payload(GetToolConfigResponse {
                config: ToolRegistry::discover(request.repo_root).config(&request.tool_id),
            })
        }
        METHOD_PATCH_TOOL_CONFIG => {
            let request: PatchToolConfigRequest = decode_payload(payload)?;
            let config = ToolRegistry::discover(request.repo_root)
                .patch_config(&request.tool_id, request.values)
                .map_err(anyhow::Error::msg)?;
            encode_payload(GetToolConfigResponse {
                config: Some(config),
            })
        }
        _ => unreachable!("registry method was filtered by the IPC dispatcher"),
    }
}

fn decode_payload<T: serde::de::DeserializeOwned>(payload: Value) -> anyhow::Result<T> {
    serde_json::from_value(payload)
        .map_err(|error| anyhow::anyhow!("invalid router payload: {error}"))
}

fn encode_payload(payload: impl serde::Serialize) -> anyhow::Result<Value> {
    serde_json::to_value(payload).map_err(Into::into)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnqueueTurnIdentity {
    pub(crate) commander_session_id: String,
    pub(crate) child_session_id: String,
    pub(crate) runtime_id: String,
    pub(crate) transaction_id: String,
}

pub(crate) fn enqueue_turn_identity(request: &IpcRequest) -> Option<EnqueueTurnIdentity> {
    if request.method == METHOD_REGISTER_CHILD_SESSION {
        let child: router_contract::RegisterChildSessionRequest =
            serde_json::from_value(request.payload.clone()).ok()?;
        child.validate().ok()?;
        return Some(EnqueueTurnIdentity {
            commander_session_id: child.parent_session_id,
            child_session_id: child.child_session_id,
            runtime_id: child.child_runtime_id,
            transaction_id: child.child_transaction_id,
        });
    }
    if request.method != METHOD_ENQUEUE_TURN {
        return None;
    }
    let session_id = request
        .payload
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)?;
    let runtime_id = request
        .payload
        .get("runtime_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)?;
    let commander_session_id = request
        .payload
        .pointer("/payload/parent_session_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| session_id.clone());
    Some(EnqueueTurnIdentity {
        commander_session_id,
        child_session_id: session_id,
        runtime_id,
        transaction_id: request.request_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build_state, runtime_utils::tokio_runtime};

    #[test]
    fn execution_shutdown_sets_daemon_exit_flag() -> anyhow::Result<()> {
        let endpoint_root = tempfile::tempdir()?;
        let endpoint_path = endpoint_root.path().join("router.addr");
        let state = build_state();
        let runtime = tokio_runtime()?;
        let response = crate::daemon::with_router_addr_path_for_test(&endpoint_path, || {
            runtime.block_on(handle_ipc_request(
                &state,
                IpcRequest {
                    request_id: "shutdown-test".to_string(),
                    kind: "call".to_string(),
                    method: "execution.shutdown".to_string(),
                    payload: json!({}),
                    deadline_ms: None,
                },
            ))
        });

        assert!(response.ok, "shutdown failed: {:?}", response.error);
        assert!(state.shutdown.load(Ordering::SeqCst));
        assert_eq!(response.payload["status"], "shutting_down");
        assert_eq!(response.payload["runtime_workers_stopped"], 0);
        assert!(response.payload["background_process_scopes_terminated"]
            .as_u64()
            .is_some());
        assert!(!endpoint_path.exists());
        Ok(())
    }

    #[test]
    fn terminal_forwarder_identity_tracks_turn_and_public_child_requests() {
        let turn = IpcRequest {
            request_id: "turn".to_string(),
            kind: "call".to_string(),
            method: "execution.enqueue_turn".to_string(),
            payload: json!({
                "session_id": "session-1",
                "runtime_id": "runtime-1",
                "payload": {}
            }),
            deadline_ms: None,
        };
        assert_eq!(
            enqueue_turn_identity(&turn),
            Some(EnqueueTurnIdentity {
                commander_session_id: "session-1".to_string(),
                child_session_id: "session-1".to_string(),
                runtime_id: "runtime-1".to_string(),
                transaction_id: "turn".to_string(),
            })
        );

        let command_run = IpcRequest {
            method: "execution.command_run".to_string(),
            payload: json!({ "session_id": "session-1" }),
            ..turn
        };
        assert_eq!(enqueue_turn_identity(&command_run), None);

        let delegated = IpcRequest {
            request_id: "delegated-turn".to_string(),
            method: "execution.enqueue_turn".to_string(),
            payload: json!({
                "session_id": "child-1",
                "runtime_id": "runtime-child-1",
                "payload": {"parent_session_id": "commander-1"}
            }),
            ..command_run.clone()
        };
        assert_eq!(
            enqueue_turn_identity(&delegated),
            Some(EnqueueTurnIdentity {
                commander_session_id: "commander-1".to_string(),
                child_session_id: "child-1".to_string(),
                runtime_id: "runtime-child-1".to_string(),
                transaction_id: "delegated-turn".to_string(),
            })
        );

        let public_child = IpcRequest {
            request_id: "public-child-admission".to_string(),
            method: METHOD_REGISTER_CHILD_SESSION.to_string(),
            payload: json!({
                "parent_session_id": "commander-1",
                "parent_mission_revision_sha256": "a".repeat(64),
                "child_session_id": "child-1",
                "child_runtime_id": "runtime-child-1",
                "child_transaction_id": "transaction-child-1",
                "child_lease_id": "lease-child-1",
                "callback_request_id": "transaction-child-1",
                "effect_id": "runtime-child-1.message",
                "delegated_input_sha256": "b".repeat(64),
                "session_directory": "/tmp/child-1",
                "session_name": "delegated child",
                "created_at_ms": 1_788_000_000_000_i64,
                "execution_payload": {"prompt": "delegated work"}
            }),
            ..delegated.clone()
        };
        assert_eq!(
            enqueue_turn_identity(&public_child),
            Some(EnqueueTurnIdentity {
                commander_session_id: "commander-1".to_string(),
                child_session_id: "child-1".to_string(),
                runtime_id: "runtime-child-1".to_string(),
                transaction_id: "transaction-child-1".to_string(),
            })
        );
        let mut changed_effect = public_child;
        changed_effect.payload["effect_id"] = json!("foreign.message");
        assert_eq!(enqueue_turn_identity(&changed_effect), None);

        let blank_session = IpcRequest {
            method: "execution.enqueue_turn".to_string(),
            payload: json!({ "session_id": "   " }),
            ..command_run
        };
        assert_eq!(enqueue_turn_identity(&blank_session), None);
    }

    #[test]
    fn kill_session_workers_fails_closed_without_durable_runtime() -> anyhow::Result<()> {
        let state = build_state();
        state
            .execution
            .set_session_lease_for_test("kill-session", true);

        let response = tokio_runtime()?.block_on(handle_ipc_request(
            &state,
            IpcRequest {
                request_id: "kill-session-workers-test".to_string(),
                kind: "call".to_string(),
                method: "execution.kill_session_workers".to_string(),
                payload: json!({ "session_id": "kill-session" }),
                deadline_ms: None,
            },
        ));

        assert!(response.ok, "kill failed: {:?}", response.error);
        assert_eq!(response.payload["status"], "error");
        assert_eq!(response.payload["session_id"], "kill-session");
        assert_eq!(response.payload["active_turn_removed"], false);
        assert_eq!(response.payload["runtime_terminalized"], false);
        assert_eq!(response.payload["terminalization_pending"], false);
        assert!(response.payload["terminalization_error"]
            .as_str()
            .is_some_and(|error| !error.trim().is_empty()));

        let probe = tokio_runtime()?.block_on(handle_ipc_request(
            &state,
            IpcRequest {
                request_id: "probe-after-kill".to_string(),
                kind: "call".to_string(),
                method: "execution.probe_sessions".to_string(),
                payload: json!({ "session_ids": ["kill-session"] }),
                deadline_ms: None,
            },
        ));
        assert_eq!(probe.payload["sessions"][0]["status"], "terminalizing");
        assert_eq!(probe.payload["sessions"][0]["active_turn"], false);
        Ok(())
    }

    #[test]
    fn registry_ipc_decodes_typed_requests_and_preserves_behavior() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let commands = temp.path().join(".tura").join("commands");
        std::fs::create_dir_all(&commands)?;
        std::fs::write(commands.join("audit.md"), "Audit {{args}}")?;
        let state = build_state();

        let list = tokio_runtime()?.block_on(handle_ipc_request(
            &state,
            IpcRequest::call(
                "commands-list",
                METHOD_LIST_COMMANDS,
                serde_json::to_value(ListCommandsRequest {
                    directory: Some(temp.path().display().to_string()),
                })?,
            ),
        ));
        assert!(list.ok, "command list failed: {:?}", list.error);
        let list: ListCommandsResponse = serde_json::from_value(list.payload)?;
        assert!(list.commands.iter().any(|command| command.name == "audit"));

        let execute = tokio_runtime()?.block_on(handle_ipc_request(
            &state,
            IpcRequest::call(
                "commands-execute",
                METHOD_EXECUTE_COMMAND,
                serde_json::to_value(ExecuteCommandRequest {
                    directory: Some(temp.path().display().to_string()),
                    command: "audit".to_string(),
                    args: Some(vec!["runtime".to_string()]),
                })?,
            ),
        ));
        assert!(execute.ok, "command execute failed: {:?}", execute.error);
        let execute: router_contract::ExecuteCommandResponse =
            serde_json::from_value(execute.payload)?;
        assert_eq!(execute.output, "Audit runtime");

        let malformed = tokio_runtime()?.block_on(handle_ipc_request(
            &state,
            IpcRequest::call(
                "commands-malformed",
                METHOD_LIST_COMMANDS,
                json!({ "directory": null, "legacy": true }),
            ),
        ));
        assert!(!malformed.ok);
        assert!(malformed
            .error
            .as_deref()
            .is_some_and(|error| error.contains("unknown field `legacy`")));
        Ok(())
    }
}
