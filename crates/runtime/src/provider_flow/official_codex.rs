use chrono::Utc;
use lifecycle::{RuntimeAggregate, RuntimeState};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::provider_flow::call::flush_runtime_events;
use crate::provider_flow::errors::finish_runtime_failure_with_retry_policy;
use crate::provider_flow::provider_response::apply_provider_response;
use crate::runtime_event_writer::RuntimeEventWriter;
use tura_llm_rust::official_codex_app_server::{
    run_official_codex_turn, CodexAppServerExecutable, CodexExecutionLedger,
    CodexObservedCommandAccess, CodexReadOnlyCommandObservation, CodexReadOnlyEffectObservation,
    OfficialCodexServerRequest, OfficialCodexServerRequestFuture,
    OfficialCodexServerRequestHandler, OfficialCodexTurnRequest,
};

pub(crate) struct OfficialCodexRuntimeInput {
    pub(crate) messages: Vec<Value>,
    pub(crate) turn_context: Option<String>,
    pub(crate) dynamic_tools: Vec<Value>,
    pub(crate) session_directory: PathBuf,
    pub(crate) allowed_command_run_commands: Option<BTreeSet<String>>,
    pub(crate) disable_permission_restrictions: bool,
    pub(crate) jspace_contract: Option<Value>,
}

pub(crate) async fn call_runtime_official_codex(
    runtime: &mut RuntimeAggregate,
    provider: &tura_llm_rust::ProviderConfig,
    input: OfficialCodexRuntimeInput,
    mut runtime_event_writer: Option<&mut RuntimeEventWriter>,
) -> Result<(), String> {
    let executable = resolve_codex_executable()?;
    let allowed_command_run_commands = input.allowed_command_run_commands.clone();
    let mut handler = RuntimeOfficialCodexHandler {
        session_directory: input.session_directory.clone(),
        session_id: runtime.session_id.clone(),
        runtime_id: runtime.runtime_id.clone(),
        allowed_command_run_commands: input.allowed_command_run_commands,
        disable_permission_restrictions: input.disable_permission_restrictions,
        jspace_contract: input.jspace_contract,
    };
    let request = OfficialCodexTurnRequest {
        tura_session_id: runtime.session_id.clone(),
        runtime_id: runtime.runtime_id.clone(),
        session_directory: input.session_directory,
        model: provider.model.clone(),
        messages: input.messages,
        turn_context: input.turn_context,
        executable: CodexAppServerExecutable {
            path: executable,
            prefix_args: Vec::new(),
        },
        dynamic_tools: app_server_dynamic_tools(input.dynamic_tools),
        allowed_command_run_commands,
        disable_permission_restrictions: input.disable_permission_restrictions,
    };
    let result = run_official_codex_turn(request, Some(&mut handler)).await;
    let finished_at = Utc::now();

    match result {
        Err(error) => {
            let retry_allowed = error.allows_exact_input_retry();
            let message = error.to_string();
            runtime.set_output(json!({"error": message}))?;
            flush_runtime_events(&mut runtime_event_writer, runtime)?;
            finish_runtime_failure_with_retry_policy(
                runtime,
                finished_at,
                "OFFICIAL_CODEX_APP_SERVER_FAILED",
                message,
                RuntimeState::Failed,
                retry_allowed,
            )?;
        }
        Ok(response) => {
            runtime.set_output(json!({
                "provider": "official_codex_app_server",
                "content": response.content,
                "thread_id": response.association.thread_id,
                "codex_session_id": response.association.codex_session_id,
                "executable_identity": response.association.executable_identity,
                "usage": response.usage,
                "monetary_cost_authority": "unknown",
                "authoritative_events": response.authoritative_events,
            }))?;
            apply_provider_response(runtime, &response.content, finished_at)?;
            runtime
                .mark_first_token(finished_at)
                .map_err(|error| format!("failed to mark first token: {error}"))?;
            flush_runtime_events(&mut runtime_event_writer, runtime)?;
            runtime
                .finish_success(finished_at, None)
                .map_err(|error| format!("failed to finish runtime success: {error}"))?;
        }
    }
    flush_runtime_events(&mut runtime_event_writer, runtime)?;
    Ok(())
}

struct RuntimeOfficialCodexHandler {
    session_directory: PathBuf,
    session_id: String,
    runtime_id: String,
    allowed_command_run_commands: Option<BTreeSet<String>>,
    disable_permission_restrictions: bool,
    jspace_contract: Option<Value>,
}

impl RuntimeOfficialCodexHandler {
    fn execution_ledger_path(&self, canonical_input_sha256: &str) -> Result<PathBuf, String> {
        if canonical_input_sha256.len() != 64
            || !canonical_input_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("canonical input digest is not lowercase SHA-256".to_string());
        }
        Ok(self
            .session_directory
            .join(".tura")
            .join("run")
            .join("effect_ledgers")
            .join(format!("{canonical_input_sha256}.json")))
    }

    fn write_execution_ledger(&self, ledger: &CodexExecutionLedger) -> Result<(), String> {
        if ledger.tura_session_id != self.session_id
            || !ledger.runtime_ids.iter().any(|id| id == &self.runtime_id)
        {
            return Err("execution ledger owner identity changed".to_string());
        }
        let path = self.execution_ledger_path(&ledger.canonical_input_sha256)?;
        let directory = path
            .parent()
            .ok_or_else(|| "execution ledger parent is unavailable".to_string())?;
        std::fs::create_dir_all(directory).map_err(|error| error.to_string())?;
        let temporary = directory.join(format!(
            ".{}.{}.tmp",
            ledger.canonical_input_sha256,
            uuid::Uuid::new_v4()
        ));
        let bytes = serde_json::to_vec_pretty(ledger).map_err(|error| error.to_string())?;
        let result = (|| -> Result<(), std::io::Error> {
            use std::io::Write;

            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &path)?;
            std::fs::File::open(directory)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = std::fs::remove_file(&temporary);
            return Err(error.to_string());
        }
        Ok(())
    }

    fn verify_read_only_effect_identity(
        &self,
        observation: &CodexReadOnlyEffectObservation,
    ) -> Result<(), String> {
        let expected_execution_id = format!(
            "{}:{}",
            observation.original_runtime_id, observation.tool_call_id
        );
        if observation.execution_id != expected_execution_id {
            return Err("persisted original execution identity is inconsistent".to_string());
        }
        for (enumerated_index, command) in observation.commands.iter().enumerate() {
            if command.enumerated_index != enumerated_index
                || command.access != CodexObservedCommandAccess::ReadOnly
                || !qualifies_for_synthetic_read_only_recovery(
                    &command.command_type,
                    &command.command_line,
                    &self.session_directory,
                    self.allowed_command_run_commands.as_ref(),
                )
            {
                return Err(
                    "command identity or runtime/tools access classification drifted".to_string(),
                );
            }
            let expected_claim_identity = command_call_id(
                &observation.execution_id,
                command.binding_id.as_deref(),
                command.effective_step,
                enumerated_index,
            );
            if command.claim_identity != expected_claim_identity {
                return Err("persisted exact command claim identity changed".to_string());
            }
        }
        Ok(())
    }
}

impl OfficialCodexServerRequestHandler for RuntimeOfficialCodexHandler {
    fn load_execution_ledger(
        &mut self,
        canonical_input_sha256: &str,
    ) -> Result<Option<CodexExecutionLedger>, String> {
        let path = self.execution_ledger_path(canonical_input_sha256)?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("invalid execution ledger {}: {error}", path.display()))
    }

    fn persist_execution_ledger(&mut self, ledger: &CodexExecutionLedger) -> Result<(), String> {
        self.write_execution_ledger(ledger)
    }

    fn observe_read_only_effect(
        &mut self,
        request: &OfficialCodexServerRequest,
    ) -> Result<Option<CodexReadOnlyEffectObservation>, String> {
        if request.method != "item/tool/call" {
            return Ok(None);
        }
        let tool_name = request
            .params
            .get("tool")
            .or_else(|| request.params.get("name"))
            .or_else(|| request.params.get("toolName"))
            .and_then(Value::as_str)
            .unwrap_or("command_run");
        if tool_name != "command_run" {
            return Ok(None);
        }

        let tool_call_id = request
            .params
            .get("callId")
            .or_else(|| request.params.get("call_id"))
            .or_else(|| request.params.get("id"))
            .and_then(Value::as_str)
            .filter(|tool_call_id| !tool_call_id.is_empty())
            .ok_or_else(|| "command_run tool call ID is unavailable".to_string())?
            .to_string();
        let execution_id = format!("{}:{tool_call_id}", self.runtime_id);
        let arguments = governed_dynamic_tool_arguments(&request.params, &execution_id)?;
        let Some(commands) = arguments.get("commands").and_then(Value::as_array) else {
            return Ok(None);
        };
        if commands.is_empty() {
            return Ok(None);
        }

        let mut observations = Vec::with_capacity(commands.len());
        for (enumerated_index, command) in commands.iter().enumerate() {
            let Some(command_type) = command.get("command_type").and_then(Value::as_str) else {
                return Ok(None);
            };
            let Some(command_line) = command.get("command_line").and_then(Value::as_str) else {
                return Ok(None);
            };
            if !qualifies_for_synthetic_read_only_recovery(
                command_type,
                command_line,
                &self.session_directory,
                self.allowed_command_run_commands.as_ref(),
            ) {
                return Ok(None);
            }
            let binding_present = ["id", "command_id", "commandId", "result_id"]
                .iter()
                .any(|key| command.get(*key).is_some());
            let binding_id = explicit_binding_id(command).map(str::to_string);
            if binding_present && binding_id.is_none() {
                return Ok(None);
            }
            let effective_step = (match command.get("step") {
                Some(_) => {
                    let Some(step) = u64_field(command, "step") else {
                        return Ok(None);
                    };
                    step
                }
                None => enumerated_index as u64 + 1,
            })
            .max(1);
            let claim_identity = command_call_id(
                &execution_id,
                binding_id.as_deref(),
                effective_step,
                enumerated_index,
            );
            observations.push(CodexReadOnlyCommandObservation {
                access: CodexObservedCommandAccess::ReadOnly,
                command_type: command_type.to_string(),
                command_line: command_line.to_string(),
                enumerated_index,
                effective_step,
                binding_id,
                claim_identity,
            });
        }

        Ok(Some(CodexReadOnlyEffectObservation {
            original_runtime_id: self.runtime_id.clone(),
            tool_call_id,
            execution_id,
            commands: observations,
        }))
    }

    fn verify_never_claimed_read_only_effect(
        &mut self,
        observation: &CodexReadOnlyEffectObservation,
    ) -> Result<(), String> {
        self.verify_read_only_effect_identity(observation)?;
        for command in &observation.commands {
            let encoded = safe_call_id(&command.claim_identity);
            let receipt_directory = self
                .session_directory
                .join(".tura")
                .join("run")
                .join("command_receipts");
            require_artifact_absent(
                &receipt_directory.join(format!("{encoded}.claim.json")),
                "claim",
            )?;
            require_artifact_absent(
                &receipt_directory.join(format!("{encoded}.json")),
                "terminal receipt",
            )?;
        }
        Ok(())
    }

    fn verify_completed_read_only_effect(
        &mut self,
        observation: &CodexReadOnlyEffectObservation,
    ) -> Result<(), String> {
        self.verify_read_only_effect_identity(observation)
    }

    fn handle<'a>(
        &'a mut self,
        request: OfficialCodexServerRequest,
    ) -> OfficialCodexServerRequestFuture<'a> {
        Box::pin(async move {
            if matches!(
                request.method.as_str(),
                "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
            ) {
                let decision = if self.disable_permission_restrictions {
                    "acceptForSession"
                } else {
                    "decline"
                };
                return Ok(json!({"decision": decision}));
            }
            if request.method != "item/tool/call" {
                return Err(format!("unsupported App Server request {}", request.method));
            }
            let tool_name = request
                .params
                .get("tool")
                .or_else(|| request.params.get("name"))
                .or_else(|| request.params.get("toolName"))
                .and_then(Value::as_str)
                .unwrap_or("command_run");
            if tool_name != "command_run" {
                return Err(format!("unsupported dynamic tool {tool_name}"));
            }
            let tool_call_id = request
                .params
                .get("callId")
                .or_else(|| request.params.get("call_id"))
                .or_else(|| request.params.get("id"))
                .and_then(Value::as_str)
                .ok_or_else(|| "command_run tool call ID is unavailable".to_string())?;
            let execution_id = format!("{}:{tool_call_id}", self.runtime_id);
            let arguments = governed_dynamic_tool_arguments(&request.params, &execution_id)?;
            let output = crate::router_command_run::execute_command_run_value_with_jspace(
                arguments,
                self.session_directory.clone(),
                Some(&self.session_id),
                Some(&self.runtime_id),
                self.allowed_command_run_commands.clone(),
                self.jspace_contract.clone(),
            )
            .await?;
            Ok(json!({
                "contentItems": [{"type": "inputText", "text": output.to_string()}],
                // The tool RPC completed even when an individual command returned a
                // normal non-zero exit. Keep command success in the payload so Codex
                // can handle it without misclassifying the transport as interrupted.
                "success": true,
            }))
        })
    }
}

fn dynamic_tool_arguments(params: &Value) -> Result<Value, String> {
    let arguments = params
        .get("arguments")
        .or_else(|| params.get("input"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    match arguments {
        Value::String(value) => serde_json::from_str(&value)
            .map_err(|error| format!("invalid command_run arguments: {error}")),
        Value::Object(_) => Ok(arguments),
        _ => Err("command_run arguments must be a JSON object".to_string()),
    }
}

fn governed_dynamic_tool_arguments(params: &Value, execution_id: &str) -> Result<Value, String> {
    let mut arguments = dynamic_tool_arguments(params)?;
    let Value::Object(object) = &mut arguments else {
        return Err("command_run arguments must be a JSON object".to_string());
    };
    object.insert(
        "execution_id".to_string(),
        Value::String(execution_id.to_string()),
    );
    Ok(arguments)
}

fn explicit_binding_id(command: &Value) -> Option<&str> {
    command
        .get("id")
        .or_else(|| command.get("command_id"))
        .or_else(|| command.get("commandId"))
        .or_else(|| command.get("result_id"))
        .and_then(Value::as_str)
}

fn u64_field(value: &Value, field: &str) -> Option<u64> {
    value.get(field).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
    })
}

fn command_call_id(
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

fn qualifies_for_synthetic_read_only_recovery(
    command_type: &str,
    command_line: &str,
    session_directory: &Path,
    allowed_command_run_commands: Option<&BTreeSet<String>>,
) -> bool {
    command_type == "zsh"
        && allowed_command_run_commands
            .map(|allowed| allowed.contains(command_type))
            .unwrap_or(true)
        && code_tools::commands::access(command_type, command_line, session_directory)
            .is_read_only()
}

fn safe_call_id(call_id: &str) -> String {
    let mut encoded = String::with_capacity(call_id.len());
    for character in call_id.chars() {
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

fn require_artifact_absent(path: &Path, artifact: &str) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(format!("exact {artifact} exists at {}", path.display())),
        Err(error) => Err(format!(
            "exact {artifact} state is unavailable at {}: {error}",
            path.display()
        )),
    }
}

fn app_server_dynamic_tools(tools: Vec<Value>) -> Vec<Value> {
    tools
        .into_iter()
        .filter_map(|tool| {
            let function = tool.get("function").unwrap_or(&tool);
            let name = function.get("name")?.as_str()?;
            let mut result = Map::new();
            result.insert("name".to_string(), Value::String(name.to_string()));
            if let Some(description) = function.get("description") {
                result.insert("description".to_string(), description.clone());
            }
            result.insert(
                "inputSchema".to_string(),
                function
                    .get("parameters")
                    .or_else(|| function.get("inputSchema"))
                    .cloned()
                    .unwrap_or_else(|| json!({"type": "object"})),
            );
            Some(Value::Object(result))
        })
        .collect()
}

fn resolve_codex_executable() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("TURA_CODEX_APP_SERVER_EXECUTABLE") {
        return executable_file(PathBuf::from(path));
    }
    let executable_name = if cfg!(windows) { "codex.exe" } else { "codex" };
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .map(|directory| directory.join(executable_name))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            "official_codex_app_server requires codex on PATH or TURA_CODEX_APP_SERVER_EXECUTABLE"
                .to_string()
        })
}

fn executable_file(path: PathBuf) -> Result<PathBuf, String> {
    if Path::new(&path).is_file() {
        Ok(path)
    } else {
        Err(format!(
            "Codex executable does not exist: {}",
            path.display()
        ))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn governed_arguments_replace_untrusted_execution_id() {
        let params = serde_json::json!({
            "arguments": {
                "execution_id": "callback-bridge-v2-preflight",
                "command": "printf preserved"
            }
        });
        let arguments =
            governed_dynamic_tool_arguments(&params, "runtime-id:tool-call-id").unwrap();

        assert_eq!(arguments["execution_id"], "runtime-id:tool-call-id");
        assert_eq!(arguments["command"], "printf preserved");
    }

    use super::{
        app_server_dynamic_tools, command_call_id, governed_dynamic_tool_arguments,
        qualifies_for_synthetic_read_only_recovery, safe_call_id, RuntimeOfficialCodexHandler,
    };
    use std::{collections::BTreeSet, path::Path, sync::Mutex};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tura_llm_rust::official_codex_app_server::{
        CodexExecutionLedger, OfficialCodexServerRequest, OfficialCodexServerRequestHandler,
        CODEX_EXECUTION_LEDGER_SCHEMA_VERSION,
    };

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn approval_requests_follow_session_permission_policy() {
        for method in [
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        ] {
            let request = OfficialCodexServerRequest {
                method: method.to_string(),
                params: serde_json::json!({"itemId": "item-1"}),
            };
            let mut restricted = RuntimeOfficialCodexHandler {
                session_directory: std::env::temp_dir(),
                session_id: "restricted-session".to_string(),
                runtime_id: "restricted-runtime".to_string(),
                allowed_command_run_commands: None,
                disable_permission_restrictions: false,
                jspace_contract: None,
            };
            let mut unrestricted = RuntimeOfficialCodexHandler {
                session_directory: std::env::temp_dir(),
                session_id: "unrestricted-session".to_string(),
                runtime_id: "unrestricted-runtime".to_string(),
                allowed_command_run_commands: None,
                disable_permission_restrictions: true,
                jspace_contract: None,
            };

            assert_eq!(
                restricted.handle(request.clone()).await.unwrap()["decision"],
                "decline"
            );
            assert_eq!(
                unrestricted.handle(request).await.unwrap()["decision"],
                "acceptForSession"
            );
        }
    }

    #[test]
    fn execution_ledger_survives_runtime_identity_change_for_same_mission_input() {
        let directory = tempfile::tempdir().expect("ledger directory");
        let digest = "a".repeat(64);
        let mut first = RuntimeOfficialCodexHandler {
            session_directory: directory.path().to_path_buf(),
            session_id: "session-1".to_string(),
            runtime_id: "runtime-1".to_string(),
            allowed_command_run_commands: None,
            disable_permission_restrictions: false,
            jspace_contract: None,
        };
        let ledger = CodexExecutionLedger {
            schema_version: CODEX_EXECUTION_LEDGER_SCHEMA_VERSION,
            tura_session_id: "session-1".to_string(),
            canonical_input_sha256: digest.clone(),
            runtime_ids: vec!["runtime-1".to_string()],
            effects: Vec::new(),
            interrupted_recovery: None,
            terminal_status: None,
        };
        first
            .persist_execution_ledger(&ledger)
            .expect("first runtime persists mission ledger");

        let mut retry = RuntimeOfficialCodexHandler {
            runtime_id: "runtime-2".to_string(),
            ..first
        };
        let mut recovered = retry
            .load_execution_ledger(&digest)
            .expect("retry reads ledger")
            .expect("ledger exists");
        recovered.runtime_ids.push("runtime-2".to_string());
        retry
            .persist_execution_ledger(&recovered)
            .expect("retry adopts the same mission ledger");

        let reread = retry
            .load_execution_ledger(&digest)
            .expect("read adopted ledger")
            .expect("adopted ledger exists");
        assert_eq!(reread.runtime_ids, ["runtime-1", "runtime-2"]);
        assert!(retry
            .load_execution_ledger(&"b".repeat(64))
            .expect("different input lookup")
            .is_none());
    }

    #[tokio::test]
    async fn command_run_request_uses_router_and_preserves_terminal_receipt() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock router listener");
        let address = listener.local_addr().expect("mock router address");
        let router = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("router accept");
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let request: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["method"], "execution.command_run");
            assert_eq!(request["payload"]["session_id"], "session-1");
            assert_eq!(request["payload"]["runtime_id"], "runtime-1");
            let response = serde_json::json!({
                "id": request["request_id"],
                "request_id": request["request_id"],
                "ok": true,
                "payload": {"result": {
                    "results": [{
                        "success": true,
                        "terminal_receipt": {
                            "schema_version": "tura_command_terminal_receipt_v1",
                            "terminal_state": "completed",
                            "execution_id": "runtime-1:call-1"
                        }
                    }, {
                        "success": false,
                        "output": {
                            "exit_code": 1,
                            "failure_class": "workload_exit_nonzero"
                        }
                    }]
                }},
                "error": ""
            });
            writer
                .write_all(response.to_string().as_bytes())
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
        });
        let previous = std::env::var_os("TURA_ROUTER_ADDR");
        #[allow(
            unsafe_code,
            reason = "test serializes the Rust 2024 process-environment mutation"
        )]
        unsafe {
            std::env::set_var("TURA_ROUTER_ADDR", address.to_string())
        };
        let mut handler = RuntimeOfficialCodexHandler {
            session_directory: std::env::temp_dir(),
            session_id: "session-1".to_string(),
            runtime_id: "runtime-1".to_string(),
            allowed_command_run_commands: None,
            disable_permission_restrictions: false,
            jspace_contract: None,
        };
        let response = handler
            .handle(OfficialCodexServerRequest {
                method: "item/tool/call".to_string(),
                params: serde_json::json!({
                    "callId": "call-1",
                    "tool": "command_run",
                    "arguments": {"commands": []}
                }),
            })
            .await
            .expect("dynamic command_run response");
        let output: serde_json::Value =
            serde_json::from_str(response["contentItems"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(response["success"], true);
        assert_eq!(output["results"][1]["success"], false);
        assert_eq!(
            output["results"][0]["terminal_receipt"]["schema_version"],
            "tura_command_terminal_receipt_v1"
        );
        router.await.unwrap();
        #[allow(
            unsafe_code,
            reason = "test serializes the Rust 2024 process-environment mutation"
        )]
        unsafe {
            if let Some(previous) = previous {
                std::env::set_var("TURA_ROUTER_ADDR", previous);
            } else {
                std::env::remove_var("TURA_ROUTER_ADDR");
            }
        }
    }

    #[test]
    fn dynamic_tools_use_app_server_shape() {
        let tools = app_server_dynamic_tools(vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "command_run",
                "description": "governed command runner",
                "parameters": {"type": "object"}
            }
        })]);
        assert_eq!(tools[0]["name"], "command_run");
        assert_eq!(tools[0]["inputSchema"]["type"], "object");
    }

    #[test]
    fn official_admission_prohibits_fallback_without_capturing_existing_routes() {
        let provider = |name: &str| tura_llm_rust::ProviderConfig {
            provider: name.to_string(),
            base_url: "http://provider.invalid".to_string(),
            model: "model-test".to_string(),
            temperature: 0.0,
        };
        let official = tura_llm_rust::RouteConfig {
            default_temperature: 0.0,
            providers: vec![provider("official_codex_app_server")],
        };
        assert_eq!(
            official
                .official_codex_app_server_provider()
                .expect("official route admission")
                .map(|provider| provider.provider.as_str()),
            Some("official_codex_app_server")
        );

        let mixed = tura_llm_rust::RouteConfig {
            default_temperature: 0.0,
            providers: vec![provider("official_codex_app_server"), provider("openai")],
        };
        assert!(mixed
            .official_codex_app_server_provider()
            .expect_err("official fallback must be rejected")
            .to_string()
            .contains("fallback is prohibited"));

        let legacy = tura_llm_rust::RouteConfig {
            default_temperature: 0.0,
            providers: vec![provider("codex"), provider("openai")],
        };
        assert!(legacy
            .official_codex_app_server_provider()
            .expect_err("legacy Codex provider must not remain selectable")
            .to_string()
            .contains("legacy provider 'codex' is disabled"));

        let non_codex = tura_llm_rust::RouteConfig {
            default_temperature: 0.0,
            providers: vec![provider("openai")],
        };
        assert!(non_codex
            .official_codex_app_server_provider()
            .expect("non-Codex routes remain admitted")
            .is_none());
    }

    #[test]
    fn command_identity_mirrors_explicit_and_implicit_command_run_binding() {
        let execution_id = "runtime-original:call-original";
        assert_eq!(
            command_call_id(execution_id, Some("result-7"), 4, 2),
            "runtime-original:call-original:result-7"
        );
        assert_eq!(
            command_call_id(
                execution_id,
                Some("runtime-original:call-original:result-7"),
                4,
                2,
            ),
            "runtime-original:call-original:result-7"
        );
        assert_eq!(
            command_call_id(execution_id, None, 4, 2),
            "runtime-original:call-original:step:4:index:2"
        );
    }

    #[test]
    fn safe_call_id_matches_command_receipt_encoding() {
        assert_eq!(
            safe_call_id("runtime-original:call-original:step:4:index:2"),
            "runtime-original_x3a_call-original_x3a_step_x3a_4_x3a_index_x3a_2"
        );
        assert_eq!(safe_call_id("receipt.name-1_2"), "receipt.name-1_2");
        assert_eq!(safe_call_id("\u{e9}\u{96ea}"), "_xe9__x96ea_");
        assert_eq!(safe_call_id(""), "command_run");
    }

    #[test]
    fn read_only_observation_parses_and_validates_step_values() {
        let mut handler = RuntimeOfficialCodexHandler {
            session_directory: Path::new(".").to_path_buf(),
            session_id: "session-original".to_string(),
            runtime_id: "runtime-original".to_string(),
            allowed_command_run_commands: None,
            disable_permission_restrictions: false,
            jspace_contract: None,
        };
        let observation = handler
            .observe_read_only_effect(&OfficialCodexServerRequest {
                method: "item/tool/call".to_string(),
                params: serde_json::json!({
                    "callId": "call-original",
                    "tool": "command_run",
                    "arguments": {
                        "commands": [
                            {
                                "command_type": "zsh",
                                "command_line": "pwd",
                                "step": "7",
                                "id": "result-7"
                            },
                            {
                                "command_type": "zsh",
                                "command_line": "pwd",
                                "step": 0
                            },
                            {
                                "command_type": "zsh",
                                "command_line": "pwd"
                            }
                        ]
                    }
                }),
            })
            .expect("read-only observation")
            .expect("admitted read-only commands");
        assert_eq!(observation.commands[0].effective_step, 7);
        assert_eq!(
            observation.commands[0].claim_identity,
            "runtime-original:call-original:result-7"
        );
        assert_eq!(observation.commands[1].effective_step, 1);
        assert_eq!(
            observation.commands[1].claim_identity,
            "runtime-original:call-original:step:1:index:1"
        );
        assert_eq!(observation.commands[2].effective_step, 3);
        assert_eq!(
            observation.commands[2].claim_identity,
            "runtime-original:call-original:step:3:index:2"
        );

        for step in [
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("invalid"),
        ] {
            let rejected = handler
                .observe_read_only_effect(&OfficialCodexServerRequest {
                    method: "item/tool/call".to_string(),
                    params: serde_json::json!({
                        "callId": "call-original",
                        "tool": "command_run",
                        "arguments": {
                            "commands": [{
                                "command_type": "zsh",
                                "command_line": "pwd",
                                "step": step.clone()
                            }]
                        }
                    }),
                })
                .expect("invalid step observation");
            assert!(rejected.is_none(), "invalid step {step} was admitted");
        }
    }

    #[test]
    fn synthetic_recovery_rejects_write_external_and_non_shell_commands() {
        let session_directory = Path::new(".");
        assert!(!qualifies_for_synthetic_read_only_recovery(
            "zsh",
            "touch denied",
            session_directory,
            None,
        ));
        assert!(!qualifies_for_synthetic_read_only_recovery(
            "zsh",
            "curl https://example.com",
            session_directory,
            None,
        ));
        assert!(!qualifies_for_synthetic_read_only_recovery(
            "task_status",
            "status",
            session_directory,
            None,
        ));
        let allowed = BTreeSet::from(["task_status".to_string()]);
        assert!(!qualifies_for_synthetic_read_only_recovery(
            "zsh",
            "rg -n needle source.rs",
            session_directory,
            Some(&allowed),
        ));
    }
}
