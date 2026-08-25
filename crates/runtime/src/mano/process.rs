use chrono::Utc;
use tracing::{error, info};

use crate::agent_router::activate_agents_by_session_type;
use crate::checkpoint::session_snapshot::{persist_session_checkpoint, SessionDeltaWriter};
use crate::manas::runtime_turn::RetryProviderInput;
use crate::manas::{process_manas_internal, ManasInput};
use crate::mano::{ManoOverrides, ManoProcessResult};
use crate::runtime_event_writer::RuntimeEventWriter;
use crate::session_bootstrap::{
    bootstrap_orchestration_session, create_session_with_topic, initial_messages_for_session,
};
use crate::session_log_client::SessionLogClient;
use crate::state_machine::agent_management::{AgentCapabilityItem, AgentManagement};
use lifecycle::{RuntimeId, RuntimeState, SessionCommand, SessionInput, SessionManagement};
use runtime_contract::LifecycleExecutionContext;
use serde_json::Value;
use session_log_contract::{
    CreateSessionRequest, RuntimeReplay, SessionLogCommand, SessionLogResponse,
};
use std::collections::HashSet;
use std::path::PathBuf;

const MAX_RUNTIME_RETRY_LINEAGE_DEPTH: usize = 64;

pub struct OrchestrationConfig {
    pub redis_url: String,
    pub session_directory: Option<PathBuf>,
    pub jspace_contract: Option<Value>,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://localhost:6379".to_string(),
            session_directory: None,
            jspace_contract: None,
        }
    }
}

pub fn orchestrate(input: SessionInput) -> Result<ManoProcessResult, String> {
    orchestrate_with_config_and_session(
        input,
        OrchestrationConfig::default(),
        None,
        None,
        None,
        None,
    )
}

pub fn orchestrate_for_session(
    input: SessionInput,
    session_id: String,
) -> Result<ManoProcessResult, String> {
    orchestrate_with_config_and_session(
        input,
        OrchestrationConfig::default(),
        Some(session_id),
        None,
        None,
        None,
    )
}

pub fn orchestrate_for_session_in_directory(
    input: SessionInput,
    session_id: String,
    session_directory: PathBuf,
) -> Result<ManoProcessResult, String> {
    orchestrate_with_config_and_session(
        input,
        OrchestrationConfig {
            session_directory: Some(session_directory),
            ..OrchestrationConfig::default()
        },
        Some(session_id),
        None,
        None,
        None,
    )
}

pub fn orchestrate_for_session_with_lease_in_directory(
    input: SessionInput,
    session_id: String,
    runtime_id: RuntimeId,
    lease_id: String,
    session_directory: PathBuf,
) -> Result<ManoProcessResult, String> {
    orchestrate_for_session_with_lease_and_lifecycle_in_directory(
        input,
        session_id,
        runtime_id,
        lease_id,
        session_directory,
        None,
        None,
    )
}

pub fn orchestrate_for_session_with_lease_and_lifecycle_in_directory(
    input: SessionInput,
    session_id: String,
    runtime_id: RuntimeId,
    lease_id: String,
    session_directory: PathBuf,
    lifecycle: Option<LifecycleExecutionContext>,
    fallback_from_id: Option<RuntimeId>,
) -> Result<ManoProcessResult, String> {
    orchestrate_for_session_with_lease_and_lifecycle_and_jspace_in_directory(
        input,
        session_id,
        runtime_id,
        lease_id,
        session_directory,
        lifecycle,
        None,
        fallback_from_id,
    )
}

pub fn orchestrate_for_session_with_lease_and_lifecycle_and_jspace_in_directory(
    input: SessionInput,
    session_id: String,
    runtime_id: RuntimeId,
    lease_id: String,
    session_directory: PathBuf,
    lifecycle: Option<LifecycleExecutionContext>,
    jspace_contract: Option<Value>,
    fallback_from_id: Option<RuntimeId>,
) -> Result<ManoProcessResult, String> {
    orchestrate_with_config_and_session(
        input,
        OrchestrationConfig {
            session_directory: Some(session_directory),
            jspace_contract,
            ..OrchestrationConfig::default()
        },
        Some(session_id.clone()),
        Some(runtime_id.clone()),
        fallback_from_id,
        Some(RuntimeEventWriter::new_with_lifecycle(
            session_id, runtime_id, lease_id, lifecycle,
        )?),
    )
}

fn orchestrate_with_config_and_session(
    input: SessionInput,
    config: OrchestrationConfig,
    gateway_session_id: Option<String>,
    initial_runtime_id: Option<RuntimeId>,
    initial_fallback_from_id: Option<RuntimeId>,
    runtime_event_writer: Option<RuntimeEventWriter>,
) -> Result<ManoProcessResult, String> {
    let now = Utc::now();
    let create_missing_session = runtime_event_writer.is_none();

    info!(
        user_input = %input.user_input,
        "starting orchestration"
    );

    let mut session = bootstrap_orchestration_session(
        input,
        config.session_directory.clone(),
        gateway_session_id,
        now,
    )
    .map_err(|e| {
        error!(error = %e, "failed to bootstrap session");
        format!("failed to bootstrap session: {e}")
    })?;
    if let Some(jspace_contract) = config.jspace_contract {
        session.jspace_contract = Some(jspace_contract);
    }

    info!(
        session_id = %session.session_id,
        task_type = ?session.task_type,
        "session created"
    );

    let mut agents = match activate_agents_by_session_type(&session) {
        Ok(a) => a,
        Err(e) => {
            error!(error = %e, "failed to activate agents");
            return Err(format!("failed to activate agents: {e}"));
        }
    };
    apply_planning_capability_override(&mut agents, &session);
    session.planning_enabled = agents.first().is_some_and(agent_has_planning_capability);
    session.reflection_enabled = agents.first().is_some_and(|agent| agent.reflection);
    session.op_manual_enabled = operation_manual_enabled_for_session(&session, &agents);

    info!(
        session_id = %session.session_id,
        agent_count = agents.len(),
        "agents activated"
    );

    let initial_retry_provider_input = match initial_fallback_from_id.as_deref() {
        Some(fallback_from_id) => Some(retry_provider_input(
            &session.session_id,
            fallback_from_id,
            &session.input.user_input,
        )?),
        None => None,
    };
    let initial_messages = initial_messages_for_session(&mut session)?;
    if create_missing_session {
        ensure_canonical_session(&session)?;
    }
    let mut session_delta_writer = Some(SessionDeltaWriter::new(&session)?);
    persist_session_checkpoint(&mut session_delta_writer, &session, "initial")?;

    let mut session_clone = session.clone();

    let manas_input = ManasInput {
        agents: &mut agents,
        session: &mut session_clone,
        initial_messages,
        redis_url: &config.redis_url,
        initial_runtime_id,
        initial_fallback_from_id,
        initial_retry_provider_input,
        runtime_event_writer,
        session_delta_writer,
    };

    let manas_result =
        match process_manas_internal(manas_input, crate::manas::ManasOverrides::default()) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "manas processing failed");
                return Err(format!("manas processing failed: {e}"));
            }
        };

    info!(
        session_id = %manas_result.session.session_id,
        final_turn = manas_result.session.session_current_turn,
        final_state = ?manas_result.session.state,
        "orchestration completed"
    );

    Ok(ManoProcessResult {
        session: manas_result.session,
        agents: manas_result.agents,
        final_error: manas_result.final_error,
    })
}

fn retry_provider_input(
    session_id: &str,
    fallback_from_id: &str,
    expected_user_input: &str,
) -> Result<RetryProviderInput, String> {
    let client = SessionLogClient::discover().map_err(|error| {
        format!(
            "RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{fallback_from_id}:session_log_discovery:{error}"
        )
    })?;
    resolve_retry_provider_input(
        session_id,
        fallback_from_id,
        expected_user_input,
        |runtime_id| client.replay_runtime(runtime_id.to_string()),
    )
}

fn resolve_retry_provider_input<F>(
    session_id: &str,
    fallback_from_id: &str,
    expected_user_input: &str,
    mut replay_runtime: F,
) -> Result<RetryProviderInput, String>
where
    F: FnMut(&str) -> Result<Option<RuntimeReplay>, String>,
{
    let mut runtime_id = fallback_from_id.to_string();
    let mut visited = HashSet::new();

    for _ in 0..MAX_RUNTIME_RETRY_LINEAGE_DEPTH {
        if !visited.insert(runtime_id.clone()) {
            return Err(format!(
                "RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:lineage_cycle"
            ));
        }
        let replay = replay_runtime(&runtime_id).map_err(|error| {
            format!("RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:replay_failed:{error}")
        })?;
        let replay = replay.ok_or_else(|| {
            format!("RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:runtime_missing")
        })?;
        let aggregate = replay.aggregate;
        if aggregate.runtime_id != runtime_id || aggregate.session_id != session_id {
            return Err(format!(
                "RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:identity_mismatch"
            ));
        }
        if !matches!(
            aggregate.state,
            RuntimeState::Failed | RuntimeState::TimedOut
        ) {
            return Err(format!(
                "RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:source_not_retryable"
            ));
        }
        let provider_input = aggregate.input.as_ref().ok_or_else(|| {
            format!("RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:input_missing")
        })?;
        let messages = provider_input
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                format!("RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:messages_missing")
            })?;
        let input_matches = messages
            .iter()
            .rev()
            .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
            .and_then(|message| message.get("content"))
            .is_some_and(|content| {
                crate::context::user_input_content_matches(content, expected_user_input)
            });
        if !input_matches {
            return Err(format!(
                "RUNTIME_RETRY_INPUT_MISMATCH:{session_id}:{runtime_id}"
            ));
        }

        let tools = provider_input
            .get("tools")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                format!("RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:tools_missing")
            })?;

        match aggregate.fallback_from_id {
            Some(parent_runtime_id) => runtime_id = parent_runtime_id,
            None => return Ok(RetryProviderInput { messages, tools }),
        }
    }

    Err(format!(
        "RUNTIME_RETRY_SOURCE_INVALID:{session_id}:{runtime_id}:lineage_too_deep"
    ))
}

fn ensure_canonical_session(session: &SessionManagement) -> Result<(), String> {
    let client = SessionLogClient::discover().map_err(|error| {
        format!(
            "failed to discover session_log client for session {}: {error}",
            session.session_id
        )
    })?;
    if client
        .get_session(session.session_id.clone())
        .map_err(|error| {
            format!(
                "failed to query canonical session {}: {error}",
                session.session_id
            )
        })?
        .is_some()
    {
        return Ok(());
    }

    let directory = session.session_directory.to_string_lossy().to_string();
    match client.call_typed_sync(SessionLogCommand::CreateSession(Box::new(
        CreateSessionRequest {
            command_id: format!("create:{}", session.session_id),
            session_id: session.session_id.clone(),
            creation_command: SessionCommand::CreateSession {
                task_plan: session.task_plan.clone(),
            },
            copy_context: false,
            workspace: directory.clone(),
            session_directory: directory,
            name: session.session_name.clone(),
            created_at: session.session_created_at.timestamp_millis(),
            model: None,
            agent: session.input.agent.clone(),
            session_type: "coding".to_string(),
            kill_processes_on_start: false,
            validator_enabled: false,
            force_planning: false,
            model_variant: None,
            model_acceleration_enabled: false,
            disable_permission_restrictions: session.disable_permission_restrictions,
            use_last_tool_call_response: session.use_last_tool_call_response,
            auto_session_name: session.auto_session_name,
            initial_task_plan_patch: None,
        },
    )))? {
        SessionLogResponse::SessionCommandApplied { .. } => Ok(()),
        SessionLogResponse::Error { error } => Err(format!(
            "failed to create canonical session {}: {error}",
            session.session_id
        )),
        other => Err(format!(
            "unexpected session_log response while creating canonical session {}: {other:?}",
            session.session_id
        )),
    }
}

fn apply_planning_capability_override(agents: &mut [AgentManagement], session: &SessionManagement) {
    let Some(enabled) = session.input.planning_mode_override else {
        return;
    };
    let Some(agent) = agents.first_mut() else {
        return;
    };
    if enabled {
        if !agent_has_planning_capability(agent) {
            let capability_directory = agent
                .agent_capabilities
                .iter()
                .find(|capability| capability.capability_name == "command_run")
                .or_else(|| agent.agent_capabilities.first())
                .map(|capability| capability.capability_directory.clone())
                .unwrap_or_else(|| {
                    session
                        .session_directory
                        .join("crates")
                        .join("tools")
                        .join("src")
                });
            agent.agent_capabilities.push(AgentCapabilityItem {
                capability_name: "planning".to_string(),
                capability_directory,
            });
        }
    } else {
        agent
            .agent_capabilities
            .retain(|capability| capability.capability_name != "planning");
    }
}

fn agent_has_planning_capability(agent: &AgentManagement) -> bool {
    agent
        .agent_capabilities
        .iter()
        .any(|capability| capability.capability_name == "planning")
}

fn operation_manual_enabled_for_session(
    session: &SessionManagement,
    agents: &[AgentManagement],
) -> bool {
    session.goal_mode
        || session.reflection_enabled
        || (!session.no_op_manual && agents.first().is_some_and(|agent| agent.op_manual))
}

pub fn process_from_user_internal(
    input: SessionInput,
    overrides: ManoOverrides,
) -> Result<ManoProcessResult, String> {
    let mut session = match overrides.session_factory {
        Some(session_factory) => session_factory(input)?,
        None => create_session_with_topic(input, None)?,
    };

    let agents = match overrides.manas_entry {
        Some(manas_entry) => manas_entry(&session)?,
        None => {
            let mut agts = activate_agents_by_session_type(&session)?;
            apply_planning_capability_override(&mut agts, &session);
            session.planning_enabled = agts.first().is_some_and(agent_has_planning_capability);
            session.reflection_enabled = agts.first().is_some_and(|agent| agent.reflection);
            session.op_manual_enabled = operation_manual_enabled_for_session(&session, &agts);
            agts
        }
    };
    session.op_manual_enabled = operation_manual_enabled_for_session(&session, &agents);

    Ok(ManoProcessResult {
        session,
        agents,
        final_error: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{build_messages_from_session, USER_AGENT_CONTEXT_ROLE};
    use chrono::Utc;
    use lifecycle::{
        ProviderConfig, RuntimeAggregate, RuntimeError, RuntimeProviderConfig, RuntimeState,
        SessionInput, SessionManagement, ToolChoice,
    };
    use std::collections::HashMap;
    use std::fs;

    fn retry_replay(
        runtime_id: &str,
        fallback_from_id: Option<&str>,
        messages: Vec<Value>,
        tools: Vec<Value>,
    ) -> RuntimeReplay {
        let now = Utc::now();
        let mut aggregate = RuntimeAggregate::new_with_fallback(
            runtime_id.to_string(),
            "retry-session".to_string(),
            "retry-agent".to_string(),
            RuntimeProviderConfig {
                base: ProviderConfig {
                    tura_llm_name: "official_codex_app_server".to_string(),
                    default_model_tier: None,
                    current_model: Some("gpt-test".to_string()),
                    stream: true,
                    temperature: 0.0,
                    max_tokens: 1024,
                    tool_choice: ToolChoice::Auto,
                    time_out_ms: 30_000,
                },
                thinking: true,
                provider_name: "official_codex_app_server".to_string(),
                model_name: "gpt-test".to_string(),
                provider_url_name: "local".to_string(),
                llm_provider_name: "openai".to_string(),
            },
            now,
            fallback_from_id.map(str::to_string),
        )
        .expect("retry lineage should be valid");
        aggregate
            .set_input(serde_json::json!({
                "messages": messages,
                "tools": tools,
                "options": { "stream": true }
            }))
            .expect("runtime input should be recorded");
        aggregate
            .finish_failure(
                now,
                RuntimeError {
                    error_code: Some("INTERRUPTED".to_string()),
                    error_text: Some("provider interrupted".to_string()),
                    retry_allowed: true,
                    fallback_allowed: true,
                    fallback_to_id: None,
                },
                RuntimeState::Failed,
                None,
            )
            .expect("retry source should be terminal");
        RuntimeReplay {
            aggregate,
            revision: 1,
            next_event_seq: 2,
        }
    }

    #[test]
    fn retry_replay_uses_root_provider_input_across_failed_attempts() {
        let root_messages = vec![
            serde_json::json!({ "role": "developer", "content": "original snapshot" }),
            serde_json::json!({ "role": "user", "content": "same mission" }),
        ];
        let root_tools = vec![serde_json::json!({ "name": "original_tool" })];
        let drifted_messages = vec![
            serde_json::json!({ "role": "developer", "content": "drifted snapshot" }),
            serde_json::json!({ "role": "user", "content": "same mission" }),
        ];
        let drifted_tools = vec![serde_json::json!({ "name": "drifted_tool" })];
        let replays = HashMap::from([
            (
                "runtime-root".to_string(),
                retry_replay(
                    "runtime-root",
                    None,
                    root_messages.clone(),
                    root_tools.clone(),
                ),
            ),
            (
                "runtime-retry".to_string(),
                retry_replay(
                    "runtime-retry",
                    Some("runtime-root"),
                    drifted_messages,
                    drifted_tools,
                ),
            ),
        ]);

        let resolved = resolve_retry_provider_input(
            "retry-session",
            "runtime-retry",
            "same mission",
            |runtime_id| Ok(replays.get(runtime_id).cloned()),
        )
        .expect("same mission should resolve to the original provider snapshot");

        assert_eq!(resolved.messages, root_messages);
        assert_eq!(resolved.tools, root_tools);
    }

    #[test]
    fn retry_replay_rejects_changed_user_input() {
        let replay = retry_replay(
            "runtime-root",
            None,
            vec![
                serde_json::json!({ "role": "user", "content": "changed mission" }),
                serde_json::json!({ "role": "assistant", "content": "intermediate" }),
                serde_json::json!({
                    "role": "user",
                    "content": [{ "type": "input_text", "text": "original mission" }]
                }),
            ],
            vec![serde_json::json!({ "name": "command_run" })],
        );

        let error = resolve_retry_provider_input(
            "retry-session",
            "runtime-root",
            "changed mission",
            |runtime_id| Ok((runtime_id == "runtime-root").then(|| replay.clone())),
        )
        .expect_err("changed input must not inherit a prior provider turn");

        assert_eq!(
            error,
            "RUNTIME_RETRY_INPUT_MISMATCH:retry-session:runtime-root"
        );
    }

    #[test]
    fn planning_override_removes_planning_from_started_agent_state() {
        let input = SessionInput {
            user_input: "inspect".to_string(),
            file_input: Vec::new(),
            agent: Some("thoughtful".to_string()),
            runtime_context: None,
            planning_mode_override: Some(false),
        };
        let mut session = SessionManagement::new(
            "planning-off".to_string(),
            "planning-off".to_string(),
            std::env::current_dir().expect("current dir should resolve"),
            false,
            "coding".to_string(),
            input,
            "inspect".to_string(),
            Utc::now(),
        );
        let mut agents = activate_agents_by_session_type(&session).expect("agents should activate");

        apply_planning_capability_override(&mut agents, &session);
        session.planning_enabled = agents.first().is_some_and(agent_has_planning_capability);
        session.reflection_enabled = agents.first().is_some_and(|agent| agent.reflection);

        assert!(!session.planning_enabled);
        assert!(session.reflection_enabled);
        assert!(!agent_has_planning_capability(&agents[0]));
    }

    #[test]
    fn planning_override_adds_planning_to_started_agent_state() {
        let input = SessionInput {
            user_input: "inspect".to_string(),
            file_input: Vec::new(),
            agent: Some("general".to_string()),
            runtime_context: None,
            planning_mode_override: Some(true),
        };
        let mut session = SessionManagement::new(
            "planning-on".to_string(),
            "planning-on".to_string(),
            std::env::current_dir().expect("current dir should resolve"),
            false,
            "general".to_string(),
            input,
            "inspect".to_string(),
            Utc::now(),
        );
        let mut agents = activate_agents_by_session_type(&session).expect("agents should activate");

        apply_planning_capability_override(&mut agents, &session);
        session.planning_enabled = agents.first().is_some_and(agent_has_planning_capability);
        session.reflection_enabled = agents.first().is_some_and(|agent| agent.reflection);

        assert!(session.planning_enabled);
        assert!(!session.reflection_enabled);
        assert!(agent_has_planning_capability(&agents[0]));
    }

    #[test]
    fn resumed_session_initial_messages_include_prior_image_tool_context_and_new_user_turn() {
        let root = std::env::temp_dir().join(format!(
            "tura-resume-image-context-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&root).expect("test workspace should be created");
        let old_input = SessionInput {
            user_input: "inspect image".to_string(),
            file_input: Vec::new(),
            agent: None,
            runtime_context: None,
            planning_mode_override: None,
        };
        let mut session = SessionManagement::new(
            "resume-image-session".to_string(),
            "resume-image".to_string(),
            root.clone(),
            false,
            "coding".to_string(),
            old_input,
            "inspect image".to_string(),
            Utc::now(),
        );
        session.session_current_turn = 2;
        session.push_log(
            serde_json::json!({
                "type": "tool_result",
                "tool_name": "command_run",
                "context_messages": [
                    {
                        "type": "function_call",
                        "name": "command_run",
                        "call_id": "call_image",
                        "arguments": "{\"commands\":[{\"step\":1,\"command_type\":\"read_media\",\"command_line\":\"read_media image.png\"}]}",
                        "status": "completed"
                    },
                    {
                        "type": "function_call_output",
                        "call_id": "call_image",
                        "output": [
                            {"type": "input_text", "text": "image inspected"},
                            {"type": "input_image", "image_url": "data:image/png;base64,AAA"}
                        ]
                    }
                ]
            })
            .to_string(),
            Utc::now(),
        );
        session.prepare_for_new_user_turn(
            SessionInput {
                user_input: "what was in the previous image?".to_string(),
                file_input: Vec::new(),
                agent: None,
                runtime_context: None,
                planning_mode_override: None,
            },
            Utc::now(),
        );

        let messages =
            initial_messages_for_session(&mut session).expect("resume messages should build");
        let serialized = serde_json::to_string(&messages).expect("messages json");

        assert!(serialized.contains("data:image/png;base64,AAA"));
        assert!(!messages.iter().any(|message| {
            message.get("role").and_then(serde_json::Value::as_str) == Some("developer")
        }));
        assert!(messages.iter().any(|message| {
            message.get("role").and_then(serde_json::Value::as_str) == Some("user")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn initial_user_input_media_markers_become_input_images() {
        let root = std::env::temp_dir().join(format!(
            "tura-inline-input-image-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&root).expect("test workspace should be created");
        let input = SessionInput {
            user_input: "[Image 1: screen.png]\n[MEDIA:data:image/png;base64,AAA:MEDIA]"
                .to_string(),
            file_input: Vec::new(),
            agent: None,
            runtime_context: None,
            planning_mode_override: None,
        };
        let mut session = SessionManagement::new(
            "inline-image-session".to_string(),
            "inline image".to_string(),
            root.clone(),
            false,
            "coding".to_string(),
            input,
            "inline image".to_string(),
            Utc::now(),
        );

        let messages =
            initial_messages_for_session(&mut session).expect("initial messages should build");
        let user_message = messages
            .iter()
            .rev()
            .find(|message| message.get("role").and_then(serde_json::Value::as_str) == Some("user"))
            .expect("initial user message should exist");
        let content = user_message["content"]
            .as_array()
            .expect("media input should become content array");

        assert!(content.iter().any(|part| {
            part.get("type").and_then(serde_json::Value::as_str) == Some("input_image")
                && part.get("image_url").and_then(serde_json::Value::as_str)
                    == Some("data:image/png;base64,AAA")
        }));
        assert!(content.iter().any(|part| {
            part.get("type").and_then(serde_json::Value::as_str) == Some("input_text")
                && part
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn initial_messages_persist_workspace_file_snapshot_for_cache_reuse() {
        let root = std::env::temp_dir().join(format!(
            "tura-initial-snapshot-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(root.join("src")).expect("test workspace should be created");
        fs::write(root.join("src").join("lib.rs"), "fn main() {}\n").expect("fixture should write");
        let input = SessionInput {
            user_input: "inspect this workspace".to_string(),
            file_input: Vec::new(),
            agent: None,
            runtime_context: None,
            planning_mode_override: None,
        };
        let mut session = SessionManagement::new(
            "snapshot-session".to_string(),
            "snapshot".to_string(),
            root.clone(),
            false,
            "coding".to_string(),
            input,
            "inspect this workspace".to_string(),
            Utc::now(),
        );

        let initial =
            initial_messages_for_session(&mut session).expect("initial messages should build");
        let replayed = build_messages_from_session(&session);

        let initial_snapshot = initial
            .iter()
            .find(|message| {
                message["content"]
                    .as_str()
                    .is_some_and(|content| content.contains("<WORKSPACE_SNAPSHOT>"))
            })
            .expect("initial messages should include workspace snapshot");
        assert!(initial_snapshot["content"]
            .as_str()
            .expect("snapshot content should be text")
            .contains("src/lib.rs"));
        assert_eq!(initial_snapshot["role"], "developer");
        let replayed_snapshot = replayed
            .iter()
            .find(|message| {
                message["content"]
                    .as_str()
                    .is_some_and(|content| content.contains("<WORKSPACE_SNAPSHOT>"))
            })
            .expect("replayed context should include workspace snapshot");
        assert_eq!(replayed_snapshot["role"], "developer");
        assert_eq!(replayed_snapshot["content"], initial_snapshot["content"]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn initial_runtime_context_uses_user_agent_storage_and_user_replay() {
        let root = std::env::temp_dir().join(format!(
            "tura-runtime-context-tag-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&root).expect("test workspace should be created");
        let input = SessionInput {
            user_input: "inspect this workspace".to_string(),
            file_input: Vec::new(),
            agent: None,
            runtime_context: Some("client runtime context".to_string()),
            planning_mode_override: None,
        };
        let mut session = SessionManagement::new(
            "runtime-context-tag-session".to_string(),
            "runtime context tag".to_string(),
            root.clone(),
            false,
            "coding".to_string(),
            input,
            "inspect this workspace".to_string(),
            Utc::now(),
        );

        let initial =
            initial_messages_for_session(&mut session).expect("initial messages should build");
        let initial_context = initial
            .iter()
            .find(|message| message["content"] == "client runtime context")
            .expect("initial messages should include runtime context");
        assert_eq!(initial_context["role"], USER_AGENT_CONTEXT_ROLE);

        let stored_context = session
            .session_log
            .iter()
            .map(|entry| entry.value())
            .find(|entry| entry["content"] == "client runtime context")
            .expect("runtime context should be stored");
        assert_eq!(stored_context["role"], USER_AGENT_CONTEXT_ROLE);

        let replayed = build_messages_from_session(&session);
        let replayed_context = replayed
            .iter()
            .find(|message| message["content"] == "client runtime context")
            .expect("runtime context should replay into provider context");
        assert_eq!(replayed_context["role"], "user");

        let _ = fs::remove_dir_all(root);
    }
}
