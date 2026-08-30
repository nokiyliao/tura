use anyhow::{Result, anyhow};
use std::time::Instant;

use crate::profile_timings;
use session_log_contract::{
    CommandCheckpoint, ContextSlice, GetSessionRequest, ListSessionRecordsRequest,
    ListSessionsRequest, Page, PersistSessionDeltaRequest, ReadContextSliceRequest,
    ReplayRuntimeRequest, RuntimeReplay, SessionLogCommand, SessionLogResponse, SessionRecord,
    SessionSnapshot, WorkspaceSummary,
};

#[derive(Debug, Clone, Default)]
pub struct SessionLogClient;

impl SessionLogClient {
    pub fn discover() -> Result<Self> {
        Ok(Self)
    }

    pub(crate) fn call_typed(
        &self,
        command: SessionLogCommand,
    ) -> Result<SessionLogResponse, String> {
        self.call(command).map_err(|error| error.to_string())
    }

    pub(crate) fn call_typed_sync(
        &self,
        command: SessionLogCommand,
    ) -> Result<SessionLogResponse, String> {
        call_session_service(&command).map_err(|error| error.to_string())
    }

    pub(crate) fn persist_session_delta(
        &self,
        request: PersistSessionDeltaRequest,
    ) -> Result<(u64, u64), String> {
        match self.call_typed_sync(SessionLogCommand::PersistSessionDelta(Box::new(request)))? {
            SessionLogResponse::SessionDeltaPersisted {
                next_sequence,
                next_management_sequence,
            } => Ok((next_sequence, next_management_sequence)),
            SessionLogResponse::Error { error } => {
                Err(format!("session_log persist_session_delta failed: {error}"))
            }
            other => Err(format!(
                "unexpected session_log response for persist_session_delta: {other:?}"
            )),
        }
    }

    pub(crate) fn read_context_slice(
        &self,
        session_id: String,
        max_estimated_tokens: u64,
    ) -> Result<ContextSlice, String> {
        match self.call_typed_sync(SessionLogCommand::ReadContextSlice(
            ReadContextSliceRequest {
                session_id,
                max_estimated_tokens,
            },
        ))? {
            SessionLogResponse::ContextSlice { context } => Ok(context),
            SessionLogResponse::Error { error } => {
                Err(format!("session_log read_context_slice failed: {error}"))
            }
            other => Err(format!(
                "unexpected session_log response for read_context_slice: {other:?}"
            )),
        }
    }

    pub(crate) fn replay_runtime(
        &self,
        runtime_id: String,
    ) -> Result<Option<RuntimeReplay>, String> {
        match self.call_typed_sync(SessionLogCommand::ReplayRuntime(ReplayRuntimeRequest {
            runtime_id,
        }))? {
            SessionLogResponse::RuntimeReplayed { runtime } => Ok(runtime.map(|runtime| *runtime)),
            SessionLogResponse::Error { error } => {
                Err(format!("session_log replay_runtime failed: {error}"))
            }
            other => Err(format!(
                "unexpected session_log response for replay_runtime: {other:?}"
            )),
        }
    }

    pub fn apply_command_checkpoint(&self, checkpoint: CommandCheckpoint) -> Result<()> {
        match self.call(SessionLogCommand::ApplyCommandCheckpoint(Box::new(
            checkpoint,
        )))? {
            SessionLogResponse::Ok => Ok(()),
            SessionLogResponse::Error { error } => {
                Err(session_log_error("apply_command_checkpoint", error))
            }
            other => Err(unexpected_session_log_response(
                "apply_command_checkpoint",
                other,
            )),
        }
    }

    pub fn list_workspaces(&self) -> Result<Vec<WorkspaceSummary>> {
        match self.call(SessionLogCommand::ListWorkspaces)? {
            SessionLogResponse::Workspaces { workspaces } => Ok(workspaces),
            SessionLogResponse::Error { error } => Err(session_log_error("list_workspaces", error)),
            other => Err(unexpected_session_log_response("list_workspaces", other)),
        }
    }

    pub fn list_sessions(
        &self,
        workspace: String,
        page: u64,
        page_size: u64,
    ) -> Result<(Page, Vec<SessionSnapshot>)> {
        match self.call(SessionLogCommand::ListSessions(ListSessionsRequest {
            workspace,
            page,
            page_size,
        }))? {
            SessionLogResponse::Sessions { page, sessions } => Ok((page, sessions)),
            SessionLogResponse::Error { error } => Err(session_log_error("list_sessions", error)),
            other => Err(unexpected_session_log_response("list_sessions", other)),
        }
    }

    pub fn get_session(&self, session_id: String) -> Result<Option<SessionSnapshot>> {
        match self.call(SessionLogCommand::GetSession(GetSessionRequest {
            session_id,
        }))? {
            SessionLogResponse::Session { session } => Ok(session.map(|session| *session)),
            SessionLogResponse::Error { error } => Err(session_log_error("get_session", error)),
            other => Err(unexpected_session_log_response("get_session", other)),
        }
    }

    pub fn list_session_records(
        &self,
        session_id: String,
        page: u64,
        page_size: u64,
    ) -> Result<(Page, Vec<SessionRecord>)> {
        match self.call(SessionLogCommand::ListSessionRecords(
            ListSessionRecordsRequest {
                session_id,
                page,
                page_size,
            },
        ))? {
            SessionLogResponse::Records { page, records } => Ok((page, records)),
            SessionLogResponse::Error { error } => {
                Err(session_log_error("list_session_records", error))
            }
            other => Err(unexpected_session_log_response(
                "list_session_records",
                other,
            )),
        }
    }

    fn call(&self, command: SessionLogCommand) -> Result<SessionLogResponse> {
        let command_name = session_log_command_name(&command);
        let async_write = session_log_contract::client::is_async_write(&command);
        let command_payload = if async_write || profile_timings::enabled() {
            Some(serde_json::to_vec(&command)?)
        } else {
            None
        };
        let command_bytes = command_payload
            .as_ref()
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        if async_write {
            let enqueue_start = Instant::now();
            if let Some(payload) = command_payload.as_deref() {
                session_log_contract::client::enqueue_serialized_command(payload)?;
            } else {
                session_log_contract::client::enqueue_command(&command)?;
            }
            profile_timings::log_elapsed(
                "session_log_client.enqueue_async_write",
                enqueue_start,
                serde_json::json!({
                    "command": command_name,
                    "command_bytes": command_bytes,
                }),
            );
            return Ok(SessionLogResponse::Ok);
        }
        let ipc_start = Instant::now();
        let ipc_result = call_session_service(&command);
        profile_timings::log_elapsed(
            "session_log_client.call_service",
            ipc_start,
            serde_json::json!({
                "command": command_name,
                "async_write": async_write,
                "command_bytes": command_bytes,
                "success": ipc_result.is_ok(),
            }),
        );
        ipc_result
    }
}

fn session_log_command_name(command: &SessionLogCommand) -> &'static str {
    match command {
        SessionLogCommand::Health => "health",
        SessionLogCommand::CreateSession(_) => "create_session",
        SessionLogCommand::ExecuteSessionCommand(_) => "execute_session_command",
        SessionLogCommand::UpdateSession(_) => "update_session",
        SessionLogCommand::UpdateSessionTodos(_) => "update_session_todos",
        SessionLogCommand::RegisterRuntime(_) => "register_runtime",
        SessionLogCommand::ActivateRuntimeLease(_) => "activate_runtime_lease",
        SessionLogCommand::CommitRuntimeEvent(_) => "commit_runtime_event",
        SessionLogCommand::AppendSessionFeedEvent(_) => "append_session_feed_event",
        SessionLogCommand::ReadSessionFeed(_) => "read_session_feed",
        SessionLogCommand::SubscribeSessionFeed => "subscribe_session_feed",
        SessionLogCommand::ReplayRuntime(_) => "replay_runtime",
        SessionLogCommand::GetRuntimeLease(_) => "get_runtime_lease",
        SessionLogCommand::ListRuntimeLocations(_) => "list_runtime_locations",
        SessionLogCommand::MaintainRuntimeLocations(_) => "maintain_runtime_locations",
        SessionLogCommand::RecoveryCloseRuntime(_) => "recovery_close_runtime",
        SessionLogCommand::PersistSessionDelta(_) => "persist_session_delta",
        SessionLogCommand::ReadContextSlice(_) => "read_context_slice",
        SessionLogCommand::ApplyCommandCheckpoint(_) => "apply_command_checkpoint",
        SessionLogCommand::GetSession(_) => "get_session",
        SessionLogCommand::ListWorkspaces => "list_workspaces",
        SessionLogCommand::ListSessions(_) => "list_sessions",
        SessionLogCommand::ListSessionSummaries(_) => "list_session_summaries",
        SessionLogCommand::ListSessionRecords(_) => "list_session_records",
        SessionLogCommand::MarkSessionInterrupted(_) => "mark_session_interrupted",
        SessionLogCommand::DeleteSession(_) => "delete_session",
        SessionLogCommand::DeleteWorkspace(_) => "delete_workspace",
        SessionLogCommand::Shutdown => "shutdown",
    }
}

fn session_log_error(operation: &str, error: String) -> anyhow::Error {
    anyhow!("session_log {operation} failed: {error}")
}

fn unexpected_session_log_response(operation: &str, response: SessionLogResponse) -> anyhow::Error {
    anyhow!("unexpected session_log response for {operation}: {response:?}")
}

fn call_session_service(command: &SessionLogCommand) -> Result<SessionLogResponse> {
    call_session_service_with(command, session_log_contract::client::call_service)
}

fn call_session_service_with(
    command: &SessionLogCommand,
    transport: impl FnOnce(&SessionLogCommand) -> Result<SessionLogResponse>,
) -> Result<SessionLogResponse> {
    transport(command)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_location_maintenance_has_a_stable_profile_operation_name() {
        let command = SessionLogCommand::MaintainRuntimeLocations(
            session_log_contract::MaintainRuntimeLocationsRequest {
                mode: session_log_contract::RuntimeLocationMaintenanceMode::DryRun,
                page_size: 1,
                expected_dry_run_sha256: None,
            },
        );
        assert_eq!(
            session_log_command_name(&command),
            "maintain_runtime_locations"
        );
    }

    #[test]
    fn data_operation_invokes_exactly_one_transport_without_a_health_preflight() {
        let calls = std::cell::Cell::new(0);
        let command = SessionLogCommand::GetSession(GetSessionRequest {
            session_id: "direct-data-operation".to_string(),
        });
        let response = call_session_service_with(&command, |actual| {
            calls.set(calls.get() + 1);
            assert!(matches!(actual, SessionLogCommand::GetSession(_)));
            Ok(SessionLogResponse::Session { session: None })
        })
        .expect("direct session read");

        assert!(matches!(
            response,
            SessionLogResponse::Session { session: None }
        ));
        assert_eq!(calls.get(), 1);
    }
}
