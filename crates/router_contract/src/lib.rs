#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const IPC_KIND_CALL: &str = "call";
pub const IPC_KIND_HEALTH_CHECK: &str = "health_check";
pub const METHOD_HEALTH_CHECK: &str = "health_check";
pub const METHOD_ENQUEUE_TURN: &str = "execution.enqueue_turn";
pub const METHOD_REGISTER_CHILD_SESSION: &str = "execution.register_child_session";
pub const METHOD_ACKNOWLEDGE_CHILD_CALLBACK: &str = "execution.acknowledge_child_callback";
pub const METHOD_LIST_COMMANDS: &str = "registry.commands.list";
pub const METHOD_EXECUTE_COMMAND: &str = "registry.commands.execute";
pub const METHOD_LIST_TOOLS: &str = "registry.tools.list";
pub const METHOD_GET_TOOL: &str = "registry.tools.get";
pub const METHOD_PATCH_TOOL: &str = "registry.tools.patch";
pub const METHOD_GET_TOOL_CONFIG: &str = "registry.tools.config.get";
pub const METHOD_PATCH_TOOL_CONFIG: &str = "registry.tools.config.patch";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouterEndpoint {
    pub addr: String,
    pub version: String,
    #[serde(default)]
    pub binary_sha256: Option<String>,
    pub pid: Option<u32>,
    pub process_start_time: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IpcRequest {
    pub request_id: String,
    pub kind: String,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

impl IpcRequest {
    pub fn call(request_id: impl Into<String>, method: impl Into<String>, payload: Value) -> Self {
        Self {
            request_id: request_id.into(),
            kind: IPC_KIND_CALL.to_string(),
            method: method.into(),
            payload,
            deadline_ms: None,
        }
    }

    pub fn health_check(request_id: impl Into<String>, deadline_ms: u64) -> Self {
        Self {
            request_id: request_id.into(),
            kind: IPC_KIND_HEALTH_CHECK.to_string(),
            method: METHOD_HEALTH_CHECK.to_string(),
            payload: Value::Object(Default::default()),
            deadline_ms: Some(deadline_ms),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IpcResponse {
    pub request_id: String,
    pub ok: bool,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub error: Option<String>,
}

impl IpcResponse {
    pub fn ok(request_id: impl Into<String>, payload: Value) -> Self {
        Self {
            request_id: request_id.into(),
            ok: true,
            payload,
            error: None,
        }
    }

    pub fn error(request_id: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            ok: false,
            payload: Value::Null,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EnqueueTurnRequest {
    pub runtime_id: String,
    pub session_id: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RegisterChildSessionRequest {
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
    pub session_directory: String,
    pub session_name: String,
    pub created_at_ms: i64,
    pub execution_payload: Value,
}

impl RegisterChildSessionRequest {
    pub fn canonical_effect_id(&self) -> String {
        format!("{}.message", self.child_runtime_id)
    }

    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("parent_session_id", self.parent_session_id.as_str()),
            ("child_session_id", self.child_session_id.as_str()),
            ("child_runtime_id", self.child_runtime_id.as_str()),
            ("child_transaction_id", self.child_transaction_id.as_str()),
            ("child_lease_id", self.child_lease_id.as_str()),
            ("callback_request_id", self.callback_request_id.as_str()),
            ("effect_id", self.effect_id.as_str()),
            ("session_directory", self.session_directory.as_str()),
            ("session_name", self.session_name.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("CHILD_ADMISSION_IDENTITY_MISSING:{name}"));
            }
        }
        match self.commander_thread_id.as_deref() {
            None => {
                return Err("CHILD_ADMISSION_IDENTITY_MISSING:commander_thread_id".to_string());
            }
            Some(value) if value.trim().is_empty() => {
                return Err("CHILD_ADMISSION_IDENTITY_INVALID:commander_thread_id".to_string());
            }
            Some(_) => {}
        }
        for (name, value) in [
            (
                "parent_mission_revision_sha256",
                self.parent_mission_revision_sha256.as_str(),
            ),
            (
                "delegated_input_sha256",
                self.delegated_input_sha256.as_str(),
            ),
        ] {
            if value.len() != 64
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(format!("CHILD_ADMISSION_IDENTITY_INVALID:{name}"));
            }
        }
        if self.parent_session_id == self.child_session_id {
            return Err("CHILD_ADMISSION_IDENTITY_CONFLICT:parent_equals_child".to_string());
        }
        if self.callback_request_id != self.child_transaction_id {
            return Err(
                "CHILD_ADMISSION_CALLBACK_IDENTITY_CONFLICT:callback_request_id".to_string(),
            );
        }
        let canonical_effect_id = self.canonical_effect_id();
        if self.effect_id != canonical_effect_id {
            return Err(format!(
                "CHILD_ADMISSION_EFFECT_IDENTITY_CONFLICT:expected={canonical_effect_id},actual={}",
                self.effect_id
            ));
        }
        if self.created_at_ms <= 0 {
            return Err("CHILD_ADMISSION_IDENTITY_INVALID:created_at_ms".to_string());
        }
        if !self.execution_payload.is_object() {
            return Err("CHILD_ADMISSION_PAYLOAD_INVALID:execution_payload".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RegisterChildSessionOutcome {
    Admitted,
    AlreadyAdmitted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegisterChildSessionResponse {
    pub outcome: RegisterChildSessionOutcome,
    pub parent_session_id: String,
    pub child_session_id: String,
    pub child_runtime_id: String,
    pub child_transaction_id: String,
    pub callback_request_id: String,
    pub effect_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AcknowledgeChildCallbackEffectIdentity {
    Exact { effect_id: String },
    ProvenZeroEffect {
        classification: String,
        evidence_sha256: String,
    },
    UnsettledEffect {
        classification: String,
        evidence_sha256: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AcknowledgeChildCallbackRequest {
    pub parent_session_id: String,
    pub parent_mission_revision_sha256: String,
    pub commander_thread_id: String,
    pub child_session_id: String,
    pub child_runtime_id: String,
    pub child_lease_id: String,
    pub transaction_id: String,
    pub event_id: String,
    pub callback_payload_sha256: String,
    pub effect_identity: AcknowledgeChildCallbackEffectIdentity,
}

impl AcknowledgeChildCallbackRequest {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("parent_session_id", self.parent_session_id.as_str()),
            ("commander_thread_id", self.commander_thread_id.as_str()),
            ("child_session_id", self.child_session_id.as_str()),
            ("child_runtime_id", self.child_runtime_id.as_str()),
            ("child_lease_id", self.child_lease_id.as_str()),
            ("transaction_id", self.transaction_id.as_str()),
            ("event_id", self.event_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("CHILD_CALLBACK_ACK_IDENTITY_MISSING:{name}"));
            }
        }
        for (name, value) in [
            (
                "parent_mission_revision_sha256",
                self.parent_mission_revision_sha256.as_str(),
            ),
            ("callback_payload_sha256", self.callback_payload_sha256.as_str()),
        ] {
            if !is_lower_hex_sha256(value) {
                return Err(format!("CHILD_CALLBACK_ACK_IDENTITY_INVALID:{name}"));
            }
        }
        if self.parent_session_id == self.child_session_id {
            return Err("CHILD_CALLBACK_ACK_IDENTITY_CONFLICT:parent_equals_child".to_string());
        }
        match &self.effect_identity {
            AcknowledgeChildCallbackEffectIdentity::Exact { effect_id } => {
                if effect_id.trim().is_empty() {
                    return Err("CHILD_CALLBACK_ACK_IDENTITY_MISSING:effect_id".to_string());
                }
            }
            AcknowledgeChildCallbackEffectIdentity::ProvenZeroEffect {
                classification,
                evidence_sha256,
            }
            | AcknowledgeChildCallbackEffectIdentity::UnsettledEffect {
                classification,
                evidence_sha256,
            } => {
                if classification.trim().is_empty() {
                    return Err(
                        "CHILD_CALLBACK_ACK_IDENTITY_MISSING:effect_classification".to_string(),
                    );
                }
                if !is_lower_hex_sha256(evidence_sha256) {
                    return Err(
                        "CHILD_CALLBACK_ACK_IDENTITY_INVALID:effect_evidence_sha256".to_string(),
                    );
                }
            }
        }
        Ok(())
    }
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcknowledgeChildCallbackOutcome {
    Acknowledged,
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AcknowledgeChildCallbackResponse {
    pub outcome: AcknowledgeChildCallbackOutcome,
    pub parent_session_id: String,
    pub parent_mission_revision_sha256: String,
    pub commander_thread_id: String,
    pub child_session_id: String,
    pub child_runtime_id: String,
    pub child_lease_id: String,
    pub transaction_id: String,
    pub event_id: String,
    pub callback_payload_sha256: String,
    pub effect_identity: AcknowledgeChildCallbackEffectIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CancelRuntimeRequest {
    pub session_id: String,
    pub runtime_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProbeSessionsRequest {
    #[serde(default)]
    pub session_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListCommandsRequest {
    pub directory: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommandSpec {
    pub name: String,
    pub description: String,
    pub agent: Option<String>,
    pub model: Option<String>,
    pub source: String,
    pub template: Option<String>,
    pub subtask: bool,
    pub hints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListCommandsResponse {
    pub commands: Vec<CommandSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecuteCommandRequest {
    pub directory: Option<String>,
    pub command: String,
    pub args: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecuteCommandResponse {
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToolRegistryRequest {
    pub repo_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToolRequest {
    pub repo_root: String,
    pub tool_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConfigurableEntry {
    pub key: String,
    #[serde(default)]
    pub label: String,
    pub description: String,
    #[serde(rename = "type")]
    pub value_type: String,
    pub default: Value,
    #[serde(default, rename = "enum")]
    pub enum_values: Vec<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default = "default_config_scope")]
    pub scope: String,
}

fn default_config_scope() -> String {
    "workspace".to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolState {
    Discovered,
    Configured,
    Enabled,
    Disabled,
    Unavailable,
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ToolView {
    pub id: String,
    pub name: String,
    pub description: String,
    pub core: bool,
    pub category: String,
    pub execution: String,
    pub enabled: bool,
    pub aliases: Vec<String>,
    pub supports_macro_command: bool,
    pub mutating: bool,
    pub network: bool,
    pub configurable: Vec<ConfigurableEntry>,
    pub state: ToolState,
    pub binary: Option<String>,
    pub binary_path: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToolPatch {
    pub enabled: Option<bool>,
    pub aliases: Option<Vec<String>>,
    pub core: Option<bool>,
    pub execution: Option<String>,
    pub binary: Option<String>,
    pub mutating: Option<bool>,
    pub network: Option<bool>,
    pub policy: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PatchToolRequest {
    pub repo_root: String,
    pub tool_id: String,
    pub patch: ToolPatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ToolConfigResponse {
    pub id: String,
    pub configurable: Vec<ConfigurableEntry>,
    pub values: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PatchToolConfigRequest {
    pub repo_root: String,
    pub tool_id: String,
    pub values: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListToolsResponse {
    pub tools: Vec<ToolView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GetToolResponse {
    pub tool: Option<ToolView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GetToolConfigResponse {
    pub config: Option<ToolConfigResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn child_request() -> RegisterChildSessionRequest {
        RegisterChildSessionRequest {
            parent_session_id: "parent-1".to_string(),
            parent_mission_revision_sha256: "a".repeat(64),
            commander_thread_id: Some("commander-thread-1".to_string()),
            child_session_id: "child-1".to_string(),
            child_runtime_id: "runtime-1".to_string(),
            child_transaction_id: "callback-1".to_string(),
            child_lease_id: "lease-1".to_string(),
            callback_request_id: "callback-1".to_string(),
            effect_id: "runtime-1.message".to_string(),
            delegated_input_sha256: "b".repeat(64),
            session_directory: "/tmp/child-1".to_string(),
            session_name: "delegated child".to_string(),
            created_at_ms: 1_788_000_000_000,
            execution_payload: json!({"prompt": "perform delegated work"}),
        }
    }

    fn callback_ack_request() -> AcknowledgeChildCallbackRequest {
        AcknowledgeChildCallbackRequest {
            parent_session_id: "parent-1".to_string(),
            parent_mission_revision_sha256: "a".repeat(64),
            commander_thread_id: "commander-thread-1".to_string(),
            child_session_id: "child-1".to_string(),
            child_runtime_id: "runtime-1".to_string(),
            child_lease_id: "lease-1".to_string(),
            transaction_id: "callback-1".to_string(),
            event_id: "event-1".to_string(),
            callback_payload_sha256: "b".repeat(64),
            effect_identity: AcknowledgeChildCallbackEffectIdentity::Exact {
                effect_id: "runtime-1.message".to_string(),
            },
        }
    }

    #[test]
    fn child_admission_contract_binds_all_required_identities() {
        let request = child_request();
        request.validate().expect("valid child admission");
        let encoded = serde_json::to_value(&request).expect("serialize request");
        let decoded: RegisterChildSessionRequest =
            serde_json::from_value(encoded).expect("deserialize request");
        assert_eq!(decoded, request);
    }

    #[test]
    fn child_admission_contract_rejects_missing_and_changed_callback_identity() {
        let mut missing_commander_thread = child_request();
        missing_commander_thread.commander_thread_id = None;
        assert_eq!(
            missing_commander_thread.validate().unwrap_err(),
            "CHILD_ADMISSION_IDENTITY_MISSING:commander_thread_id"
        );

        let mut blank_commander_thread = child_request();
        blank_commander_thread.commander_thread_id = Some("   ".to_string());
        assert_eq!(
            blank_commander_thread.validate().unwrap_err(),
            "CHILD_ADMISSION_IDENTITY_INVALID:commander_thread_id"
        );

        let mut missing_parent = child_request();
        missing_parent.parent_session_id.clear();
        assert_eq!(
            missing_parent.validate().unwrap_err(),
            "CHILD_ADMISSION_IDENTITY_MISSING:parent_session_id"
        );

        let mut missing_child = child_request();
        missing_child.child_session_id.clear();
        assert_eq!(
            missing_child.validate().unwrap_err(),
            "CHILD_ADMISSION_IDENTITY_MISSING:child_session_id"
        );

        let mut changed_callback = child_request();
        changed_callback.callback_request_id = "callback-2".to_string();
        assert_eq!(
            changed_callback.validate().unwrap_err(),
            "CHILD_ADMISSION_CALLBACK_IDENTITY_CONFLICT:callback_request_id"
        );

        let mut missing_effect = child_request();
        missing_effect.effect_id.clear();
        assert_eq!(
            missing_effect.validate().unwrap_err(),
            "CHILD_ADMISSION_IDENTITY_MISSING:effect_id"
        );

        let mut changed_effect = child_request();
        changed_effect.effect_id = "foreign.message".to_string();
        assert_eq!(
            changed_effect.validate().unwrap_err(),
            "CHILD_ADMISSION_EFFECT_IDENTITY_CONFLICT:expected=runtime-1.message,actual=foreign.message"
        );

        let mut uppercase_revision = child_request();
        uppercase_revision.parent_mission_revision_sha256 = "A".repeat(64);
        assert_eq!(
            uppercase_revision.validate().unwrap_err(),
            "CHILD_ADMISSION_IDENTITY_INVALID:parent_mission_revision_sha256"
        );
    }

    #[test]
    fn child_callback_ack_contract_binds_exact_and_zero_effect_identities() {
        let exact = callback_ack_request();
        exact.validate().expect("valid exact callback ack");
        let decoded: AcknowledgeChildCallbackRequest = serde_json::from_value(
            serde_json::to_value(&exact).expect("serialize exact callback ack"),
        )
        .expect("deserialize exact callback ack");
        assert_eq!(decoded, exact);

        let mut zero = callback_ack_request();
        zero.effect_identity = AcknowledgeChildCallbackEffectIdentity::ProvenZeroEffect {
            classification: "pre_provider_zero_effect".to_string(),
            evidence_sha256: "c".repeat(64),
        };
        zero.validate().expect("valid zero-effect callback ack");
    }

    #[test]
    fn child_callback_ack_contract_rejects_missing_or_malformed_identity() {
        let mut missing_thread = callback_ack_request();
        missing_thread.commander_thread_id = "   ".to_string();
        assert_eq!(
            missing_thread.validate().unwrap_err(),
            "CHILD_CALLBACK_ACK_IDENTITY_MISSING:commander_thread_id"
        );

        let mut bad_payload_hash = callback_ack_request();
        bad_payload_hash.callback_payload_sha256 = "B".repeat(64);
        assert_eq!(
            bad_payload_hash.validate().unwrap_err(),
            "CHILD_CALLBACK_ACK_IDENTITY_INVALID:callback_payload_sha256"
        );

        let mut missing_effect = callback_ack_request();
        missing_effect.effect_identity = AcknowledgeChildCallbackEffectIdentity::Exact {
            effect_id: String::new(),
        };
        assert_eq!(
            missing_effect.validate().unwrap_err(),
            "CHILD_CALLBACK_ACK_IDENTITY_MISSING:effect_id"
        );
    }
}
