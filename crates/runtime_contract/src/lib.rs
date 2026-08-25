#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use lifecycle::SessionState;

pub const WORKER_KIND_CALL: &str = "call";
pub const WORKER_KIND_HEALTH_CHECK: &str = "health_check";
pub const DEFAULT_MAXIMUM_RUNTIME_LLM_TURNS: u64 = 256;
pub const MAXIMUM_RUNTIME_LLM_TURN_OPTIONS: [u64; 5] = [64, 128, 256, 1_080, 2_560];
pub const DEFAULT_MAXIMUM_PARALLEL_RUNTIME_WORKERS: usize = 24;
pub const MAXIMUM_PARALLEL_RUNTIME_WORKER_OPTIONS: [usize; 5] = [6, 12, 24, 48, 128];
pub const TASK_CONTEXT_CAPSULE_SCHEMA_VERSION: &str = "task_context_capsule_v1";
pub const MAXIMUM_TASK_CONTEXT_SUMMARY_CHARS: usize = 32_000;
pub const MAXIMUM_TASK_CONTEXT_EVIDENCE_REFS: usize = 64;

pub fn maximum_runtime_llm_turns(value: Option<u64>) -> u64 {
    value
        .filter(|value| MAXIMUM_RUNTIME_LLM_TURN_OPTIONS.contains(value))
        .unwrap_or(DEFAULT_MAXIMUM_RUNTIME_LLM_TURNS)
}

pub fn maximum_parallel_runtime_workers(value: Option<usize>) -> usize {
    value
        .filter(|value| MAXIMUM_PARALLEL_RUNTIME_WORKER_OPTIONS.contains(value))
        .unwrap_or(DEFAULT_MAXIMUM_PARALLEL_RUNTIME_WORKERS)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CallContext {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub input: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LifecycleExecutionContext {
    pub transaction_id: String,
    pub commander_session_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub goal_id: Option<String>,
    #[serde(default)]
    pub operator_override: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskContextMission {
    pub mission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub mode: String,
    pub current_predicate: String,
    pub objective: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskContextEvidenceReference {
    pub id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskContextCapsule {
    pub schema_version: String,
    pub mission: TaskContextMission,
    pub context_summary: String,
    pub dcf_generation: Value,
    pub surface: Value,
    pub authority: Value,
    pub evidence_refs: Vec<TaskContextEvidenceReference>,
    pub focused_verifiers: Vec<Value>,
    pub jspace_semantic_sha256: String,
    pub semantic_sha256: String,
}

impl TaskContextCapsule {
    pub fn from_value(value: Value) -> Result<Self, String> {
        let capsule: Self = serde_json::from_value(value)
            .map_err(|error| format!("TASK_CONTEXT_CAPSULE_MALFORMED: {error}"))?;
        capsule.validate()?;
        Ok(capsule)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != TASK_CONTEXT_CAPSULE_SCHEMA_VERSION {
            return Err(format!(
                "TASK_CONTEXT_SCHEMA_VERSION_UNSUPPORTED: expected {TASK_CONTEXT_CAPSULE_SCHEMA_VERSION}, got {}",
                self.schema_version
            ));
        }
        for (field, value) in [
            ("mission_id", self.mission.mission_id.as_str()),
            ("mode", self.mission.mode.as_str()),
            ("current_predicate", self.mission.current_predicate.as_str()),
            ("objective", self.mission.objective.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("TASK_CONTEXT_MISSION_INVALID: {field} is empty"));
            }
        }
        if self
            .mission
            .task_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("TASK_CONTEXT_MISSION_INVALID: task_id is empty".to_string());
        }
        if self.context_summary.trim().is_empty() {
            return Err("TASK_CONTEXT_SUMMARY_MISSING: context_summary is empty".to_string());
        }
        if self.context_summary.chars().count() > MAXIMUM_TASK_CONTEXT_SUMMARY_CHARS {
            return Err(format!(
                "TASK_CONTEXT_SUMMARY_TOO_LARGE: context_summary exceeds {MAXIMUM_TASK_CONTEXT_SUMMARY_CHARS} characters"
            ));
        }
        if self.evidence_refs.len() > MAXIMUM_TASK_CONTEXT_EVIDENCE_REFS {
            return Err(format!(
                "TASK_CONTEXT_EVIDENCE_INVALID: evidence_refs exceeds {MAXIMUM_TASK_CONTEXT_EVIDENCE_REFS} entries"
            ));
        }
        for evidence in &self.evidence_refs {
            if evidence.id.trim().is_empty() || evidence.kind.trim().is_empty() {
                return Err(
                    "TASK_CONTEXT_EVIDENCE_INVALID: evidence id and kind must be non-empty"
                        .to_string(),
                );
            }
            if evidence
                .sha256
                .as_deref()
                .is_some_and(|digest| !is_lower_sha256(digest))
            {
                return Err(
                    "TASK_CONTEXT_EVIDENCE_INVALID: evidence sha256 must be lowercase SHA-256"
                        .to_string(),
                );
            }
        }
        if !is_lower_sha256(&self.jspace_semantic_sha256) {
            return Err(
                "TASK_CONTEXT_JSPACE_DIGEST_INVALID: jspace_semantic_sha256 must be lowercase SHA-256"
                    .to_string(),
            );
        }
        if !is_lower_sha256(&self.semantic_sha256) {
            return Err(
                "TASK_CONTEXT_SEMANTIC_DIGEST_INVALID: semantic_sha256 must be lowercase SHA-256"
                    .to_string(),
            );
        }
        let mut payload = serde_json::to_value(self)
            .map_err(|error| format!("TASK_CONTEXT_CAPSULE_MALFORMED: {error}"))?;
        payload
            .as_object_mut()
            .expect("TaskContextCapsule serializes as an object")
            .remove("semantic_sha256");
        let expected = semantic_sha256(&payload);
        if self.semantic_sha256 != expected {
            return Err(format!(
                "TASK_CONTEXT_SEMANTIC_DIGEST_MISMATCH: expected {expected}, got {}",
                self.semantic_sha256
            ));
        }
        Ok(())
    }

    pub fn bind_jspace(&self, jspace_contract: Option<&Value>) -> Result<(), String> {
        let digest = jspace_contract
            .and_then(|contract| contract.get("semantic_sha256"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                "TASK_CONTEXT_JSPACE_BINDING_MISSING: capsule requires a J-Space contract"
                    .to_string()
            })?;
        if digest != self.jspace_semantic_sha256 {
            return Err(format!(
                "TASK_CONTEXT_JSPACE_BINDING_MISMATCH: capsule={} contract={digest}",
                self.jspace_semantic_sha256
            ));
        }
        Ok(())
    }

    pub fn provider_context(&self) -> String {
        let evidence_ids = self
            .evidence_refs
            .iter()
            .map(|reference| format!("{}:{}", reference.kind, reference.id))
            .collect::<Vec<_>>();
        let verifier_ids = self
            .focused_verifiers
            .iter()
            .filter_map(|verifier| {
                verifier
                    .get("verifier_id")
                    .or_else(|| verifier.get("command"))
                    .and_then(Value::as_str)
            })
            .collect::<Vec<_>>();
        format!(
            "Task Context Capsule {}\nMission: {}\nMode: {}\nObjective: {}\nCurrent predicate: {}\nBounded context: {}\nEvidence references: {}\nFocused verifiers: {}\nJ-Space digest: {}",
            self.semantic_sha256,
            self.mission.mission_id,
            self.mission.mode,
            self.mission.objective,
            self.mission.current_predicate,
            self.context_summary,
            evidence_ids.join(", "),
            verifier_ids.join(", "),
            self.jspace_semantic_sha256,
        )
    }
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn semantic_sha256(value: &Value) -> String {
    let canonical = canonical_json(value);
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => serde_json::to_string(value).expect("JSON strings are encodable"),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|key| format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("JSON object keys are encodable"),
                        canonical_json(&values[key])
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

impl CallContext {
    pub fn new(method: String, path: String, input: Value) -> Self {
        Self {
            request_id: uuid::Uuid::new_v4().to_string(),
            method,
            path,
            input,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerEnvelope {
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
}

impl WorkerEnvelope {
    pub fn health_check() -> Self {
        Self {
            kind: WORKER_KIND_HEALTH_CHECK.to_string(),
            payload: Value::Object(Default::default()),
        }
    }

    pub fn call(context: CallContext) -> Self {
        Self {
            kind: WORKER_KIND_CALL.to_string(),
            payload: serde_json::json!({ "input": context }),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunAgentRequest {
    pub runtime_id: String,
    pub lease_id: String,
    #[serde(default)]
    pub fallback_from_id: Option<String>,
    #[serde(default)]
    pub lifecycle: Option<LifecycleExecutionContext>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub goal_id: Option<String>,
    #[serde(default)]
    pub operator_override: bool,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub session_type: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub input: Option<Value>,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub depth: Option<usize>,
    #[serde(default)]
    pub runtime_context: Option<String>,
    #[serde(default)]
    pub planning_mode_override: Option<bool>,
    #[serde(default)]
    pub jspace_contract: Option<Value>,
    #[serde(default)]
    pub task_context_capsule: Option<Value>,
    #[serde(default)]
    pub no_op_manual: bool,
    #[serde(default)]
    pub return_log: bool,
    #[serde(default)]
    pub maximum_parallel_runtime_workers: Option<usize>,
    #[serde(default)]
    pub worker_env: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeWorkerResponse {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_state: Option<SessionState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_started_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub session_log: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::RunAgentRequest;

    #[test]
    fn run_agent_request_preserves_optional_retry_lineage_on_the_wire() {
        let retry: RunAgentRequest = serde_json::from_value(serde_json::json!({
            "runtime_id": "runtime-retry",
            "lease_id": "lease-retry",
            "fallback_from_id": "runtime-failed"
        }))
        .expect("retry request should decode");
        assert_eq!(retry.fallback_from_id.as_deref(), Some("runtime-failed"));

        let first: RunAgentRequest = serde_json::from_value(serde_json::json!({
            "runtime_id": "runtime-first",
            "lease_id": "lease-first"
        }))
        .expect("first request should decode without retry lineage");
        assert_eq!(first.fallback_from_id, None);
    }
}
