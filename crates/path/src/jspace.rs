//! Local admission and Path x Operation enforcement for an optional DCF
//! J-Space contract.
//!
//! The matcher contains no DCF, provider, network, or LLM dependency. A
//! contract is parsed and digest-checked once by `JSpaceAdmissionCache`; later
//! checks use only the compiled tries and the existing workspace boundary.

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const JSPACE_SCHEMA_VERSION: &str = "jspace_contract_v1";
pub const JSPACE_EXPANSION_REQUIRED: &str = "JSPACE_EXPANSION_REQUIRED";

const KNOWN_OPERATIONS: &[&str] = &["read", "create", "modify", "delete", "command"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JSpaceError {
    code: String,
    operation: String,
    target: String,
    detail: String,
}

impl JSpaceError {
    pub fn new(
        code: impl Into<String>,
        operation: &str,
        target: &str,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            operation: operation.to_string(),
            target: target.to_string(),
            detail: detail.into(),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn target(&self) -> &str {
        &self.target
    }
}

impl Display for JSpaceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}: operation={} target={} detail={}",
            self.code,
            if self.operation.is_empty() {
                "<none>"
            } else {
                &self.operation
            },
            if self.target.is_empty() {
                "<none>"
            } else {
                &self.target
            },
            self.detail
        )
    }
}

impl Error for JSpaceError {}

#[derive(Clone, Debug)]
struct PathTrieNode {
    children: HashMap<String, usize>,
    exact: bool,
    recursive: bool,
}

#[derive(Clone, Debug, Default)]
struct PathTrie {
    nodes: Vec<PathTrieNode>,
}

impl PathTrie {
    fn new() -> Self {
        Self {
            nodes: vec![PathTrieNode {
                children: HashMap::new(),
                exact: false,
                recursive: false,
            }],
        }
    }

    fn insert(&mut self, raw_scope: &str) -> Result<(), JSpaceError> {
        let (components, recursive) = scope_components(raw_scope)?;
        let mut node_index = 0;
        for component in components {
            let next_index = if let Some(index) = self.nodes[node_index].children.get(&component) {
                *index
            } else {
                let index = self.nodes.len();
                self.nodes.push(PathTrieNode {
                    children: HashMap::new(),
                    exact: false,
                    recursive: false,
                });
                self.nodes[node_index].children.insert(component, index);
                index
            };
            node_index = next_index;
        }
        if recursive {
            self.nodes[node_index].recursive = true;
        } else {
            self.nodes[node_index].exact = true;
        }
        Ok(())
    }

    fn matches(&self, relative: &str) -> bool {
        let mut node_index = 0;
        if self.nodes[node_index].recursive {
            return true;
        }
        for component in relative.split('/').filter(|part| !part.is_empty()) {
            let Some(next_index) = self.nodes[node_index].children.get(component) else {
                return false;
            };
            node_index = *next_index;
            if self.nodes[node_index].recursive {
                return true;
            }
        }
        self.nodes[node_index].exact
    }
}

#[derive(Clone, Debug)]
struct ByteTrieNode {
    children: HashMap<u8, usize>,
    terminal: bool,
}

#[derive(Clone, Debug, Default)]
struct BytePrefixTrie {
    nodes: Vec<ByteTrieNode>,
}

impl BytePrefixTrie {
    fn new() -> Self {
        Self {
            nodes: vec![ByteTrieNode {
                children: HashMap::new(),
                terminal: false,
            }],
        }
    }

    fn insert(&mut self, prefix: &str) -> Result<(), JSpaceError> {
        if prefix.trim().is_empty() {
            return Err(JSpaceError::new(
                "JSPACE_COMMAND_PREFIX_INVALID",
                "command",
                prefix,
                "command prefix must be non-empty",
            ));
        }
        let mut node_index = 0;
        for byte in prefix.as_bytes() {
            let next_index = if let Some(index) = self.nodes[node_index].children.get(byte) {
                *index
            } else {
                let index = self.nodes.len();
                self.nodes.push(ByteTrieNode {
                    children: HashMap::new(),
                    terminal: false,
                });
                self.nodes[node_index].children.insert(*byte, index);
                index
            };
            node_index = next_index;
        }
        self.nodes[node_index].terminal = true;
        Ok(())
    }

    fn matches(&self, command: &str) -> bool {
        let mut node_index = 0;
        for byte in command.as_bytes() {
            let Some(next_index) = self.nodes[node_index].children.get(byte) else {
                return false;
            };
            node_index = *next_index;
            if self.nodes[node_index].terminal {
                return true;
            }
        }
        self.nodes[node_index].terminal
    }
}

#[derive(Clone, Debug)]
pub struct JSpaceMatcher {
    repo_root: PathBuf,
    lexical_repo_root: PathBuf,
    digest: String,
    read_scopes: PathTrie,
    write_scopes: PathTrie,
    allowed_operations: HashSet<String>,
    denied_operations: HashSet<String>,
    command_prefixes: BytePrefixTrie,
}

impl JSpaceMatcher {
    pub fn from_value(session_root: &Path, contract: &Value) -> Result<Self, JSpaceError> {
        let object = contract.as_object().ok_or_else(|| {
            JSpaceError::new(
                "JSPACE_CONTRACT_MALFORMED",
                "admission",
                "",
                "contract must be a JSON object",
            )
        })?;
        let schema_version = required_string(object, "schema_version")?;
        if schema_version != JSPACE_SCHEMA_VERSION {
            return Err(JSpaceError::new(
                "JSPACE_SCHEMA_VERSION_UNSUPPORTED",
                "admission",
                "",
                format!("expected {JSPACE_SCHEMA_VERSION}, got {schema_version}"),
            ));
        }
        let claimed_digest = required_string(object, "semantic_sha256")?;
        if claimed_digest.len() != 64
            || !claimed_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(JSpaceError::new(
                "JSPACE_SEMANTIC_DIGEST_INVALID",
                "admission",
                "",
                "semantic_sha256 must be 64 hexadecimal characters",
            ));
        }
        let mut payload = contract.clone();
        let payload_object = payload.as_object_mut().ok_or_else(|| {
            JSpaceError::new(
                "JSPACE_CONTRACT_MALFORMED",
                "admission",
                "",
                "contract must be an object",
            )
        })?;
        payload_object.remove("semantic_sha256");
        let expected_digest = semantic_sha256(&payload);
        if expected_digest != claimed_digest {
            return Err(JSpaceError::new(
                "JSPACE_SEMANTIC_DIGEST_MISMATCH",
                "admission",
                "",
                format!("expected {expected_digest}, got {claimed_digest}"),
            ));
        }

        let contract_root = PathBuf::from(required_string(object, "repo_root")?);
        let lexical_repo_root = session_root.to_path_buf();
        let session_root = normalized_root(session_root)?;
        let contract_root = normalized_root(&contract_root)?;
        if contract_root != session_root {
            return Err(JSpaceError::new(
                "JSPACE_ROOT_MISMATCH",
                "admission",
                &contract_root.display().to_string(),
                format!("session root is {}", session_root.display()),
            ));
        }
        validate_expansion_rule(object)?;
        validate_object_field(object, "dcf_generation")?;
        validate_object_field(object, "provenance")?;
        validate_string_array_field(object, "matched_surface_ids")?;
        validate_object_array_field(object, "focused_verifiers")?;
        validate_string_array_field(object, "declared_targets")?;

        let read_values = required_string_array(object, "read_scopes")?;
        let write_values = required_string_array(object, "write_scopes")?;
        let allowed_values = required_string_array(object, "allowed_operations")?;
        let denied_values = required_string_array(object, "denied_operations")?;
        let command_values = required_string_array(object, "command_prefixes")?;
        let allowed_operations = operation_set(allowed_values, "allowed_operations")?;
        let denied_operations = operation_set(denied_values, "denied_operations")?;
        if allowed_operations.contains("delete") && denied_operations.contains("delete") {
            return Err(JSpaceError::new(
                "JSPACE_OPERATION_CONFLICT",
                "delete",
                "",
                "delete cannot be both allowed and denied",
            ));
        }
        let mut read_scopes = PathTrie::new();
        for scope in read_values {
            read_scopes.insert(&scope)?;
        }
        let mut write_scopes = PathTrie::new();
        for scope in write_values {
            write_scopes.insert(&scope)?;
        }
        let mut command_prefixes = BytePrefixTrie::new();
        for prefix in command_values {
            command_prefixes.insert(&prefix)?;
        }

        Ok(Self {
            repo_root: session_root,
            lexical_repo_root,
            digest: claimed_digest,
            read_scopes,
            write_scopes,
            allowed_operations,
            denied_operations,
            command_prefixes,
        })
    }

    pub fn semantic_sha256(&self) -> &str {
        &self.digest
    }

    pub fn repo_root(&self) -> &Path {
        &self.repo_root
    }

    pub fn check_path(&self, operation: &str, target: &Path) -> Result<(), JSpaceError> {
        self.check_operation(operation, &target.display().to_string())?;
        let relative = self.resolve_target(target, operation)?;
        let allowed = if operation == "read" {
            self.read_scopes.matches(&relative)
        } else {
            if operation == "delete" && !self.allowed_operations.contains("delete") {
                return Err(JSpaceError::new(
                    "JSPACE_DELETE_NOT_GRANTED",
                    operation,
                    &target.display().to_string(),
                    "delete requires an explicit delete operation grant",
                ));
            }
            self.write_scopes.matches(&relative)
        };
        if allowed {
            return Ok(());
        }
        Err(JSpaceError::new(
            JSPACE_EXPANSION_REQUIRED,
            operation,
            &target.display().to_string(),
            "exact target is inside the root but outside the declared scope",
        ))
    }

    pub fn check_command(&self, command_type: &str, command_line: &str) -> Result<(), JSpaceError> {
        let command_type = normalize_command_type(command_type);
        if !is_known_command_type(&command_type) {
            return Err(JSpaceError::new(
                "JSPACE_UNKNOWN_TOOL",
                "command",
                &command_type,
                "command type is not in the local J-Space tool set",
            ));
        }
        self.check_operation("command", &command_type)?;
        match command_type.as_str() {
            "shell_command" | "bash" | "zsh" => {
                let (command, workdir) = shell_command_parts(command_line);
                if !self.command_prefixes.matches(&command) {
                    return Err(JSpaceError::new(
                        "JSPACE_COMMAND_DENIED",
                        "command",
                        &command,
                        "command does not match an admitted prefix",
                    ));
                }
                if let Some(workdir) = workdir {
                    self.resolve_target(Path::new(&workdir), "command")?;
                }
                Ok(())
            }
            "apply_patch" | "planning" | "task_status" => Ok(()),
            "web_discover" => Err(JSpaceError::new(
                "JSPACE_NETWORK_DENIED",
                "network",
                &command_type,
                "network command is not admitted by J-Space",
            )),
            "generate_media" => Err(JSpaceError::new(
                "JSPACE_INSTALL_DENIED",
                "install",
                &command_type,
                "external media command is not admitted by J-Space",
            )),
            "read_media" => Err(JSpaceError::new(
                "JSPACE_COMMAND_DENIED",
                "command",
                &command_type,
                "external command is not admitted by J-Space",
            )),
            _ => Err(JSpaceError::new(
                "JSPACE_UNKNOWN_TOOL",
                "command",
                &command_type,
                "command type is not supported",
            )),
        }
    }

    pub fn ensure_in_root(&self, target: &Path, operation: &str) -> Result<(), JSpaceError> {
        self.resolve_target(target, operation).map(|_| ())
    }

    fn check_operation(&self, operation: &str, target: &str) -> Result<(), JSpaceError> {
        if !KNOWN_OPERATIONS.contains(&operation) {
            return Err(JSpaceError::new(
                "JSPACE_UNKNOWN_OPERATION",
                operation,
                target,
                "operation is not in the shared J-Space schema",
            ));
        }
        if self.denied_operations.contains(operation) {
            return Err(JSpaceError::new(
                "JSPACE_OPERATION_DENIED",
                operation,
                target,
                "operation is explicitly denied",
            ));
        }
        if !self.allowed_operations.contains(operation) {
            return Err(JSpaceError::new(
                "JSPACE_OPERATION_DENIED",
                operation,
                target,
                "operation is not explicitly allowed",
            ));
        }
        Ok(())
    }

    fn resolve_target(&self, target: &Path, operation: &str) -> Result<String, JSpaceError> {
        if target.as_os_str().is_empty() {
            return Err(JSpaceError::new(
                "JSPACE_TARGET_INVALID",
                operation,
                "",
                "target must be non-empty",
            ));
        }
        if target
            .components()
            .any(|component| component == Component::ParentDir)
        {
            return Err(JSpaceError::new(
                "JSPACE_PATH_TRAVERSAL",
                operation,
                &target.display().to_string(),
                "parent traversal is not admitted",
            ));
        }
        let lexical = if target.is_absolute() {
            target.to_path_buf()
        } else {
            self.repo_root.join(target)
        };
        let lexical_within_root = lexical.strip_prefix(&self.repo_root).is_ok()
            || (target.is_absolute() && target.strip_prefix(&self.lexical_repo_root).is_ok());
        let resolved = resolve_existing_boundary(&lexical).map_err(|error| {
            JSpaceError::new(
                "JSPACE_PATH_RESOLUTION_FAILED",
                operation,
                &target.display().to_string(),
                error,
            )
        })?;
        if resolved.strip_prefix(&self.repo_root).is_err() {
            let code = if lexical_within_root {
                "JSPACE_SYMLINK_ESCAPE"
            } else {
                "JSPACE_PATH_OUTSIDE_ROOT"
            };
            return Err(JSpaceError::new(
                code,
                operation,
                &target.display().to_string(),
                format!("resolved target is {}", resolved.display()),
            ));
        }
        let relative = resolved
            .strip_prefix(&self.repo_root)
            .map_err(|_| {
                JSpaceError::new(
                    "JSPACE_PATH_OUTSIDE_ROOT",
                    operation,
                    &target.display().to_string(),
                    "target cannot be represented relative to the admitted root",
                )
            })?
            .to_string_lossy()
            .replace('\\', "/");
        Ok(relative)
    }
}

#[derive(Clone, Default)]
pub struct JSpaceAdmissionCache {
    entries: Arc<Mutex<HashMap<String, Arc<JSpaceMatcher>>>>,
    admissions: Arc<AtomicUsize>,
}

impl std::fmt::Debug for JSpaceAdmissionCache {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JSpaceAdmissionCache")
            .field(
                "entries",
                &self
                    .entries
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .len(),
            )
            .field("admissions", &self.admissions.load(Ordering::SeqCst))
            .finish()
    }
}

impl JSpaceAdmissionCache {
    pub fn admit(
        &self,
        session_id: &str,
        session_root: &Path,
        contract: Option<&Value>,
    ) -> Result<Option<Arc<JSpaceMatcher>>, JSpaceError> {
        let Some(contract) = contract else {
            return Ok(None);
        };
        if session_id.trim().is_empty() {
            return Err(JSpaceError::new(
                "JSPACE_SESSION_ID_MISSING",
                "admission",
                "",
                "a J-Space contract requires a session identity",
            ));
        }
        let normalized_session_root = normalized_root(session_root)?;
        let claimed_digest = contract
            .get("semantic_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                JSpaceError::new(
                    "JSPACE_CONTRACT_MALFORMED",
                    "admission",
                    "",
                    "semantic_sha256 is required",
                )
            })?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = entries.get(session_id) {
            if existing.repo_root() != normalized_session_root {
                return Err(JSpaceError::new(
                    "JSPACE_ROOT_MISMATCH",
                    "admission",
                    session_id,
                    format!(
                        "existing root is {}, session root is {}",
                        existing.repo_root().display(),
                        normalized_session_root.display()
                    ),
                ));
            }
            if existing.semantic_sha256() != claimed_digest {
                return Err(JSpaceError::new(
                    "JSPACE_CONTRACT_CHANGED",
                    "admission",
                    session_id,
                    format!(
                        "existing digest {} cannot be replaced by {}",
                        existing.semantic_sha256(),
                        claimed_digest
                    ),
                ));
            }
            return Ok(Some(Arc::clone(existing)));
        }
        let matcher = Arc::new(JSpaceMatcher::from_value(
            &normalized_session_root,
            contract,
        )?);
        entries.insert(session_id.to_string(), Arc::clone(&matcher));
        self.admissions.fetch_add(1, Ordering::SeqCst);
        Ok(Some(matcher))
    }

    pub fn admissions(&self) -> usize {
        self.admissions.load(Ordering::SeqCst)
    }
}

fn required_string(object: &Map<String, Value>, key: &str) -> Result<String, JSpaceError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            JSpaceError::new(
                "JSPACE_CONTRACT_MALFORMED",
                "admission",
                key,
                "required non-empty string is missing",
            )
        })
}

fn required_string_array(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, JSpaceError> {
    let Some(Value::Array(values)) = object.get(key) else {
        return Err(JSpaceError::new(
            "JSPACE_CONTRACT_MALFORMED",
            "admission",
            key,
            "required array of strings is missing",
        ));
    };
    values
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                JSpaceError::new(
                    "JSPACE_CONTRACT_MALFORMED",
                    "admission",
                    key,
                    "array member is not a string",
                )
            })
        })
        .collect()
}

fn operation_set(values: Vec<String>, field: &str) -> Result<HashSet<String>, JSpaceError> {
    let mut result = HashSet::new();
    for value in values {
        if !KNOWN_OPERATIONS.contains(&value.as_str())
            && !matches!(value.as_str(), "network" | "install" | "system_mutation")
        {
            return Err(JSpaceError::new(
                "JSPACE_UNKNOWN_OPERATION",
                "admission",
                field,
                format!("unknown operation {value}"),
            ));
        }
        result.insert(value);
    }
    Ok(result)
}

fn validate_object_field(object: &Map<String, Value>, key: &str) -> Result<(), JSpaceError> {
    if !object.get(key).is_some_and(Value::is_object) {
        return Err(JSpaceError::new(
            "JSPACE_CONTRACT_MALFORMED",
            "admission",
            key,
            "required object is missing",
        ));
    }
    Ok(())
}

fn validate_string_array_field(object: &Map<String, Value>, key: &str) -> Result<(), JSpaceError> {
    required_string_array(object, key).map(|_| ())
}

fn validate_object_array_field(object: &Map<String, Value>, key: &str) -> Result<(), JSpaceError> {
    let Some(Value::Array(values)) = object.get(key) else {
        return Err(JSpaceError::new(
            "JSPACE_CONTRACT_MALFORMED",
            "admission",
            key,
            "required object array is missing",
        ));
    };
    if values.iter().all(Value::is_object) {
        Ok(())
    } else {
        Err(JSpaceError::new(
            "JSPACE_CONTRACT_MALFORMED",
            "admission",
            key,
            "array member is not an object",
        ))
    }
}

fn validate_expansion_rule(object: &Map<String, Value>) -> Result<(), JSpaceError> {
    let Some(expansion) = object.get("expansion").and_then(Value::as_object) else {
        return Err(JSpaceError::new(
            "JSPACE_CONTRACT_MALFORMED",
            "admission",
            "expansion",
            "expansion rule is missing",
        ));
    };
    if expansion.get("mode").and_then(Value::as_str) != Some("exact_target_only")
        || expansion.get("error_code").and_then(Value::as_str) != Some(JSPACE_EXPANSION_REQUIRED)
        || expansion
            .get("mutation_on_expansion")
            .and_then(Value::as_bool)
            != Some(false)
    {
        return Err(JSpaceError::new(
            "JSPACE_EXPANSION_RULE_INVALID",
            "admission",
            "expansion",
            "exact-target no-mutation rule is invalid",
        ));
    }
    Ok(())
}

fn scope_components(raw_scope: &str) -> Result<(Vec<String>, bool), JSpaceError> {
    let mut scope = raw_scope.trim().replace('\\', "/");
    if scope.is_empty() {
        return Err(JSpaceError::new(
            "JSPACE_SCOPE_INVALID",
            "admission",
            raw_scope,
            "scope must be non-empty",
        ));
    }
    if scope.starts_with('/') || (scope.len() > 1 && scope.as_bytes()[1] == b':') {
        return Err(JSpaceError::new(
            "JSPACE_SCOPE_INVALID",
            "admission",
            raw_scope,
            "scope must be relative to repo_root",
        ));
    }
    let recursive = scope == "**" || scope.ends_with("/**");
    if recursive {
        scope = scope.strip_suffix("/**").unwrap_or_default().to_string();
    }
    if scope.starts_with("./") {
        scope = scope[2..].to_string();
    }
    if scope.contains('*') {
        return Err(JSpaceError::new(
            "JSPACE_SCOPE_INVALID",
            "admission",
            raw_scope,
            "only a terminal /** wildcard is supported",
        ));
    }
    let mut components = Vec::new();
    for component in scope
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
    {
        if component == ".." {
            return Err(JSpaceError::new(
                "JSPACE_PATH_TRAVERSAL",
                "admission",
                raw_scope,
                "scope contains parent traversal",
            ));
        }
        components.push(component.to_string());
    }
    Ok((components, recursive))
}

fn normalized_root(path: &Path) -> Result<PathBuf, JSpaceError> {
    if !path.exists() || !path.is_dir() {
        return Err(JSpaceError::new(
            "JSPACE_ROOT_INVALID",
            "admission",
            &path.display().to_string(),
            "repo root must be an existing directory",
        ));
    }
    Ok(crate::normalize_path(path))
}

fn resolve_existing_boundary(path: &Path) -> Result<PathBuf, String> {
    let mut missing = Vec::new();
    let mut existing = path.to_path_buf();
    while !existing.exists() {
        let Some(name) = existing.file_name() else {
            return Err(format!("no existing ancestor for {}", path.display()));
        };
        missing.push(name.to_os_string());
        existing.pop();
    }
    let mut resolved = existing
        .canonicalize()
        .map_err(|error| format!("failed to canonicalize {}: {error}", existing.display()))?;
    for component in missing.iter().rev() {
        resolved.push(component);
    }
    Ok(crate::normalize_path(&resolved))
}

fn normalize_command_type(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "bash" | "zsh" | "shell" | "shells" | "shell_command" | "shll" | "shall" => {
            "shell_command".to_string()
        }
        "apply_patch" => "apply_patch".to_string(),
        "planning" => "planning".to_string(),
        "task_status" => "task_status".to_string(),
        "web_discover" | "web_search" | "web_fetch" => "web_discover".to_string(),
        "generate_media" | "image_gen" | "generate_image" => "generate_media".to_string(),
        "read_media" | "view_media" | "inspect_media" => "read_media".to_string(),
        other => other.to_string(),
    }
}

fn is_known_command_type(command_type: &str) -> bool {
    matches!(
        command_type,
        "shell_command"
            | "bash"
            | "zsh"
            | "apply_patch"
            | "planning"
            | "task_status"
            | "web_discover"
            | "generate_media"
            | "read_media"
    )
}

fn shell_command_parts(raw: &str) -> (String, Option<String>) {
    let parsed = serde_json::from_str::<Value>(raw).ok();
    let command = parsed
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|object| object.get("command").or_else(|| object.get("cmd")))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| raw.trim())
        .to_string();
    let workdir = parsed
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|object| object.get("workdir").or_else(|| object.get("cwd")))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    (command, workdir)
}

pub fn semantic_sha256(payload: &Value) -> String {
    let canonical = canonical_json(payload);
    let digest = Sha256::digest(canonical.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => canonical_json_string(value),
        Value::Array(values) => {
            let members = values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{members}]")
        }
        Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            let members = keys
                .into_iter()
                .filter_map(|key| {
                    object.get(key).map(|value| {
                        format!("{}:{}", canonical_json_string(key), canonical_json(value))
                    })
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{members}}}")
        }
    }
}

fn canonical_json_string(value: &str) -> String {
    let mut output = String::from("\"");
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                output.push_str(&format!("\\u{:04x}", character as u32))
            }
            character if character.is_ascii() => output.push(character),
            character => {
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units).iter() {
                    output.push_str(&format!("\\u{:04x}", unit));
                }
            }
        }
    }
    output.push('"');
    output
}

#[cfg(test)]
mod tests {
    use super::{semantic_sha256, JSpaceAdmissionCache, JSpaceMatcher, JSPACE_EXPANSION_REQUIRED};
    use serde_json::{json, Value};
    use std::fs;

    fn contract(root: &std::path::Path) -> Value {
        let mut value = json!({
            "schema_version": "jspace_contract_v1",
            "repo_root": root,
            "dcf_generation": {"generation_id": "g"},
            "provenance": {"matched_surface_ids": ["surface"]},
            "matched_surface_ids": ["surface"],
            "read_scopes": ["src/**"],
            "write_scopes": ["src/**"],
            "allowed_operations": ["read", "create", "modify", "command"],
            "denied_operations": ["network", "install", "system_mutation"],
            "command_prefixes": ["git status"],
            "focused_verifiers": [],
            "declared_targets": [],
            "expansion": {
                "mode": "exact_target_only",
                "error_code": "JSPACE_EXPANSION_REQUIRED",
                "mutation_on_expansion": false
            }
        });
        let digest = semantic_sha256(&value);
        value["semantic_sha256"] = Value::String(digest);
        value
    }

    #[test]
    fn admission_reuses_same_digest_and_rejects_changed_digest() {
        let root = tempfile::tempdir().expect("root");
        let cache = JSpaceAdmissionCache::default();
        let value = contract(root.path());
        let first = cache
            .admit("session", root.path(), Some(&value))
            .expect("first admission");
        let second = cache
            .admit("session", root.path(), Some(&value))
            .expect("reuse");
        assert!(std::sync::Arc::ptr_eq(
            &first.expect("matcher"),
            &second.expect("matcher")
        ));
        assert_eq!(cache.admissions(), 1);

        let mut changed = value.clone();
        changed["read_scopes"] = json!(["other/**"]);
        let mut payload = changed.clone();
        payload
            .as_object_mut()
            .expect("object")
            .remove("semantic_sha256");
        changed["semantic_sha256"] = Value::String(semantic_sha256(&payload));
        let error = cache
            .admit("session", root.path(), Some(&changed))
            .expect_err("digest change");
        assert_eq!(error.code(), "JSPACE_CONTRACT_CHANGED");
    }

    #[test]
    fn admission_rejects_root_change_for_an_existing_session() {
        let root = tempfile::tempdir().expect("root");
        let other = tempfile::tempdir().expect("other");
        let cache = JSpaceAdmissionCache::default();
        let value = contract(root.path());
        cache
            .admit("session", root.path(), Some(&value))
            .expect("first admission");

        let error = cache
            .admit("session", other.path(), Some(&value))
            .expect_err("root change");
        assert_eq!(error.code(), "JSPACE_ROOT_MISMATCH");
    }

    #[test]
    fn path_operation_and_command_checks_are_local_and_fail_closed() {
        let root = tempfile::tempdir().expect("root");
        fs::create_dir(root.path().join("src")).expect("src");
        let matcher =
            JSpaceMatcher::from_value(root.path(), &contract(root.path())).expect("matcher");
        matcher
            .check_path("read", &root.path().join("src").join("main.rs"))
            .expect("read");
        matcher
            .check_path("modify", &root.path().join("src").join("main.rs"))
            .expect("modify");
        assert_eq!(
            matcher
                .check_path("delete", &root.path().join("src").join("main.rs"))
                .expect_err("delete")
                .code(),
            "JSPACE_OPERATION_DENIED"
        );
        assert_eq!(
            matcher
                .check_path("modify", &root.path().join("other.rs"))
                .expect_err("expansion")
                .code(),
            JSPACE_EXPANSION_REQUIRED
        );
        assert!(matcher
            .check_command("shell_command", "git status --short")
            .is_ok());
        assert_eq!(
            matcher
                .check_command("shell_command", "curl https://example.test")
                .expect_err("command")
                .code(),
            "JSPACE_COMMAND_DENIED"
        );
    }

    #[test]
    fn traversal_and_symlink_escape_are_rejected_before_scope_lookup() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        fs::create_dir(root.path().join("src")).expect("src");
        std::os::unix::fs::symlink(outside.path(), root.path().join("src").join("link"))
            .expect("link");
        let matcher =
            JSpaceMatcher::from_value(root.path(), &contract(root.path())).expect("matcher");
        assert_eq!(
            matcher
                .check_path("read", std::path::Path::new("../outside"))
                .expect_err("traversal")
                .code(),
            "JSPACE_PATH_TRAVERSAL"
        );
        assert_eq!(
            matcher
                .check_path("read", &root.path().join("src/link/file"))
                .expect_err("symlink")
                .code(),
            "JSPACE_SYMLINK_ESCAPE"
        );
    }
}
