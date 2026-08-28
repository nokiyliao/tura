//! Agent registry owned by the router.
//!
//! The router owns agent discovery, resolution, and spec delivery. The runtime
//! activates `AgentManagement` from the delivered spec and runs the MANAS loop.
//! The router does not own prompt assembly, provider formatting, or agent loops.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tura_agents::store::{discover_agents, project_root_from_env_or_cwd};

const RELEASE_ROOT_ENV: &str = "TURA_RELEASE_BIN_DIR";

/// Resolved agent spec delivered from the router to a runtime worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentSpec {
    pub agent_name: String,
    /// Default provider id; credential truth and OAuth handling stay in provider.
    pub provider: String,
    pub capabilities: Vec<String>,
    /// Session types and topics that select this agent.
    pub session_types: Vec<String>,
    pub validator_enabled: bool,
    #[serde(default)]
    pub default_config: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<tura_agents::store::AgentConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCatalogItem {
    pub name: String,
    pub description: String,
    pub mode: String,
    pub native: bool,
    pub hidden: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<AgentModel>,
    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,
    pub permission: PermissionRuleset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentModel {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionRuleset {
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertAgentRequest {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub config: Option<tura_agents::store::AgentConfig>,
    #[serde(default)]
    pub prompt: Option<String>,
}

#[derive(Clone, Debug)]
struct AgentDefinition {
    agent_name: &'static str,
    aliases: &'static [&'static str],
    provider: &'static str,
    capabilities: &'static [&'static str],
    session_types: &'static [&'static str],
    validator_enabled: bool,
}

const AGENT_TABLE: &[AgentDefinition] = &[
    AgentDefinition {
        agent_name: "coding_agent",
        aliases: &["coding", "programming", "development", "testing"],
        provider: "anthropic",
        capabilities: &["command_run", "file_edit", "code_search"],
        session_types: &["coding", "programming", "development", "testing"],
        validator_enabled: false,
    },
    AgentDefinition {
        agent_name: "general_agent",
        aliases: &["general"],
        provider: "anthropic",
        capabilities: &["command_run", "web_discover"],
        session_types: &["general"],
        validator_enabled: false,
    },
];

const DEFAULT_AGENT_INDEX: usize = 1;

/// In-memory agent registry loaded from static and dynamic definitions.
#[derive(Clone, Debug)]
pub struct AgentRegistry {
    name_index: HashMap<String, usize>,
    session_type_index: HashMap<String, usize>,
    dynamic_specs: HashMap<String, AgentSpec>,
}

impl AgentRegistry {
    pub fn from_static() -> Self {
        let mut name_index = HashMap::new();
        let mut session_type_index = HashMap::new();
        let mut dynamic_specs = HashMap::new();
        for (index, def) in AGENT_TABLE.iter().enumerate() {
            name_index.insert(def.agent_name.to_string(), index);
            for alias in def.aliases {
                name_index.insert((*alias).to_string(), index);
            }
            for session_type in def.session_types {
                session_type_index.insert((*session_type).to_string(), index);
            }
        }
        for root in agent_lookup_roots() {
            for agent in discover_agents(&root) {
                let id = agent.summary.id.to_ascii_lowercase();
                let aliases = agent.summary.aliases.clone();
                let spec = spec_from_stored_agent(agent);
                dynamic_specs.entry(id).or_insert_with(|| spec.clone());
                for alias in aliases {
                    dynamic_specs
                        .entry(alias.to_ascii_lowercase())
                        .or_insert_with(|| spec.clone());
                }
            }
        }
        Self {
            name_index,
            session_type_index,
            dynamic_specs,
        }
    }

    /// Resolve by explicit agent name.
    pub fn resolve_by_name(&self, name: &str) -> Option<AgentSpec> {
        self.resolve_by_name_from_roots(name, &agent_lookup_roots())
    }

    pub fn resolve_by_name_from_root(&self, name: &str, project_root: &Path) -> Option<AgentSpec> {
        tura_agents::store::load_agent(project_root, name)
            .map(spec_from_stored_agent)
            .or_else(|| self.resolve_by_name(name))
    }

    fn resolve_by_name_from_roots(&self, name: &str, roots: &[PathBuf]) -> Option<AgentSpec> {
        let key = name.trim().to_ascii_lowercase();
        if let Some(spec) = self.dynamic_specs.get(&key) {
            return Some(spec.clone());
        }
        if let Some(spec) = self
            .name_index
            .get(&key)
            .map(|index| spec_from(&AGENT_TABLE[*index]))
        {
            return Some(spec);
        }

        roots
            .iter()
            .find_map(|root| tura_agents::store::load_agent(root, &key).map(spec_from_stored_agent))
    }

    /// Resolve by session type or topic when no explicit agent is selected.
    pub fn resolve_by_session_type(&self, session_type: &str) -> AgentSpec {
        let key = session_type.trim().to_ascii_lowercase();
        if let Some(spec) = self.dynamic_specs.get(&key) {
            return spec.clone();
        }
        let index = self
            .session_type_index
            .get(&key)
            .copied()
            .unwrap_or(DEFAULT_AGENT_INDEX);
        spec_from(&AGENT_TABLE[index])
    }

    /// Resolve by explicit agent first, then fall back to session type.
    pub fn resolve(&self, agent: Option<&str>, session_type: Option<&str>) -> AgentSpec {
        if let Some(agent) = agent
            && let Some(spec) = self.resolve_by_name(agent)
        {
            return spec;
        }
        self.resolve_by_session_type(session_type.unwrap_or("general"))
    }

    pub fn resolve_for_project(
        &self,
        agent: Option<&str>,
        session_type: Option<&str>,
        project_root: Option<&Path>,
    ) -> Result<AgentSpec, String> {
        if let Some(agent) = agent {
            let agent = agent.trim();
            if agent.is_empty() {
                return Err("explicit agent name is empty".to_string());
            }
            if let Some(project_root) = project_root
                && let Some(spec) = self.resolve_by_name_from_root(agent, project_root)
            {
                return Ok(spec);
            }
            if let Some(spec) = self.resolve_by_name(agent) {
                return Ok(spec);
            }
            return Err(format!("unknown explicit agent `{agent}`"));
        }
        Ok(self.resolve_by_session_type(session_type.unwrap_or("general")))
    }

    pub fn list_catalog(&self) -> Vec<AgentCatalogItem> {
        discover_agents(&project_root_from_env_or_cwd())
            .into_iter()
            .map(catalog_item_from_stored_agent)
            .collect()
    }

    pub fn get_stored(&self, agent_id: &str) -> Option<tura_agents::store::StoredAgent> {
        tura_agents::store::load_agent(&project_root_from_env_or_cwd(), agent_id)
    }

    pub fn upsert(
        &self,
        agent_id: Option<String>,
        payload: UpsertAgentRequest,
    ) -> Result<tura_agents::store::StoredAgent, String> {
        let project_root = project_root_from_env_or_cwd();
        let agent_id = agent_id
            .or(payload.id)
            .or_else(|| {
                payload
                    .config
                    .as_ref()
                    .map(|config| config.agent_name.clone())
            })
            .ok_or_else(|| "agent id is required".to_string())?;
        let mut config = payload.config.unwrap_or(
            tura_agents::store::load_agent(&project_root, &agent_id)
                .map(|agent| agent.config)
                .unwrap_or(tura_agents::store::default_agent_config(
                    &project_root,
                    &agent_id,
                )?),
        );
        config.agent_name = agent_id;
        tura_agents::store::save_dynamic_agent(&project_root, &config, payload.prompt.as_deref())
    }

    pub fn delete(&self, agent_id: &str) -> Result<bool, String> {
        tura_agents::store::delete_dynamic_agent(&project_root_from_env_or_cwd(), agent_id)
    }
}

fn agent_lookup_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    push_unique_root(&mut roots, project_root_from_env_or_cwd());
    if let Some(root) = std::env::var_os(RELEASE_ROOT_ENV).map(PathBuf::from) {
        push_unique_root(&mut roots, root);
    }
    if let Some(root) = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf))
    {
        push_unique_root(&mut roots, root);
    }
    roots
}

fn push_unique_root(roots: &mut Vec<PathBuf>, root: PathBuf) {
    if !roots.contains(&root) {
        roots.push(root);
    }
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::from_static()
    }
}

fn spec_from(def: &AgentDefinition) -> AgentSpec {
    AgentSpec {
        agent_name: def.agent_name.to_string(),
        provider: def.provider.to_string(),
        capabilities: def.capabilities.iter().map(|s| s.to_string()).collect(),
        session_types: def.session_types.iter().map(|s| s.to_string()).collect(),
        validator_enabled: def.validator_enabled,
        default_config: true,
        config: None,
    }
}

fn spec_from_stored_agent(agent: tura_agents::store::StoredAgent) -> AgentSpec {
    AgentSpec {
        agent_name: agent.summary.id.clone(),
        provider: agent
            .summary
            .provider
            .unwrap_or_else(|| "default".to_string()),
        capabilities: agent.summary.capabilities,
        session_types: vec![agent.summary.id],
        validator_enabled: agent
            .config
            .validator
            .get("need_validator")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        default_config: agent.config.default_config,
        config: Some(agent.config),
    }
}

fn catalog_item_from_stored_agent(agent: tura_agents::store::StoredAgent) -> AgentCatalogItem {
    let mut options = HashMap::new();
    options.insert(
        "source".to_string(),
        serde_json::json!(agent.summary.source),
    );
    options.insert("path".to_string(), serde_json::json!(agent.summary.path));
    options.insert(
        "aliases".to_string(),
        serde_json::json!(agent.summary.aliases),
    );
    options.insert(
        "capabilities".to_string(),
        serde_json::json!(agent.summary.capabilities),
    );
    options.insert(
        "default_config".to_string(),
        serde_json::json!(agent.config.default_config),
    );
    AgentCatalogItem {
        name: agent.summary.id,
        description: agent.summary.description,
        mode: "primary".to_string(),
        native: agent.summary.source == tura_agents::store::AgentSource::Static,
        hidden: agent.summary.hidden,
        model: None,
        options,
        permission: PermissionRuleset {
            allow: vec!["*".to_string()],
            deny: vec![],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_session_type_to_coding() {
        let registry = AgentRegistry::from_static();
        let spec = registry.resolve_by_session_type("coding");
        assert!(spec.agent_name == "coding_agent" || spec.agent_name == "thoughtful");
    }

    #[test]
    fn falls_back_to_general() {
        let registry = AgentRegistry::from_static();
        let spec = registry.resolve(Some("nonexistent"), Some("unknown_topic"));
        assert_eq!(spec.agent_name, "general_agent");
    }

    #[test]
    fn explicit_agent_takes_priority() {
        let registry = AgentRegistry::from_static();
        let spec = registry.resolve(Some("coding"), Some("general"));
        assert!(spec.agent_name == "coding_agent" || spec.agent_name == "thoughtful");
    }

    #[test]
    fn static_agent_registry_exposes_expected_capabilities() {
        let registry = AgentRegistry::from_static();
        let spec = registry
            .resolve_by_name("coding")
            .expect("coding alias should resolve");
        assert!(spec.agent_name == "coding_agent" || spec.agent_name == "thoughtful");
        assert!(
            spec.capabilities.contains(&"command_run".to_string())
                || spec.capabilities.contains(&"shells".to_string())
        );
        assert!(
            spec.capabilities.contains(&"file_edit".to_string())
                || spec.capabilities.contains(&"apply_patch".to_string())
        );
        assert!(
            spec.session_types.contains(&"coding".to_string()) || spec.agent_name == "thoughtful"
        );
    }

    #[test]
    fn resolves_dynamic_agent_created_after_registry_startup() {
        let project = tempfile::tempdir().expect("project");
        let registry = AgentRegistry::from_static();
        let config = tura_agents::store::AgentConfig {
            agent_name: "fresh-executor".to_string(),
            description: Some("fresh executor".to_string()),
            aliases: vec![],
            icon_emoji: None,
            agent_directory: "agents/src/fresh-executor".into(),
            parent_agent_id: None,
            report_to_user: true,
            default_config: false,
            reflection: false,
            op_manual: false,
            self_reflection: false,
            provider: serde_json::json!({
                "tura_llm_name": "thinking",
                "tool_choice": "Auto"
            }),
            agent_prompt: vec![],
            agent_capabilities: vec![serde_json::json!({
                "capability_name": "apply_patch"
            })],
            validator: serde_json::json!({
                "need_validator": false,
                "validator_name": null
            }),
        };
        tura_agents::store::save_dynamic_agent(project.path(), &config, Some("prompt"))
            .expect("save dynamic agent");

        let spec = registry
            .resolve_by_name_from_root("fresh-executor", project.path())
            .expect("fresh dynamic agent should resolve without router restart");

        assert_eq!(spec.agent_name, "fresh-executor");
        assert_eq!(spec.capabilities, vec!["apply_patch"]);
        assert!(spec.config.is_some());
    }

    #[test]
    fn request_project_agent_overrides_router_startup_snapshot() {
        let project = tempfile::tempdir().expect("project");
        let registry = AgentRegistry::from_static();
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
            .expect("save request-scoped direct agent");

        let spec = registry
            .resolve_by_name_from_root("direct", project.path())
            .expect("request-scoped direct agent should resolve");
        let current_model = spec
            .config
            .as_ref()
            .and_then(|config| config.provider.get("current_model"))
            .and_then(serde_json::Value::as_str);

        assert_eq!(current_model, Some("missing-route"));
    }

    #[test]
    fn explicit_unknown_agent_does_not_fall_back_to_session_type() {
        let registry = AgentRegistry::from_static();

        let error = registry
            .resolve_for_project(Some("missing-executor"), Some("coding"), None)
            .expect_err("an explicit unknown agent must fail closed");

        assert_eq!(error, "unknown explicit agent `missing-executor`");
    }

    #[test]
    fn resolves_packaged_agent_after_empty_workspace_root() {
        let workspace = tempfile::tempdir().expect("workspace");
        let release = tempfile::tempdir().expect("release");
        let registry = AgentRegistry::from_static();
        let config = tura_agents::store::AgentConfig {
            agent_name: "packaged-executor".to_string(),
            description: Some("packaged executor".to_string()),
            aliases: vec!["packaged-alias".to_string()],
            icon_emoji: None,
            agent_directory: "agents/src/packaged-executor".into(),
            parent_agent_id: None,
            report_to_user: true,
            default_config: true,
            reflection: false,
            op_manual: false,
            self_reflection: false,
            provider: serde_json::json!({
                "tura_llm_name": "thinking",
                "tool_choice": "Auto"
            }),
            agent_prompt: vec![],
            agent_capabilities: vec![serde_json::json!({
                "capability_name": "shells"
            })],
            validator: serde_json::json!({
                "need_validator": false,
                "validator_name": null
            }),
        };
        tura_agents::store::save_dynamic_agent(release.path(), &config, Some("prompt"))
            .expect("save packaged agent");

        let roots = vec![workspace.path().to_path_buf(), release.path().to_path_buf()];
        let spec = registry
            .resolve_by_name_from_roots("packaged-alias", &roots)
            .expect("release registry should be used after an empty workspace registry");

        assert_eq!(spec.agent_name, "packaged-executor");
        assert_eq!(spec.capabilities, vec!["shells"]);
        assert!(spec.config.is_some());
    }
}
