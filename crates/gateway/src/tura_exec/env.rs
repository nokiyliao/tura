use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::cli::CliConfig;

const FORCED_CAPABILITY_DIRECTORIES_ENV: &str = "TURA_FORCED_CAPABILITY_DIRECTORIES";

#[derive(Deserialize)]
struct CapabilityManifest {
    id: String,
    #[serde(default)]
    core: bool,
    execution: String,
    runtime: CapabilityRuntime,
}

#[derive(Deserialize)]
struct CapabilityRuntime {
    binary: String,
}

pub(crate) fn configure_runtime_env(config: &CliConfig) -> Result<(), String> {
    configure_capability_directories(config)?;
    // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
    #[allow(
        unsafe_code,
        reason = "Rust 2024 process-environment mutation audited at the caller"
    )]
    unsafe {
        std::env::set_var("TURA_FRONTEND_SOURCE", "cli")
    };
    if let Some(model) = config.model.as_deref() {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_SESSION_MODEL_OVERRIDE", normalize_model(model))
        };
    }
    if let Some(reasoning) = config.reasoning_effort.as_deref() {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_SESSION_REASONING_EFFORT", reasoning)
        };
    }
    if config.priority {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_SESSION_ACCELERATION_ENABLED", "1")
        };
    }
    if config.goal_mode {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_GOAL_MODE", "1")
        };
    } else {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::remove_var("TURA_GOAL_MODE")
        };
    }
    if config.no_op_manual {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_NO_OP_MANUAL", "1")
        };
    } else {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::remove_var("TURA_NO_OP_MANUAL")
        };
    }
    if let Some(max_tokens) = config.max_tokens {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_SESSION_MAX_TOKENS", max_tokens.to_string())
        };
    }
    if let Some(shell) = config.command_run_shell.as_deref() {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_COMMAND_RUN_SHELL", shell)
        };
    }
    if config.command_run_sandbox {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_COMMAND_RUN_SANDBOX", "1")
        };
    } else {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::remove_var("TURA_COMMAND_RUN_SANDBOX")
        };
    }
    if let Some(timeout_secs) = config.timeout_secs {
        // This is an outer observation budget only. Router command_run keeps
        // its own explicit per-command deadline and terminal receipt.
        // SAFETY: CLI startup performs process-environment mutation before worker threads exist.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at CLI startup"
        )]
        unsafe {
            std::env::set_var(
                "TURA_EXEC_ROUTER_READ_TIMEOUT_SECS",
                timeout_secs.to_string(),
            );
            std::env::set_var(
                "TURA_EXEC_EMBEDDED_RUNTIME_TIMEOUT_SECS",
                timeout_secs.to_string(),
            );
        }
    }
    configure_release_runtime_env()?;
    configure_progress_env(config);
    Ok(())
}

fn configure_capability_directories(config: &CliConfig) -> Result<(), String> {
    let directories = validate_capability_directories(config)?;
    if directories.is_empty() {
        // SAFETY: CLI startup performs process-environment mutation before worker threads exist.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at CLI startup"
        )]
        unsafe {
            std::env::remove_var(FORCED_CAPABILITY_DIRECTORIES_ENV)
        };
        return Ok(());
    }
    let encoded = serde_json::to_string(&directories)
        .map_err(|err| format!("failed to encode --capability directories: {err}"))?;
    // SAFETY: CLI startup performs process-environment mutation before worker threads exist.
    #[allow(
        unsafe_code,
        reason = "Rust 2024 process-environment mutation audited at CLI startup"
    )]
    unsafe {
        std::env::set_var(FORCED_CAPABILITY_DIRECTORIES_ENV, encoded)
    };
    Ok(())
}

fn validate_capability_directories(config: &CliConfig) -> Result<Vec<PathBuf>, String> {
    let process_cwd = std::env::current_dir()
        .map_err(|err| format!("failed to resolve current directory: {err}"))?;
    let session_cwd = if config.cwd.is_absolute() {
        config.cwd.clone()
    } else {
        process_cwd.join(&config.cwd)
    };
    let mut command_ids = BTreeMap::<String, PathBuf>::new();
    let mut directories = Vec::new();
    for requested in &config.capability_directories {
        let candidate = if requested.is_absolute() {
            requested.clone()
        } else {
            session_cwd.join(requested)
        };
        let directory = candidate.canonicalize().map_err(|err| {
            format!(
                "invalid --capability directory {}: {err}",
                candidate.display()
            )
        })?;
        if !directory.is_dir() {
            return Err(format!(
                "invalid --capability directory {}: expected a directory",
                directory.display()
            ));
        }
        let manifest_path = directory.join("command.toml");
        let manifest_text = std::fs::read_to_string(&manifest_path).map_err(|err| {
            format!(
                "invalid --capability directory {}: failed to read command.toml: {err}",
                directory.display()
            )
        })?;
        let manifest: CapabilityManifest = toml::from_str(&manifest_text).map_err(|err| {
            format!(
                "invalid command manifest {}: {err}",
                manifest_path.display()
            )
        })?;
        let command_id = manifest.id.trim();
        if command_id.is_empty() {
            return Err(format!(
                "command id is empty in {}",
                manifest_path.display()
            ));
        }
        if manifest.core
            || manifest.execution.trim() != "one_shot"
            || manifest.runtime.binary.trim().is_empty()
        {
            return Err(format!(
                "forced capability {command_id} must be a non-core one_shot command with runtime.binary"
            ));
        }
        if let Some(previous) = command_ids.insert(command_id.to_string(), directory.clone()) {
            return Err(format!(
                "duplicate --capability command id {command_id}: {} and {}",
                previous.display(),
                directory.display()
            ));
        }
        validate_capability_files(&directory, command_id)?;
        if !directories.contains(&directory) {
            directories.push(directory);
        }
    }
    Ok(directories)
}

fn validate_capability_files(directory: &Path, command_id: &str) -> Result<(), String> {
    for file_name in ["prompt.md", "policy.toml"] {
        let path = directory.join(file_name);
        if !path.is_file() {
            return Err(format!(
                "invalid --capability directory {}: missing {file_name}",
                directory.display()
            ));
        }
    }
    let policy_path = directory.join("policy.toml");
    let policy = std::fs::read_to_string(&policy_path)
        .map_err(|err| format!("failed to read {}: {err}", policy_path.display()))?;
    policy
        .parse::<toml::Value>()
        .map_err(|err| format!("invalid policy {}: {err}", policy_path.display()))?;

    let schema_path = directory.join("schema.json");
    let schema_text = std::fs::read_to_string(&schema_path)
        .map_err(|err| format!("failed to read {}: {err}", schema_path.display()))?;
    let schema: serde_json::Value = serde_json::from_str(&schema_text)
        .map_err(|err| format!("invalid command schema {}: {err}", schema_path.display()))?;
    let schema_name = schema.get("name").and_then(serde_json::Value::as_str);
    if schema_name != Some(command_id) || !schema.get("input_schema").is_some_and(|v| v.is_object())
    {
        return Err(format!(
            "invalid command schema {}: name must equal {command_id:?} and input_schema must be an object",
            schema_path.display()
        ));
    }
    Ok(())
}

pub(crate) fn normalize_model(model: &str) -> String {
    if model.contains('/') {
        model.to_string()
    } else {
        format!("openai/{model}")
    }
}

fn configure_progress_env(config: &CliConfig) {
    if config.json {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::set_var("TURA_CLI_LIVE_JSONL", "1")
        };
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::remove_var("TURA_CLI_PROGRESS")
        };
    } else {
        // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
        #[allow(
            unsafe_code,
            reason = "Rust 2024 process-environment mutation audited at the caller"
        )]
        unsafe {
            std::env::remove_var("TURA_CLI_LIVE_JSONL")
        };
        if config.quiet {
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            #[allow(
                unsafe_code,
                reason = "Rust 2024 process-environment mutation audited at the caller"
            )]
            unsafe {
                std::env::remove_var("TURA_CLI_PROGRESS")
            };
        } else {
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            #[allow(
                unsafe_code,
                reason = "Rust 2024 process-environment mutation audited at the caller"
            )]
            unsafe {
                std::env::set_var("TURA_CLI_PROGRESS", "1")
            };
        }
    }
}

fn project_root_from_exe() -> String {
    std::env::current_exe()
        .ok()
        .as_deref()
        .and_then(find_project_root_from)
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .as_deref()
                .and_then(find_project_root_from)
        })
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
        .display()
        .to_string()
}

fn configure_release_runtime_env() -> Result<(), String> {
    let release_root = PathBuf::from(project_root_from_exe());
    let project_root = execution_project_root(
        std::env::var_os("TURA_PROJECT_ROOT").map(PathBuf::from),
        release_root.clone(),
    )?;
    // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
    #[allow(
        unsafe_code,
        reason = "Rust 2024 process-environment mutation audited at the caller"
    )]
    unsafe {
        std::env::set_var("TURA_PROJECT_ROOT", &project_root)
    };
    if std::env::var_os("TURA_PROVIDER_CONFIG").is_none() {
        let provider_config = [
            project_root.join("config").join("provider_config.json"),
            release_root.join("config").join("provider_config.json"),
        ]
        .into_iter()
        .find(|path| path.is_file());
        if let Some(provider_config) = provider_config {
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            #[allow(
                unsafe_code,
                reason = "Rust 2024 process-environment mutation audited at the caller"
            )]
            unsafe {
                std::env::set_var("TURA_PROVIDER_CONFIG", provider_config)
            };
        }
    }
    if std::env::var_os("TURA_ENV_PATH").is_none() {
        let env_path = [project_root.join(".env"), release_root.join(".env")]
            .into_iter()
            .find(|path| path.is_file());
        if let Some(env_path) = env_path {
            // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
            #[allow(
                unsafe_code,
                reason = "Rust 2024 process-environment mutation audited at the caller"
            )]
            unsafe {
                std::env::set_var("TURA_ENV_PATH", env_path)
            };
        }
    }
    Ok(())
}

fn execution_project_root(
    explicit_root: Option<PathBuf>,
    release_root: PathBuf,
) -> Result<PathBuf, String> {
    let Some(explicit_root) = explicit_root else {
        return Ok(release_root);
    };
    if explicit_root.is_dir() {
        return Ok(explicit_root);
    }
    Err(format!(
        "TURA_PROJECT_ROOT_INVALID:{}",
        explicit_root.display()
    ))
}

fn find_project_root_from(path: &Path) -> Option<PathBuf> {
    let start = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or(path)
    };
    start
        .ancestors()
        .find(|candidate| {
            candidate.join("agents").join("src").is_dir()
                && (candidate.join("personas").join("src").is_dir()
                    || candidate
                        .join("config")
                        .join("provider_config.json")
                        .exists())
        })
        .map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use super::execution_project_root;

    #[test]
    fn explicit_agent_only_project_root_is_preserved() {
        let project = tempfile::tempdir().expect("project");
        std::fs::create_dir_all(project.path().join("agents/src/direct"))
            .expect("create agent-only project");
        let release = tempfile::tempdir().expect("release");

        let selected = execution_project_root(
            Some(project.path().to_path_buf()),
            release.path().to_path_buf(),
        )
        .expect("explicit root");

        assert_eq!(selected, project.path());
    }
}
