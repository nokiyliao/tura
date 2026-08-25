use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub healthy: bool,
    /// True only after startup Session feed projection replay has completed.
    #[serde(default = "default_ready")]
    pub ready: bool,
    /// `ready` while the projection can be served, `starting` during replay, or
    /// `failed` if startup reconstruction did not complete.
    #[serde(default = "default_status")]
    pub status: String,
    /// Reconstruction failure when `status` is `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub version: String,
    /// Canonical runtime/project root this gateway is serving (TURA_PROJECT_ROOT).
    /// Lets clients tell whether a reachable gateway belongs to their own package.
    #[serde(default)]
    pub root: String,
    /// Canonical instance home this gateway owns. TUI/GUI use this as the
    /// lifecycle identity so different frontends share one session database.
    #[serde(default)]
    pub home: String,
    /// Directory of the running gateway executable.
    #[serde(default)]
    pub exe_dir: String,
    /// Process ID of the gateway that produced this health response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Gateway process start time from the host process table, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_time: Option<u64>,
    /// Provider LLM call log directory when dev logging is enabled; absent in production.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev_log_path: Option<String>,
}

fn default_ready() -> bool {
    true
}

fn default_status() -> String {
    "ready".to_string()
}

impl Default for HealthResponse {
    fn default() -> Self {
        Self {
            healthy: true,
            ready: true,
            status: default_status(),
            error: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
            root: String::new(),
            home: String::new(),
            exe_dir: String::new(),
            pid: None,
            process_start_time: None,
            dev_log_path: None,
        }
    }
}
