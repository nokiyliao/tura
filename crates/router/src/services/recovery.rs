//! Router restart recovery hooks.
//!
//! First-phase recovery starts session_db, asks it to replay the durable queue,
//! and treats non-reattachable runtime work as interrupted. Command execution
//! claims are reconciled at the next command admission because session
//! directories are owned by the runtime request, not by the router daemon.

use anyhow::Result;
use serde_json::json;

use super::session_db::SessionDbService;

pub fn recover_after_start(session_db: &SessionDbService) -> Result<serde_json::Value> {
    let session_db_status = session_db.start()?;
    Ok(json!({
        "session_db": session_db_status,
        "queue_replay": "requested",
        "runtime_reattach": false,
        "orphan_policy": "mark_interrupted_with_terminal_receipt",
        "command_execution_recovery": "reconcile_on_session_admission",
        "replay_policy": "diagnosed_only_after_no_authoritative_publication_or_idempotent_cas_proof"
    }))
}
