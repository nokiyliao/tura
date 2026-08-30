use super::SessionLogStore;
use anyhow::{Context, Result};
use fs2::FileExt;
use lifecycle::RuntimeEvent;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};
use session_log_contract::{
    MaintainRuntimeLocationsRequest, RuntimeLocationMaintenanceMode,
    RuntimeLocationMaintenanceReceipt,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

const SCHEMA_VERSION: &str = "runtime_location_maintenance_v1";
const RUST_TRIM_WHITESPACE: &str = "\u{0009}\u{000a}\u{000b}\u{000c}\u{000d}\u{0020}\u{0085}\u{00a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}";
const INCOMPLETE_PREDICATE: &str = "NOT (terminal_proven = 1
    AND terminal_revision IS NOT NULL
    AND terminal_event_seq IS NOT NULL
    AND terminal_evidence_id IS NOT NULL
    AND TRIM(terminal_evidence_id, ?1) != '')";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocationRow {
    runtime_id: String,
    session_id: String,
    workspace_db_path: String,
    terminal_proven: bool,
    terminal_revision: Option<u64>,
    terminal_event_seq: Option<u64>,
    terminal_evidence_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Proof {
    revision: u64,
    event_seq: u64,
    evidence_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanEntry {
    row: LocationRow,
    disposition: String,
    proof: Option<Proof>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaintenancePlan {
    schema_version: String,
    entries: Vec<PlanEntry>,
}

impl SessionLogStore {
    pub fn maintain_runtime_locations(
        &self,
        request: MaintainRuntimeLocationsRequest,
    ) -> Result<RuntimeLocationMaintenanceReceipt> {
        let page_size = request.page_size.clamp(1, 500);
        validate_request(&request)?;

        let _maintenance_lock = MaintenanceLock::acquire(&self.index_db_path)?;
        if matches!(request.mode, RuntimeLocationMaintenanceMode::Apply) {
            let expected = request
                .expected_dry_run_sha256
                .as_deref()
                .context("apply requires expected_dry_run_sha256")?;
            if let Some(receipt) = self.read_existing_receipt(expected)? {
                return Ok(RuntimeLocationMaintenanceReceipt {
                    replayed_receipt: true,
                    ..receipt
                });
            }
        }

        self.with_index_connection(|conn| {
            let behavior = if matches!(request.mode, RuntimeLocationMaintenanceMode::Apply) {
                TransactionBehavior::Immediate
            } else {
                TransactionBehavior::Deferred
            };
            let tx = conn.transaction_with_behavior(behavior)?;
            let plan = build_plan(&tx, page_size)?;
            let canonical_input_sha256 = digest_json(&plan)?;
            if let Some(expected) = request.expected_dry_run_sha256.as_deref()
                && expected != canonical_input_sha256
            {
                anyhow::bail!(
                    "RUNTIME_LOCATION_MAINTENANCE_DIGEST_MISMATCH: expected {expected}, got {canonical_input_sha256}"
                );
            }
            let mut dispositions = BTreeMap::new();
            for entry in &plan.entries {
                *dispositions.entry(entry.disposition.clone()).or_insert(0) += 1;
            }
            let active_lease_count = dispositions
                .get("preserve_active_lease")
                .copied()
                .unwrap_or(0);
            let pre_incomplete_count = plan.entries.len() as u64;
            let mut backfilled_count = 0_u64;
            let mut deleted_count = 0_u64;

            if matches!(request.mode, RuntimeLocationMaintenanceMode::Apply) {
                if active_lease_count != 0 {
                    anyhow::bail!(
                        "RUNTIME_LOCATION_MAINTENANCE_NOT_QUIESCENT: {active_lease_count} active lease(s)"
                    );
                }
                for entry in &plan.entries {
                    match entry.disposition.as_str() {
                        "backfill_terminal_event_proof" => {
                            let proof = entry.proof.as_ref().context("backfill proof missing")?;
                            let changed = tx.execute(
                                "UPDATE runtime_locations SET terminal_proven = 1,
                                 terminal_revision = ?4, terminal_event_seq = ?5,
                                 terminal_evidence_id = ?6
                                 WHERE runtime_id = ?1 AND session_id = ?2
                                   AND workspace_db_path = ?3 AND terminal_proven = 0
                                   AND terminal_revision IS NULL AND terminal_event_seq IS NULL
                                   AND terminal_evidence_id IS NULL",
                                params![entry.row.runtime_id, entry.row.session_id,
                                    entry.row.workspace_db_path, proof.revision,
                                    proof.event_seq, proof.evidence_id],
                            )?;
                            if changed != 1 {
                                anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_CAS_CONFLICT:{}", entry.row.runtime_id);
                            }
                            backfilled_count += 1;
                        }
                        "delete_missing_db_session_absent" => {
                            let session_exists = tx.query_row(
                                "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
                                params![entry.row.session_id], |row| row.get::<_, bool>(0))?;
                            if session_exists {
                                anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_CAS_CONFLICT:{}:session_present", entry.row.runtime_id);
                            }
                            let changed = tx.execute(
                                "DELETE FROM runtime_locations WHERE runtime_id = ?1
                                 AND session_id = ?2 AND workspace_db_path = ?3
                                 AND terminal_proven = 0 AND terminal_revision IS NULL
                                 AND terminal_event_seq IS NULL AND terminal_evidence_id IS NULL",
                                params![entry.row.runtime_id, entry.row.session_id, entry.row.workspace_db_path],
                            )?;
                            if changed != 1 {
                                anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_CAS_CONFLICT:{}", entry.row.runtime_id);
                            }
                            deleted_count += 1;
                        }
                        _ => {}
                    }
                }
            }

            let post_incomplete_count = if matches!(request.mode, RuntimeLocationMaintenanceMode::Apply) {
                count_incomplete(&tx)?
            } else {
                pre_incomplete_count
                    .saturating_sub(backfillable_count(&plan))
                    .saturating_sub(deletable_count(&plan))
            };
            tx.commit()?;

            let mut receipt = RuntimeLocationMaintenanceReceipt {
                schema_version: SCHEMA_VERSION.to_string(),
                mode: request.mode.clone(),
                canonical_input_sha256: canonical_input_sha256.clone(),
                pre_incomplete_count,
                post_incomplete_count,
                backfilled_count: if matches!(request.mode, RuntimeLocationMaintenanceMode::DryRun) { backfillable_count(&plan) } else { backfilled_count },
                deleted_count: if matches!(request.mode, RuntimeLocationMaintenanceMode::DryRun) { deletable_count(&plan) } else { deleted_count },
                mutation_count: backfilled_count + deleted_count,
                active_lease_count,
                dispositions,
                replayed_receipt: false,
                manifest_path: None,
            };
            if matches!(request.mode, RuntimeLocationMaintenanceMode::Apply) {
                let manifest = self.publish_receipt(&receipt)?;
                receipt.manifest_path = Some(manifest.to_string_lossy().into_owned());
            }
            Ok(receipt)
        })
    }

    fn receipt_root(&self, digest: &str) -> PathBuf {
        self.data_dir
            .join("maintenance/runtime_locations_v1")
            .join(digest)
    }

    fn read_existing_receipt(
        &self,
        digest: &str,
    ) -> Result<Option<RuntimeLocationMaintenanceReceipt>> {
        let root = self.receipt_root(digest);
        let manifest = root.join("manifest.json");
        if !manifest.is_file() {
            return Ok(None);
        }
        let receipt_bytes = std::fs::read(root.join("receipt.json"))?;
        let manifest_value: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest)?)?;
        let receipt: RuntimeLocationMaintenanceReceipt = serde_json::from_slice(&receipt_bytes)?;
        if receipt.canonical_input_sha256 != digest
            || receipt.mode != RuntimeLocationMaintenanceMode::Apply
            || manifest_value["canonical_input_sha256"] != digest
            || manifest_value["receipt_sha256"] != format!("{:x}", Sha256::digest(&receipt_bytes))
        {
            anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_RECEIPT_IDENTITY_CONFLICT");
        }
        Ok(Some(receipt))
    }

    fn publish_receipt(&self, receipt: &RuntimeLocationMaintenanceReceipt) -> Result<PathBuf> {
        let root = self.receipt_root(&receipt.canonical_input_sha256);
        std::fs::create_dir_all(&root)?;
        let receipt_bytes = serde_json::to_vec(receipt)?;
        write_atomic(&root.join("receipt.json"), &receipt_bytes)?;
        let manifest = serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "canonical_input_sha256": receipt.canonical_input_sha256,
            "receipt_sha256": format!("{:x}", Sha256::digest(&receipt_bytes)),
        });
        let manifest_path = root.join("manifest.json");
        write_atomic(&manifest_path, &serde_json::to_vec(&manifest)?)?;
        Ok(manifest_path)
    }
}

fn validate_request(request: &MaintainRuntimeLocationsRequest) -> Result<()> {
    match request.mode {
        RuntimeLocationMaintenanceMode::DryRun if request.expected_dry_run_sha256.is_some() => {
            anyhow::bail!("dry-run must not provide expected_dry_run_sha256")
        }
        RuntimeLocationMaintenanceMode::Apply => {
            let digest = request
                .expected_dry_run_sha256
                .as_deref()
                .unwrap_or_default();
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                anyhow::bail!("apply expected_dry_run_sha256 must be lowercase SHA-256");
            }
        }
        _ => {}
    }
    Ok(())
}

fn build_plan(tx: &Transaction<'_>, page_size: u64) -> Result<MaintenancePlan> {
    let mut after = None::<String>;
    let mut entries = Vec::new();
    loop {
        let mut statement = tx.prepare(&format!(
            "SELECT runtime_id, session_id, workspace_db_path, terminal_proven,
                    terminal_revision, terminal_event_seq, terminal_evidence_id
             FROM runtime_locations WHERE {INCOMPLETE_PREDICATE}
               AND (?2 IS NULL OR runtime_id > ?2)
             ORDER BY runtime_id ASC LIMIT ?3"
        ))?;
        let rows = statement
            .query_map(params![RUST_TRIM_WHITESPACE, after, page_size], |row| {
                Ok(LocationRow {
                    runtime_id: row.get(0)?,
                    session_id: row.get(1)?,
                    workspace_db_path: row.get(2)?,
                    terminal_proven: row.get(3)?,
                    terminal_revision: row.get(4)?,
                    terminal_event_seq: row.get(5)?,
                    terminal_evidence_id: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|row| row.runtime_id.clone());
        for row in rows {
            entries.push(classify_row(tx, row)?);
        }
    }
    Ok(MaintenancePlan {
        schema_version: SCHEMA_VERSION.to_string(),
        entries,
    })
}

fn classify_row(index: &Transaction<'_>, row: LocationRow) -> Result<PlanEntry> {
    let empty_proof = !row.terminal_proven
        && row.terminal_revision.is_none()
        && row.terminal_event_seq.is_none()
        && row.terminal_evidence_id.is_none();
    if !empty_proof {
        return Ok(entry(row, "preserve_partial_or_conflicting_proof", None));
    }
    let path = Path::new(&row.workspace_db_path);
    if !path.is_absolute() {
        return Ok(entry(row, "preserve_relative_or_malformed_path", None));
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let session_exists = index.query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
                params![row.session_id],
                |r| r.get::<_, bool>(0),
            )?;
            return Ok(entry(
                row,
                if session_exists {
                    "preserve_missing_db_session_present"
                } else {
                    "delete_missing_db_session_absent"
                },
                None,
            ));
        }
        Err(_) => return Ok(entry(row, "preserve_workspace_db_unreadable", None)),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Ok(entry(row, "preserve_path_type_or_symlink", None));
    }
    let canonical = match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(_) => return Ok(entry(row, "preserve_workspace_db_unreadable", None)),
    };
    if canonical != path {
        return Ok(entry(row, "preserve_path_drift", None));
    }
    let conn = match Connection::open_with_flags(
        &canonical,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(conn) => conn,
        Err(_) => return Ok(entry(row, "preserve_workspace_db_unreadable", None)),
    };
    let runtime = conn.query_row(
        "SELECT session_id, revision, last_event_seq, terminal, lease_active FROM runtimes WHERE runtime_id = ?1",
        params![row.runtime_id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?, r.get::<_, u64>(2)?, r.get::<_, bool>(3)?, r.get::<_, bool>(4)?)))
        .optional()?;
    let Some((session_id, revision, last_event_seq, terminal, lease_active)) = runtime else {
        return Ok(entry(row, "preserve_runtime_missing", None));
    };
    if session_id != row.session_id {
        return Ok(entry(row, "preserve_runtime_identity_conflict", None));
    }
    if lease_active {
        return Ok(entry(row, "preserve_active_lease", None));
    }
    if !terminal {
        return Ok(entry(row, "preserve_nonterminal", None));
    }
    let event = conn
        .query_row(
            "SELECT event_seq, revision, idempotency_key, event_json FROM runtime_events
         WHERE runtime_id = ?1 ORDER BY event_seq DESC LIMIT 1",
            params![row.runtime_id],
            |r| {
                Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((event_seq, event_revision, evidence_id, event_json)) = event else {
        return Ok(entry(row, "preserve_terminal_event_missing", None));
    };
    let unique = conn.query_row(
        "SELECT COUNT(*) FROM runtime_events WHERE idempotency_key = ?1",
        params![evidence_id],
        |r| r.get::<_, u64>(0),
    )? == 1;
    let parsed = serde_json::from_str::<RuntimeEvent>(&event_json).ok();
    let terminal_kind = matches!(
        parsed,
        Some(RuntimeEvent::RuntimeFinished { .. } | RuntimeEvent::RuntimeFailed { .. })
    );
    if revision != event_revision
        || last_event_seq != event_seq
        || evidence_id.trim().is_empty()
        || !unique
        || !terminal_kind
    {
        return Ok(entry(row, "preserve_terminal_event_contradiction", None));
    }
    Ok(entry(
        row,
        "backfill_terminal_event_proof",
        Some(Proof {
            revision,
            event_seq,
            evidence_id,
        }),
    ))
}

fn entry(row: LocationRow, disposition: &str, proof: Option<Proof>) -> PlanEntry {
    PlanEntry {
        row,
        disposition: disposition.to_string(),
        proof,
    }
}

fn count_incomplete(tx: &Transaction<'_>) -> Result<u64> {
    tx.query_row(
        &format!("SELECT COUNT(*) FROM runtime_locations WHERE {INCOMPLETE_PREDICATE}"),
        params![RUST_TRIM_WHITESPACE],
        |row| row.get(0),
    )
    .map_err(Into::into)
}
fn backfillable_count(plan: &MaintenancePlan) -> u64 {
    plan.entries
        .iter()
        .filter(|e| e.disposition == "backfill_terminal_event_proof")
        .count() as u64
}
fn deletable_count(plan: &MaintenancePlan) -> u64 {
    plan.entries
        .iter()
        .filter(|e| e.disposition == "delete_missing_db_session_absent")
        .count() as u64
}
fn digest_json(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    File::open(&tmp)?.sync_all()?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

struct MaintenanceLock(File);
impl MaintenanceLock {
    fn acquire(index_path: &Path) -> Result<Self> {
        let path = PathBuf::from(format!(
            "{}.runtime-location-maintenance.lock",
            index_path.display()
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.try_lock_exclusive()
            .with_context(|| format!("RUNTIME_LOCATION_MAINTENANCE_LOCKED:{}", path.display()))?;
        Ok(Self(file))
    }
}
impl Drop for MaintenanceLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lifecycle::{RuntimeError, RuntimeState};
    use tempfile::TempDir;

    fn request(
        mode: RuntimeLocationMaintenanceMode,
        digest: Option<String>,
        page_size: u64,
    ) -> MaintainRuntimeLocationsRequest {
        MaintainRuntimeLocationsRequest {
            mode,
            page_size,
            expected_dry_run_sha256: digest,
        }
    }

    fn fixture(event: RuntimeEvent, contradictory: bool) -> (TempDir, SessionLogStore, String) {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = SessionLogStore::open(temp.path().join("db")).expect("store");
        let workspace = temp.path().join("workspace.sqlite3");
        store
            .with_workspace_connection(&workspace, |_| Ok(()))
            .expect("workspace schema");
        let workspace = workspace.canonicalize().expect("canonical workspace");
        let event_json = serde_json::to_string(&event).expect("event json");
        let event_revision = if contradictory { 6 } else { 7 };
        store.with_workspace_connection(&workspace, |conn| {
            conn.pragma_update(None, "foreign_keys", "OFF")?;
            conn.execute("INSERT INTO runtimes(runtime_id, session_id, revision, last_event_seq, terminal, lease_active) VALUES ('r1','s1',7,9,1,0)", [])?;
            conn.execute("INSERT INTO runtime_events(runtime_id,event_seq,revision,idempotency_key,event_json) VALUES ('r1',9,?1,'e1',?2)", params![event_revision,event_json])?;
            Ok(())
        }).expect("runtime fixture");
        store.with_index_connection(|conn| {
            conn.execute("INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path) VALUES ('r1','s1',?1)", params![workspace.to_string_lossy()])?;
            Ok(())
        }).expect("location fixture");
        (temp, store, workspace.to_string_lossy().into_owned())
    }

    #[test]
    fn dry_run_apply_and_replay_terminal_finished_proof() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 1))
            .expect("dry run");
        assert_eq!(dry.backfilled_count, 1);
        assert_eq!(dry.mutation_count, 0);
        let applied = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                1,
            ))
            .expect("apply");
        assert_eq!(applied.mutation_count, 1);
        assert!(
            applied
                .manifest_path
                .as_ref()
                .is_some_and(|path| Path::new(path).is_file())
        );
        let replay = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect("replay");
        assert!(replay.replayed_receipt);
        assert_eq!(replay.mutation_count, 1);
    }

    #[test]
    fn runtime_failed_is_terminal_but_contradictory_revision_is_preserved() {
        let error = RuntimeError {
            error_code: Some("test".to_string()),
            error_text: Some("failure".to_string()),
            retry_allowed: false,
            fallback_allowed: false,
            fallback_to_id: None,
        };
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFailed {
                finished_at: chrono::Utc::now(),
                error,
                state: RuntimeState::Failed,
                usage: None,
            },
            true,
        );
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 10))
            .expect("dry run");
        assert_eq!(dry.backfilled_count, 0);
        assert_eq!(
            dry.dispositions
                .get("preserve_terminal_event_contradiction"),
            Some(&1)
        );
    }

    #[test]
    fn missing_database_delete_requires_session_absence_and_relative_is_preserved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = SessionLogStore::open(temp.path().join("db")).expect("store");
        let missing = temp.path().join("missing.sqlite3");
        store.with_index_connection(|conn| {
            conn.execute("INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path) VALUES ('delete','absent',?1)", params![missing.to_string_lossy()])?;
            conn.execute("INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path) VALUES ('relative','absent','relative.sqlite3')", [])?;
            conn.execute("INSERT INTO sessions(session_id,workspace,workspace_db_path,updated_at,state) VALUES ('present','w',?1,0,'idle')", params![missing.to_string_lossy()])?;
            conn.execute("INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path) VALUES ('keep','present',?1)", params![missing.to_string_lossy()])?;
            Ok(())
        }).expect("fixtures");
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 2))
            .expect("dry run");
        assert_eq!(dry.deleted_count, 1);
        assert_eq!(
            dry.dispositions.get("preserve_missing_db_session_present"),
            Some(&1)
        );
        assert_eq!(
            dry.dispositions.get("preserve_relative_or_malformed_path"),
            Some(&1)
        );
    }

    #[test]
    fn keyset_pages_more_than_five_hundred_without_skipping() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = SessionLogStore::open(temp.path().join("db")).expect("store");
        store.with_index_connection(|conn| {
            let tx = conn.transaction()?;
            for index in 0..503 {
                tx.execute("INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path) VALUES (?1,?2,?3)", params![format!("r{index:04}"), format!("s{index:04}"), temp.path().join(format!("missing-{index}.sqlite3")).to_string_lossy()])?;
            }
            tx.commit()?;
            Ok(())
        }).expect("fixtures");
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 250))
            .expect("dry run");
        assert_eq!(dry.pre_incomplete_count, 503);
        assert_eq!(dry.deleted_count, 503);
    }

    #[test]
    fn stale_digest_is_zero_mutation() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some("a".repeat(64)),
                500,
            ))
            .expect_err("stale digest");
        assert!(error.to_string().contains("DIGEST_MISMATCH"));
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("still pending");
        assert_eq!(dry.backfilled_count, 1);
    }

    #[test]
    fn active_lease_blocks_apply_without_mutation() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        store
            .with_index_connection(|conn| {
                conn.execute(
                    "UPDATE runtime_locations SET terminal_evidence_id = NULL WHERE runtime_id = 'r1'",
                    [],
                )?;
                Ok(())
            })
            .expect("index remains incomplete");
        let workspace = store
            .runtime_workspace_db_path("r1")
            .expect("route")
            .expect("workspace");
        store
            .with_workspace_connection(&workspace, |conn| {
                conn.execute(
                    "UPDATE runtimes SET lease_active = 1 WHERE runtime_id = 'r1'",
                    [],
                )?;
                Ok(())
            })
            .expect("active lease");
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("dry run");
        assert_eq!(dry.active_lease_count, 1);
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect_err("active lease must block apply");
        assert!(error.to_string().contains("NOT_QUIESCENT"));
    }

    #[test]
    fn update_cas_conflict_rolls_back() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("dry run");
        store
            .with_index_connection(|conn| {
                conn.execute_batch(
                    "CREATE TRIGGER reject_maintenance BEFORE UPDATE ON runtime_locations
                     BEGIN SELECT RAISE(IGNORE); END;",
                )?;
                Ok(())
            })
            .expect("trigger");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect_err("CAS conflict");
        assert!(error.to_string().contains("CAS_CONFLICT:r1"));
        let still_incomplete = store
            .list_runtime_locations(session_log_contract::ListRuntimeLocationsRequest {
                page: 0,
                page_size: 10,
                after_runtime_id: None,
            })
            .expect("locations")
            .1;
        assert_eq!(still_incomplete.len(), 1);
    }
}
