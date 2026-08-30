use super::SessionLogStore;
use anyhow::{Context, Result};
use fs2::FileExt;
use lifecycle::{RuntimeEvent, RuntimeState};
use rusqlite::{
    params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use session_log_contract::{
    MaintainRuntimeLocationsRequest, RuntimeLocationMaintenanceMode,
    RuntimeLocationMaintenanceReceipt,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA_VERSION: &str = "runtime_location_maintenance_v1";
const RUST_TRIM_WHITESPACE: &str = "\u{0009}\u{000a}\u{000b}\u{000c}\u{000d}\u{0020}\u{0085}\u{00a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}";
const INCOMPLETE_PREDICATE: &str = "NOT (terminal_proven = 1
    AND terminal_revision IS NOT NULL
    AND terminal_event_seq IS NOT NULL
    AND terminal_evidence_id IS NOT NULL
    AND TRIM(terminal_evidence_id, ?1) != '')";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LocationRow {
    runtime_id: String,
    session_id: String,
    workspace_db_path: String,
    terminal_proven: bool,
    terminal_revision: Option<u64>,
    terminal_event_seq: Option<u64>,
    terminal_evidence_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Proof {
    workspace_canonical_path: String,
    runtime_session_id: String,
    runtime_revision: u64,
    runtime_last_event_seq: u64,
    runtime_terminal: bool,
    runtime_lease_active: bool,
    revision: u64,
    event_seq: u64,
    evidence_id: String,
    evidence_id_occurrences: u64,
    event_json: String,
    event_json_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PlanEntry {
    row: LocationRow,
    disposition: String,
    proof: Option<Proof>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct MaintenancePlan {
    schema_version: String,
    entries: Vec<PlanEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ExpectedRowState {
    BackfilledTerminalProof {
        terminal_revision: u64,
        terminal_event_seq: u64,
        terminal_evidence_id: String,
    },
    Absent,
    Preserved {
        disposition: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ExpectedPostcondition {
    runtime_id: String,
    session_id: String,
    workspace_db_path: String,
    expected: ExpectedRowState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct MaintenanceIntent {
    schema_version: String,
    canonical_input_sha256: String,
    plan: MaintenancePlan,
    expected_postconditions: Vec<ExpectedPostcondition>,
    expected_receipt: RuntimeLocationMaintenanceReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PreparedMarker {
    schema_version: String,
    canonical_input_sha256: String,
    intent_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct MaintenanceManifest {
    schema_version: String,
    canonical_input_sha256: String,
    intent_sha256: String,
    prepared_sha256: String,
    receipt_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedState {
    AllPre,
    AllPost,
    MixedOrDrift,
}

struct VerifiedDurablePoststate;

impl SessionLogStore {
    pub fn maintain_runtime_locations(
        &self,
        request: MaintainRuntimeLocationsRequest,
    ) -> Result<RuntimeLocationMaintenanceReceipt> {
        let page_size = request.page_size.clamp(1, 500);
        validate_request(&request)?;

        let _maintenance_lock = MaintenanceLock::acquire(&self.index_db_path)?;
        if matches!(request.mode, RuntimeLocationMaintenanceMode::DryRun) {
            return self.with_index_connection(|conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
                let plan = build_plan(&tx, page_size)?;
                let canonical_input_sha256 = digest_json(&plan)?;
                let receipt = receipt_for_plan(
                    &plan,
                    canonical_input_sha256,
                    RuntimeLocationMaintenanceMode::DryRun,
                );
                tx.commit()?;
                Ok(receipt)
            });
        }

        let expected = request
            .expected_dry_run_sha256
            .as_deref()
            .context("apply requires expected_dry_run_sha256")?;
        if let Some(receipt) = self.read_existing_receipt(expected)? {
            return Ok(replayed_receipt(receipt));
        }
        if let Some(intent) = self.read_or_prepare_intent(expected)? {
            return self.recover_prepared_intent(intent);
        }

        self.with_index_connection(|conn| {
            enable_maintenance_durability(conn)?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let plan = build_plan(&tx, page_size)?;
            let canonical_input_sha256 = digest_json(&plan)?;
            if expected != canonical_input_sha256 {
                anyhow::bail!(
                    "RUNTIME_LOCATION_MAINTENANCE_DIGEST_MISMATCH: expected {expected}, got {canonical_input_sha256}"
                );
            }
            let intent = intent_for_plan(plan, canonical_input_sha256)?;
            if intent.expected_receipt.active_lease_count != 0 {
                anyhow::bail!(
                    "RUNTIME_LOCATION_MAINTENANCE_NOT_QUIESCENT: {} active lease(s)",
                    intent.expected_receipt.active_lease_count
                );
            }
            self.publish_intent(&intent)?;
            let (backfilled_count, deleted_count) = apply_exact_plan(&tx, &intent.plan)?;
            let post_incomplete_count = count_incomplete(&tx)?;
            if post_incomplete_count != intent.expected_receipt.post_incomplete_count
                || backfilled_count != intent.expected_receipt.backfilled_count
                || deleted_count != intent.expected_receipt.deleted_count
            {
                anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_POSTCONDITION_MISMATCH");
            }
            verify_exact_postconditions(&tx, &intent)?;
            tx.commit()?;
            durable_checkpoint(&self.index_db_path, conn)?;
            let durable_poststate = verify_committed_poststate(conn, &intent)?;
            self.finalize_intent(&intent, false, durable_poststate)
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
        if !path_is_regular_nonsymlink(&manifest) {
            if std::fs::symlink_metadata(&manifest).is_ok() {
                anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_RECEIPT_IDENTITY_CONFLICT");
            }
            return Ok(None);
        }
        let manifest_bytes = read_stable_regular(&manifest)?;
        let manifest_value: MaintenanceManifest = serde_json::from_slice(&manifest_bytes)?;
        let intent_bytes = read_stable_regular(&root.join("intent.json"))?;
        let prepared_bytes = read_stable_regular(&root.join("prepared.json"))?;
        let receipt_bytes = read_stable_regular(&root.join("receipt.json"))?;
        let receipt: RuntimeLocationMaintenanceReceipt = serde_json::from_slice(&receipt_bytes)?;
        if serde_json::to_vec(&manifest_value)? != manifest_bytes
            || serde_json::to_vec(&receipt)? != receipt_bytes
            || manifest_value.schema_version != SCHEMA_VERSION
            || receipt.schema_version != SCHEMA_VERSION
            || receipt.canonical_input_sha256 != digest
            || receipt.mode != RuntimeLocationMaintenanceMode::Apply
            || manifest_value.canonical_input_sha256 != digest
            || manifest_value.intent_sha256 != sha256_bytes(&intent_bytes)
            || manifest_value.prepared_sha256 != sha256_bytes(&prepared_bytes)
            || manifest_value.receipt_sha256 != sha256_bytes(&receipt_bytes)
        {
            anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_RECEIPT_IDENTITY_CONFLICT");
        }
        let intent = validate_intent_bytes(digest, &intent_bytes)?;
        validate_prepared_bytes(digest, &intent_bytes, &prepared_bytes)?;
        if receipt != intent.expected_receipt {
            anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_RECEIPT_IDENTITY_CONFLICT");
        }
        self.verify_final_receipt_poststate(&intent)?;
        Ok(Some(receipt_with_manifest(receipt, &manifest)))
    }

    fn read_or_prepare_intent(&self, digest: &str) -> Result<Option<MaintenanceIntent>> {
        let root = self.receipt_root(digest);
        let intent_path = root.join("intent.json");
        let prepared_path = root.join("prepared.json");
        if !path_is_regular_nonsymlink(&intent_path) {
            if std::fs::symlink_metadata(&intent_path).is_ok()
                || std::fs::symlink_metadata(&prepared_path).is_ok()
            {
                anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_INTENT_IDENTITY_CONFLICT");
            }
            return Ok(None);
        }
        let intent_bytes = read_stable_regular(&intent_path)?;
        let intent = validate_intent_bytes(digest, &intent_bytes)?;
        let prepared = prepared_bytes(digest, &intent_bytes)?;
        write_immutable(&prepared_path, &prepared)?;
        validate_prepared_bytes(digest, &intent_bytes, &read_stable_regular(&prepared_path)?)?;
        Ok(Some(intent))
    }

    fn publish_intent(&self, intent: &MaintenanceIntent) -> Result<()> {
        let root = self.receipt_root(&intent.canonical_input_sha256);
        ensure_receipt_root(&root)?;
        let intent_bytes = serde_json::to_vec(intent)?;
        write_immutable(&root.join("intent.json"), &intent_bytes)?;
        write_immutable(
            &root.join("prepared.json"),
            &prepared_bytes(&intent.canonical_input_sha256, &intent_bytes)?,
        )?;
        Ok(())
    }

    fn recover_prepared_intent(
        &self,
        intent: MaintenanceIntent,
    ) -> Result<RuntimeLocationMaintenanceReceipt> {
        self.with_index_connection(|conn| {
            enable_maintenance_durability(conn)?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            match prepared_state(&tx, &intent)? {
                PreparedState::AllPost => {
                    verify_exact_postconditions(&tx, &intent)?;
                    tx.commit()?;
                    durable_checkpoint(&self.index_db_path, conn)?;
                    let durable_poststate = verify_committed_poststate(conn, &intent)?;
                    self.finalize_intent(&intent, true, durable_poststate)
                }
                PreparedState::AllPre => {
                    let current_plan = build_plan(&tx, 500)?;
                    if current_plan != intent.plan
                        || digest_json(&current_plan)? != intent.canonical_input_sha256
                    {
                        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_PREPARED_INTENT_DRIFT");
                    }
                    if intent.expected_receipt.active_lease_count != 0 {
                        anyhow::bail!(
                            "RUNTIME_LOCATION_MAINTENANCE_NOT_QUIESCENT: {} active lease(s)",
                            intent.expected_receipt.active_lease_count
                        );
                    }
                    let (backfilled, deleted) = apply_exact_plan(&tx, &intent.plan)?;
                    if backfilled != intent.expected_receipt.backfilled_count
                        || deleted != intent.expected_receipt.deleted_count
                        || count_incomplete(&tx)? != intent.expected_receipt.post_incomplete_count
                    {
                        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_POSTCONDITION_MISMATCH");
                    }
                    verify_exact_postconditions(&tx, &intent)?;
                    tx.commit()?;
                    durable_checkpoint(&self.index_db_path, conn)?;
                    let durable_poststate = verify_committed_poststate(conn, &intent)?;
                    self.finalize_intent(&intent, false, durable_poststate)
                }
                PreparedState::MixedOrDrift => {
                    anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_PREPARED_INTENT_MIXED_OR_DRIFT")
                }
            }
        })
    }

    fn verify_final_receipt_poststate(&self, intent: &MaintenanceIntent) -> Result<()> {
        self.with_index_connection(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
            match prepared_state(&tx, intent)? {
                PreparedState::AllPost => verify_exact_postconditions(&tx, intent)?,
                PreparedState::AllPre => {
                    anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_STALE_FINAL_RECEIPT_ALL_PRE")
                }
                PreparedState::MixedOrDrift => {
                    anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_FINAL_RECEIPT_MIXED_OR_DRIFT")
                }
            }
            tx.commit()?;
            Ok(())
        })
    }

    fn finalize_intent(
        &self,
        intent: &MaintenanceIntent,
        recovered_without_mutation: bool,
        _durable_poststate: VerifiedDurablePoststate,
    ) -> Result<RuntimeLocationMaintenanceReceipt> {
        let root = self.receipt_root(&intent.canonical_input_sha256);
        let intent_bytes = read_stable_regular(&root.join("intent.json"))?;
        let prepared_bytes = read_stable_regular(&root.join("prepared.json"))?;
        let receipt_bytes = serde_json::to_vec(&intent.expected_receipt)?;
        write_immutable(&root.join("receipt.json"), &receipt_bytes)?;
        let manifest = MaintenanceManifest {
            schema_version: SCHEMA_VERSION.to_string(),
            canonical_input_sha256: intent.canonical_input_sha256.clone(),
            intent_sha256: sha256_bytes(&intent_bytes),
            prepared_sha256: sha256_bytes(&prepared_bytes),
            receipt_sha256: sha256_bytes(&receipt_bytes),
        };
        let manifest_path = root.join("manifest.json");
        write_immutable(&manifest_path, &serde_json::to_vec(&manifest)?)?;
        let mut receipt = receipt_with_manifest(intent.expected_receipt.clone(), &manifest_path);
        if recovered_without_mutation {
            receipt = replayed_receipt(receipt);
        }
        Ok(receipt)
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

fn receipt_for_plan(
    plan: &MaintenancePlan,
    canonical_input_sha256: String,
    mode: RuntimeLocationMaintenanceMode,
) -> RuntimeLocationMaintenanceReceipt {
    let mut dispositions = BTreeMap::new();
    for entry in &plan.entries {
        *dispositions.entry(entry.disposition.clone()).or_insert(0) += 1;
    }
    let backfilled_count = backfillable_count(plan);
    let deleted_count = deletable_count(plan);
    let pre_incomplete_count = plan.entries.len() as u64;
    RuntimeLocationMaintenanceReceipt {
        schema_version: SCHEMA_VERSION.to_string(),
        mode: mode.clone(),
        canonical_input_sha256,
        pre_incomplete_count,
        post_incomplete_count: pre_incomplete_count
            .saturating_sub(backfilled_count)
            .saturating_sub(deleted_count),
        backfilled_count,
        deleted_count,
        mutation_count: if matches!(mode, RuntimeLocationMaintenanceMode::Apply) {
            backfilled_count + deleted_count
        } else {
            0
        },
        active_lease_count: dispositions
            .get("preserve_active_lease")
            .copied()
            .unwrap_or(0),
        dispositions,
        replayed_receipt: false,
        manifest_path: None,
    }
}

fn expected_postconditions(plan: &MaintenancePlan) -> Result<Vec<ExpectedPostcondition>> {
    plan.entries
        .iter()
        .filter_map(expected_postcondition_for_entry)
        .collect()
}

fn expected_postcondition_for_entry(entry: &PlanEntry) -> Option<Result<ExpectedPostcondition>> {
    match entry.disposition.as_str() {
        "backfill_terminal_event_proof" => Some(
            entry
                .proof
                .as_ref()
                .context("backfill proof missing")
                .map(|proof| ExpectedPostcondition {
                    runtime_id: entry.row.runtime_id.clone(),
                    session_id: entry.row.session_id.clone(),
                    workspace_db_path: entry.row.workspace_db_path.clone(),
                    expected: ExpectedRowState::BackfilledTerminalProof {
                        terminal_revision: proof.revision,
                        terminal_event_seq: proof.event_seq,
                        terminal_evidence_id: proof.evidence_id.clone(),
                    },
                }),
        ),
        "delete_missing_db_session_absent" => Some(Ok(ExpectedPostcondition {
            runtime_id: entry.row.runtime_id.clone(),
            session_id: entry.row.session_id.clone(),
            workspace_db_path: entry.row.workspace_db_path.clone(),
            expected: ExpectedRowState::Absent,
        })),
        _ => Some(Ok(ExpectedPostcondition {
            runtime_id: entry.row.runtime_id.clone(),
            session_id: entry.row.session_id.clone(),
            workspace_db_path: entry.row.workspace_db_path.clone(),
            expected: ExpectedRowState::Preserved {
                disposition: entry.disposition.clone(),
            },
        })),
    }
}

fn intent_for_plan(
    plan: MaintenancePlan,
    canonical_input_sha256: String,
) -> Result<MaintenanceIntent> {
    Ok(MaintenanceIntent {
        schema_version: SCHEMA_VERSION.to_string(),
        expected_postconditions: expected_postconditions(&plan)?,
        expected_receipt: receipt_for_plan(
            &plan,
            canonical_input_sha256.clone(),
            RuntimeLocationMaintenanceMode::Apply,
        ),
        canonical_input_sha256,
        plan,
    })
}

fn replayed_receipt(
    mut receipt: RuntimeLocationMaintenanceReceipt,
) -> RuntimeLocationMaintenanceReceipt {
    receipt.replayed_receipt = true;
    receipt.mutation_count = 0;
    receipt
}

fn receipt_with_manifest(
    mut receipt: RuntimeLocationMaintenanceReceipt,
    manifest: &Path,
) -> RuntimeLocationMaintenanceReceipt {
    receipt.manifest_path = Some(manifest.to_string_lossy().into_owned());
    receipt
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn path_is_regular_nonsymlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_file() && !metadata.file_type().is_symlink())
        .unwrap_or(false)
}

fn read_stable_regular(path: &Path) -> Result<Vec<u8>> {
    let before = std::fs::symlink_metadata(path)?;
    if !before.file_type().is_file() || before.file_type().is_symlink() {
        anyhow::bail!(
            "RUNTIME_LOCATION_MAINTENANCE_MEMBER_PATH_INVALID:{}",
            path.display()
        );
    }
    let bytes = std::fs::read(path)?;
    let after = std::fs::symlink_metadata(path)?;
    if before.len() != after.len()
        || before.modified()? != after.modified()?
        || bytes.len() as u64 != after.len()
    {
        anyhow::bail!(
            "RUNTIME_LOCATION_MAINTENANCE_MEMBER_READ_DRIFT:{}",
            path.display()
        );
    }
    Ok(bytes)
}

fn fsync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn ensure_receipt_root(root: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = root;
    loop {
        match std::fs::symlink_metadata(cursor) {
            Ok(metadata) => {
                if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
                    anyhow::bail!(
                        "RUNTIME_LOCATION_MAINTENANCE_RECEIPT_ROOT_INVALID:{}",
                        cursor.display()
                    );
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(cursor.to_path_buf());
                cursor = cursor
                    .parent()
                    .context("maintenance receipt root has no existing ancestor")?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    for directory in missing.into_iter().rev() {
        std::fs::create_dir(&directory)?;
        let parent = directory
            .parent()
            .context("maintenance receipt directory has no parent")?;
        fsync_directory(parent)?;
        fsync_directory(&directory)?;
    }
    if let Some(parent) = root.parent() {
        fsync_directory(parent)?;
    }
    fsync_directory(root)?;
    Ok(())
}

fn write_immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    if path_is_regular_nonsymlink(path) {
        if read_stable_regular(path)? == bytes {
            return Ok(());
        }
        anyhow::bail!(
            "RUNTIME_LOCATION_MAINTENANCE_MEMBER_IDENTITY_CONFLICT:{}",
            path.display()
        );
    }
    if std::fs::symlink_metadata(path).is_ok() {
        anyhow::bail!(
            "RUNTIME_LOCATION_MAINTENANCE_MEMBER_PATH_INVALID:{}",
            path.display()
        );
    }
    let parent = path.parent().context("maintenance member parent missing")?;
    ensure_receipt_root(parent)?;
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(
        ".{}.tmp-{}-{nanos}-{counter}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("member"),
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        match std::fs::hard_link(&temporary, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !path_is_regular_nonsymlink(path) || read_stable_regular(path)? != bytes {
                    anyhow::bail!(
                        "RUNTIME_LOCATION_MAINTENANCE_MEMBER_IDENTITY_CONFLICT:{}",
                        path.display()
                    );
                }
            }
            Err(error) => return Err(error.into()),
        }
        std::fs::remove_file(&temporary)?;
        fsync_directory(parent)?;
        if read_stable_regular(path)? != bytes {
            anyhow::bail!(
                "RUNTIME_LOCATION_MAINTENANCE_MEMBER_READBACK_MISMATCH:{}",
                path.display()
            );
        }
        Ok(())
    })();
    if temporary.exists() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn prepared_bytes(digest: &str, intent_bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&PreparedMarker {
        schema_version: SCHEMA_VERSION.to_string(),
        canonical_input_sha256: digest.to_string(),
        intent_sha256: sha256_bytes(intent_bytes),
    })?)
}

fn validate_prepared_bytes(digest: &str, intent_bytes: &[u8], bytes: &[u8]) -> Result<()> {
    let value: PreparedMarker = serde_json::from_slice(bytes)?;
    if serde_json::to_vec(&value)? != bytes
        || value.schema_version != SCHEMA_VERSION
        || value.canonical_input_sha256 != digest
        || value.intent_sha256 != sha256_bytes(intent_bytes)
    {
        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_PREPARED_IDENTITY_CONFLICT");
    }
    Ok(())
}

fn validate_intent_bytes(digest: &str, bytes: &[u8]) -> Result<MaintenanceIntent> {
    let intent: MaintenanceIntent = serde_json::from_slice(bytes)?;
    let expected_intent = intent_for_plan(intent.plan.clone(), digest.to_string())?;
    if serde_json::to_vec(&intent)? != bytes
        || intent.schema_version != SCHEMA_VERSION
        || intent.canonical_input_sha256 != digest
        || digest_json(&intent.plan)? != digest
        || intent != expected_intent
    {
        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_INTENT_IDENTITY_CONFLICT");
    }
    Ok(intent)
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
    let mut conn = match Connection::open_with_flags(
        &canonical,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(conn) => conn,
        Err(_) => return Ok(entry(row, "preserve_workspace_db_unreadable", None)),
    };
    let workspace = match conn.transaction_with_behavior(TransactionBehavior::Deferred) {
        Ok(transaction) => transaction,
        Err(_) => return Ok(entry(row, "preserve_workspace_db_unreadable", None)),
    };
    let runtime = workspace.query_row(
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
    let event = workspace
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
    let evidence_id_occurrences = workspace.query_row(
        "SELECT COUNT(*) FROM runtime_events WHERE idempotency_key = ?1",
        params![evidence_id],
        |r| r.get::<_, u64>(0),
    )?;
    let parsed = serde_json::from_str::<RuntimeEvent>(&event_json).ok();
    let terminal_kind = matches!(
        parsed,
        Some(RuntimeEvent::RuntimeFinished { .. })
            | Some(RuntimeEvent::RuntimeFailed {
                state: RuntimeState::Failed | RuntimeState::TimedOut | RuntimeState::Cancelled,
                ..
            })
    );
    if revision != event_revision
        || last_event_seq != event_seq
        || evidence_id.trim().is_empty()
        || evidence_id_occurrences != 1
        || !terminal_kind
    {
        return Ok(entry(row, "preserve_terminal_event_contradiction", None));
    }
    Ok(entry(
        row,
        "backfill_terminal_event_proof",
        Some(Proof {
            workspace_canonical_path: canonical.to_string_lossy().into_owned(),
            runtime_session_id: session_id,
            runtime_revision: revision,
            runtime_last_event_seq: last_event_seq,
            runtime_terminal: terminal,
            runtime_lease_active: lease_active,
            revision,
            event_seq,
            evidence_id,
            evidence_id_occurrences,
            event_json_sha256: sha256_bytes(event_json.as_bytes()),
            event_json,
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

fn load_location_row(tx: &Transaction<'_>, runtime_id: &str) -> Result<Option<LocationRow>> {
    tx.query_row(
        "SELECT runtime_id, session_id, workspace_db_path, terminal_proven,
                terminal_revision, terminal_event_seq, terminal_evidence_id
         FROM runtime_locations WHERE runtime_id = ?1",
        params![runtime_id],
        |row| {
            Ok(LocationRow {
                runtime_id: row.get(0)?,
                session_id: row.get(1)?,
                workspace_db_path: row.get(2)?,
                terminal_proven: row.get(3)?,
                terminal_revision: row.get(4)?,
                terminal_event_seq: row.get(5)?,
                terminal_evidence_id: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn entry_is_pre(tx: &Transaction<'_>, entry: &PlanEntry) -> Result<bool> {
    let Some(current) = load_location_row(tx, &entry.row.runtime_id)? else {
        return Ok(false);
    };
    if current != entry.row {
        return Ok(false);
    }
    Ok(classify_row(tx, current)? == *entry)
}

fn entry_is_post(tx: &Transaction<'_>, entry: &PlanEntry) -> Result<bool> {
    match entry.disposition.as_str() {
        "backfill_terminal_event_proof" => {
            let Some(proof) = entry.proof.as_ref() else {
                return Ok(false);
            };
            let Some(current) = load_location_row(tx, &entry.row.runtime_id)? else {
                return Ok(false);
            };
            if current.runtime_id != entry.row.runtime_id
                || current.session_id != entry.row.session_id
                || current.workspace_db_path != entry.row.workspace_db_path
                || !current.terminal_proven
                || current.terminal_revision != Some(proof.revision)
                || current.terminal_event_seq != Some(proof.event_seq)
                || current.terminal_evidence_id.as_deref() != Some(proof.evidence_id.as_str())
            {
                return Ok(false);
            }
            Ok(classify_row(tx, entry.row.clone())? == *entry)
        }
        "delete_missing_db_session_absent" => {
            if load_location_row(tx, &entry.row.runtime_id)?.is_some() {
                return Ok(false);
            }
            let session_exists = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
                params![entry.row.session_id],
                |row| row.get::<_, bool>(0),
            )?;
            Ok(!session_exists
                && matches!(
                    std::fs::symlink_metadata(&entry.row.workspace_db_path),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound
                ))
        }
        _ => Ok(true),
    }
}

fn prepared_state(tx: &Transaction<'_>, intent: &MaintenanceIntent) -> Result<PreparedState> {
    let mut pre = 0_u64;
    let mut post = 0_u64;
    let mut mutation_entries = 0_u64;
    for entry in &intent.plan.entries {
        if !matches!(
            entry.disposition.as_str(),
            "backfill_terminal_event_proof" | "delete_missing_db_session_absent"
        ) {
            continue;
        }
        mutation_entries += 1;
        if entry_is_pre(tx, entry)? {
            pre += 1;
        } else if entry_is_post(tx, entry)? {
            post += 1;
        } else {
            return Ok(PreparedState::MixedOrDrift);
        }
    }
    if mutation_entries == 0 || post == mutation_entries {
        Ok(PreparedState::AllPost)
    } else if pre == mutation_entries {
        Ok(PreparedState::AllPre)
    } else {
        Ok(PreparedState::MixedOrDrift)
    }
}

fn verify_exact_postconditions(tx: &Transaction<'_>, intent: &MaintenanceIntent) -> Result<()> {
    if expected_postconditions(&intent.plan)? != intent.expected_postconditions {
        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_POSTCONDITION_IDENTITY_CONFLICT");
    }
    for entry in &intent.plan.entries {
        let postcondition_holds = if matches!(
            entry.disposition.as_str(),
            "backfill_terminal_event_proof" | "delete_missing_db_session_absent"
        ) {
            entry_is_post(tx, entry)?
        } else {
            entry_is_pre(tx, entry)?
        };
        if !postcondition_holds {
            anyhow::bail!(
                "RUNTIME_LOCATION_MAINTENANCE_EXACT_POSTCONDITION_MISMATCH:{}",
                entry.row.runtime_id
            );
        }
    }
    let post_incomplete_count = count_incomplete(tx)?;
    if post_incomplete_count != intent.expected_receipt.post_incomplete_count {
        anyhow::bail!(
            "RUNTIME_LOCATION_MAINTENANCE_AGGREGATE_POSTCONDITION_MISMATCH:expected={},actual={post_incomplete_count}",
            intent.expected_receipt.post_incomplete_count
        );
    }
    Ok(())
}

fn verify_committed_poststate(
    conn: &mut Connection,
    intent: &MaintenanceIntent,
) -> Result<VerifiedDurablePoststate> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    verify_exact_postconditions(&tx, intent)?;
    tx.commit()?;
    Ok(VerifiedDurablePoststate)
}

fn enable_maintenance_durability(conn: &Connection) -> Result<()> {
    let journal_mode =
        conn.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_JOURNAL_MODE_MISMATCH:{journal_mode}");
    }
    conn.pragma_update(None, "synchronous", "FULL")?;
    let synchronous = conn.pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))?;
    if synchronous != 2 {
        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_DURABILITY_MODE_MISMATCH:{synchronous}");
    }
    Ok(())
}

fn durable_checkpoint(index_path: &Path, conn: &Connection) -> Result<()> {
    #[cfg(test)]
    if DURABILITY_CHECKPOINT_FAILURE.with(|failure| failure.replace(false)) {
        anyhow::bail!("RUNTIME_LOCATION_MAINTENANCE_INJECTED_DURABILITY_FAILURE");
    }

    let (busy, log_frames, checkpointed_frames) =
        conn.query_row("PRAGMA wal_checkpoint(FULL)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
    if busy != 0 || checkpointed_frames < log_frames {
        anyhow::bail!(
            "RUNTIME_LOCATION_MAINTENANCE_DURABILITY_CHECKPOINT_INCOMPLETE:busy={busy},log={log_frames},checkpointed={checkpointed_frames}"
        );
    }
    File::open(index_path)?.sync_all()?;
    let mut wal_path = index_path.as_os_str().to_os_string();
    wal_path.push("-wal");
    let wal_path = PathBuf::from(wal_path);
    match std::fs::symlink_metadata(&wal_path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            File::open(&wal_path)?.sync_all()?;
        }
        Ok(_) => anyhow::bail!(
            "RUNTIME_LOCATION_MAINTENANCE_WAL_PATH_INVALID:{}",
            wal_path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    if let Some(parent) = index_path.parent() {
        fsync_directory(parent)?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static DURABILITY_CHECKPOINT_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn inject_durability_checkpoint_failure() {
    DURABILITY_CHECKPOINT_FAILURE.with(|failure| failure.set(true));
}

fn apply_exact_plan(tx: &Transaction<'_>, plan: &MaintenancePlan) -> Result<(u64, u64)> {
    let mut backfilled_count = 0_u64;
    let mut deleted_count = 0_u64;
    for entry in &plan.entries {
        match entry.disposition.as_str() {
            "backfill_terminal_event_proof" => {
                if !entry_is_pre(tx, entry)? {
                    anyhow::bail!(
                        "RUNTIME_LOCATION_MAINTENANCE_CAS_CONFLICT:{}:precondition",
                        entry.row.runtime_id
                    );
                }
                let proof = entry.proof.as_ref().context("backfill proof missing")?;
                let changed = tx.execute(
                    "UPDATE runtime_locations SET terminal_proven = 1,
                     terminal_revision = ?4, terminal_event_seq = ?5,
                     terminal_evidence_id = ?6
                     WHERE runtime_id = ?1 AND session_id = ?2
                       AND workspace_db_path = ?3 AND terminal_proven = 0
                       AND terminal_revision IS NULL AND terminal_event_seq IS NULL
                       AND terminal_evidence_id IS NULL",
                    params![
                        entry.row.runtime_id,
                        entry.row.session_id,
                        entry.row.workspace_db_path,
                        proof.revision,
                        proof.event_seq,
                        proof.evidence_id
                    ],
                )?;
                if changed != 1 {
                    anyhow::bail!(
                        "RUNTIME_LOCATION_MAINTENANCE_CAS_CONFLICT:{}",
                        entry.row.runtime_id
                    );
                }
                backfilled_count += 1;
            }
            "delete_missing_db_session_absent" => {
                if !entry_is_pre(tx, entry)? {
                    anyhow::bail!(
                        "RUNTIME_LOCATION_MAINTENANCE_CAS_CONFLICT:{}:precondition",
                        entry.row.runtime_id
                    );
                }
                let changed = tx.execute(
                    "DELETE FROM runtime_locations WHERE runtime_id = ?1
                     AND session_id = ?2 AND workspace_db_path = ?3
                     AND terminal_proven = 0 AND terminal_revision IS NULL
                     AND terminal_event_seq IS NULL AND terminal_evidence_id IS NULL",
                    params![
                        entry.row.runtime_id,
                        entry.row.session_id,
                        entry.row.workspace_db_path
                    ],
                )?;
                if changed != 1 {
                    anyhow::bail!(
                        "RUNTIME_LOCATION_MAINTENANCE_CAS_CONFLICT:{}",
                        entry.row.runtime_id
                    );
                }
                deleted_count += 1;
            }
            _ => {}
        }
    }
    Ok((backfilled_count, deleted_count))
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
    use lifecycle::RuntimeError;
    use std::sync::{Arc, Barrier};
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

    fn prepare_without_commit(store: &SessionLogStore) -> MaintenanceIntent {
        store
            .with_index_connection(|conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let plan = build_plan(&tx, 500)?;
                let canonical_input_sha256 = digest_json(&plan)?;
                let intent = intent_for_plan(plan, canonical_input_sha256)?;
                store.publish_intent(&intent)?;
                drop(tx);
                Ok(intent)
            })
            .expect("prepared intent")
    }

    fn inject_false_final_manifest(store: &SessionLogStore, intent: &MaintenanceIntent) {
        let root = store.receipt_root(&intent.canonical_input_sha256);
        let intent_bytes = read_stable_regular(&root.join("intent.json")).expect("intent bytes");
        let prepared_bytes =
            read_stable_regular(&root.join("prepared.json")).expect("prepared bytes");
        let receipt_bytes = serde_json::to_vec(&intent.expected_receipt).expect("receipt bytes");
        write_immutable(&root.join("receipt.json"), &receipt_bytes).expect("false receipt");
        let manifest = MaintenanceManifest {
            schema_version: SCHEMA_VERSION.to_string(),
            canonical_input_sha256: intent.canonical_input_sha256.clone(),
            intent_sha256: sha256_bytes(&intent_bytes),
            prepared_sha256: sha256_bytes(&prepared_bytes),
            receipt_sha256: sha256_bytes(&receipt_bytes),
        };
        write_immutable(
            &root.join("manifest.json"),
            &serde_json::to_vec(&manifest).expect("manifest bytes"),
        )
        .expect("false manifest");
    }

    fn terminal_proven(store: &SessionLogStore) -> bool {
        store
            .with_index_connection(|conn| {
                conn.query_row(
                    "SELECT terminal_proven FROM runtime_locations WHERE runtime_id = 'r1'",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .expect("terminal proof state")
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
        let same_inputs_different_page = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("stable dry run");
        assert_eq!(
            same_inputs_different_page.canonical_input_sha256,
            dry.canonical_input_sha256
        );
        let applied = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                1,
            ))
            .expect("apply");
        assert_eq!(applied.mutation_count, 1);
        assert!(applied
            .manifest_path
            .as_ref()
            .is_some_and(|path| Path::new(path).is_file()));
        let default_synchronous = store
            .with_index_connection(|conn| {
                conn.pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
                    .map_err(Into::into)
            })
            .expect("connection default synchronous mode");
        assert_eq!(
            default_synchronous, 1,
            "FULL must remain maintenance-scoped"
        );
        let replay = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect("replay");
        assert!(replay.replayed_receipt);
        assert_eq!(replay.mutation_count, 0);
        assert_eq!(replay.backfilled_count, 1);

        let manifest = PathBuf::from(replay.manifest_path.expect("replay manifest"));
        let mut noncanonical = std::fs::read(&manifest).expect("manifest bytes");
        noncanonical.push(b'\n');
        std::fs::write(&manifest, noncanonical).expect("tamper manifest bytes");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect_err("noncanonical final manifest must not be accepted");
        assert!(error.to_string().contains("RECEIPT_IDENTITY_CONFLICT"));
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

    #[test]
    fn after_update_evidence_tamper_fails_exact_postcondition_and_rolls_back() {
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
                    "CREATE TRIGGER tamper_maintenance_evidence
                     AFTER UPDATE OF terminal_proven ON runtime_locations
                     WHEN NEW.runtime_id = 'r1'
                     BEGIN
                       UPDATE runtime_locations SET terminal_evidence_id = 'tampered'
                       WHERE runtime_id = NEW.runtime_id;
                     END;",
                )?;
                Ok(())
            })
            .expect("tamper trigger");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect_err("post-update tamper must roll back");
        assert!(error
            .to_string()
            .contains("EXACT_POSTCONDITION_MISMATCH:r1"));
        assert!(!terminal_proven(&store));
        assert!(!store
            .receipt_root(&dry.canonical_input_sha256)
            .join("manifest.json")
            .exists());
    }

    #[test]
    fn final_receipt_with_all_pre_database_is_typed_stale_not_replayed() {
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
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect("initial apply");
        store
            .with_index_connection(|conn| {
                conn.execute(
                    "UPDATE runtime_locations SET terminal_proven = 0,
                     terminal_revision = NULL, terminal_event_seq = NULL,
                     terminal_evidence_id = NULL WHERE runtime_id = 'r1'",
                    [],
                )?;
                Ok(())
            })
            .expect("restore exact all-pre database state");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect_err("final receipt must not override all-pre database");
        assert!(error.to_string().contains("STALE_FINAL_RECEIPT_ALL_PRE"));
        assert!(!terminal_proven(&store));
    }

    #[test]
    fn final_receipt_with_tampered_evidence_is_typed_drift_not_replayed() {
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
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect("initial apply");
        store
            .with_index_connection(|conn| {
                conn.execute(
                    "UPDATE runtime_locations SET terminal_evidence_id = 'tampered'
                     WHERE runtime_id = 'r1'",
                    [],
                )?;
                Ok(())
            })
            .expect("tamper committed evidence");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect_err("tampered final poststate must fail closed");
        assert!(error.to_string().contains("FINAL_RECEIPT_MIXED_OR_DRIFT"));
        let evidence = store
            .with_index_connection(|conn| {
                conn.query_row(
                    "SELECT terminal_evidence_id FROM runtime_locations WHERE runtime_id = 'r1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map_err(Into::into)
            })
            .expect("tampered evidence remains untouched");
        assert_eq!(evidence, "tampered");
    }

    #[test]
    fn final_receipt_with_aggregate_drift_is_not_replayed() {
        let (temp, store, _) = fixture(
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
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect("initial apply");
        store
            .with_index_connection(|conn| {
                conn.execute(
                    "INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path)
                     VALUES ('later','later-session',?1)",
                    params![temp.path().join("later-missing.sqlite3").to_string_lossy()],
                )?;
                Ok(())
            })
            .expect("later incomplete location");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect_err("aggregate drift must not replay final receipt");
        assert!(error
            .to_string()
            .contains("AGGREGATE_POSTCONDITION_MISMATCH"));
    }

    #[test]
    fn final_receipt_revalidates_preserved_workspace_runtime_fields() {
        let (_temp, store, workspace) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        store
            .with_workspace_connection(Path::new(&workspace), |conn| {
                conn.pragma_update(None, "foreign_keys", "OFF")?;
                conn.execute(
                    "INSERT INTO runtimes(runtime_id, session_id, revision,
                     last_event_seq, terminal, lease_active)
                     VALUES ('r2','s2',0,0,0,0)",
                    [],
                )?;
                Ok(())
            })
            .expect("preserved nonterminal runtime");
        store
            .with_index_connection(|conn| {
                conn.execute(
                    "INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path)
                     VALUES ('r2','s2',?1)",
                    params![workspace],
                )?;
                Ok(())
            })
            .expect("preserved location");
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("dry run");
        assert_eq!(dry.dispositions.get("preserve_nonterminal"), Some(&1));
        store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect("initial apply");
        store
            .with_workspace_connection(Path::new(&workspace), |conn| {
                conn.execute(
                    "UPDATE runtimes SET terminal = 1 WHERE runtime_id = 'r2'",
                    [],
                )?;
                Ok(())
            })
            .expect("drift preserved runtime terminal field");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect_err("preserved runtime drift must invalidate replay");
        assert!(error
            .to_string()
            .contains("EXACT_POSTCONDITION_MISMATCH:r2"));
    }

    #[test]
    fn injected_manifest_before_database_poststate_is_rejected() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let intent = prepare_without_commit(&store);
        inject_false_final_manifest(&store, &intent);
        assert!(!terminal_proven(&store));
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(intent.canonical_input_sha256),
                500,
            ))
            .expect_err("manifest without database poststate must fail closed");
        assert!(error.to_string().contains("STALE_FINAL_RECEIPT_ALL_PRE"));
    }

    #[test]
    fn durability_checkpoint_failure_never_publishes_final_and_recovers_all_post() {
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
        inject_durability_checkpoint_failure();
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect_err("injected durability checkpoint failure");
        assert!(error.to_string().contains("INJECTED_DURABILITY_FAILURE"));
        assert!(terminal_proven(&store));
        let root = store.receipt_root(&dry.canonical_input_sha256);
        assert!(!root.join("receipt.json").exists());
        assert!(!root.join("manifest.json").exists());

        let recovered = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect("recover durable all-post state");
        assert!(recovered.replayed_receipt);
        assert_eq!(recovered.mutation_count, 0);
        assert!(root.join("manifest.json").is_file());
    }

    #[test]
    fn terminal_event_bytes_are_bound_into_the_dry_run_digest() {
        let (_temp, store, workspace) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let first = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("first dry run");
        let changed = RuntimeEvent::RuntimeFailed {
            finished_at: chrono::Utc::now(),
            error: RuntimeError {
                error_code: Some("cancelled".to_string()),
                error_text: Some("cancelled by test".to_string()),
                retry_allowed: false,
                fallback_allowed: false,
                fallback_to_id: None,
            },
            state: RuntimeState::Cancelled,
            usage: None,
        };
        store
            .with_workspace_connection(Path::new(&workspace), |conn| {
                conn.execute(
                    "UPDATE runtime_events SET event_json = ?1
                     WHERE runtime_id = 'r1' AND event_seq = 9",
                    params![serde_json::to_string(&changed)?],
                )?;
                Ok(())
            })
            .expect("change authoritative terminal event bytes");
        let second = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("second dry run");
        assert_ne!(first.canonical_input_sha256, second.canonical_input_sha256);
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(first.canonical_input_sha256),
                500,
            ))
            .expect_err("stale event-bound digest must fail");
        assert!(error.to_string().contains("DIGEST_MISMATCH"));
        assert!(!terminal_proven(&store));
    }

    #[test]
    fn runtime_failed_cancelled_is_accepted_as_canonical_terminal_proof() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFailed {
                finished_at: chrono::Utc::now(),
                error: RuntimeError {
                    error_code: Some("cancelled".to_string()),
                    error_text: Some("cancelled by operator".to_string()),
                    retry_allowed: false,
                    fallback_allowed: false,
                    fallback_to_id: None,
                },
                state: RuntimeState::Cancelled,
                usage: None,
            },
            false,
        );
        let dry = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 500))
            .expect("cancelled dry run");
        assert_eq!(dry.backfilled_count, 1);
        assert_eq!(
            dry.dispositions.get("backfill_terminal_event_proof"),
            Some(&1)
        );
    }

    #[test]
    fn prepared_intent_before_index_commit_reexecutes_exact_all_pre_plan() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let intent = prepare_without_commit(&store);
        assert!(!terminal_proven(&store));
        let applied = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(intent.canonical_input_sha256),
                500,
            ))
            .expect("recover all-pre intent");
        assert_eq!(applied.mutation_count, 1);
        assert!(!applied.replayed_receipt);
        assert!(terminal_proven(&store));
        assert!(applied
            .manifest_path
            .as_ref()
            .is_some_and(|path| Path::new(path).is_file()));
    }

    #[test]
    fn final_receipt_io_failure_after_commit_is_recoverable_without_second_mutation() {
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
        let root = store.receipt_root(&dry.canonical_input_sha256);
        std::fs::create_dir_all(root.join("receipt.json")).expect("force final receipt collision");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256.clone()),
                500,
            ))
            .expect_err("final receipt publication must fail after commit");
        assert!(error.to_string().contains("MEMBER_PATH_INVALID"));
        assert!(terminal_proven(&store));
        assert!(!root.join("manifest.json").exists());

        std::fs::remove_dir(root.join("receipt.json")).expect("remove injected collision");
        let recovered = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(dry.canonical_input_sha256),
                500,
            ))
            .expect("recover committed state");
        assert!(recovered.replayed_receipt);
        assert_eq!(recovered.mutation_count, 0);
        assert_eq!(recovered.backfilled_count, 1);
        assert!(root.join("manifest.json").is_file());
    }

    #[test]
    fn prepared_intent_fails_closed_on_mixed_or_drifted_index_state() {
        let (_temp, store, workspace) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let event = serde_json::to_string(&RuntimeEvent::RuntimeFinished {
            finished_at: chrono::Utc::now(),
            usage: None,
        })
        .expect("second terminal event");
        store
            .with_workspace_connection(Path::new(&workspace), |conn| {
                conn.pragma_update(None, "foreign_keys", "OFF")?;
                conn.execute(
                    "INSERT INTO runtimes(runtime_id, session_id, revision,
                     last_event_seq, terminal, lease_active)
                     VALUES ('r2','s2',3,4,1,0)",
                    [],
                )?;
                conn.execute(
                    "INSERT INTO runtime_events(runtime_id,event_seq,revision,
                     idempotency_key,event_json) VALUES ('r2',4,3,'e2',?1)",
                    params![event],
                )?;
                Ok(())
            })
            .expect("second runtime fixture");
        store
            .with_index_connection(|conn| {
                conn.execute(
                    "INSERT INTO runtime_locations(runtime_id,session_id,workspace_db_path)
                     VALUES ('r2','s2',?1)",
                    params![workspace],
                )?;
                Ok(())
            })
            .expect("second location fixture");
        let intent = prepare_without_commit(&store);
        store
            .with_index_connection(|conn| {
                conn.execute(
                    "UPDATE runtime_locations SET terminal_proven = 1,
                     terminal_revision = 7, terminal_event_seq = 9,
                     terminal_evidence_id = 'e1' WHERE runtime_id = 'r1'",
                    [],
                )?;
                Ok(())
            })
            .expect("inject mixed state");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(intent.canonical_input_sha256.clone()),
                500,
            ))
            .expect_err("mixed state must fail closed");
        assert!(error.to_string().contains("MIXED_OR_DRIFT"));
        assert!(!store
            .receipt_root(&intent.canonical_input_sha256)
            .join("manifest.json")
            .exists());
    }

    #[test]
    fn tampered_prepared_intent_is_rejected_before_index_mutation() {
        let (_temp, store, _) = fixture(
            RuntimeEvent::RuntimeFinished {
                finished_at: chrono::Utc::now(),
                usage: None,
            },
            false,
        );
        let intent = prepare_without_commit(&store);
        let path = store
            .receipt_root(&intent.canonical_input_sha256)
            .join("intent.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("intent bytes"))
                .expect("intent json");
        value["expected_receipt"]["mutation_count"] = serde_json::json!(99);
        std::fs::write(&path, serde_json::to_vec(&value).expect("tampered json"))
            .expect("tamper intent");
        let error = store
            .maintain_runtime_locations(request(
                RuntimeLocationMaintenanceMode::Apply,
                Some(intent.canonical_input_sha256),
                500,
            ))
            .expect_err("tampered intent must fail");
        assert!(error.to_string().contains("INTENT_IDENTITY_CONFLICT"));
        assert!(!terminal_proven(&store));
    }

    #[test]
    fn maintenance_lock_rejects_a_concurrent_instance() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = SessionLogStore::open(temp.path().join("db")).expect("store");
        let _held = MaintenanceLock::acquire(&store.index_db_path).expect("first lock");
        let error = store
            .maintain_runtime_locations(request(RuntimeLocationMaintenanceMode::DryRun, None, 1))
            .expect_err("second maintenance instance must be rejected");
        assert!(error
            .to_string()
            .contains("RUNTIME_LOCATION_MAINTENANCE_LOCKED"));
    }

    #[test]
    fn immutable_member_publication_is_collision_safe_for_identical_concurrent_writers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("receipt.json");
        let barrier = Arc::new(Barrier::new(2));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let barrier = barrier.clone();
            let path = path.clone();
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                write_immutable(&path, br#"{"ok":true}"#)
            }));
        }
        for join in joins {
            join.join()
                .expect("writer thread")
                .expect("immutable write");
        }
        assert_eq!(
            std::fs::read(&path).expect("published member"),
            br#"{"ok":true}"#
        );
        let leftovers = std::fs::read_dir(temp.path())
            .expect("receipt root")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".receipt.json.tmp-")
            })
            .count();
        assert_eq!(leftovers, 0);
    }
}
