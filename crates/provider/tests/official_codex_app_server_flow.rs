use runtime_contract::CommanderContinuationBinding;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use tura_llm_rust::official_codex_app_server::{
    CodexAppServerExecutable, CodexCommandRunCommandObservation, CodexCommandRunEffectObservation,
    CodexExecutionLedger, CodexObservedCommandAccess, CodexObservedToolEffectState,
    CodexReadOnlyCommandObservation, CodexReadOnlyEffectObservation, OfficialCodexServerRequest,
    OfficialCodexServerRequestFuture, OfficialCodexServerRequestHandler, OfficialCodexTurnRequest,
    canonical_thread_revision_sha256, load_thread_association, run_official_codex_turn,
};

const UNCLAIMED_READ_ONLY_COMMAND_LINE: &str = r#"rg -n -A18 -B6 "struct TurnRequestContext|TurnRequestContext \{" crates/provider/src/official_codex_app_server.rs"#;
const UNCLAIMED_READ_ONLY_CLAIM_IDENTITY: &str = "runtime-official-1:call-original:step:1:index:0";

fn read_only_claim_identity(
    execution_id: &str,
    effective_step: u64,
    enumerated_index: impl std::fmt::Display,
    binding_id: Option<&str>,
) -> String {
    match binding_id {
        Some(binding_id)
            if binding_id == execution_id
                || binding_id.starts_with(&format!("{execution_id}:")) =>
        {
            binding_id.to_string()
        }
        Some(binding_id) => format!("{execution_id}:{binding_id}"),
        None => format!("{execution_id}:step:{effective_step}:index:{enumerated_index}"),
    }
}

fn encode_read_only_receipt_identity_for_test(identity: &str) -> String {
    if identity.is_empty() {
        return "command_run".to_string();
    }
    let mut encoded = String::new();
    for character in identity.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
            encoded.push(character);
        } else {
            encoded.push_str(&format!("_x{:x}_", u32::from(character)));
        }
    }
    encoded
}

fn verify_read_only_artifact_absent_for_test(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(format!(
            "read-only recovery artifact exists: {}",
            path.display()
        )),
        Err(error) => Err(format!(
            "failed to prove read-only recovery artifact absence at {}: {error}",
            path.display()
        )),
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "app-server") {
        fake_app_server(&args);
        return;
    }
    if args.iter().any(|arg| arg == "--version") {
        println!("codex-cli 9.9.9-test");
        return;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        run_hostile_flow().await;
        run_interrupted_effect_recovery().await;
        run_commander_target_convergence().await;
    });
}

async fn run_commander_target_convergence() {
    let root = tempfile::tempdir().expect("commander target tempdir");
    let session_directory = root.path().join("session");
    fs::create_dir_all(&session_directory).expect("commander target session directory");
    let pre_turns = vec![
        json!({"id": "turn-commander-0", "status": "completed", "items": []}),
        json!({"id": "turn-commander-1", "status": "completed", "items": []}),
    ];
    let pre_revision = canonical_thread_revision_sha256("commander-thread-1", &pre_turns)
        .expect("commander pre revision");
    let digest = "c".repeat(64);
    let target_capture = root.path().join("commander-target.jsonl");
    let mut target = request(
        &session_directory,
        &target_capture,
        vec![json!({"role": "user", "content": "commander callback input"})],
    );
    target.runtime_id = format!("callback-continuation-runtime-{digest}");
    target.commander_continuation = Some(CommanderContinuationBinding {
        target_thread_id: "commander-thread-1".to_string(),
        requested_action: "MISSION_VERIFICATION".to_string(),
        continuation_request_id: format!("callback-continuation-request-{digest}"),
        child_session_id: "child-session-1".to_string(),
        child_transaction_id: "child-transaction-1".to_string(),
        child_runtime_id: "child-runtime-1".to_string(),
        callback_payload_sha256: "a".repeat(64),
        effect_identity_sha256: "b".repeat(64),
        pre_revision_sha256: pre_revision.clone(),
    });
    let mut target_handler = ReceiptHandler::new(
        &session_directory,
        &root.path().join("commander-target-count"),
        false,
    );
    let response = run_official_codex_turn(target.clone(), Some(&mut target_handler))
        .await
        .expect("commander target convergence");
    let proof = response
        .commander_convergence_proof
        .expect("machine convergence proof");
    assert_eq!(proof.target_thread_id, "commander-thread-1");
    assert_eq!(proof.pre_revision_sha256, pre_revision);
    assert_eq!(proof.target_turn_id, "turn-commander-new");
    assert_ne!(proof.post_revision_sha256, proof.pre_revision_sha256);
    assert_eq!(
        response.content,
        Value::String("commander converged".to_string())
    );
    assert!(
        load_thread_association(&session_directory, "tura-session-1")
            .expect("private association read")
            .is_none(),
        "target continuation must not overwrite the private association"
    );
    let first_protocol_message_count = captured_messages(&target_capture).len();
    let original_runtime_id = target.runtime_id.clone();
    target.runtime_id = "callback-continuation-recovery-runtime-test".to_string();
    target.fallback_from_id = Some(original_runtime_id.clone());
    let replay = run_official_codex_turn(target.clone(), Some(&mut target_handler))
        .await
        .expect("durable convergence proof replay");
    assert_eq!(replay.commander_convergence_proof, Some(proof.clone()));
    assert_eq!(replay.content, json!("commander converged"));
    assert_eq!(
        captured_messages(&target_capture).len(),
        first_protocol_message_count,
        "durable proof replay must not submit or read another target turn"
    );
    let previous_recovery_runtime_id = target.runtime_id.clone();
    target.runtime_id = "callback-continuation-recovery-runtime-bounded-retry".to_string();
    target.fallback_from_id = Some(previous_recovery_runtime_id);
    let bounded_replay = run_official_codex_turn(target.clone(), Some(&mut target_handler))
        .await
        .expect("bounded recovery chain proof replay");
    assert_eq!(bounded_replay.commander_convergence_proof, Some(proof.clone()));
    assert_eq!(
        captured_messages(&target_capture).len(),
        first_protocol_message_count,
        "bounded recovery proof replay must not submit another target turn"
    );
    let mut wrong_fallback = target;
    wrong_fallback.runtime_id = "callback-continuation-recovery-runtime-wrong".to_string();
    wrong_fallback.fallback_from_id = Some("unrelated-runtime".to_string());
    let wrong_fallback_error = run_official_codex_turn(wrong_fallback, Some(&mut target_handler))
        .await
        .expect_err("unbound convergence fallback must fail before app-server protocol");
    assert!(
        wrong_fallback_error
            .to_string()
            .contains("runtime/request identity mismatch")
    );
    assert_eq!(
        captured_messages(&target_capture).len(),
        first_protocol_message_count
    );

    let stale_directory = root.path().join("stale-session");
    fs::create_dir_all(&stale_directory).expect("stale session directory");
    let stale_capture = root.path().join("commander-stale.jsonl");
    let mut stale = request(
        &stale_directory,
        &stale_capture,
        vec![json!({"role": "user", "content": "stale commander callback"})],
    );
    stale.runtime_id = format!("callback-continuation-runtime-{digest}");
    stale.commander_continuation = response_binding(&digest, "f".repeat(64));
    let error = run_official_codex_turn(stale, None)
        .await
        .expect_err("stale target preimage must fail before turn/start");
    assert!(
        error
            .to_string()
            .contains("COMMANDER_TARGET_PREIMAGE_MISMATCH")
    );
    let captured = captured_messages(&stale_capture);
    let methods = methods(&captured);
    assert!(!methods.iter().any(|method| *method == "turn/start"));

    let mismatch_directory = root.path().join("mismatch-session");
    fs::create_dir_all(&mismatch_directory).expect("mismatch session directory");
    let mut mismatch = request(
        &mismatch_directory,
        &root.path().join("commander-target-mismatch.jsonl"),
        vec![json!({"role": "user", "content": "mismatch commander callback"})],
    );
    mismatch.runtime_id = format!("callback-continuation-runtime-{digest}");
    mismatch.commander_continuation = response_binding(&digest, pre_revision);
    let mismatch_error = run_official_codex_turn(mismatch, None)
        .await
        .expect_err("target thread mismatch must fail closed");
    assert!(
        mismatch_error
            .to_string()
            .contains("thread/resume returned")
    );
}

fn response_binding(
    digest: &str,
    pre_revision_sha256: String,
) -> Option<CommanderContinuationBinding> {
    Some(CommanderContinuationBinding {
        target_thread_id: "commander-thread-1".to_string(),
        requested_action: "MISSION_VERIFICATION".to_string(),
        continuation_request_id: format!("callback-continuation-request-{digest}"),
        child_session_id: "child-session-1".to_string(),
        child_transaction_id: "child-transaction-1".to_string(),
        child_runtime_id: "child-runtime-1".to_string(),
        callback_payload_sha256: "a".repeat(64),
        effect_identity_sha256: "b".repeat(64),
        pre_revision_sha256,
    })
}

async fn run_hostile_flow() {
    let root = tempfile::tempdir().expect("tempdir");
    let session_directory = root.path().join("session");
    fs::create_dir_all(&session_directory).expect("session directory");
    let auth_path = root.path().join("home").join(".codex").join("auth.json");
    fs::create_dir_all(&auth_path).expect("unreadable auth fixture");
    let _home = EnvGuard::set("HOME", auth_path.parent().unwrap().parent().unwrap());

    let first_capture = root.path().join("first.jsonl");
    let mut first_request = request(
        &session_directory,
        &first_capture,
        vec![
            json!({"role": "system", "content": "Use the governed Tura tools."}),
            json!({
                "role": "user",
                "content": "first official turn",
                "previous_response_id": "resp_syntactically_valid_but_nonexistent"
            }),
        ],
    );
    first_request.turn_context = Some("bounded runtime context".to_string());
    let first = run_official_codex_turn(first_request, None)
        .await
        .expect("first official turn");

    assert_eq!(first.content, Value::String("official reply".to_string()));
    assert_eq!(first.association.thread_id, "thread-official-1");
    assert_eq!(first.association.codex_session_id, "session-official-1");
    assert_eq!(
        first.association.executable_identity.canonical_path,
        fs::canonicalize(std::env::current_exe().expect("test executable"))
            .expect("canonical executable")
    );
    assert_eq!(
        first.association.executable_identity.version,
        "codex-cli 9.9.9-test"
    );
    assert_eq!(
        first.association.executable_identity.sha256,
        format!(
            "{:x}",
            Sha256::digest(fs::read(std::env::current_exe().unwrap()).expect("executable bytes"))
        )
    );
    assert_eq!(first.usage.as_ref().unwrap().input_tokens, Some(17));
    assert_eq!(
        first.usage.as_ref().unwrap().monetary_cost_authority,
        "unknown"
    );

    let persisted = load_thread_association(&session_directory, "tura-session-1")
        .expect("association read")
        .expect("association present");
    assert_eq!(persisted, first.association);
    assert!(persisted.active_turn_id.is_none());
    assert!(
        load_thread_association(&session_directory, "tura-session-2")
            .expect("independent association read")
            .is_none(),
        "a terminal session association must not capture a successor in the same workspace"
    );

    let first_lines = captured_messages(&first_capture);
    assert_eq!(
        methods(&first_lines),
        ["initialize", "initialized", "thread/start", "turn/start",]
    );
    assert_eq!(first_lines[0]["params"]["clientInfo"]["name"], "tura");
    assert_eq!(first_lines[0]["params"]["clientInfo"]["title"], "Tura");
    let first_thread_start = first_lines
        .iter()
        .find(|message| message.get("method").and_then(Value::as_str) == Some("thread/start"))
        .expect("first thread/start");
    assert_eq!(first_thread_start["params"]["approvalPolicy"], "on-request");
    assert_eq!(first_thread_start["params"]["sandbox"], "workspace-write");
    let first_turn_start = first_lines
        .iter()
        .find(|message| message.get("method").and_then(Value::as_str) == Some("turn/start"))
        .expect("first turn/start");
    assert_eq!(
        first_turn_start["params"]["input"],
        json!([
            {"type": "text", "text": "bounded runtime context"},
            {"type": "text", "text": "first official turn"}
        ])
    );
    let first_wire = first_lines
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!first_wire.contains("previous_response_id"), "{first_wire}");
    assert!(
        !first_wire.contains("resp_syntactically_valid_but_nonexistent"),
        "{first_wire}"
    );
    assert!(
        !first_wire.contains("chatgpt.com/backend-api"),
        "{first_wire}"
    );
    assert!(!first_wire.contains("auth.json"), "{first_wire}");

    let unrestricted_directory = root.path().join("unrestricted-session");
    fs::create_dir_all(&unrestricted_directory).expect("unrestricted session directory");
    let unrestricted_capture = root.path().join("unrestricted.jsonl");
    let mut unrestricted_request = request(
        &unrestricted_directory,
        &unrestricted_capture,
        vec![json!({"role": "user", "content": "authorized unrestricted turn"})],
    );
    unrestricted_request.disable_permission_restrictions = true;
    run_official_codex_turn(unrestricted_request, None)
        .await
        .expect("authorized unrestricted turn");
    let unrestricted_messages = captured_messages(&unrestricted_capture);
    let unrestricted_thread_start = unrestricted_messages
        .iter()
        .find(|message| message.get("method").and_then(Value::as_str) == Some("thread/start"))
        .expect("unrestricted thread/start");
    assert_eq!(
        unrestricted_thread_start["params"]["approvalPolicy"],
        "never"
    );
    assert_eq!(
        unrestricted_thread_start["params"]["sandbox"],
        "danger-full-access"
    );

    let second_capture = root.path().join("second.jsonl");
    let second = run_official_codex_turn(
        request(
            &session_directory,
            &second_capture,
            vec![json!({"role": "user", "content": "second official turn"})],
        ),
        None,
    )
    .await
    .expect("resumed official turn");
    assert_eq!(second.association.thread_id, "thread-official-1");

    let second_lines = captured_messages(&second_capture);
    assert_eq!(
        methods(&second_lines),
        [
            "initialize",
            "initialized",
            "thread/resume",
            "thread/read",
            "turn/start",
        ]
    );
    assert_eq!(second_lines[2]["params"]["threadId"], "thread-official-1");
    assert_eq!(second_lines[4]["params"]["threadId"], "thread-official-1");

    let recovery_directory = root.path().join("recovery-session");
    fs::create_dir_all(&recovery_directory).expect("recovery session directory");
    let disconnect_capture = root.path().join("disconnect.jsonl");
    let disconnect_error = run_official_codex_turn(
        request(
            &recovery_directory,
            &disconnect_capture,
            vec![json!({"role": "user", "content": "recover exactly once"})],
        ),
        None,
    )
    .await
    .expect_err("disconnect before turn/start response");
    assert!(disconnect_error.to_string().contains("closed stdout"));
    let uncertain = load_thread_association(&recovery_directory, "tura-session-1")
        .expect("uncertain association read")
        .expect("uncertain association present");
    assert!(uncertain.active_turn_id.is_none());
    assert_eq!(
        uncertain.turn_attempt.as_ref().unwrap().state,
        tura_llm_rust::official_codex_app_server::CodexTurnSubmissionState::Prepared
    );

    let recovery_capture = root.path().join("recover.jsonl");
    let recovered = run_official_codex_turn(
        request(
            &recovery_directory,
            &recovery_capture,
            vec![json!({"role": "user", "content": "recover exactly once"})],
        ),
        None,
    )
    .await
    .expect("authoritative recovery");
    assert_eq!(
        recovered.content,
        Value::String("recovered official reply".to_string())
    );
    assert!(recovered.association.turn_attempt.is_none());
    assert!(recovered.association.active_turn_id.is_none());
    assert_eq!(
        methods(&captured_messages(&recovery_capture)),
        ["initialize", "initialized", "thread/resume", "thread/read"]
    );

    assert_changed_mission_rejected(root.path(), "messages", |request| {
        request.messages[0]["content"] = json!("changed prior assistant context");
    })
    .await;
    assert_changed_mission_rejected(root.path(), "model", |request| {
        request.model = "different-model".to_string();
    })
    .await;
    assert_changed_mission_rejected(root.path(), "tool", |request| {
        request.dynamic_tools = vec![json!({
            "name": "command_run",
            "description": "changed tool contract",
            "inputSchema": {"type": "object"}
        })];
    })
    .await;
    assert_changed_mission_rejected(root.path(), "permission", |request| {
        request.disable_permission_restrictions = true;
    })
    .await;
    assert_changed_mission_rejected(root.path(), "executable", |request| {
        request
            .executable
            .prefix_args
            .push("--changed-executable-semantics".to_string());
    })
    .await;

    println!(
        "official_codex_app_server_flow: exact stdio identity and exactly-once recovery passed"
    );
}

async fn assert_changed_mission_rejected(
    root: &Path,
    scenario: &str,
    mutate: impl FnOnce(&mut OfficialCodexTurnRequest),
) {
    let session_directory = root.join(format!("{scenario}-mission-session"));
    fs::create_dir_all(&session_directory).expect("mission session directory");
    let messages = || {
        vec![
            json!({"role": "assistant", "content": "prior assistant context"}),
            json!({"role": "user", "content": "same canonical user input"}),
        ]
    };
    run_official_codex_turn(
        request(
            &session_directory,
            &root.join(format!("disconnect-{scenario}.jsonl")),
            messages(),
        ),
        None,
    )
    .await
    .expect_err("mission setup disconnect");

    let changed_capture = root.join(format!("changed-{scenario}.jsonl"));
    let mut changed = request(&session_directory, &changed_capture, messages());
    mutate(&mut changed);
    let error = run_official_codex_turn(changed, None)
        .await
        .expect_err("changed canonical mission must fail closed");
    assert!(
        error.to_string().contains("different input"),
        "{scenario}: {error}"
    );
    assert_eq!(
        methods(&captured_messages(&changed_capture)),
        ["initialize", "initialized", "thread/resume", "thread/read"]
    );
}

async fn run_interrupted_effect_recovery() {
    let root = tempfile::tempdir().expect("recovery tempdir");

    let policy_directory = root.path().join("policy-denial-session");
    fs::create_dir_all(&policy_directory).expect("policy-denial session directory");
    let policy_count = root.path().join("policy-denial-execution-count");
    let mut policy_handler = ReceiptHandler::policy_denial(&policy_directory, &policy_count);
    let policy_capture = root.path().join("policy").join("delivered-failure.jsonl");
    fs::create_dir_all(policy_capture.parent().unwrap()).expect("policy capture directory");
    let delivered_policy_denial = run_official_codex_turn(
        effect_request(
            &policy_directory,
            &policy_capture,
            "observe a deterministic J-Space policy denial",
        ),
        Some(&mut policy_handler),
    )
    .await
    .expect("deterministic policy denial must remain deliverable without a receipt");
    assert_eq!(
        delivered_policy_denial.content,
        Value::String("failed command observed".to_string())
    );
    assert_eq!(execution_count(&policy_count), 0);
    assert!(!policy_directory.join(".tura/run/command_receipts").exists());
    assert!(
        delivered_policy_denial
            .association
            .observed_tool_effects
            .is_empty()
    );

    let failed_directory = root.path().join("delivered-failure-session");
    fs::create_dir_all(&failed_directory).expect("delivered-failure session directory");
    let failed_count = root.path().join("delivered-failure-execution-count");
    let mut failed_handler = ReceiptHandler::failing(&failed_directory, &failed_count);
    let delivered_failure = run_official_codex_turn(
        effect_request(
            &failed_directory,
            &root.path().join("delivered-failure.jsonl"),
            "observe a known read-only command miss",
        ),
        Some(&mut failed_handler),
    )
    .await
    .expect("known failed command result must remain deliverable");
    assert_eq!(
        delivered_failure.content,
        Value::String("failed command observed".to_string())
    );
    assert_eq!(execution_count(&failed_count), 1);
    assert!(
        delivered_failure
            .association
            .observed_tool_effects
            .is_empty()
    );

    let same_directory = root.path().join("same-input-session");
    fs::create_dir_all(&same_directory).expect("same-input session directory");
    let same_count = root.path().join("same-input-execution-count");
    let mut same_handler = ReceiptHandler::new(&same_directory, &same_count, false);
    let lost_error = run_official_codex_turn(
        effect_request(
            &same_directory,
            &root.path().join("effect-interrupt.jsonl"),
            "recover interrupted mission",
        ),
        Some(&mut same_handler),
    )
    .await
    .expect_err("provider loss after completed command receipt");
    assert!(lost_error.to_string().contains("closed stdout"));
    assert_eq!(execution_count(&same_count), 1);

    let recovery_capture = root.path().join("effect-recover.jsonl");
    let mut recovery_request = effect_request(
        &same_directory,
        &recovery_capture,
        "recover interrupted mission",
    );
    recovery_request.runtime_id = "runtime-official-restarted".to_string();
    recovery_request.fallback_from_id = Some("runtime-official-1".to_string());
    let recovered = run_official_codex_turn(recovery_request, Some(&mut same_handler))
        .await
        .expect("same-input interrupted recovery");
    assert_eq!(
        recovered.content,
        Value::String("recovered after provider loss".to_string())
    );
    assert_eq!(recovered.association.thread_id, "thread-recovered-1");
    assert_eq!(execution_count(&same_count), 1, "command executed twice");
    assert!(recovered.association.interrupted_recovery.is_none());
    assert!(recovered.association.observed_tool_effects.is_empty());
    let recovery_messages = captured_messages(&recovery_capture);
    assert!(recovery_messages.iter().any(|message| {
        message.get("method").and_then(Value::as_str) == Some("thread/start")
            && message["params"]["model"] == "gpt-5.6-sol"
    }));
    assert!(recovery_messages.iter().any(|message| {
        message.get("method").and_then(Value::as_str) == Some("thread/inject_items")
            && message["params"]["threadId"] == "thread-recovered-1"
            && message["params"]["items"].as_array().is_some_and(|items| {
                items.iter().any(|item| item["type"] == "function_call")
                    && items
                        .iter()
                        .any(|item| item["type"] == "function_call_output")
            })
    }));
    assert!(recovery_messages.iter().any(|message| {
        message.get("method").and_then(Value::as_str) == Some("turn/start")
            && message["params"]["threadId"] == "thread-recovered-1"
            && message["params"]["input"] == json!([])
    }));
    assert!(
        !recovery_messages
            .iter()
            .any(|message| message.to_string().contains("previous_response_id"))
    );

    let completed_directory = root.path().join("completed-read-only-session");
    fs::create_dir_all(&completed_directory).expect("completed read-only session directory");
    let completed_count = root.path().join("completed-read-only-execution-count");
    let mut completed_handler =
        ReceiptHandler::completed_response_lost(&completed_directory, &completed_count);
    run_official_codex_turn(
        effect_request(
            &completed_directory,
            &root
                .path()
                .join("completed-read-only-effect-interrupt.jsonl"),
            "recover a durably completed read-only command",
        ),
        Some(&mut completed_handler),
    )
    .await
    .expect_err("provider transport must lose the completed read-only response");
    assert_eq!(execution_count(&completed_count), 1);
    let completed = load_only_execution_ledger(&completed_directory);
    assert_eq!(completed.effects.len(), 1);
    assert!(completed.effects[0].response.is_none());
    let completed_receipt_path =
        completed_directory
            .join(".tura/run/command_receipts")
            .join(format!(
                "{}.json",
                encode_read_only_receipt_identity_for_test(UNCLAIMED_READ_ONLY_CLAIM_IDENTITY)
            ));
    assert!(completed_receipt_path.is_file());

    let completed_recovery_capture = root.path().join("completed-read-only-effect-recover.jsonl");
    let completed_recovered = run_official_codex_turn(
        effect_request(
            &completed_directory,
            &completed_recovery_capture,
            "recover a durably completed read-only command",
        ),
        Some(&mut completed_handler),
    )
    .await
    .expect("completed read-only receipt must recover without replay");
    assert_eq!(
        completed_recovered.content,
        Value::String("recovered after provider loss".to_string())
    );
    assert_eq!(
        execution_count(&completed_count),
        1,
        "command executed twice"
    );
    assert!(
        completed_recovered
            .association
            .observed_tool_effects
            .is_empty()
    );
    assert!(
        captured_messages(&completed_recovery_capture)
            .iter()
            .any(|message| {
                message
                    .to_string()
                    .contains("reconstructed_from_durable_terminal_receipt")
            })
    );

    let durable_batch_directory = root.path().join("durable-batch-session");
    fs::create_dir_all(&durable_batch_directory).expect("durable batch session directory");
    let durable_batch_count = root.path().join("durable-batch-execution-count");
    let mut durable_batch_handler =
        ReceiptHandler::durable_batch_response_lost(&durable_batch_directory, &durable_batch_count);
    run_official_codex_turn(
        effect_request(
            &durable_batch_directory,
            &root.path().join("durable-batch-effect-interrupt.jsonl"),
            "recover six durably completed command results",
        ),
        Some(&mut durable_batch_handler),
    )
    .await
    .expect_err("provider transport must lose the durable batch response");
    assert_eq!(execution_count(&durable_batch_count), 1);
    let mut interrupted_batch = load_only_execution_ledger(&durable_batch_directory);
    assert_eq!(interrupted_batch.effects.len(), 1);
    assert_eq!(
        interrupted_batch.effects[0].state,
        CodexObservedToolEffectState::Observed
    );
    assert!(interrupted_batch.effects[0].response.is_none());
    assert_eq!(
        interrupted_batch.effects[0].runtime_id.as_deref(),
        Some("runtime-official-1")
    );
    assert_eq!(
        interrupted_batch.effects[0]
            .command_run_observation
            .as_ref()
            .expect("persisted command identities")
            .commands
            .len(),
        6
    );

    // Existing interrupted ledgers predate effect-scoped runtime and command identities.
    // A single owned runtime remains unambiguous and may be normalized from the request.
    interrupted_batch.effects[0].runtime_id = None;
    interrupted_batch.effects[0].command_run_observation = None;
    persist_only_execution_ledger(&durable_batch_directory, &interrupted_batch);

    let durable_batch_recovery_capture = root.path().join("durable-batch-effect-recover.jsonl");
    let durable_batch_recovered = run_official_codex_turn(
        effect_request(
            &durable_batch_directory,
            &durable_batch_recovery_capture,
            "recover six durably completed command results",
        ),
        Some(&mut durable_batch_handler),
    )
    .await
    .expect("complete durable batch receipts must recover without replay");
    assert_eq!(
        durable_batch_recovered.content,
        Value::String("recovered after provider loss".to_string())
    );
    assert_eq!(
        execution_count(&durable_batch_count),
        1,
        "durable command batch executed twice"
    );
    let durable_batch_messages = captured_messages(&durable_batch_recovery_capture);
    let injected_batch = durable_batch_messages
        .iter()
        .find(|message| {
            message.get("method").and_then(Value::as_str) == Some("thread/inject_items")
        })
        .expect("durable batch recovery history");
    let recovered_output = injected_batch["params"]["items"]
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|item| item["type"] == "function_call_output")
        })
        .and_then(|item| item["output"].as_str())
        .expect("durable batch function output");
    assert_eq!(
        recovered_output
            .matches("reconstructed_from_durable_terminal_receipt")
            .count(),
        6
    );
    assert!(recovered_output.contains("\"exit_code\":2"));
    assert!(recovered_output.contains("\"success\":false"));
    assert!(recovered_output.contains("runtime-official-1:call-original:step:2:index:1"));
    assert!(recovered_output.contains("runtime-official-1:call-original:result-two"));
    assert!(recovered_output.contains("runtime-official-1:call-original:result-five"));

    let unclaimed_directory = root.path().join("unclaimed-read-only-session");
    fs::create_dir_all(&unclaimed_directory).expect("unclaimed read-only session directory");
    let unclaimed_count = root.path().join("unclaimed-read-only-observation-count");
    let mut unclaimed_handler = ReceiptHandler::unclaimed(&unclaimed_directory, &unclaimed_count);
    run_official_codex_turn(
        effect_request(
            &unclaimed_directory,
            &root.path().join("unclaimed-effect-interrupt.jsonl"),
            "recover an unclaimed read-only observation",
        ),
        Some(&mut unclaimed_handler),
    )
    .await
    .expect_err("provider loss after observing an unclaimed read-only command");
    let unclaimed = load_only_execution_ledger(&unclaimed_directory);
    assert_eq!(unclaimed.effects.len(), 1);
    let unclaimed_effect = &unclaimed.effects[0];
    assert_eq!(
        unclaimed_effect.state,
        CodexObservedToolEffectState::Observed
    );
    assert!(unclaimed_effect.response.is_none());
    let unclaimed_observation = unclaimed_effect
        .read_only_observation
        .as_ref()
        .expect("unclaimed read-only observation");
    assert_eq!(
        unclaimed_observation.commands[0].claim_identity,
        UNCLAIMED_READ_ONLY_CLAIM_IDENTITY
    );
    assert!(unclaimed_effect.command_receipts.is_empty());
    let unclaimed_receipt_directory = unclaimed_directory.join(".tura/run/command_receipts");
    verify_read_only_artifact_absent_for_test(&unclaimed_receipt_directory)
        .expect("unclaimed receipt directory absent");
    assert_eq!(execution_count(&unclaimed_count), 1);
    fs::create_dir_all(&unclaimed_receipt_directory).expect("unclaimed receipt directory");
    fs::write(
        unclaimed_receipt_directory.join("unrelated-receipt.json"),
        "{}",
    )
    .expect("unrelated receipt write");

    let unclaimed_recovered = run_official_codex_turn(
        effect_request(
            &unclaimed_directory,
            &root.path().join("unclaimed-effect-recover.jsonl"),
            "recover an unclaimed read-only observation",
        ),
        Some(&mut unclaimed_handler),
    )
    .await
    .expect("unclaimed read-only observation must recover as terminal zero-mutation");
    assert_eq!(
        unclaimed_recovered.content,
        Value::String("recovered after provider loss".to_string())
    );
    assert_eq!(
        execution_count(&unclaimed_count),
        1,
        "unclaimed read-only command was replayed"
    );
    assert!(
        unclaimed_recovered
            .association
            .observed_tool_effects
            .is_empty()
    );

    for (case_name, receipt_suffix) in [("claim", ".claim.json"), ("terminal", ".json")] {
        let blocked_directory = root
            .path()
            .join(format!("unclaimed-read-only-{case_name}-session"));
        fs::create_dir_all(&blocked_directory).expect("blocked unclaimed session directory");
        let blocked_count = root
            .path()
            .join(format!("unclaimed-read-only-{case_name}-count"));
        let mut blocked_handler = ReceiptHandler::unclaimed(&blocked_directory, &blocked_count);
        run_official_codex_turn(
            effect_request(
                &blocked_directory,
                &root
                    .path()
                    .join(format!("unclaimed-effect-{case_name}-interrupt.jsonl")),
                "recover an unclaimed read-only observation",
            ),
            Some(&mut blocked_handler),
        )
        .await
        .expect_err("provider loss after observing a blocked unclaimed command");
        assert_eq!(execution_count(&blocked_count), 1);
        let receipt_directory = blocked_directory.join(".tura/run/command_receipts");
        fs::create_dir_all(&receipt_directory).expect("blocked receipt directory");
        fs::write(
            receipt_directory.join(format!(
                "{}{receipt_suffix}",
                encode_read_only_receipt_identity_for_test(UNCLAIMED_READ_ONLY_CLAIM_IDENTITY)
            )),
            "{}",
        )
        .expect("blocking receipt write");
        let blocked_error = run_official_codex_turn(
            effect_request(
                &blocked_directory,
                &root
                    .path()
                    .join(format!("unclaimed-effect-{case_name}-recover.jsonl")),
                "recover an unclaimed read-only observation",
            ),
            Some(&mut blocked_handler),
        )
        .await
        .expect_err("claimed unclaimed observation must fail closed");
        assert!(
            blocked_error
                .to_string()
                .contains("OFFICIAL_CODEX_INTERRUPTED_RECOVERY_UNCERTAIN_EFFECT")
        );
        assert_eq!(execution_count(&blocked_count), 1);
    }

    let conflict_directory = root.path().join("conflicting-effect-session");
    fs::create_dir_all(&conflict_directory).expect("conflicting-effect session directory");
    let conflict_count = root.path().join("conflicting-effect-execution-count");
    let mut conflict_handler = ReceiptHandler::new(&conflict_directory, &conflict_count, false);
    run_official_codex_turn(
        effect_request(
            &conflict_directory,
            &root.path().join("conflict-interrupt.jsonl"),
            "conflicting effect mission",
        ),
        Some(&mut conflict_handler),
    )
    .await
    .expect_err("conflicting-effect provider loss");
    let mut conflicting_ledger = load_only_execution_ledger(&conflict_directory);
    conflicting_ledger.effects[0]
        .request_params
        .as_mut()
        .expect("interrupted effect must preserve its canonical request")["arguments"]["commands"]
        [0]["command_line"] = json!("sleep 91");
    persist_only_execution_ledger(&conflict_directory, &conflicting_ledger);
    let conflict_error = run_official_codex_turn(
        effect_request(
            &conflict_directory,
            &root.path().join("conflict-recover.jsonl"),
            "conflicting effect mission",
        ),
        Some(&mut conflict_handler),
    )
    .await
    .expect_err("conflicting recovery effect must fail closed");
    assert!(
        conflict_error
            .to_string()
            .contains("OFFICIAL_CODEX_INTERRUPTED_RECOVERY_CONFLICTING_EFFECT")
    );
    assert_eq!(execution_count(&conflict_count), 1);

    let changed_directory = root.path().join("changed-input-session");
    fs::create_dir_all(&changed_directory).expect("changed-input session directory");
    let changed_count = root.path().join("changed-input-execution-count");
    let mut changed_handler = ReceiptHandler::new(&changed_directory, &changed_count, false);
    run_official_codex_turn(
        effect_request(
            &changed_directory,
            &root.path().join("changed-interrupt.jsonl"),
            "canonical mission",
        ),
        Some(&mut changed_handler),
    )
    .await
    .expect_err("changed-input provider loss");
    let changed_error = run_official_codex_turn(
        effect_request(
            &changed_directory,
            &root.path().join("changed-recover.jsonl"),
            "different canonical mission",
        ),
        Some(&mut changed_handler),
    )
    .await
    .expect_err("changed input must fail closed");
    assert!(changed_error.to_string().contains("different input"));
    assert_eq!(execution_count(&changed_count), 1);

    let uncertain_directory = root.path().join("uncertain-effect-session");
    fs::create_dir_all(&uncertain_directory).expect("uncertain-effect session directory");
    let uncertain_count = root.path().join("uncertain-effect-execution-count");
    let mut uncertain_handler = ReceiptHandler::new(&uncertain_directory, &uncertain_count, true);
    let uncertain_initial = run_official_codex_turn(
        effect_request(
            &uncertain_directory,
            &root.path().join("uncertain-interrupt.jsonl"),
            "uncertain effect mission",
        ),
        Some(&mut uncertain_handler),
    )
    .await
    .expect_err("uncertain effect must not be delivered");
    assert!(
        uncertain_initial
            .to_string()
            .contains("OFFICIAL_CODEX_INTERRUPTED_RECOVERY_UNCERTAIN_EFFECT")
    );
    let uncertain_recovery = run_official_codex_turn(
        effect_request(
            &uncertain_directory,
            &root.path().join("uncertain-recover.jsonl"),
            "uncertain effect mission",
        ),
        Some(&mut uncertain_handler),
    )
    .await
    .expect_err("uncertain effect recovery must fail closed");
    assert!(
        uncertain_recovery
            .to_string()
            .contains("OFFICIAL_CODEX_INTERRUPTED_RECOVERY_UNCERTAIN_EFFECT")
    );
    assert_eq!(execution_count(&uncertain_count), 1);

    println!(
        "official_codex_app_server_flow: interrupted receipt recovery and fail-closed gates passed"
    );
}

struct ReceiptHandler {
    session_directory: PathBuf,
    execution_count_path: PathBuf,
    uncertain: bool,
    failed: bool,
    unclaimed: bool,
    observe_read_only: bool,
    drop_response_after_receipt: bool,
    policy_denial: bool,
    durable_batch: bool,
    command_run_observation: Option<CodexCommandRunEffectObservation>,
}

impl ReceiptHandler {
    fn new(session_directory: &Path, execution_count_path: &Path, uncertain: bool) -> Self {
        Self {
            session_directory: session_directory.to_path_buf(),
            execution_count_path: execution_count_path.to_path_buf(),
            uncertain,
            failed: false,
            unclaimed: false,
            observe_read_only: false,
            drop_response_after_receipt: false,
            policy_denial: false,
            durable_batch: false,
            command_run_observation: None,
        }
    }

    fn failing(session_directory: &Path, execution_count_path: &Path) -> Self {
        Self {
            session_directory: session_directory.to_path_buf(),
            execution_count_path: execution_count_path.to_path_buf(),
            uncertain: false,
            failed: true,
            unclaimed: false,
            observe_read_only: false,
            drop_response_after_receipt: false,
            policy_denial: false,
            durable_batch: false,
            command_run_observation: None,
        }
    }

    fn unclaimed(session_directory: &Path, execution_count_path: &Path) -> Self {
        Self {
            session_directory: session_directory.to_path_buf(),
            execution_count_path: execution_count_path.to_path_buf(),
            uncertain: false,
            failed: false,
            unclaimed: true,
            observe_read_only: true,
            drop_response_after_receipt: false,
            policy_denial: false,
            durable_batch: false,
            command_run_observation: None,
        }
    }

    fn completed_response_lost(session_directory: &Path, execution_count_path: &Path) -> Self {
        Self {
            session_directory: session_directory.to_path_buf(),
            execution_count_path: execution_count_path.to_path_buf(),
            uncertain: false,
            failed: false,
            unclaimed: false,
            observe_read_only: true,
            drop_response_after_receipt: true,
            policy_denial: false,
            durable_batch: false,
            command_run_observation: None,
        }
    }

    fn policy_denial(session_directory: &Path, execution_count_path: &Path) -> Self {
        Self {
            session_directory: session_directory.to_path_buf(),
            execution_count_path: execution_count_path.to_path_buf(),
            uncertain: false,
            failed: false,
            unclaimed: false,
            observe_read_only: false,
            drop_response_after_receipt: false,
            policy_denial: true,
            durable_batch: false,
            command_run_observation: None,
        }
    }

    fn durable_batch_response_lost(session_directory: &Path, execution_count_path: &Path) -> Self {
        Self {
            session_directory: session_directory.to_path_buf(),
            execution_count_path: execution_count_path.to_path_buf(),
            uncertain: false,
            failed: false,
            unclaimed: false,
            observe_read_only: false,
            drop_response_after_receipt: false,
            policy_denial: false,
            durable_batch: true,
            command_run_observation: None,
        }
    }
}

impl OfficialCodexServerRequestHandler for ReceiptHandler {
    fn load_execution_ledger(
        &mut self,
        canonical_input_sha256: &str,
    ) -> Result<Option<CodexExecutionLedger>, String> {
        let path = self
            .session_directory
            .join(".tura/run/effect_ledgers")
            .join(format!("{canonical_input_sha256}.json"));
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string())
    }

    fn persist_execution_ledger(&mut self, ledger: &CodexExecutionLedger) -> Result<(), String> {
        let directory = self.session_directory.join(".tura/run/effect_ledgers");
        fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
        fs::write(
            directory.join(format!("{}.json", ledger.canonical_input_sha256)),
            serde_json::to_vec_pretty(ledger).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())
    }

    fn observe_command_run_effect(
        &mut self,
        request: &OfficialCodexServerRequest,
        runtime_id: &str,
    ) -> Result<Option<CodexCommandRunEffectObservation>, String> {
        if request.method != "item/tool/call"
            || request.params.get("tool").and_then(Value::as_str) != Some("command_run")
        {
            return Ok(None);
        }
        let tool_call_id = request
            .params
            .get("callId")
            .and_then(Value::as_str)
            .ok_or_else(|| "command_run tool call ID is unavailable".to_string())?
            .to_string();
        let commands = request
            .params
            .get("arguments")
            .and_then(|arguments| arguments.get("commands"))
            .and_then(Value::as_array)
            .filter(|commands| !commands.is_empty())
            .ok_or_else(|| "command_run commands are unavailable".to_string())?;
        let execution_id = format!("{runtime_id}:{tool_call_id}");
        let mut observed = Vec::with_capacity(commands.len());
        for (enumerated_index, command) in commands.iter().enumerate() {
            let command_type = command
                .get("command_type")
                .and_then(Value::as_str)
                .ok_or_else(|| "command_type is unavailable".to_string())?;
            let binding_id = ["id", "command_id", "commandId", "result_id"]
                .iter()
                .find_map(|key| command.get(*key).and_then(Value::as_str))
                .map(str::to_string);
            let effective_step = command
                .get("step")
                .and_then(|step| {
                    step.as_u64()
                        .or_else(|| step.as_str().and_then(|step| step.parse::<u64>().ok()))
                })
                .unwrap_or(enumerated_index as u64 + 1)
                .max(1);
            let claim_identity = read_only_claim_identity(
                &execution_id,
                effective_step,
                enumerated_index,
                binding_id.as_deref(),
            );
            observed.push(CodexCommandRunCommandObservation {
                command_type: command_type.to_string(),
                enumerated_index,
                effective_step,
                binding_id,
                claim_identity,
            });
        }
        let observation = CodexCommandRunEffectObservation {
            runtime_id: runtime_id.to_string(),
            tool_call_id,
            execution_id,
            commands: observed,
        };
        self.command_run_observation = Some(observation.clone());
        Ok(Some(observation))
    }

    fn observe_read_only_effect(
        &mut self,
        request: &OfficialCodexServerRequest,
    ) -> Result<Option<CodexReadOnlyEffectObservation>, String> {
        if !self.observe_read_only
            || request.method != "item/tool/call"
            || request.params.get("tool").and_then(Value::as_str) != Some("command_run")
        {
            return Ok(None);
        }
        let Some(tool_call_id) = request.params.get("callId").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(commands) = request
            .params
            .get("arguments")
            .and_then(|arguments| arguments.get("commands"))
            .and_then(Value::as_array)
        else {
            return Ok(None);
        };
        if commands.len() != 1 {
            return Ok(None);
        }
        let command = &commands[0];
        let Some(command_type) = command.get("command_type").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(command_line) = command.get("command_line").and_then(Value::as_str) else {
            return Ok(None);
        };
        if command_type != "zsh" || command_line != UNCLAIMED_READ_ONLY_COMMAND_LINE {
            return Ok(None);
        }
        let enumerated_index = 0;
        let effective_step = command
            .get("step")
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .max(1);
        let binding_id = command
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let original_runtime_id = "runtime-official-1".to_string();
        let tool_call_id = tool_call_id.to_string();
        let execution_id = format!("{original_runtime_id}:{tool_call_id}");
        let claim_identity = read_only_claim_identity(
            &execution_id,
            effective_step,
            enumerated_index,
            binding_id.as_deref(),
        );
        if self.unclaimed {
            let count = execution_count(&self.execution_count_path) + 1;
            fs::write(&self.execution_count_path, count.to_string()).map_err(|error| {
                format!(
                    "failed to record unclaimed read-only observation count at {}: {error}",
                    self.execution_count_path.display()
                )
            })?;
        }
        Ok(Some(CodexReadOnlyEffectObservation {
            original_runtime_id,
            tool_call_id,
            execution_id,
            commands: vec![CodexReadOnlyCommandObservation {
                access: CodexObservedCommandAccess::ReadOnly,
                command_type: command_type.to_string(),
                command_line: command_line.to_string(),
                enumerated_index,
                effective_step,
                binding_id,
                claim_identity,
            }],
        }))
    }

    fn verify_never_claimed_read_only_effect(
        &mut self,
        observation: &CodexReadOnlyEffectObservation,
    ) -> Result<(), String> {
        if !self.unclaimed {
            return Err(
                "read-only absence verification requires the unclaimed handler".to_string(),
            );
        }
        if observation.original_runtime_id != "runtime-official-1"
            || observation.tool_call_id != "call-original"
        {
            return Err("read-only observation runtime or tool-call identity drifted".to_string());
        }
        let [command] = observation.commands.as_slice() else {
            return Err(
                "read-only absence verification requires exactly one observed command".to_string(),
            );
        };
        if !matches!(&command.access, CodexObservedCommandAccess::ReadOnly) {
            return Err("observed command is not classified read-only".to_string());
        }
        if command.command_type != "zsh"
            || command.command_line != UNCLAIMED_READ_ONLY_COMMAND_LINE
            || command.enumerated_index != 0
            || command.effective_step != 1
        {
            return Err("read-only observed command identity drifted".to_string());
        }
        let execution_id = format!(
            "{}:{}",
            observation.original_runtime_id, observation.tool_call_id
        );
        let claim_identity = read_only_claim_identity(
            &execution_id,
            command.effective_step,
            command.enumerated_index,
            command.binding_id.as_deref(),
        );
        if observation.execution_id != execution_id {
            return Err("read-only execution identity drifted".to_string());
        }
        if command.claim_identity != claim_identity {
            return Err("read-only command claim identity drifted".to_string());
        }
        let encoded_identity = encode_read_only_receipt_identity_for_test(&claim_identity);
        let receipt_directory = self
            .session_directory
            .join(".tura")
            .join("run")
            .join("command_receipts");
        verify_read_only_artifact_absent_for_test(
            &receipt_directory.join(format!("{encoded_identity}.claim.json")),
        )?;
        verify_read_only_artifact_absent_for_test(
            &receipt_directory.join(format!("{encoded_identity}.json")),
        )
    }

    fn verify_completed_read_only_effect(
        &mut self,
        observation: &CodexReadOnlyEffectObservation,
    ) -> Result<(), String> {
        if !self.observe_read_only || self.unclaimed {
            return Err("completed read-only verification is unavailable".to_string());
        }
        if observation.original_runtime_id != "runtime-official-1"
            || observation.tool_call_id != "call-original"
            || observation.execution_id != "runtime-official-1:call-original"
        {
            return Err("completed read-only observation identity drifted".to_string());
        }
        let [command] = observation.commands.as_slice() else {
            return Err("completed read-only verification requires one command".to_string());
        };
        if !matches!(&command.access, CodexObservedCommandAccess::ReadOnly)
            || command.command_type != "zsh"
            || command.command_line != UNCLAIMED_READ_ONLY_COMMAND_LINE
            || command.claim_identity != UNCLAIMED_READ_ONLY_CLAIM_IDENTITY
        {
            return Err("completed read-only command identity drifted".to_string());
        }
        Ok(())
    }

    fn handle<'a>(
        &'a mut self,
        request: OfficialCodexServerRequest,
    ) -> OfficialCodexServerRequestFuture<'a> {
        Box::pin(async move {
            assert_eq!(request.method, "item/tool/call");
            assert_eq!(request.params["tool"], "command_run");
            if self.unclaimed {
                return Err("simulated interruption before command claim".to_string());
            }
            if self.policy_denial {
                let output = json!({
                    "results": [{
                        "success": false,
                        "command_type": "jspace",
                        "error": "JSPACE_COMMAND_DENIED: command does not match an admitted prefix",
                        "jspace_error_code": "JSPACE_COMMAND_DENIED",
                        "operation": "command",
                        "target": "touch /tmp/blocked",
                        "effect_state": "not_started",
                        "mutation_count": 0,
                        "authority_effect": "none",
                        "delivery_state": "deterministic_policy_denial",
                        "replayable": true,
                    }]
                });
                return Ok(json!({
                    "contentItems": [{"type": "inputText", "text": output.to_string()}],
                    "success": true,
                }));
            }
            let count = execution_count(&self.execution_count_path) + 1;
            fs::write(&self.execution_count_path, count.to_string())
                .expect("execution count write");
            let receipt_directory = self
                .session_directory
                .join(".tura")
                .join("run")
                .join("command_receipts");
            fs::create_dir_all(&receipt_directory).expect("receipt directory");
            if self.durable_batch {
                let observation = self
                    .command_run_observation
                    .as_ref()
                    .expect("durable batch command identities");
                for (index, command) in observation.commands.iter().enumerate() {
                    let call_id = command.claim_identity.clone();
                    let failed = index + 1 == observation.commands.len();
                    let receipt = json!({
                        "schema_version": "tura_command_terminal_receipt_v1",
                        "call_id": call_id,
                        "pid": 4242 + index,
                        "terminal_state": if failed {"failed"} else {"completed"},
                        "failure_class": if failed {"workload_exit_nonzero"} else {"none"},
                        "termination_origin": "workload",
                        "exit_code": if failed {2} else {0},
                        "wall_time_ms": 10,
                        "wall_timeout_ms": 300000,
                        "stall_timeout_ms": null,
                        "outcome": "known",
                        "process_reaped": true,
                        "process_group_empty": true,
                        "termination_proven": true,
                        "authority_effect": "none",
                        "authoritative_publication": "unproven",
                        "staging_authority": "none",
                        "retry_safe": false,
                        "auto_retry_allowed": false,
                        "reconcile_required": failed,
                        "replay_semantics": "diagnosed_replay_only_after_no_authoritative_publication_or_idempotent_cas_proof"
                    });
                    fs::write(
                        receipt_directory.join(format!(
                            "{}.json",
                            encode_read_only_receipt_identity_for_test(&call_id)
                        )),
                        serde_json::to_vec_pretty(&receipt).expect("batch receipt encode"),
                    )
                    .expect("batch receipt write");
                }
                self.durable_batch = false;
                return Err("simulated response loss after durable command batch".to_string());
            }
            let receipt_path = if self.observe_read_only {
                receipt_directory.join(format!(
                    "{}.json",
                    encode_read_only_receipt_identity_for_test(UNCLAIMED_READ_ONLY_CLAIM_IDENTITY)
                ))
            } else {
                receipt_directory.join("effect-command.json")
            };
            let receipt = json!({
                "schema_version": "tura_command_terminal_receipt_v1",
                "call_id": "runtime-official-1:call-original:step:1:index:0",
                "pid": 4242,
                "terminal_state": if self.failed {"failed"} else {"completed"},
                "failure_class": if self.failed {"workload_exit_nonzero"} else {"none"},
                "termination_origin": "workload",
                "exit_code": if self.failed {1} else {0},
                "wall_time_ms": 10,
                "wall_timeout_ms": 300000,
                "stall_timeout_ms": null,
                "outcome": "known",
                "process_reaped": true,
                "process_group_empty": true,
                "termination_proven": true,
                "authority_effect": "none",
                "authoritative_publication": "unproven",
                "staging_authority": "none",
                "retry_safe": false,
                "auto_retry_allowed": false,
                "reconcile_required": self.uncertain || self.failed,
                "replay_semantics": "diagnosed_replay_only_after_no_authoritative_publication_or_idempotent_cas_proof"
            });
            fs::write(
                &receipt_path,
                serde_json::to_vec_pretty(&receipt).expect("receipt encode"),
            )
            .expect("receipt write");
            if self.drop_response_after_receipt {
                self.drop_response_after_receipt = false;
                return Err("simulated response loss after durable read-only receipt".to_string());
            }
            let output = json!({
                "results": [{
                    "command_type": "task_status",
                    "output": {"status": "doing"},
                    "step": 1,
                    "success": true,
                }, {
                    "command_type": "zsh",
                    "output": {
                        "exit_code": if self.failed {1} else {0},
                        "terminal_receipt": receipt,
                        "terminal_receipt_path": receipt_path,
                    },
                    "step": 1,
                    "success": !self.failed,
                }]
            });
            Ok(json!({
                "contentItems": [{"type": "inputText", "text": output.to_string()}],
                "success": true,
            }))
        })
    }
}

fn effect_request(
    session_directory: &Path,
    capture: &Path,
    user_input: &str,
) -> OfficialCodexTurnRequest {
    let mut request = request(
        session_directory,
        capture,
        vec![json!({"role": "user", "content": user_input})],
    );
    request.dynamic_tools = vec![json!({
        "name": "command_run",
        "description": "governed command runner",
        "inputSchema": {"type": "object"}
    })];
    request
}

fn execution_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn load_only_execution_ledger(session_directory: &Path) -> CodexExecutionLedger {
    let directory = session_directory.join(".tura/run/effect_ledgers");
    let paths = fs::read_dir(&directory)
        .expect("effect ledger directory")
        .map(|entry| entry.expect("effect ledger entry").path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    let [path] = paths.as_slice() else {
        panic!("expected exactly one effect ledger, got {paths:?}");
    };
    serde_json::from_slice(&fs::read(path).expect("effect ledger bytes"))
        .expect("valid effect ledger")
}

fn persist_only_execution_ledger(
    session_directory: &Path,
    execution_ledger: &CodexExecutionLedger,
) {
    let path = session_directory
        .join(".tura/run/effect_ledgers")
        .join(format!("{}.json", execution_ledger.canonical_input_sha256));
    fs::write(
        path,
        serde_json::to_vec_pretty(execution_ledger).expect("effect ledger JSON"),
    )
    .expect("effect ledger write");
}

fn request(
    session_directory: &Path,
    capture: &Path,
    messages: Vec<Value>,
) -> OfficialCodexTurnRequest {
    // SAFETY: this standalone integration harness runs provider attempts sequentially.
    unsafe { std::env::set_var("TURA_FAKE_CODEX_CAPTURE", capture) };
    OfficialCodexTurnRequest {
        tura_session_id: "tura-session-1".to_string(),
        runtime_id: "runtime-official-1".to_string(),
        fallback_from_id: None,
        session_directory: session_directory.to_path_buf(),
        model: "gpt-5.6-sol".to_string(),
        messages,
        turn_context: None,
        executable: CodexAppServerExecutable {
            path: std::env::current_exe().expect("test executable"),
            prefix_args: vec![
                "--fake-session".to_string(),
                session_directory.display().to_string(),
            ],
        },
        dynamic_tools: Vec::new(),
        allowed_command_run_commands: None,
        disable_permission_restrictions: false,
        commander_continuation: None,
    }
}

fn fake_app_server(args: &[String]) {
    assert_eq!(
        &args[args.len() - 3..],
        ["app-server", "--listen", "stdio://"]
    );
    let capture = PathBuf::from(
        std::env::var_os("TURA_FAKE_CODEX_CAPTURE").expect("fake capture environment"),
    );
    let session_directory = argument_after(args, "--fake-session");
    let mode = capture
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let authority = capture.parent().unwrap().join("authoritative-turn.json");
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut lines = stdin.lock().lines();
    let mut recovery_history_injected = false;
    let mut commander_turn_completed = false;
    while let Some(line) = lines.next() {
        let line = line.expect("fake app-server input");
        let message: Value = serde_json::from_str(&line).expect("JSON-RPC input");
        append_capture(&capture, &message);
        let method = message["method"].as_str().expect("method");
        let id = message.get("id").cloned();
        match method {
            "initialize" => respond(
                &mut stdout,
                id,
                json!({
                    "userAgent": "codex-test",
                    "platformFamily": "unix",
                    "platformOs": "test"
                }),
            ),
            "initialized" => {}
            "thread/start" => {
                let recovery = mode.ends_with("-recover");
                respond(
                    &mut stdout,
                    id,
                    json!({
                        "thread": {
                            "id": if recovery {
                                "thread-recovered-1"
                            } else {
                                "thread-official-1"
                            },
                            "sessionId": if recovery {
                                "session-recovered-1"
                            } else {
                                "session-official-1"
                            }
                        }
                    }),
                );
            }
            "thread/resume" => respond(
                &mut stdout,
                id,
                json!({
                    "thread": {
                        "id": if mode == "commander-target-mismatch" {
                            json!("wrong-commander-thread")
                        } else {
                            message["params"]["threadId"].clone()
                        },
                        "sessionId": if mode.starts_with("commander-") {
                            "session-commander-1"
                        } else {
                            "session-official-1"
                        }
                    }
                }),
            ),
            "thread/read" => {
                let turns = if mode.starts_with("commander-") {
                    let mut turns = vec![
                        json!({"id": "turn-commander-0", "status": "completed", "items": []}),
                        json!({"id": "turn-commander-1", "status": "completed", "items": []}),
                    ];
                    if commander_turn_completed {
                        turns.push(json!({
                            "id": "turn-commander-new",
                            "status": "completed",
                            "items": [{
                                "type": "agentMessage",
                                "id": "item-commander-new",
                                "text": "commander converged",
                                "phase": "final_answer"
                            }]
                        }));
                    }
                    turns
                } else if mode == "recover" {
                    vec![
                        serde_json::from_slice::<Value>(
                            &fs::read(&authority).expect("authoritative turn"),
                        )
                        .expect("authoritative turn JSON"),
                    ]
                } else if mode.ends_with("-recover") {
                    vec![json!({
                        "id": "turn-effect-interrupted-1",
                        "status": "interrupted",
                        "items": []
                    })]
                } else {
                    Vec::new()
                };
                respond(
                    &mut stdout,
                    id,
                    json!({"thread": {
                        "id": if mode.starts_with("commander-") {
                            "commander-thread-1"
                        } else {
                            "thread-official-1"
                        },
                        "turns": turns
                    }}),
                );
            }
            "thread/inject_items" => {
                assert!(mode.ends_with("-recover"));
                assert_eq!(message["params"]["threadId"], "thread-recovered-1");
                let items = message["params"]["items"]
                    .as_array()
                    .expect("recovery items");
                assert_eq!(
                    items.first().and_then(|item| item["type"].as_str()),
                    Some("message")
                );
                assert!(items.windows(2).any(|pair| {
                    pair[0]["type"] == "function_call"
                        && pair[1]["type"] == "function_call_output"
                        && pair[0]["call_id"] == pair[1]["call_id"]
                }));
                recovery_history_injected = true;
                respond(&mut stdout, id, json!({}));
            }
            "turn/start" => {
                if mode.starts_with("commander-") {
                    assert_eq!(message["params"]["threadId"], "commander-thread-1");
                    respond(
                        &mut stdout,
                        id,
                        json!({"turn": {
                            "id": "turn-commander-new",
                            "status": "inProgress",
                            "items": []
                        }}),
                    );
                    commander_turn_completed = true;
                    notify(
                        &mut stdout,
                        "item/completed",
                        json!({
                            "threadId": "commander-thread-1",
                            "turnId": "turn-commander-new",
                            "item": {
                                "type": "agentMessage",
                                "id": "item-commander-new",
                                "text": "commander converged",
                                "phase": "final_answer"
                            }
                        }),
                    );
                    notify(
                        &mut stdout,
                        "turn/completed",
                        json!({
                            "threadId": "commander-thread-1",
                            "turn": {
                                "id": "turn-commander-new",
                                "status": "completed",
                                "items": [{
                                    "type": "agentMessage",
                                    "id": "item-commander-new",
                                    "text": "commander converged",
                                    "phase": "final_answer"
                                }]
                            }
                        }),
                    );
                    continue;
                }
                if mode.contains("effect-")
                    || mode == "delivered-failure"
                    || mode.contains("changed-")
                    || mode.contains("conflict-")
                    || mode.contains("uncertain-")
                {
                    let recovery = mode.ends_with("-recover");
                    let turn_id = if recovery {
                        "turn-effect-recovered-1"
                    } else {
                        "turn-effect-interrupted-1"
                    };
                    respond(
                        &mut stdout,
                        id,
                        json!({
                            "turn": {
                                "id": turn_id,
                                "status": "inProgress",
                                "items": []
                            }
                        }),
                    );
                    if recovery {
                        assert!(recovery_history_injected);
                        assert_eq!(message["params"]["input"], json!([]));
                        notify(
                            &mut stdout,
                            "item/completed",
                            json!({
                                "threadId": "thread-recovered-1",
                                "turnId": turn_id,
                                "item": {
                                    "type": "agentMessage",
                                    "id": "item-recovered-effect-1",
                                    "text": "recovered after provider loss",
                                    "phase": "final_answer"
                                }
                            }),
                        );
                        notify(
                            &mut stdout,
                            "turn/completed",
                            json!({
                                "threadId": "thread-recovered-1",
                                "turn": {
                                    "id": turn_id,
                                    "status": "completed",
                                    "items": [{
                                        "type": "agentMessage",
                                        "id": "item-recovered-effect-1",
                                        "text": "recovered after provider loss",
                                        "phase": "final_answer"
                                    }]
                                }
                            }),
                        );
                        continue;
                    }
                    let commands = if mode.contains("durable-batch-") {
                        vec![
                            json!({"command_type": "zsh", "command_line": "command-0"}),
                            json!({"command_type": "zsh", "command_line": "command-1"}),
                            json!({
                                "command_type": "zsh",
                                "command_line": "command-2",
                                "step": "7",
                                "command_id": "result-two"
                            }),
                            json!({
                                "command_type": "zsh",
                                "command_line": "command-3",
                                "step": 0,
                                "commandId": "result-three"
                            }),
                            json!({
                                "command_type": "zsh",
                                "command_line": "command-4",
                                "step": 9,
                                "result_id": "result-four"
                            }),
                            json!({
                                "command_type": "zsh",
                                "command_line": "command-5",
                                "step": 11,
                                "id": "result-five"
                            }),
                        ]
                    } else {
                        vec![json!({
                            "command_type": "zsh",
                            "command_line": if mode == "conflict-recover" {
                                "sleep 91"
                            } else if mode.contains("unclaimed-")
                                || mode.contains("completed-read-only-")
                            {
                                r#"rg -n -A18 -B6 "struct TurnRequestContext|TurnRequestContext \{" crates/provider/src/official_codex_app_server.rs"#
                            } else {
                                "sleep 90"
                            },
                            "step": 1
                        })]
                    };
                    server_request(
                        &mut stdout,
                        91,
                        "item/tool/call",
                        json!({
                            "callId": if recovery {"call-recovered"} else {"call-original"},
                            "tool": "command_run",
                            "arguments": {
                                "commands": commands
                            }
                        }),
                    );
                    let Some(response_line) = lines.next() else {
                        return;
                    };
                    let response_line = response_line.expect("tool response input");
                    let response: Value =
                        serde_json::from_str(&response_line).expect("tool response JSON");
                    append_capture(&capture, &response);
                    assert_eq!(response["id"], 91);
                    if !recovery && mode.contains("unclaimed-") {
                        assert!(response.get("error").is_some(), "{response}");
                        return;
                    }
                    assert!(response.get("result").is_some(), "{response}");
                    if !recovery && mode != "delivered-failure" {
                        return;
                    }
                    let final_text = if mode == "delivered-failure" {
                        "failed command observed"
                    } else {
                        "recovered after provider loss"
                    };
                    notify(
                        &mut stdout,
                        "item/completed",
                        json!({
                            "threadId": "thread-recovered-1",
                            "turnId": turn_id,
                            "item": {
                                "type": "agentMessage",
                                "id": "item-recovered-effect-1",
                                "text": final_text,
                                "phase": "final_answer"
                            }
                        }),
                    );
                    notify(
                        &mut stdout,
                        "turn/completed",
                        json!({
                            "threadId": "thread-recovered-1",
                            "turn": {
                                "id": turn_id,
                                "status": "completed",
                                "items": [{
                                    "type": "agentMessage",
                                    "id": "item-recovered-effect-1",
                                    "text": final_text,
                                    "phase": "final_answer"
                                }]
                            }
                        }),
                    );
                    continue;
                }
                if mode.starts_with("disconnect") {
                    let association = serde_json::to_value(
                        load_thread_association(&session_directory, "tura-session-1")
                            .expect("durable pre-submit association read")
                            .expect("durable pre-submit association"),
                    )
                    .expect("pre-submit association JSON");
                    assert_eq!(association["turn_attempt"]["state"], "prepared");
                    assert!(association["active_turn_id"].is_null());
                    fs::write(
                        &authority,
                        serde_json::to_vec(&json!({
                            "id": "turn-disconnected-1",
                            "status": "completed",
                            "items": [{
                                "type": "agentMessage",
                                "id": "item-recovered-1",
                                "text": "recovered official reply",
                                "phase": "final_answer"
                            }]
                        }))
                        .unwrap(),
                    )
                    .expect("authoritative turn write");
                    return;
                }
                respond(
                    &mut stdout,
                    id,
                    json!({
                        "turn": {
                            "id": "turn-official-1",
                            "status": "inProgress",
                            "items": []
                        }
                    }),
                );
                notify(
                    &mut stdout,
                    "item/agentMessage/delta",
                    json!({
                        "threadId": "thread-official-1",
                        "turnId": "turn-official-1",
                        "itemId": "item-agent-1",
                        "delta": "provisional reply"
                    }),
                );
                notify(
                    &mut stdout,
                    "item/completed",
                    json!({
                        "threadId": "thread-official-1",
                        "turnId": "turn-official-1",
                        "item": {
                            "type": "agentMessage",
                            "id": "item-agent-1",
                            "text": "official reply",
                            "phase": "final_answer"
                        }
                    }),
                );
                notify(
                    &mut stdout,
                    "thread/tokenUsage/updated",
                    json!({
                        "threadId": "thread-official-1",
                        "turnId": "turn-official-1",
                        "tokenUsage": {
                            "last": {
                                "inputTokens": 17,
                                "cachedInputTokens": 3,
                                "outputTokens": 5,
                                "reasoningOutputTokens": 2,
                                "cacheWriteInputTokens": 0,
                                "totalTokens": 22
                            },
                            "total": {
                                "inputTokens": 17,
                                "cachedInputTokens": 3,
                                "outputTokens": 5,
                                "reasoningOutputTokens": 2,
                                "cacheWriteInputTokens": 0,
                                "totalTokens": 22
                            },
                            "modelContextWindow": 260000
                        }
                    }),
                );
                notify(
                    &mut stdout,
                    "turn/completed",
                    json!({
                        "threadId": "thread-official-1",
                        "turn": {
                            "id": "turn-official-1",
                            "status": "completed",
                            "items": [{
                                "type": "agentMessage",
                                "id": "item-agent-1",
                                "text": "official reply",
                                "phase": "final_answer"
                            }]
                        }
                    }),
                );
            }
            other => panic!("unexpected fake app-server method: {other}"),
        }
    }
}

fn respond(stdout: &mut impl Write, id: Option<Value>, result: Value) {
    writeln!(stdout, "{}", json!({"id": id, "result": result})).expect("response");
    stdout.flush().expect("response flush");
}

fn notify(stdout: &mut impl Write, method: &str, params: Value) {
    writeln!(stdout, "{}", json!({"method": method, "params": params})).expect("notification");
    stdout.flush().expect("notification flush");
}

fn server_request(stdout: &mut impl Write, id: u64, method: &str, params: Value) {
    writeln!(
        stdout,
        "{}",
        json!({"id": id, "method": method, "params": params})
    )
    .expect("server request");
    stdout.flush().expect("server request flush");
}

fn argument_after(args: &[String], name: &str) -> PathBuf {
    let index = args.iter().position(|arg| arg == name).expect("argument");
    PathBuf::from(args.get(index + 1).expect("argument value"))
}

fn append_capture(path: &Path, message: &Value) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("capture file");
    writeln!(file, "{message}").expect("capture write");
    file.sync_data().expect("capture sync");
}

fn captured_messages(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .expect("capture")
        .lines()
        .map(|line| serde_json::from_str(line).expect("captured JSON"))
        .collect()
}

fn methods(messages: &[Value]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| message["method"].as_str().expect("captured method"))
        .collect()
}

struct EnvGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &Path) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: this harness is a single-threaded process before provider work starts.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: this harness is a single-threaded process after provider work finishes.
        unsafe {
            if let Some(previous) = self.previous.take() {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}
