use crate::commands::CommandResponse;
use crate::runtime::tool::ToolContext;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

use super::process::{
    attach_shell_process_scope, configure_process_scope, configure_tokio_process_scope,
    panic_cleanup_process_scope_empty, process_is_alive, retain_shell_process_scope,
    terminate_process_tree,
};
use super::response::failed_async_response;

const EXEC_OUTPUT_MAX_BYTES: usize = 1024 * 1024;
const MAX_EXEC_OUTPUT_DELTAS_PER_CALL: usize = 512;
const PROCESS_TERMINATION_GRACE_SECS: u64 = 5;

pub(super) fn run_command_with_timeout(
    mut command: Command,
    timeout_secs: u64,
    stall_timeout_secs: Option<u64>,
) -> CommandResponse {
    let started = Instant::now();
    let progress = ProgressClock::new();
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_process_scope(&mut command);
    match command.spawn() {
        Ok(mut child) => {
            let mut scope = attach_shell_process_scope(child.id(), None);
            let stdout_task = child.stdout.take().map(|stream| {
                let progress = progress.clone();
                thread::spawn(move || read_blocking_stream(stream, progress))
            });
            let stderr_task = child.stderr.take().map(|stream| {
                let progress = progress.clone();
                thread::spawn(move || read_blocking_stream(stream, progress))
            });
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        if let Some(scope) = scope.take() {
                            retain_shell_process_scope(scope, None);
                        }
                        let (stdout, stderr) = drain_blocking_stream_tasks(
                            stdout_task,
                            stderr_task,
                            Duration::from_secs(2),
                        );
                        return command_response_from_status(started, status, stdout, stderr);
                    }
                    Ok(None) => {
                        let wall_timed_out = started.elapsed() >= Duration::from_secs(timeout_secs);
                        let stalled = stall_timeout_secs.is_some_and(|seconds| {
                            progress.elapsed() >= Duration::from_secs(seconds)
                        });
                        if wall_timed_out || stalled {
                            if let Some(scope) = &scope {
                                scope.terminate();
                            }
                            terminate_process_tree(child.id());
                            let _ = child.kill();
                            let _ = child.wait();
                            let (stdout, stderr) = drain_blocking_stream_tasks(
                                stdout_task,
                                stderr_task,
                                Duration::from_secs(2),
                            );
                            let (error_type, failure_class, termination_origin, mut message) =
                                if stalled {
                                    let seconds = stall_timeout_secs.unwrap_or_default();
                                    (
                                        "CommandStalled",
                                        "progress_stall",
                                        "progress_watchdog",
                                        format!("No command progress for {seconds} seconds"),
                                    )
                                } else {
                                    (
                                        "CommandWallClockTimedOut",
                                        "wrapper_wall_clock_timeout",
                                        "command_run_wrapper",
                                        format!("Timed out after {timeout_secs} seconds"),
                                    )
                                };
                            if !stderr.is_empty() {
                                message.push_str("\nStderr tail:\n");
                                message.push_str(&tail_chars(&stderr, 4000));
                            }
                            return CommandResponse {
                                success: false,
                                exit_code: -1,
                                stdout,
                                stderr,
                                output: terminated_unknown_outcome(
                                    error_type,
                                    failure_class,
                                    termination_origin,
                                    message,
                                ),
                                changes: Vec::new(),
                            };
                        }
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(err) => {
                        return CommandResponse {
                            success: false,
                            exit_code: 1,
                            stdout: String::new(),
                            stderr: err.to_string(),
                            output: Value::String(err.to_string()),
                            changes: Vec::new(),
                        };
                    }
                }
            }
        }
        Err(err) => CommandResponse {
            success: false,
            exit_code: 1,
            stdout: String::new(),
            stderr: err.to_string(),
            output: Value::String(err.to_string()),
            changes: Vec::new(),
        },
    }
}

fn read_blocking_stream<R: Read>(mut stream: R, progress: ProgressClock) -> String {
    let mut output = CappedOutput::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                progress.mark();
                output.push(&buffer[..n]);
            }
            Err(_) => break,
        }
    }
    output.finish()
}

fn drain_blocking_stream_tasks(
    mut stdout_task: Option<JoinHandle<String>>,
    mut stderr_task: Option<JoinHandle<String>>,
    timeout: Duration,
) -> (String, String) {
    fn finished(task: &Option<JoinHandle<String>>) -> bool {
        task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    let started = Instant::now();
    while !(finished(&stdout_task) && finished(&stderr_task)) && started.elapsed() < timeout {
        thread::sleep(Duration::from_millis(10));
    }

    fn take_if_finished(task: &mut Option<JoinHandle<String>>) -> String {
        if task.as_ref().is_some_and(JoinHandle::is_finished) {
            match task.take() {
                Some(task) => task.join().unwrap_or_default(),
                None => String::new(),
            }
        } else {
            String::new()
        }
    }

    let stdout = take_if_finished(&mut stdout_task);
    let stderr = take_if_finished(&mut stderr_task);
    (stdout, stderr)
}

fn command_response_from_status(
    started: Instant,
    status: ExitStatus,
    stdout: String,
    stderr: String,
) -> CommandResponse {
    let wall = started.elapsed().as_secs_f32();
    let exit_code = status.code().unwrap_or(1);
    let mut text =
        format!("Exit code: {exit_code}\nWall time: {wall:.1} seconds\nOutput:\n{stdout}");
    if !stderr.is_empty() {
        text.push_str("\nStderr:\n");
        text.push_str(&stderr);
    }
    CommandResponse {
        success: status.success(),
        exit_code,
        stdout,
        stderr,
        output: Value::String(text),
        changes: Vec::new(),
    }
}

fn tail_chars(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    let start = chars.len().saturating_sub(max_chars);
    chars[start..].iter().collect()
}

pub(super) async fn run_tokio_command_with_timeout(
    mut command: tokio::process::Command,
    timeout_secs: u64,
    stall_timeout_secs: Option<u64>,
    ctx: &ToolContext,
) -> CommandResponse {
    let started = Instant::now();
    let progress = ProgressClock::new();
    if let Err(error) = reconcile_command_execution_claims(&ctx.session_dir, ctx.current_call_id())
    {
        return CommandResponse {
            success: false,
            exit_code: -1,
            stdout: String::new(),
            stderr: error.clone(),
            output: json!({
                "error_type": "CommandExecutionReconciliationRequired",
                "failure_class": "duplicate_or_unreconciled_execution",
                "termination_origin": "command_run_admission",
                "message": error,
                "outcome": "unknown",
                "retry_safe": false,
                "auto_retry_allowed": false,
                "reconcile_required": true
            }),
            changes: Vec::new(),
        };
    }
    if let Err(error) = claim_command_execution(ctx, timeout_secs, stall_timeout_secs) {
        return CommandResponse {
            success: false,
            exit_code: -1,
            stdout: String::new(),
            stderr: error.clone(),
            output: json!({
                "error_type": "CommandExecutionClaimFailed",
                "failure_class": "duplicate_or_unreconciled_execution",
                "termination_origin": "command_run_admission",
                "message": error,
                "outcome": "unknown",
                "retry_safe": false,
                "auto_retry_allowed": false,
                "reconcile_required": true
            }),
            changes: Vec::new(),
        };
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_tokio_process_scope(&mut command);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            let mut response = failed_async_response(&err.to_string(), 1);
            if let Err(receipt_error) = attach_durable_terminal_receipt(
                &mut response,
                ctx,
                None,
                started,
                timeout_secs,
                stall_timeout_secs,
                "spawn_failed",
                "workload_spawn_failure",
                "command_run_spawn",
                "known",
                true,
                true,
            ) {
                response.output = json!({
                    "error_type": "CommandTerminalReceiptWriteFailed",
                    "failure_class": "control_plane_receipt_failure",
                    "termination_origin": "command_run_spawn",
                    "message": receipt_error,
                    "outcome": "unknown",
                    "retry_safe": false,
                    "auto_retry_allowed": false,
                    "reconcile_required": true
                });
            }
            return response;
        }
    };
    let pid = child.id();
    let mut scope = pid.and_then(|pid| attach_shell_process_scope(pid, ctx.current_call_id()));
    if let Err(error) = update_command_claim_running(ctx, pid, timeout_secs, stall_timeout_secs) {
        let _ = child.kill().await;
        let _ = child.wait().await;
        let process_group_empty = pid.map(|pid| !process_is_alive(pid)).unwrap_or(true);
        let mut response = failed_async_response(&error, 1);
        if let Err(receipt_error) = attach_durable_terminal_receipt(
            &mut response,
            ctx,
            pid,
            started,
            timeout_secs,
            stall_timeout_secs,
            "claim_update_failed",
            "control_plane_claim_failure",
            "command_run_admission",
            "unknown",
            true,
            process_group_empty,
        ) {
            response.output = json!({
                "error_type": "CommandTerminalReceiptWriteFailed",
                "failure_class": "control_plane_receipt_failure",
                "termination_origin": "command_run_admission",
                "message": receipt_error,
                "outcome": "unknown",
                "retry_safe": false,
                "auto_retry_allowed": false,
                "reconcile_required": true
            });
        }
        return response;
    }
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let call_id = ctx.current_call_id().unwrap_or("command_run").to_string();
    let stdout_capture = stdout.as_ref().map(|_| SharedOutput::new());
    let stderr_capture = stderr.as_ref().map(|_| SharedOutput::new());
    let stdout_task = stdout.map(|reader| {
        let capture = stdout_capture
            .as_ref()
            .expect("stdout capture exists when stdout reader exists")
            .clone();
        tokio::spawn(read_stream_with_deltas(
            reader,
            ctx.clone(),
            call_id.clone(),
            "stdout",
            capture,
            progress.clone(),
        ))
    });
    let stderr_task = stderr.map(|reader| {
        let capture = stderr_capture
            .as_ref()
            .expect("stderr capture exists when stderr reader exists")
            .clone();
        tokio::spawn(read_stream_with_deltas(
            reader,
            ctx.clone(),
            call_id.clone(),
            "stderr",
            capture,
            progress.clone(),
        ))
    });
    let mut wait_task = Box::pin(child.wait());
    let mut expiration = None;
    let mut termination = None;
    let status = if ctx.cancellation.is_cancelled() {
        expiration = Some("tool task aborted".to_string());
        if let Some(scope) = &scope {
            scope.terminate();
        }
        if let Some(pid) = pid {
            terminate_process_tree(pid);
        }
        None
    } else {
        tokio::select! {
            output = &mut wait_task => output.ok(),
            _ = tokio::time::sleep(Duration::from_secs(timeout_secs)) => {
                termination = Some(("CommandWallClockTimedOut", "wrapper_wall_clock_timeout", "command_run_wrapper"));
                expiration = Some(format!("Timed out after {timeout_secs} seconds"));
                if let Some(scope) = &scope {
                    scope.terminate();
                }
                if let Some(pid) = pid {
                    terminate_process_tree(pid);
                }
                None
            }
            _ = wait_for_stall(progress.clone(), stall_timeout_secs), if stall_timeout_secs.is_some() => {
                let seconds = stall_timeout_secs.unwrap_or_default();
                termination = Some(("CommandStalled", "progress_stall", "progress_watchdog"));
                expiration = Some(format!("No command progress for {seconds} seconds"));
                if let Some(scope) = &scope {
                    scope.terminate();
                }
                if let Some(pid) = pid {
                    terminate_process_tree(pid);
                }
                None
            }
            _ = ctx.cancellation.cancelled() => {
                expiration = Some("tool task aborted".to_string());
                if let Some(scope) = &scope {
                    scope.terminate();
                }
                if let Some(pid) = pid {
                    terminate_process_tree(pid);
                }
                None
            }
        }
    };
    let mut process_reaped = status.is_some();
    if status.is_none() {
        process_reaped = tokio::time::timeout(
            Duration::from_secs(PROCESS_TERMINATION_GRACE_SECS),
            &mut wait_task,
        )
        .await
        .ok()
        .and_then(Result::ok)
        .is_some();
    }
    let process_group_empty = scope.as_ref().is_none_or(|scope| !scope.has_live_members());
    if let Some(scope) = scope.take()
        && scope.has_live_members()
    {
        retain_shell_process_scope(scope, ctx.lock_scope());
    }
    let (stdout, stderr) =
        drain_stream_tasks(stdout_task, stdout_capture, stderr_task, stderr_capture).await;

    let wall = started.elapsed().as_secs_f32();
    let was_cancelled = expiration.is_some() && termination.is_none();
    let mut response = match status {
        Some(status) => {
            let exit_code = status.code().unwrap_or(1);
            let mut text =
                format!("Exit code: {exit_code}\nWall time: {wall:.1} seconds\nOutput:\n{stdout}");
            if !stderr.is_empty() {
                text.push_str("\nStderr:\n");
                text.push_str(&stderr);
            }
            CommandResponse {
                success: status.success(),
                exit_code,
                stdout,
                stderr,
                output: Value::String(text),
                changes: Vec::new(),
            }
        }
        None => {
            let message = expiration.unwrap_or_else(|| "tool task aborted".to_string());
            let stderr = if stderr.is_empty() {
                message.clone()
            } else {
                format!("{stderr}\n{message}")
            };
            let mut text = format!("{message}\nWall time: {wall:.1} seconds\nOutput:\n{stdout}");
            if !stderr.is_empty() {
                text.push_str("\nStderr:\n");
                text.push_str(&stderr);
            }
            CommandResponse {
                success: false,
                exit_code: -1,
                stdout,
                stderr,
                output: if let Some((error_type, failure_class, termination_origin)) = termination {
                    terminated_unknown_outcome(error_type, failure_class, termination_origin, text)
                } else {
                    Value::String(text)
                },
                changes: Vec::new(),
            }
        }
    };
    let (terminal_state, failure_class, termination_origin, outcome) =
        if let Some((_, failure_class, termination_origin)) = termination {
            ("terminated", failure_class, termination_origin, "unknown")
        } else if was_cancelled {
            (
                "cancelled",
                "wrapper_cancellation",
                "runtime_cancellation",
                "unknown",
            )
        } else if response.exit_code == 0 {
            ("completed", "none", "workload", "known")
        } else {
            ("failed", "workload_exit_nonzero", "workload", "known")
        };
    if let Err(error) = attach_durable_terminal_receipt(
        &mut response,
        ctx,
        pid,
        started,
        timeout_secs,
        stall_timeout_secs,
        terminal_state,
        failure_class,
        termination_origin,
        outcome,
        process_reaped,
        process_group_empty,
    ) {
        response.success = false;
        response.exit_code = -1;
        response.stderr = if response.stderr.is_empty() {
            error.clone()
        } else {
            format!("{}\n{}", response.stderr, error)
        };
        response.output = json!({
            "error_type": "CommandTerminalReceiptWriteFailed",
            "failure_class": "control_plane_receipt_failure",
            "termination_origin": "command_run_wrapper",
            "message": error,
            "outcome": "unknown",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": true
        });
    }
    response
}

#[allow(clippy::too_many_arguments)]
fn attach_durable_terminal_receipt(
    response: &mut CommandResponse,
    ctx: &ToolContext,
    pid: Option<u32>,
    started: Instant,
    timeout_secs: u64,
    stall_timeout_secs: Option<u64>,
    terminal_state: &str,
    failure_class: &str,
    termination_origin: &str,
    outcome: &str,
    process_reaped: bool,
    process_group_empty: bool,
) -> Result<(), String> {
    let call_id = ctx.current_call_id().unwrap_or("command_run");
    let receipt = json!({
        "schema_version": "tura_command_terminal_receipt_v1",
        "call_id": call_id,
        "pid": pid,
        "terminal_state": terminal_state,
        "failure_class": failure_class,
        "termination_origin": termination_origin,
        "exit_code": response.exit_code,
        "wall_time_ms": started.elapsed().as_millis() as u64,
        "wall_timeout_ms": timeout_secs.saturating_mul(1000),
        "stall_timeout_ms": stall_timeout_secs.map(|seconds| seconds.saturating_mul(1000)),
        "outcome": outcome,
        "process_reaped": process_reaped,
        "process_group_empty": process_group_empty,
        "termination_proven": process_reaped && process_group_empty,
        "authority_effect": "none",
        "authoritative_publication": "unproven",
        "staging_authority": "none",
        "retry_safe": false,
        "auto_retry_allowed": false,
        "reconcile_required": outcome == "unknown"
            || failure_class == "workload_exit_nonzero"
            || !(process_reaped && process_group_empty),
        "replay_semantics": "diagnosed_replay_only_after_no_authoritative_publication_or_idempotent_cas_proof"
    });
    let path = ctx
        .current_call_id()
        .map(|call_id| command_receipt_path(&ctx.session_dir, call_id));
    if let Some(path) = path.as_ref() {
        durable_write_receipt(path, &receipt)?;
    }
    let prior_output = std::mem::replace(&mut response.output, Value::Null);
    let mut output = match prior_output {
        Value::Object(object) => object,
        Value::String(message) => {
            let mut object = serde_json::Map::new();
            object.insert("message".to_string(), Value::String(message));
            object
        }
        _ => serde_json::Map::new(),
    };
    output.insert("terminal_receipt".to_string(), receipt);
    if let Some(path) = path {
        output.insert(
            "terminal_receipt_path".to_string(),
            Value::String(path.display().to_string()),
        );
    }
    response.output = Value::Object(output);
    mark_claim_terminal(
        ctx,
        terminal_state,
        outcome,
        process_reaped,
        process_group_empty,
    )?;
    Ok(())
}

pub(crate) fn run_in_process_command_with_terminal_receipt<F>(
    ctx: &ToolContext,
    termination_origin: &str,
    operation: F,
) -> CommandResponse
where
    F: FnOnce() -> CommandResponse,
{
    let started = Instant::now();
    if let Err(error) = reconcile_command_execution_claims(&ctx.session_dir, ctx.current_call_id())
    {
        return CommandResponse {
            success: false,
            exit_code: -1,
            stdout: String::new(),
            stderr: error.clone(),
            output: json!({
                "error_type": "CommandExecutionReconciliationRequired",
                "failure_class": "duplicate_or_unreconciled_execution",
                "termination_origin": "command_run_admission",
                "message": error,
                "outcome": "unknown",
                "retry_safe": false,
                "auto_retry_allowed": false,
                "reconcile_required": true
            }),
            changes: Vec::new(),
        };
    }
    if let Err(error) = claim_command_execution(ctx, 0, None) {
        return CommandResponse {
            success: false,
            exit_code: -1,
            stdout: String::new(),
            stderr: error.clone(),
            output: json!({
                "error_type": "CommandExecutionClaimFailed",
                "failure_class": "duplicate_or_unreconciled_execution",
                "termination_origin": "command_run_admission",
                "message": error,
                "outcome": "unknown",
                "retry_safe": false,
                "auto_retry_allowed": false,
                "reconcile_required": true
            }),
            changes: Vec::new(),
        };
    }

    let mut response = operation();
    let (terminal_state, failure_class) = if response.exit_code == 0 {
        ("completed", "none")
    } else {
        ("failed", "workload_exit_nonzero")
    };
    if let Err(error) = attach_durable_terminal_receipt(
        &mut response,
        ctx,
        None,
        started,
        0,
        None,
        terminal_state,
        failure_class,
        termination_origin,
        "known",
        true,
        true,
    ) {
        response.success = false;
        response.exit_code = -1;
        response.stderr = if response.stderr.is_empty() {
            error.clone()
        } else {
            format!("{}\n{}", response.stderr, error)
        };
        response.output = json!({
            "error_type": "CommandTerminalReceiptWriteFailed",
            "failure_class": "control_plane_receipt_failure",
            "termination_origin": termination_origin,
            "message": error,
            "outcome": "unknown",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": true
        });
    }
    response
}

pub(crate) fn terminalize_pre_execution_zero_effect(
    ctx: &ToolContext,
    mut response: CommandResponse,
    timeout_secs: u64,
    stall_timeout_secs: Option<u64>,
) -> CommandResponse {
    let started = Instant::now();
    if let Err(error) = reconcile_command_execution_claims(&ctx.session_dir, ctx.current_call_id())
    {
        response.output = json!({
            "error_type": "CommandExecutionReconciliationRequired",
            "failure_class": "duplicate_or_unreconciled_execution",
            "termination_origin": "command_run_admission",
            "message": error,
            "outcome": "unknown",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": true
        });
        return response;
    }
    if let Err(error) = claim_command_execution(ctx, timeout_secs, stall_timeout_secs) {
        response.output = json!({
            "error_type": "CommandExecutionClaimFailed",
            "failure_class": "duplicate_or_unreconciled_execution",
            "termination_origin": "command_run_admission",
            "message": error,
            "outcome": "unknown",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": true
        });
        return response;
    }
    if let Err(error) = attach_durable_terminal_receipt(
        &mut response,
        ctx,
        None,
        started,
        timeout_secs,
        stall_timeout_secs,
        "not_started",
        "pre_execution_zero_effect",
        "command_run_pre_execution",
        "known",
        true,
        true,
    ) {
        response.output = json!({
            "error_type": "CommandTerminalReceiptWriteFailed",
            "failure_class": "control_plane_receipt_failure",
            "termination_origin": "command_run_pre_execution",
            "message": error,
            "outcome": "unknown",
            "retry_safe": false,
            "auto_retry_allowed": false,
            "reconcile_required": true
        });
    }
    response
}

fn command_receipt_path(session_dir: &Path, call_id: &str) -> PathBuf {
    session_dir
        .join(".tura/run/command_receipts")
        .join(format!("{}.json", safe_call_id(call_id)))
}

fn command_claim_path(session_dir: &Path, call_id: &str) -> PathBuf {
    session_dir
        .join(".tura/run/command_receipts")
        .join(format!("{}.claim.json", safe_call_id(call_id)))
}

fn safe_call_id(call_id: &str) -> String {
    let mut safe = String::new();
    for character in call_id.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
            safe.push(character);
        } else {
            safe.push_str(&format!("_x{:x}_", character as u32));
        }
    }
    if safe.is_empty() {
        "command_run".to_string()
    } else {
        safe
    }
}

fn claim_command_execution(
    ctx: &ToolContext,
    timeout_secs: u64,
    stall_timeout_secs: Option<u64>,
) -> Result<(), String> {
    let Some(call_id) = ctx.current_call_id() else {
        return Ok(());
    };
    let path = command_claim_path(&ctx.session_dir, call_id);
    let parent = path
        .parent()
        .ok_or_else(|| "COMMAND_EXECUTION_CLAIM_PARENT_MISSING".to_string())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let started_at_unix_ms = unix_time_ms();
    let claim = serde_json::to_vec_pretty(&json!({
        "schema_version": "tura_command_execution_claim_v1",
        "call_id": call_id,
        "state": "claimed",
        "pid": Value::Null,
        "owner_pid": std::process::id(),
        "started_at_unix_ms": started_at_unix_ms,
        "deadline_unix_ms": started_at_unix_ms
            .saturating_add(timeout_secs.saturating_mul(1000)),
        "wall_timeout_ms": timeout_secs.saturating_mul(1000),
        "stall_timeout_ms": stall_timeout_secs.map(|seconds| seconds.saturating_mul(1000)),
        "authority_effect": "none",
        "execution_count": 1,
        "replay_allowed": false
    }))
    .map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                format!("COMMAND_EXECUTION_ALREADY_CLAIMED:{}", path.display())
            } else {
                error.to_string()
            }
        })?;
    file.write_all(&claim).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    sync_receipt_directory(parent)
}

fn update_command_claim_running(
    ctx: &ToolContext,
    pid: Option<u32>,
    timeout_secs: u64,
    stall_timeout_secs: Option<u64>,
) -> Result<(), String> {
    let Some(call_id) = ctx.current_call_id() else {
        return Ok(());
    };
    let path = command_claim_path(&ctx.session_dir, call_id);
    let existing = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let mut claim: Value = serde_json::from_str(&existing).map_err(|error| error.to_string())?;
    let object = claim
        .as_object_mut()
        .ok_or_else(|| "COMMAND_EXECUTION_CLAIM_INVALID".to_string())?;
    object.insert("state".to_string(), Value::String("running".to_string()));
    object.insert("pid".to_string(), pid.map_or(Value::Null, |pid| json!(pid)));
    object.insert("started_at_unix_ms".to_string(), json!(unix_time_ms()));
    object.insert(
        "deadline_unix_ms".to_string(),
        json!(unix_time_ms().saturating_add(timeout_secs.saturating_mul(1000))),
    );
    object.insert(
        "wall_timeout_ms".to_string(),
        json!(timeout_secs.saturating_mul(1000)),
    );
    object.insert(
        "stall_timeout_ms".to_string(),
        stall_timeout_secs.map_or(Value::Null, |seconds| json!(seconds.saturating_mul(1000))),
    );
    durable_replace_json(&path, &claim)
}

fn mark_claim_terminal(
    ctx: &ToolContext,
    terminal_state: &str,
    outcome: &str,
    process_reaped: bool,
    process_group_empty: bool,
) -> Result<(), String> {
    let Some(call_id) = ctx.current_call_id() else {
        return Ok(());
    };
    let path = command_claim_path(&ctx.session_dir, call_id);
    let existing = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let mut claim: Value = serde_json::from_str(&existing).map_err(|error| error.to_string())?;
    let object = claim
        .as_object_mut()
        .ok_or_else(|| "COMMAND_EXECUTION_CLAIM_INVALID".to_string())?;
    object.insert(
        "state".to_string(),
        Value::String(terminal_state.to_string()),
    );
    object.insert("terminal_at_unix_ms".to_string(), json!(unix_time_ms()));
    object.insert("process_reaped".to_string(), json!(process_reaped));
    object.insert(
        "process_group_empty".to_string(),
        json!(process_group_empty),
    );
    object.insert(
        "reconcile_required".to_string(),
        json!(outcome == "unknown" || !(process_reaped && process_group_empty)),
    );
    durable_replace_json(&path, &claim)
}

fn command_run_batch_path(session_dir: &Path, execution_id: &str) -> PathBuf {
    session_dir
        .join(".tura/run/command_receipts")
        .join(format!("{}.batch-admission", safe_call_id(execution_id)))
}

pub fn begin_command_run_batch(
    session_dir: &Path,
    execution_id: &str,
    call_ids: &[String],
) -> Result<(), String> {
    if call_ids.is_empty() || call_ids.iter().collect::<BTreeSet<_>>().len() != call_ids.len() {
        return Err("COMMAND_RUN_BATCH_IDENTITY_INVALID".to_string());
    }
    let path = command_run_batch_path(session_dir, execution_id);
    let admission = json!({
        "schema_version": "tura_command_run_batch_admission_v1",
        "execution_id": execution_id,
        "call_ids": call_ids,
        "accepted_call_ids": [],
        "state": "admitted",
        "accepted_claim_count": 0,
        "zero_effect_proven": false
    });
    if path.exists() {
        let existing: Value = serde_json::from_slice(
            &fs::read(&path)
                .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_READ_FAILED:{error}"))?,
        )
        .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_INVALID:{error}"))?;
        if existing.get("schema_version").and_then(Value::as_str)
            == Some("tura_command_run_batch_admission_v1")
            && existing.get("execution_id").and_then(Value::as_str) == Some(execution_id)
            && existing.get("call_ids") == Some(&json!(call_ids))
        {
            return Ok(());
        }
        return Err("COMMAND_RUN_BATCH_MARKER_CONFLICT".to_string());
    }
    durable_write_receipt(&path, &admission)
}

fn command_run_batch_marker_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub fn mark_command_run_batch_call_accepted(
    session_dir: &Path,
    execution_id: &str,
    call_id: &str,
) -> Result<(), String> {
    let path = command_run_batch_path(session_dir, execution_id);
    if !path.exists() {
        return Ok(());
    }
    let _guard = command_run_batch_marker_lock()
        .lock()
        .map_err(|_| "COMMAND_RUN_BATCH_MARKER_LOCK_POISONED".to_string())?;
    let mut marker: Value = serde_json::from_slice(
        &fs::read(&path)
            .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_READ_FAILED:{error}"))?,
    )
    .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_INVALID:{error}"))?;
    let call_ids = marker
        .get("call_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "COMMAND_RUN_BATCH_MARKER_INVALID".to_string())?;
    if marker.get("schema_version").and_then(Value::as_str)
        != Some("tura_command_run_batch_admission_v1")
        || marker.get("execution_id").and_then(Value::as_str) != Some(execution_id)
        || !call_ids.iter().any(|value| value.as_str() == Some(call_id))
    {
        return Err("COMMAND_RUN_BATCH_MARKER_CONFLICT".to_string());
    }
    let state = marker
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| "COMMAND_RUN_BATCH_MARKER_INVALID".to_string())?
        .to_string();
    let accepted = marker
        .get_mut("accepted_call_ids")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| "COMMAND_RUN_BATCH_MARKER_INVALID".to_string())?;
    if accepted.iter().any(|value| value.as_str() == Some(call_id)) {
        return Ok(());
    }
    if state != "admitted" {
        return Err("COMMAND_RUN_BATCH_MARKER_CONFLICT".to_string());
    }
    accepted.push(Value::String(call_id.to_string()));
    durable_replace_json(&path, &marker)
}

pub fn complete_command_run_batch(
    session_dir: &Path,
    execution_id: &str,
    call_ids: &[String],
) -> Result<(), String> {
    update_command_run_batch(session_dir, execution_id, call_ids, "finished", None)
}

fn update_command_run_batch(
    session_dir: &Path,
    execution_id: &str,
    call_ids: &[String],
    state: &str,
    accepted_claim_count: Option<usize>,
) -> Result<(), String> {
    let path = command_run_batch_path(session_dir, execution_id);
    let mut marker: Value = serde_json::from_slice(
        &fs::read(&path)
            .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_READ_FAILED:{error}"))?,
    )
    .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_INVALID:{error}"))?;
    let current_state = marker.get("state").and_then(Value::as_str);
    if marker.get("schema_version").and_then(Value::as_str)
        != Some("tura_command_run_batch_admission_v1")
        || marker.get("execution_id").and_then(Value::as_str) != Some(execution_id)
        || marker.get("call_ids") != Some(&json!(call_ids))
    {
        return Err("COMMAND_RUN_BATCH_MARKER_CONFLICT".to_string());
    }
    if current_state == Some(state) || (state == "finished" && current_state != Some("admitted")) {
        return Ok(());
    }
    if current_state != Some("admitted") {
        return Err("COMMAND_RUN_BATCH_MARKER_CONFLICT".to_string());
    }
    let object = marker
        .as_object_mut()
        .ok_or_else(|| "COMMAND_RUN_BATCH_MARKER_INVALID".to_string())?;
    object.insert("state".to_string(), Value::String(state.to_string()));
    if let Some(count) = accepted_claim_count {
        object.insert("accepted_claim_count".to_string(), json!(count));
        object.insert("zero_effect_proven".to_string(), json!(count == 0));
    }
    object.insert("terminal_at_unix_ms".to_string(), json!(unix_time_ms()));
    durable_replace_json(&path, &marker)
}

fn terminal_receipt_proves_process_terminal(receipt: &Value, call_id: &str) -> Result<(), String> {
    let terminal_state = receipt.get("terminal_state").and_then(Value::as_str);
    if receipt.get("schema_version").and_then(Value::as_str)
        != Some("tura_command_terminal_receipt_v1")
        || receipt.get("call_id").and_then(Value::as_str) != Some(call_id)
        || !matches!(
            terminal_state,
            Some(
                "completed"
                    | "failed"
                    | "terminated"
                    | "cancelled"
                    | "not_started"
                    | "spawn_failed"
                    | "claim_update_failed"
                    | "interrupted"
            )
        )
        || receipt.get("termination_proven").and_then(Value::as_bool) != Some(true)
        || receipt.get("process_reaped").and_then(Value::as_bool) != Some(true)
        || receipt.get("process_group_empty").and_then(Value::as_bool) != Some(true)
    {
        return Err(format!(
            "COMMAND_TERMINAL_RECEIPT_PROOF_INCOMPLETE:{call_id}"
        ));
    }
    Ok(())
}

fn claim_state_is_terminal(state: &str) -> bool {
    matches!(
        state,
        "completed"
            | "failed"
            | "terminated"
            | "cancelled"
            | "not_started"
            | "spawn_failed"
            | "claim_update_failed"
            | "interrupted"
    )
}

pub async fn terminalize_interrupted_command_run_claims(
    session_dir: &Path,
    execution_id: &str,
    call_ids: &[String],
) -> Result<usize, String> {
    let marker_path = command_run_batch_path(session_dir, execution_id);
    let marker: Value = serde_json::from_slice(
        &fs::read(&marker_path)
            .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_READ_FAILED:{error}"))?,
    )
    .map_err(|error| format!("COMMAND_RUN_BATCH_MARKER_INVALID:{error}"))?;
    if marker.get("schema_version").and_then(Value::as_str)
        != Some("tura_command_run_batch_admission_v1")
        || marker.get("execution_id").and_then(Value::as_str) != Some(execution_id)
        || marker.get("call_ids") != Some(&json!(call_ids))
        || marker.get("state").and_then(Value::as_str) != Some("admitted")
    {
        return Err("COMMAND_RUN_BATCH_MARKER_CONFLICT".to_string());
    }
    let accepted_call_ids = marker
        .get("accepted_call_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "COMMAND_RUN_BATCH_MARKER_INVALID".to_string())?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "COMMAND_RUN_BATCH_MARKER_INVALID".to_string())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut claims = Vec::new();
    for call_id in call_ids {
        let path = command_claim_path(session_dir, call_id);
        if !path.exists() {
            continue;
        }
        let claim: Value =
            serde_json::from_slice(&fs::read(&path).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        if claim.get("call_id").and_then(Value::as_str) != Some(call_id) {
            return Err(format!(
                "COMMAND_EXECUTION_CLAIM_ID_CONFLICT:{}",
                path.display()
            ));
        }
        claims.push((path, claim));
    }
    let claimed_call_ids = claims
        .iter()
        .filter_map(|(_, claim)| claim.get("call_id").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    if !claimed_call_ids.is_subset(&accepted_call_ids) {
        return Err("COMMAND_RUN_PANIC_CLEANUP_UNACCEPTED_CLAIM".to_string());
    }
    if !accepted_call_ids.is_subset(&claimed_call_ids) {
        return Err("COMMAND_RUN_PANIC_CLEANUP_ACCEPTED_WITHOUT_CLAIM".to_string());
    }

    let mut receipt_proof_error = None;
    let mut processes = Vec::new();
    for (_, claim) in &claims {
        let state = claim
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("claimed");
        if claim_state_is_terminal(state) {
            continue;
        }
        let call_id = claim
            .get("call_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let receipt_path = command_receipt_path(session_dir, call_id);
        let receipt_proves_terminal = if receipt_path.exists() {
            match fs::read(&receipt_path)
                .map_err(|error| error.to_string())
                .and_then(|raw| {
                    serde_json::from_slice::<Value>(&raw).map_err(|error| error.to_string())
                })
                .and_then(|receipt| terminal_receipt_proves_process_terminal(&receipt, call_id))
            {
                Ok(()) => true,
                Err(error) => {
                    receipt_proof_error.get_or_insert(error);
                    false
                }
            }
        } else {
            false
        };
        if !receipt_proves_terminal && let Some(pid) = claim.get("pid").and_then(Value::as_u64) {
            processes.push((call_id.to_string(), pid as u32));
        }
    }
    processes.sort_unstable();
    processes.dedup();
    for (_, pid) in &processes {
        terminate_process_tree(*pid);
    }
    let cleanup_started = Instant::now();
    loop {
        let mut live = Vec::new();
        for (call_id, pid) in &processes {
            if !panic_cleanup_process_scope_empty(call_id, *pid)? {
                live.push((call_id.clone(), *pid));
            }
        }
        if live.is_empty() {
            break;
        }
        if cleanup_started.elapsed() >= Duration::from_secs(PROCESS_TERMINATION_GRACE_SECS) {
            return Err(format!(
                "COMMAND_RUN_PANIC_CLEANUP_TIMEOUT:{}:{live:?}",
                execution_id
            ));
        }
        for (_, pid) in live {
            terminate_process_tree(pid);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if let Some(error) = receipt_proof_error {
        return Err(error);
    }

    for (path, claim) in &mut claims {
        let call_id = claim
            .get("call_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("COMMAND_EXECUTION_CLAIM_ID_MISSING:{}", path.display()))?
            .to_string();
        let receipt_path = command_receipt_path(session_dir, &call_id);
        let receipt = if receipt_path.exists() {
            let receipt = serde_json::from_slice::<Value>(
                &fs::read(&receipt_path).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            terminal_receipt_proves_process_terminal(&receipt, &call_id)?;
            let claim_state = claim
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("claimed");
            if claim_state_is_terminal(claim_state)
                && receipt.get("terminal_state").and_then(Value::as_str) != Some(claim_state)
            {
                return Err(format!("COMMAND_TERMINAL_RECEIPT_STATE_CONFLICT:{call_id}"));
            }
            receipt
        } else {
            let receipt = json!({
                "schema_version": "tura_command_terminal_receipt_v1",
                "call_id": call_id,
                "pid": claim.get("pid").cloned().unwrap_or(Value::Null),
                "terminal_state": "interrupted",
                "failure_class": "worker_panic",
                "termination_origin": "router_command_run_supervisor",
                "exit_code": -1,
                "wall_time_ms": unix_time_ms().saturating_sub(
                    claim.get("started_at_unix_ms").and_then(Value::as_u64).unwrap_or(0)
                ),
                "wall_timeout_ms": claim.get("wall_timeout_ms").cloned().unwrap_or(Value::Null),
                "stall_timeout_ms": claim.get("stall_timeout_ms").cloned().unwrap_or(Value::Null),
                "outcome": "unknown",
                "process_reaped": true,
                "process_group_empty": true,
                "termination_proven": true,
                "authority_effect": "none",
                "authoritative_publication": "unproven",
                "staging_authority": "none",
                "retry_safe": false,
                "auto_retry_allowed": false,
                "reconcile_required": true,
                "replay_semantics": "diagnosed_replay_only_after_no_authoritative_publication_or_idempotent_cas_proof"
            });
            durable_write_receipt(&receipt_path, &receipt)?;
            receipt
        };
        let object = claim
            .as_object_mut()
            .ok_or_else(|| format!("COMMAND_EXECUTION_CLAIM_INVALID:{}", path.display()))?;
        object.insert(
            "state".to_string(),
            receipt
                .get("terminal_state")
                .cloned()
                .unwrap_or_else(|| Value::String("interrupted".to_string())),
        );
        object.insert("terminal_at_unix_ms".to_string(), json!(unix_time_ms()));
        object.insert(
            "process_reaped".to_string(),
            receipt
                .get("process_reaped")
                .cloned()
                .unwrap_or(json!(true)),
        );
        object.insert(
            "process_group_empty".to_string(),
            receipt
                .get("process_group_empty")
                .cloned()
                .unwrap_or(json!(true)),
        );
        object.insert(
            "reconcile_required".to_string(),
            receipt
                .get("reconcile_required")
                .cloned()
                .unwrap_or(json!(true)),
        );
        durable_replace_json(path, &claim)?;
    }
    update_command_run_batch(
        session_dir,
        execution_id,
        call_ids,
        "panic_terminalized",
        Some(claims.len()),
    )?;
    Ok(claims.len())
}

fn reconcile_command_execution_claims(
    session_dir: &Path,
    current_call_id: Option<&str>,
) -> Result<(), String> {
    let directory = session_dir.join(".tura/run/command_receipts");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json")
            || !path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.ends_with(".claim.json"))
        {
            continue;
        }
        let raw = fs::read_to_string(&path).map_err(|error| error.to_string())?;
        let claim: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
        let Some(object) = claim.as_object() else {
            return Err(format!(
                "COMMAND_EXECUTION_CLAIM_INVALID:{}",
                path.display()
            ));
        };
        let call_id = object
            .get("call_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("COMMAND_EXECUTION_CLAIM_ID_MISSING:{}", path.display()))?;
        let state = object
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("claimed");
        if matches!(
            state,
            "completed"
                | "failed"
                | "terminated"
                | "cancelled"
                | "not_started"
                | "spawn_failed"
                | "claim_update_failed"
                | "interrupted"
        ) {
            continue;
        }
        if current_call_id == Some(call_id) {
            return Err(format!(
                "COMMAND_EXECUTION_ALREADY_CLAIMED:{}",
                path.display()
            ));
        }
        let owner_pid = object
            .get("owner_pid")
            .and_then(Value::as_u64)
            .map(|pid| pid as u32);
        let deadline_unix_ms = object
            .get("deadline_unix_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if owner_pid.is_some_and(process_is_alive) && unix_time_ms() < deadline_unix_ms {
            continue;
        }
        let pid = object
            .get("pid")
            .and_then(Value::as_u64)
            .map(|pid| pid as u32);
        if pid.is_some_and(process_is_alive) {
            continue;
        }
        let receipt_path = command_receipt_path(session_dir, call_id);
        if !receipt_path.exists() {
            durable_write_receipt(
                &receipt_path,
                &json!({
                    "schema_version": "tura_command_terminal_receipt_v1",
                    "call_id": call_id,
                    "pid": pid,
                    "terminal_state": "interrupted",
                    "failure_class": "orphaned_execution_claim",
                    "termination_origin": "router_recovery",
                    "outcome": "unknown",
                    "process_reaped": false,
                    "process_group_empty": false,
                    "termination_proven": false,
                    "authority_effect": "none",
                    "authoritative_publication": "unproven",
                    "staging_authority": "none",
                    "retry_safe": false,
                    "auto_retry_allowed": false,
                    "reconcile_required": true,
                    "replay_semantics": "diagnosed_replay_only_after_no_authoritative_publication_or_idempotent_cas_proof"
                }),
            )?;
        }
        let mut interrupted = claim;
        if let Some(object) = interrupted.as_object_mut() {
            object.insert(
                "state".to_string(),
                Value::String("interrupted".to_string()),
            );
            object.insert("terminal_at_unix_ms".to_string(), json!(unix_time_ms()));
            object.insert("reconcile_required".to_string(), Value::Bool(true));
        }
        durable_replace_json(&path, &interrupted)?;
    }
    Ok(())
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn durable_write_receipt(path: &Path, receipt: &Value) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(receipt).map_err(|error| error.to_string())?;
    if path.exists() {
        let existing = fs::read(path).map_err(|error| error.to_string())?;
        if existing == bytes {
            return Ok(());
        }
        return Err(format!(
            "COMMAND_TERMINAL_RECEIPT_CONFLICT:{}",
            path.display()
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| "COMMAND_TERMINAL_RECEIPT_PARENT_MISSING".to_string())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temp, path).map_err(|error| error.to_string())?;
    sync_receipt_directory(parent)?;
    Ok(())
}

fn durable_replace_json(path: &Path, value: &Value) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let parent = path
        .parent()
        .ok_or_else(|| "COMMAND_JSON_PARENT_MISSING".to_string())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let temp = path.with_extension(format!("tmp-{}-{}", std::process::id(), unix_time_ms()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temp, path).map_err(|error| error.to_string())?;
    sync_receipt_directory(parent)
}

#[cfg(unix)]
fn sync_receipt_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

#[cfg(not(unix))]
fn sync_receipt_directory(_path: &Path) -> Result<(), String> {
    Ok(())
}

fn terminated_unknown_outcome(
    error_type: &str,
    failure_class: &str,
    termination_origin: &str,
    message: String,
) -> Value {
    json!({
        "error_type": error_type,
        "failure_class": failure_class,
        "termination_origin": termination_origin,
        "message": message,
        "outcome": "unknown",
        "authoritative_publication": "unproven",
        "staging_authority": "none",
        "retry_safe": false,
        "auto_retry_allowed": false,
        "reconcile_required": true,
        "replay_semantics": "diagnosed_replay_only_after_no_authoritative_publication_or_idempotent_cas_proof",
        "guidance": "Read back authoritative publication and reconcile private staging before deciding whether to replay."
    })
}

async fn wait_for_stall(progress: ProgressClock, stall_timeout_secs: Option<u64>) {
    let Some(seconds) = stall_timeout_secs else {
        std::future::pending::<()>().await;
        return;
    };
    let limit = Duration::from_secs(seconds.max(1));
    loop {
        let elapsed = progress.elapsed();
        if elapsed >= limit {
            return;
        }
        tokio::time::sleep((limit - elapsed).min(Duration::from_millis(250))).await;
    }
}

async fn read_stream_with_deltas<R>(
    mut reader: R,
    ctx: ToolContext,
    call_id: String,
    stream: &'static str,
    output: SharedOutput,
    progress: ProgressClock,
) -> String
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = [0_u8; 8192];
    let mut emitted_deltas = 0_usize;
    let mut truncation_delta_emitted = false;
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                progress.mark();
                let (accepted, truncated) = output.push(&buffer[..n]);
                if emitted_deltas < MAX_EXEC_OUTPUT_DELTAS_PER_CALL && accepted > 0 {
                    emitted_deltas += 1;
                    ctx.record_event(crate::runtime::tool::ToolRuntimeEvent::OutputDelta {
                        call_id: call_id.clone(),
                        stream: stream.to_string(),
                        text: String::from_utf8_lossy(&buffer[..accepted]).to_string(),
                    });
                } else if truncated && !truncation_delta_emitted {
                    truncation_delta_emitted = true;
                    ctx.record_event(crate::runtime::tool::ToolRuntimeEvent::OutputDelta {
                        call_id: call_id.clone(),
                        stream: stream.to_string(),
                        text: format!(
                            "\n[{stream} output truncated after {EXEC_OUTPUT_MAX_BYTES} bytes]\n"
                        ),
                    });
                }
            }
            Err(_) => break,
        }
    }
    output.snapshot()
}

#[derive(Clone)]
struct ProgressClock {
    last_activity: Arc<Mutex<Instant>>,
}

impl ProgressClock {
    fn new() -> Self {
        Self {
            last_activity: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn mark(&self) {
        if let Ok(mut last_activity) = self.last_activity.lock() {
            *last_activity = Instant::now();
        }
    }

    fn elapsed(&self) -> Duration {
        self.last_activity
            .lock()
            .map(|last_activity| last_activity.elapsed())
            .unwrap_or_default()
    }
}

#[derive(Clone)]
struct SharedOutput {
    inner: Arc<Mutex<CappedOutput>>,
}

impl SharedOutput {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CappedOutput::new())),
        }
    }

    fn push(&self, chunk: &[u8]) -> (usize, bool) {
        let Ok(mut output) = self.inner.lock() else {
            return (0, false);
        };
        let accepted = output.push(chunk);
        (accepted, output.truncated())
    }

    fn snapshot(&self) -> String {
        let Ok(output) = self.inner.lock() else {
            return String::new();
        };
        output.to_text()
    }
}

struct CappedOutput {
    bytes: Vec<u8>,
    total_bytes: usize,
    truncated: bool,
}

impl CappedOutput {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            total_bytes: 0,
            truncated: false,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> usize {
        self.total_bytes = self.total_bytes.saturating_add(chunk.len());
        let remaining = EXEC_OUTPUT_MAX_BYTES.saturating_sub(self.bytes.len());
        let accepted = remaining.min(chunk.len());
        if accepted > 0 {
            self.bytes.extend_from_slice(&chunk[..accepted]);
        }
        if accepted < chunk.len() {
            self.truncated = true;
        }
        accepted
    }

    fn truncated(&self) -> bool {
        self.truncated
    }

    fn finish(self) -> String {
        self.to_text()
    }

    fn to_text(&self) -> String {
        let mut text = String::from_utf8_lossy(&self.bytes).to_string();
        if self.truncated {
            text.push_str(&format!(
                "\n[output truncated after {} bytes; {} bytes were read]\n",
                EXEC_OUTPUT_MAX_BYTES, self.total_bytes
            ));
        }
        text
    }
}

async fn drain_stream_tasks(
    mut stdout_task: Option<tokio::task::JoinHandle<String>>,
    stdout_capture: Option<SharedOutput>,
    mut stderr_task: Option<tokio::task::JoinHandle<String>>,
    stderr_capture: Option<SharedOutput>,
) -> (String, String) {
    async fn wait_task(task: &mut Option<tokio::task::JoinHandle<String>>) -> String {
        match task.as_mut() {
            Some(task) => task.await.unwrap_or_default(),
            None => String::new(),
        }
    }

    match tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(wait_task(&mut stdout_task), wait_task(&mut stderr_task))
    })
    .await
    {
        Ok(outputs) => outputs,
        Err(_) => {
            if let Some(task) = stdout_task {
                task.abort();
            }
            if let Some(task) = stderr_task {
                task.abort();
            }
            (
                stdout_capture.map_or_else(String::new, |capture| capture.snapshot()),
                stderr_capture.map_or_else(String::new, |capture| capture.snapshot()),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::response::failed_async_response;
    use super::{
        ProgressClock, SharedOutput, begin_command_run_batch, claim_command_execution,
        command_claim_path, command_receipt_path, command_run_batch_path, durable_replace_json,
        durable_write_receipt, mark_command_run_batch_call_accepted, read_stream_with_deltas,
        reconcile_command_execution_claims, run_command_with_timeout,
        run_tokio_command_with_timeout, tail_chars, terminalize_interrupted_command_run_claims,
        terminalize_pre_execution_zero_effect,
    };
    use crate::runtime::tool::{ToolContext, ToolRuntimeEvent};
    use serde_json::{Value, json};
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;
    use tokio::io::AsyncWriteExt;

    fn success_command() -> Command {
        if cfg!(windows) {
            let mut command = Command::new("powershell");
            command.args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Write-Output shell-ok",
            ]);
            command
        } else {
            let mut command = Command::new("sh");
            command.args(["-c", "printf shell-ok"]);
            command
        }
    }

    fn write_test_claim(workspace: &std::path::Path, call_id: &str, state: &str) {
        let path = command_claim_path(workspace, call_id);
        fs::create_dir_all(path.parent().expect("claim parent")).expect("claim directory");
        durable_replace_json(
            &path,
            &json!({
                "schema_version": "tura_command_execution_claim_v1",
                "call_id": call_id,
                "state": state,
                "pid": Value::Null,
                "execution_count": 1
            }),
        )
        .expect("test claim");
    }

    fn write_test_terminal_receipt(
        workspace: &std::path::Path,
        call_id: &str,
        terminal_state: &str,
    ) {
        durable_write_receipt(
            &command_receipt_path(workspace, call_id),
            &json!({
                "schema_version": "tura_command_terminal_receipt_v1",
                "call_id": call_id,
                "terminal_state": terminal_state,
                "termination_proven": true,
                "process_reaped": true,
                "process_group_empty": true
            }),
        )
        .expect("test receipt");
    }

    #[tokio::test]
    async fn panic_cleanup_uses_exact_ids_filters_terminal_claims_and_ignores_unrelated_corruption()
    {
        let workspace =
            std::env::temp_dir().join(format!("tura-exact-panic-cleanup-{}", std::process::id()));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("workspace");
        let execution_id = "exact-batch";
        let completed_id = execution_id.to_string();
        let running_id = format!("{execution_id}:status_update");
        let call_ids = vec![completed_id.clone(), running_id.clone()];
        begin_command_run_batch(&workspace, execution_id, &call_ids).expect("batch admission");
        mark_command_run_batch_call_accepted(&workspace, execution_id, &completed_id)
            .expect("completed acceptance");
        mark_command_run_batch_call_accepted(&workspace, execution_id, &running_id)
            .expect("running acceptance");
        write_test_claim(&workspace, &completed_id, "completed");
        write_test_terminal_receipt(&workspace, &completed_id, "completed");
        write_test_claim(&workspace, &running_id, "running");
        fs::write(
            workspace.join(".tura/run/command_receipts/unrelated.claim.json"),
            b"not-json",
        )
        .expect("unrelated corrupt claim");

        assert_eq!(
            terminalize_interrupted_command_run_claims(&workspace, execution_id, &call_ids,)
                .await
                .expect("exact batch cleanup"),
            2
        );

        let completed: Value = serde_json::from_slice(
            &fs::read(command_claim_path(&workspace, &completed_id)).expect("completed claim"),
        )
        .expect("completed claim JSON");
        let running: Value = serde_json::from_slice(
            &fs::read(command_claim_path(&workspace, &running_id)).expect("running claim"),
        )
        .expect("running claim JSON");
        assert_eq!(completed["state"], "completed");
        assert_eq!(running["state"], "interrupted");
        assert_eq!(running["process_reaped"], true);
        assert_eq!(running["process_group_empty"], true);
        let _ = fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn panic_cleanup_zero_claims_needs_and_terminalizes_durable_admission_marker() {
        let workspace = std::env::temp_dir().join(format!(
            "tura-zero-claim-panic-cleanup-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("workspace");
        let execution_id = "zero-claim-batch";
        let call_ids = vec![execution_id.to_string()];
        let missing =
            terminalize_interrupted_command_run_claims(&workspace, execution_id, &call_ids)
                .await
                .expect_err("missing admission marker cannot prove zero effect");
        assert!(missing.contains("COMMAND_RUN_BATCH_MARKER_READ_FAILED"));

        begin_command_run_batch(&workspace, execution_id, &call_ids).expect("batch admission");
        mark_command_run_batch_call_accepted(&workspace, execution_id, execution_id)
            .expect("accepted command");
        let accepted_without_claim =
            terminalize_interrupted_command_run_claims(&workspace, execution_id, &call_ids)
                .await
                .expect_err("accepted command without claim is ambiguous");
        assert!(
            accepted_without_claim.contains("COMMAND_RUN_PANIC_CLEANUP_ACCEPTED_WITHOUT_CLAIM")
        );

        let execution_id = "durable-zero-claim-batch";
        let call_ids = vec![execution_id.to_string()];
        begin_command_run_batch(&workspace, execution_id, &call_ids).expect("zero batch admission");
        assert_eq!(
            terminalize_interrupted_command_run_claims(&workspace, execution_id, &call_ids,)
                .await
                .expect("durable zero effect"),
            0
        );
        let marker: Value = serde_json::from_slice(
            &fs::read(command_run_batch_path(&workspace, execution_id)).expect("batch marker"),
        )
        .expect("batch marker JSON");
        assert_eq!(marker["state"], "panic_terminalized");
        assert_eq!(marker["accepted_claim_count"], 0);
        assert_eq!(marker["zero_effect_proven"], true);
        let _ = fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn panic_cleanup_rejects_incomplete_existing_terminal_receipt() {
        let workspace = std::env::temp_dir().join(format!(
            "tura-incomplete-receipt-panic-cleanup-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("workspace");
        let execution_id = "incomplete-receipt-batch";
        let call_ids = vec![execution_id.to_string()];
        begin_command_run_batch(&workspace, execution_id, &call_ids).expect("batch admission");
        mark_command_run_batch_call_accepted(&workspace, execution_id, execution_id)
            .expect("running acceptance");
        write_test_claim(&workspace, execution_id, "running");
        durable_write_receipt(
            &command_receipt_path(&workspace, execution_id),
            &json!({
                "schema_version": "tura_command_terminal_receipt_v1",
                "call_id": execution_id,
                "terminal_state": "interrupted",
                "termination_proven": false,
                "process_reaped": false,
                "process_group_empty": false
            }),
        )
        .expect("incomplete receipt");

        let error = terminalize_interrupted_command_run_claims(&workspace, execution_id, &call_ids)
            .await
            .expect_err("incomplete process proof must fail closed");
        assert!(error.contains("COMMAND_TERMINAL_RECEIPT_PROOF_INCOMPLETE"));
        let claim: Value = serde_json::from_slice(
            &fs::read(command_claim_path(&workspace, execution_id)).expect("claim"),
        )
        .expect("claim JSON");
        assert_eq!(claim["state"], "running");
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn live_claim_owner_prevents_parallel_startup_from_being_reconciled_as_orphan() {
        let workspace =
            std::env::temp_dir().join(format!("tura-live-claim-owner-{}", std::process::id()));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("workspace");
        let call_id = "runtime:tool-call:parallel-a";
        let context = ToolContext::new(workspace.clone()).with_call_id(call_id.to_string());

        claim_command_execution(&context, 30, None).expect("claim command");
        reconcile_command_execution_claims(&workspace, Some("runtime:tool-call:parallel-b"))
            .expect("live sibling claim must remain active");

        let claim: Value = serde_json::from_slice(
            &fs::read(command_claim_path(&workspace, call_id)).expect("read claim"),
        )
        .expect("claim JSON");
        assert_eq!(claim["state"], "claimed");
        assert_eq!(claim["owner_pid"], std::process::id());
        assert!(!command_receipt_path(&workspace, call_id).exists());
        let _ = fs::remove_dir_all(workspace);
    }

    fn failing_command() -> Command {
        if cfg!(windows) {
            let mut command = Command::new("powershell");
            command.args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Write-Error shell-bad; exit 7",
            ]);
            command
        } else {
            let mut command = Command::new("sh");
            command.args(["-c", "printf shell-bad >&2; exit 7"]);
            command
        }
    }

    fn slow_command() -> Command {
        if cfg!(windows) {
            let mut command = Command::new("powershell");
            command.args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 5",
            ]);
            command
        } else {
            let mut command = Command::new("sh");
            command.args(["-c", "sleep 5"]);
            command
        }
    }

    fn tokio_slow_command() -> tokio::process::Command {
        let command = slow_command();
        let mut tokio_command = tokio::process::Command::new(command.get_program());
        tokio_command.args(command.get_args());
        tokio_command
    }

    fn tokio_output_then_sleep_command() -> tokio::process::Command {
        if cfg!(windows) {
            let mut command = tokio::process::Command::new("powershell");
            command.args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Write-Output retained-output; Start-Sleep -Seconds 5",
            ]);
            command
        } else {
            let mut command = tokio::process::Command::new("sh");
            command.args(["-c", "printf retained-output; sleep 5"]);
            command
        }
    }

    fn tokio_over_fifteen_seconds_command() -> tokio::process::Command {
        if cfg!(windows) {
            let mut command = tokio::process::Command::new("powershell");
            command.args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 16; Write-Output long-command-ok",
            ]);
            command
        } else {
            let mut command = tokio::process::Command::new("sh");
            command.args(["-c", "sleep 16; printf long-command-ok"]);
            command
        }
    }

    #[test]
    fn tail_chars_preserves_unicode_boundaries() {
        assert_eq!(tail_chars("alpha", 10), "alpha");
        assert_eq!(tail_chars("aébc", 3), "ébc");
        assert_eq!(tail_chars("abcdef", 0), "");
    }

    #[test]
    fn run_command_with_timeout_captures_success_output_and_exit_code() {
        let response = run_command_with_timeout(success_command(), 10, None);

        assert!(response.success, "{}", response.stderr);
        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("shell-ok"), "{response:?}");
        assert!(
            response
                .output
                .as_str()
                .unwrap_or_default()
                .contains("Exit code: 0")
        );
    }

    #[test]
    fn run_command_with_timeout_captures_failure_stderr() {
        let response = run_command_with_timeout(failing_command(), 10, None);

        assert!(!response.success);
        assert_eq!(response.exit_code, 7);
        assert!(response.stderr.contains("shell-bad"), "{response:?}");
        assert!(
            response
                .output
                .as_str()
                .unwrap_or_default()
                .contains("Stderr:")
        );
    }

    #[test]
    fn run_command_with_timeout_reports_spawn_error() {
        let response =
            run_command_with_timeout(Command::new("__tura_missing_shell_binary__"), 1, None);

        assert!(!response.success);
        assert_eq!(response.exit_code, 1);
        assert!(!response.stderr.is_empty());
        assert_eq!(
            response.output.as_str().unwrap_or_default(),
            response.stderr.as_str()
        );
    }

    #[test]
    fn pre_execution_failure_writes_exact_known_zero_effect_receipt() {
        let workspace = std::env::temp_dir().join(format!(
            "tura-pre-execution-zero-effect-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("workspace");
        let call_id = "runtime:call:step:1:index:0";
        let context = ToolContext::new(workspace.clone()).with_call_id(call_id.to_string());
        let response = terminalize_pre_execution_zero_effect(
            &context,
            failed_async_response("denied before execution", 126),
            5,
            None,
        );

        assert!(!response.success);
        assert_eq!(response.output["terminal_receipt"]["call_id"], call_id);
        assert_eq!(
            response.output["terminal_receipt"]["terminal_state"],
            "not_started"
        );
        assert_eq!(
            response.output["terminal_receipt"]["failure_class"],
            "pre_execution_zero_effect"
        );
        assert_eq!(response.output["terminal_receipt"]["outcome"], "known");
        assert_eq!(
            response.output["terminal_receipt"]["reconcile_required"],
            false
        );
        assert!(command_receipt_path(&workspace, call_id).is_file());
        let claim: Value = serde_json::from_slice(
            &fs::read(command_claim_path(&workspace, call_id)).expect("command claim"),
        )
        .expect("command claim JSON");
        assert_eq!(claim["execution_count"], 1);
        assert_eq!(claim["state"], "not_started");

        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn run_command_with_timeout_kills_slow_command() {
        let response = run_command_with_timeout(slow_command(), 1, None);

        assert!(!response.success);
        assert_eq!(response.exit_code, -1);
        assert!(
            response.output["message"]
                .as_str()
                .unwrap_or_default()
                .contains("Timed out after 1 seconds")
        );
        assert_eq!(response.output["outcome"], "unknown");
    }

    #[tokio::test]
    async fn read_stream_with_deltas_returns_output_and_records_each_chunk() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let context = ToolContext::new(PathBuf::from("workspace"));
        let read_context = context.clone();
        let task = tokio::spawn(async move {
            read_stream_with_deltas(
                reader,
                read_context,
                "call-1".to_string(),
                "stdout",
                SharedOutput::new(),
                ProgressClock::new(),
            )
            .await
        });

        writer.write_all(b"alpha").await.expect("write first chunk");
        writer
            .write_all(b"bravo")
            .await
            .expect("write second chunk");
        drop(writer);

        assert_eq!(task.await.expect("reader task"), "alphabravo");
        let events = context.events();
        assert!(!events.is_empty());
        let mut combined = String::new();
        for event in events {
            let ToolRuntimeEvent::OutputDelta {
                call_id,
                stream,
                text,
            } = event
            else {
                panic!("unexpected event: {event:?}");
            };
            assert_eq!(call_id, "call-1");
            assert_eq!(stream, "stdout");
            combined.push_str(&text);
        }
        assert_eq!(combined, "alphabravo");
    }

    #[tokio::test]
    async fn run_tokio_command_with_timeout_honors_pre_cancelled_context() {
        let context = ToolContext::new(PathBuf::from("workspace"));
        context.cancellation.cancel();

        let response =
            run_tokio_command_with_timeout(tokio_slow_command(), 10, None, &context).await;

        assert!(!response.success);
        assert_eq!(response.exit_code, -1);
        assert_eq!(response.stderr, "tool task aborted");
        assert!(context.events().is_empty());
    }

    #[tokio::test]
    async fn run_tokio_command_with_timeout_retains_stdout_on_timeout() {
        let context = ToolContext::new(PathBuf::from("workspace"));

        let response =
            run_tokio_command_with_timeout(tokio_output_then_sleep_command(), 1, None, &context)
                .await;

        assert!(!response.success);
        assert_eq!(response.exit_code, -1);
        assert!(
            response.stdout.contains("retained-output"),
            "stdout should keep bytes read before timeout: {response:?}"
        );
        assert!(response.stderr.contains("Timed out after 1 seconds"));
        assert!(
            response.output["message"]
                .as_str()
                .unwrap_or_default()
                .contains("retained-output")
        );
        assert_eq!(response.output["retry_safe"], false);
    }

    #[tokio::test]
    async fn command_longer_than_legacy_fifteen_second_gate_completes_with_receipt() {
        let workspace =
            std::env::temp_dir().join(format!("tura-long-command-receipt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("workspace");
        let context = ToolContext::new(workspace.clone())
            .with_call_id("runtime-command-run-1:long-command".to_string());

        let response = run_tokio_command_with_timeout(
            tokio_over_fifteen_seconds_command(),
            20,
            None,
            &context,
        )
        .await;

        assert!(response.success, "{response:?}");
        assert!(response.stdout.contains("long-command-ok"));
        assert_eq!(
            response.output["terminal_receipt"]["terminal_state"],
            "completed"
        );
        assert_eq!(response.output["terminal_receipt"]["outcome"], "known");
        assert_eq!(
            response.output["terminal_receipt"]["termination_proven"],
            true
        );
        let _ = fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn wrapper_timeout_writes_distinct_terminal_receipt_with_process_proof() {
        let workspace =
            std::env::temp_dir().join(format!("tura-timeout-receipt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("workspace");
        let context = ToolContext::new(workspace.clone())
            .with_call_id("runtime-command-run-1:timeout/command".to_string());

        let response =
            run_tokio_command_with_timeout(tokio_output_then_sleep_command(), 1, None, &context)
                .await;

        assert!(!response.success);
        assert_eq!(
            response.output["terminal_receipt"]["failure_class"],
            "wrapper_wall_clock_timeout"
        );
        assert_eq!(
            response.output["terminal_receipt"]["termination_origin"],
            "command_run_wrapper"
        );
        assert_eq!(
            response.output["terminal_receipt"]["reconcile_required"],
            true
        );
        assert_eq!(
            response.output["terminal_receipt"]["termination_proven"],
            true
        );
        let claim_path = fs::read_dir(workspace.join(".tura/run/command_receipts"))
            .expect("command claim directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension().and_then(|value| value.to_str()) == Some("json")
                    && path
                        .file_name()
                        .and_then(|value| value.to_str())
                        .is_some_and(|name| name.ends_with(".claim.json"))
            })
            .expect("terminal claim");
        let claim: Value =
            serde_json::from_str(&fs::read_to_string(claim_path).expect("read terminal claim"))
                .expect("terminal claim JSON");
        assert_eq!(claim["state"], "terminated");
        assert_eq!(claim["reconcile_required"], true);
        let _ = fs::remove_dir_all(workspace);
    }
}
