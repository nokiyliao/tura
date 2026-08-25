use serde_json::json;
use session_log_contract::{
    client::{open_session_feed_subscription, SessionFeedSubscriptionCancellation},
    SessionFeedEntry, SessionFeedEvent,
};
use std::sync::{atomic::Ordering, Arc};

use crate::app::build_state;
use crate::ipc_handlers::{enqueue_turn_identity, handle_ipc_request};
use crate::process_info::current_process_start_time;
use crate::services::{
    recovery::recover_after_start, runtime_orphans::cleanup_orphan_runtime_workers,
};
use crate::shutdown::start_idle_shutdown_monitor;
use router_contract::{IpcRequest, IpcResponse, RouterEndpoint};

pub(crate) async fn serve_stdio() -> anyhow::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let state = build_state();
    let _ = recover_after_start(&state.session_db)?;
    let stdin = tokio::io::stdin();
    // Shared, locked writer: each request is handled on its own task and writes
    // its response (tagged with `request_id`) when ready, so a slow call (e.g. a
    // long-running `execution.enqueue_turn`) never head-of-line blocks a
    // concurrent `health_check`. The gateway client multiplexes responses back
    // to per-call mailboxes by `request_id`.
    let stdout = Arc::new(tokio::sync::Mutex::new(tokio::io::stdout()));
    let mut lines = BufReader::new(stdin).lines();
    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim().to_string();
        if trimmed.is_empty() {
            continue;
        }
        let state = state.clone();
        let stdout = Arc::clone(&stdout);
        tokio::spawn(async move {
            let response = match serde_json::from_str::<IpcRequest>(&trimmed) {
                Ok(request) => handle_ipc_request(&state, request).await,
                Err(error) => {
                    IpcResponse::error("invalid", format!("invalid ipc request: {error}"))
                }
            };
            if let Ok(encoded) = serde_json::to_string(&response) {
                let mut out = stdout.lock().await;
                let _ = out.write_all(format!("{encoded}\n").as_bytes()).await;
                let _ = out.flush().await;
            }
        });
    }
    Ok(())
}

/// File (under the instance's db dir) recording the running router daemon's
/// socket endpoint, so any front can probe-and-connect rather than spawn its own.
pub(crate) fn router_addr_path() -> std::path::PathBuf {
    session_log_contract::client::default_db_dir().join("router.addr")
}

fn publish_router_addr(addr: &std::net::SocketAddr) -> anyhow::Result<()> {
    let path = router_addr_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let pid = std::process::id();
    let record = RouterEndpoint {
        addr: addr.to_string(),
        version: tura_path::instance_version(),
        pid: Some(pid),
        process_start_time: current_process_start_time(pid),
    };
    let tmp = path.with_extension("addr.tmp");
    std::fs::write(&tmp, serde_json::to_string(&record)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub(crate) fn unpublish_router_addr() {
    let _ = std::fs::remove_file(router_addr_path());
}

pub(crate) async fn serve_socket() -> anyhow::Result<()> {
    use tokio::net::TcpListener;
    use tokio::time::{timeout, Duration};

    let _router_lock = RouterDaemonLock::acquire()?;
    let orphan_report = cleanup_orphan_runtime_workers();
    if !orphan_report.killed.is_empty() {
        eprintln!(
            "router startup cleanup: killed orphan runtime workers {:?}",
            orphan_report.killed
        );
    }
    let state = build_state();
    let _ = recover_after_start(&state.session_db)?;
    // The daemon owns the backend: bring up the single session_db owner now.
    let _ = state.session_db.start();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    publish_router_addr(&addr)?;
    // SAFETY: the caller ensures no concurrent foreign environment access races with this mutation.
    #[allow(
        unsafe_code,
        reason = "Rust 2024 process-environment mutation audited at the caller"
    )]
    unsafe {
        std::env::set_var("TURA_ROUTER_ADDR", addr.to_string())
    };
    eprintln!("router socket daemon listening on {addr}");
    start_idle_shutdown_monitor(state.clone());

    while !state.shutdown.load(Ordering::SeqCst) {
        let accepted = match timeout(Duration::from_millis(250), listener.accept()).await {
            Ok(accepted) => accepted?,
            Err(_) => continue,
        };
        let (stream, _) = accepted;
        let state = state.clone();
        tokio::spawn(async move {
            let _ = handle_socket_connection(state, stream).await;
        });
    }
    unpublish_router_addr();
    code_tools::shell_executor::terminate_retained_shell_process_scopes();
    Ok(())
}

async fn handle_socket_connection(
    state: crate::app::AppState,
    stream: tokio::net::TcpStream,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::Mutex as AsyncMutex;

    let connection_guard = ConnectionLifecycleGuard::new(state.lifecycle.clone());
    let (read, write) = stream.into_split();
    let write = Arc::new(AsyncMutex::new(write));
    let pending_tasks = Arc::new(AsyncMutex::new(Vec::<tokio::task::JoinHandle<()>>::new()));
    let mut lines = BufReader::new(read).lines();
    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim().to_string();
        if trimmed.is_empty() {
            continue;
        }
        let parsed = match serde_json::from_str::<IpcRequest>(&trimmed) {
            Ok(request) => request,
            Err(error) => {
                let response =
                    IpcResponse::error("invalid", format!("invalid ipc request: {error}"));
                if let Ok(encoded) = serde_json::to_string(&response) {
                    let mut writer = write.lock().await;
                    let _ = writer.write_all(format!("{encoded}\n").as_bytes()).await;
                    let _ = writer.flush().await;
                }
                continue;
            }
        };
        state.lifecycle.mark_activity();
        let active_runtime = enqueue_turn_identity(&parsed);
        let abort_on_disconnect = should_abort_request_on_connection_close(&parsed);
        let state_for_task = state.clone();
        let write_for_task = Arc::clone(&write);
        let feed_forwarder = if let Some((session_id, _)) = active_runtime.as_ref() {
            match start_session_round_forwarder(
                session_id.clone(),
                parsed.request_id.clone(),
                state.execution.clone(),
                Arc::clone(&write),
            )
            .await
            {
                Ok(forwarder) => Some(forwarder),
                Err(error) => {
                    eprintln!(
                        "router session round forwarding unavailable for {session_id}: {error:#}"
                    );
                    None
                }
            }
        } else {
            None
        };
        let handle = tokio::spawn(async move {
            let response = handle_ipc_request(&state_for_task, parsed).await;
            if let Some(forwarder) = feed_forwarder {
                forwarder.stop().await;
            }
            if let Ok(encoded) = serde_json::to_string(&response) {
                let mut writer = write_for_task.lock().await;
                let _ = writer.write_all(format!("{encoded}\n").as_bytes()).await;
                let _ = writer.flush().await;
            }
        });
        if abort_on_disconnect {
            pending_tasks.lock().await.push(handle);
        }
    }
    let tasks = pending_tasks.lock().await.drain(..).collect::<Vec<_>>();
    for task in tasks {
        task.abort();
    }
    drop(connection_guard);
    Ok(())
}

type SocketWriter = Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>;

#[derive(Default)]
struct TerminalCallbackGate {
    callbacks: std::collections::HashMap<String, serde_json::Value>,
    deliveries:
        std::collections::HashMap<String, crate::services::execution::TerminalDeliveryIdentity>,
    completed: std::collections::HashSet<String>,
}

impl TerminalCallbackGate {
    fn accept_callback(
        &mut self,
        runtime_id: String,
        callback: serde_json::Value,
    ) -> anyhow::Result<
        Option<(
            serde_json::Value,
            crate::services::execution::TerminalDeliveryIdentity,
        )>,
    > {
        if self.completed.contains(&runtime_id) {
            return Ok(None);
        }
        if let Some(delivery) = self.deliveries.remove(&runtime_id) {
            self.completed.insert(runtime_id);
            return Ok(Some((callback, delivery)));
        }
        if let Some(existing) = self.callbacks.get(&runtime_id) {
            if existing == &callback {
                return Ok(None);
            }
            return Err(anyhow::anyhow!(
                "TERMINAL_CALLBACK_IDENTITY_CONFLICT:{runtime_id}"
            ));
        }
        self.callbacks.insert(runtime_id, callback);
        Ok(None)
    }

    fn accept_delivery(
        &mut self,
        delivery: crate::services::execution::TerminalDeliveryIdentity,
    ) -> anyhow::Result<
        Option<(
            serde_json::Value,
            crate::services::execution::TerminalDeliveryIdentity,
        )>,
    > {
        let runtime_id = delivery.runtime_id.clone();
        if self.completed.contains(&runtime_id) {
            return Ok(None);
        }
        if let Some(callback) = self.callbacks.remove(&runtime_id) {
            self.completed.insert(runtime_id);
            return Ok(Some((callback, delivery)));
        }
        if let Some(existing) = self.deliveries.get(&runtime_id) {
            if existing == &delivery {
                return Ok(None);
            }
            return Err(anyhow::anyhow!(
                "TERMINAL_DELIVERY_IDENTITY_CONFLICT:{runtime_id}"
            ));
        }
        self.deliveries.insert(runtime_id, delivery);
        Ok(None)
    }
}

struct SessionRoundForwarder {
    cancellation: Option<SessionFeedSubscriptionCancellation>,
    reader: Option<tokio::task::JoinHandle<()>>,
    writer: Option<tokio::task::JoinHandle<()>>,
}

impl SessionRoundForwarder {
    async fn stop(mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            let _ = cancellation.cancel();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.await;
        }
        if let Some(writer) = self.writer.take() {
            let _ = writer.await;
        }
    }
}

impl Drop for SessionRoundForwarder {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            let _ = cancellation.cancel();
        }
    }
}

async fn start_session_round_forwarder(
    session_id: String,
    request_id: String,
    execution: crate::services::execution::ExecutionService,
    write: SocketWriter,
) -> anyhow::Result<SessionRoundForwarder> {
    let subscription = tokio::task::spawn_blocking(open_session_feed_subscription)
        .await
        .map_err(|error| anyhow::anyhow!("session feed subscriber task failed: {error}"))??;
    let cancellation = subscription.cancellation_handle()?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let writer_execution = execution.clone();
    let reader = tokio::task::spawn_blocking(move || {
        let mut subscription = subscription;
        let mut terminal_gate = TerminalCallbackGate::default();
        while let Ok(Some(entry)) = subscription.next_entry() {
            let terminal_callback = agent_message_is_terminal(&entry);
            if let Some(callback) = session_round_callback(entry.clone(), &session_id, &request_id)
                && let Some(runtime_id) = entry.runtime_id.as_ref()
            {
                if terminal_callback {
                    match terminal_gate.accept_callback(runtime_id.clone(), callback) {
                        Ok(Some((callback, delivery))) => {
                            if sender
                                .blocking_send((vec![callback], Some(delivery)))
                                .is_err()
                            {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            eprintln!("router terminal callback blocked: {error:#}");
                            return;
                        }
                    }
                } else if sender.blocking_send((vec![callback], None)).is_err() {
                    return;
                }
            }
            if entry.session_id == session_id {
                match execution.intake_terminal_feed_entry(&entry, &request_id) {
                    Ok(Some(delivery)) => match terminal_gate.accept_delivery(delivery) {
                        Ok(Some((callback, delivery))) => {
                            if sender
                                .blocking_send((vec![callback], Some(delivery)))
                                .is_err()
                            {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            eprintln!("router terminal delivery blocked: {error:#}");
                            return;
                        }
                    },
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!(
                            "router terminal receipt intake blocked for {session_id}: {error:#}"
                        );
                    }
                }
            }
        }
    });
    let writer = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;

        while let Some((callbacks, delivery)) = receiver.recv().await {
            let mut writer = write.lock().await;
            let mut batch_delivered = true;
            for callback in callbacks {
                let Ok(encoded) = serde_json::to_string(&callback) else {
                    batch_delivered = false;
                    break;
                };
                if writer
                    .write_all(format!("{encoded}\n").as_bytes())
                    .await
                    .is_err()
                {
                    batch_delivered = false;
                    break;
                }
            }
            if !batch_delivered || writer.flush().await.is_err() {
                break;
            }
            if let Some(delivery) = delivery
                && let Err(error) = writer_execution.acknowledge_terminal_delivery(&delivery)
            {
                eprintln!(
                    "router terminal callback acknowledgement blocked for {}: {error:#}",
                    delivery.commander_session_id
                );
            }
        }
    });
    Ok(SessionRoundForwarder {
        cancellation: Some(cancellation),
        reader: Some(reader),
        writer: Some(writer),
    })
}

fn session_round_callback(
    entry: SessionFeedEntry,
    session_id: &str,
    request_id: &str,
) -> Option<serde_json::Value> {
    if entry.session_id != session_id {
        return None;
    }
    let SessionFeedEvent::AgentMessage {
        message_id,
        reply_message,
        runtime_status,
        context_tokens,
        usage,
        created_at,
        updated_at,
        ..
    } = entry.event
    else {
        return None;
    };
    Some(json!({
        "request_id": request_id,
        "kind": "gateway.callback",
        "method": "session.agent_message",
        "payload": {
            "session_id": entry.session_id,
            "runtime_id": entry.runtime_id,
            "event_id": entry.event_id,
            "body": {
                "type": "item.completed",
                "item": {
                    "id": message_id,
                    "type": "agent_message",
                    "status": "completed",
                    "text": reply_message,
                    "runtime_status": runtime_status,
                    "context_tokens": context_tokens,
                    "usage": usage,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            }
        }
    }))
}

fn agent_message_is_terminal(entry: &SessionFeedEntry) -> bool {
    matches!(
        &entry.event,
        SessionFeedEvent::AgentMessage {
            runtime_status: Some(status),
            ..
        } if !status.live
    )
}

fn should_abort_request_on_connection_close(request: &IpcRequest) -> bool {
    !matches!(
        request.method.as_str(),
        "execution.command_run" | "execution.enqueue_turn"
    )
}

struct ConnectionLifecycleGuard {
    lifecycle: crate::front_lifecycle::FrontLifecycle,
}

impl ConnectionLifecycleGuard {
    fn new(lifecycle: crate::front_lifecycle::FrontLifecycle) -> Self {
        lifecycle.connection_opened();
        Self { lifecycle }
    }
}

impl Drop for ConnectionLifecycleGuard {
    fn drop(&mut self) {
        self.lifecycle.connection_closed();
    }
}

struct RouterDaemonLock {
    file: std::fs::File,
    path: std::path::PathBuf,
}

impl RouterDaemonLock {
    fn acquire() -> anyhow::Result<Self> {
        use fs2::FileExt;
        use std::io::{Seek, SeekFrom, Write};

        let dir = tura_path::locks_dir();
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("router-{}.lock", tura_path::build_kind()));
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        file.try_lock_exclusive().map_err(|error| {
            anyhow::anyhow!(
                "another router daemon already owns {}: {error}",
                path.display()
            )
        })?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        let pid = std::process::id();
        writeln!(file, "pid={pid}")?;
        writeln!(
            file,
            "process_start_time={}",
            current_process_start_time(pid).unwrap_or_default()
        )?;
        writeln!(file, "kind=router")?;
        writeln!(file, "build_kind={}", tura_path::build_kind())?;
        writeln!(file, "home={}", tura_path::instance_home().display())?;
        Ok(Self { file, path })
    }
}

impl Drop for RouterDaemonLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn durable_execution_requests_are_detached_from_runtime_socket_disconnect() {
        let request = IpcRequest {
            request_id: "command-run".to_string(),
            kind: "call".to_string(),
            method: "execution.command_run".to_string(),
            payload: json!({}),
            deadline_ms: None,
        };
        assert!(!should_abort_request_on_connection_close(&request));

        let request = IpcRequest {
            method: "execution.enqueue_turn".to_string(),
            ..request
        };
        assert!(!should_abort_request_on_connection_close(&request));

        let request = IpcRequest {
            method: "execution.get_status".to_string(),
            ..request
        };
        assert!(should_abort_request_on_connection_close(&request));
    }

    #[test]
    fn agent_message_feed_entry_becomes_round_callback() {
        let callback = session_round_callback(
            SessionFeedEntry {
                session_id: "session-1".to_string(),
                cursor: 7,
                runtime_id: Some("runtime-round-2".to_string()),
                event_id: "runtime-round-2:feed:3".to_string(),
                event: SessionFeedEvent::AgentMessage {
                    message_id: "runtime-round-2.message".to_string(),
                    part_id: "runtime-round-2.message".to_string(),
                    reply_message: "I found the failing boundary; next I will patch it."
                        .to_string(),
                    new_learning: String::new(),
                    runtime_status: None,
                    context_tokens: None,
                    usage: None,
                    created_at: 10,
                    updated_at: 20,
                },
            },
            "session-1",
            "request-1",
        )
        .expect("matching agent message should become a callback");

        assert_eq!(callback["request_id"], "request-1");
        assert_eq!(callback["kind"], "gateway.callback");
        assert_eq!(callback["method"], "session.agent_message");
        assert_eq!(callback["payload"]["session_id"], "session-1");
        assert_eq!(callback["payload"]["runtime_id"], "runtime-round-2");
        assert_eq!(
            callback["payload"]["body"]["item"]["text"],
            "I found the failing boundary; next I will patch it."
        );
        assert_eq!(callback["payload"]["body"]["item"]["type"], "agent_message");
    }

    #[test]
    fn only_terminal_agent_message_is_held_for_durable_delivery() {
        let base = SessionFeedEntry {
            session_id: "session-1".to_string(),
            cursor: 7,
            runtime_id: Some("runtime-1".to_string()),
            event_id: "runtime-1:feed:3".to_string(),
            event: SessionFeedEvent::AgentMessage {
                message_id: "runtime-1.message".to_string(),
                part_id: "runtime-1.message".to_string(),
                reply_message: "progress".to_string(),
                new_learning: String::new(),
                runtime_status: None,
                context_tokens: None,
                usage: None,
                created_at: 10,
                updated_at: 20,
            },
        };
        assert!(!agent_message_is_terminal(&base));

        let terminal = SessionFeedEntry {
            event: SessionFeedEvent::AgentMessage {
                message_id: "runtime-1.message".to_string(),
                part_id: "runtime-1.message".to_string(),
                reply_message: "done".to_string(),
                new_learning: String::new(),
                runtime_status: Some(lifecycle::RuntimeProjection::new(
                    "runtime-1".to_string(),
                    lifecycle::RuntimeState::Finished,
                )),
                context_tokens: None,
                usage: None,
                created_at: 10,
                updated_at: 21,
            },
            ..base
        };
        assert!(agent_message_is_terminal(&terminal));
    }

    #[test]
    fn round_callback_ignores_other_sessions_and_non_message_events() {
        let entry = SessionFeedEntry {
            session_id: "session-1".to_string(),
            cursor: 1,
            runtime_id: Some("runtime-1".to_string()),
            event_id: "event-1".to_string(),
            event: SessionFeedEvent::TodosUpdated {
                todos: Vec::new(),
                updated_at: 1,
            },
        };
        assert!(session_round_callback(entry.clone(), "session-1", "request-1").is_none());

        let agent_entry = SessionFeedEntry {
            event: SessionFeedEvent::AgentMessage {
                message_id: "message-1".to_string(),
                part_id: "part-1".to_string(),
                reply_message: "round text".to_string(),
                new_learning: String::new(),
                runtime_status: None,
                context_tokens: None,
                usage: None,
                created_at: 1,
                updated_at: 1,
            },
            ..entry
        };
        assert!(session_round_callback(agent_entry, "session-2", "request-1").is_none());
    }

    #[test]
    fn terminal_callback_gate_pairs_callback_before_delivery_once() {
        let mut gate = TerminalCallbackGate::default();
        let callback = json!({"text": "done"});
        assert!(gate
            .accept_callback("runtime-1".to_string(), callback.clone())
            .expect("queue callback")
            .is_none());
        let delivery = terminal_delivery("runtime-1");
        let (paired_callback, paired_delivery) = gate
            .accept_delivery(delivery.clone())
            .expect("pair delivery")
            .expect("ready pair");
        assert_eq!(paired_callback, callback);
        assert_eq!(paired_delivery, delivery);
        assert!(gate
            .accept_delivery(terminal_delivery("runtime-1"))
            .expect("ignore duplicate delivery")
            .is_none());
    }

    #[test]
    fn terminal_callback_gate_pairs_delivery_before_callback_once() {
        let mut gate = TerminalCallbackGate::default();
        let delivery = terminal_delivery("runtime-2");
        assert!(gate
            .accept_delivery(delivery.clone())
            .expect("queue delivery")
            .is_none());
        let callback = json!({"text": "done later"});
        let (paired_callback, paired_delivery) = gate
            .accept_callback("runtime-2".to_string(), callback.clone())
            .expect("pair callback")
            .expect("ready pair");
        assert_eq!(paired_callback, callback);
        assert_eq!(paired_delivery, delivery);
        assert!(gate
            .accept_callback("runtime-2".to_string(), callback)
            .expect("ignore duplicate callback")
            .is_none());
    }

    fn terminal_delivery(runtime_id: &str) -> crate::services::execution::TerminalDeliveryIdentity {
        crate::services::execution::TerminalDeliveryIdentity {
            commander_session_id: "commander-1".to_string(),
            transaction_id: "transaction-1".to_string(),
            event_id: format!("{runtime_id}:terminal"),
            runtime_id: runtime_id.to_string(),
        }
    }

    #[tokio::test]
    async fn command_run_survives_runtime_socket_disconnect_until_router_finishes(
    ) -> anyhow::Result<()> {
        let state = build_state();
        let workspace = tempfile::tempdir()?;
        let started = workspace.path().join("started.txt");
        let done = workspace.path().join("done.txt");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let task = tokio::spawn(async move {
                let _ = handle_socket_connection(server_state, stream).await;
            });
            Ok::<_, anyhow::Error>(task)
        });

        let mut client = tokio::net::TcpStream::connect(addr).await?;
        let request = IpcRequest {
            request_id: "disconnect-command-run".to_string(),
            kind: "call".to_string(),
            method: "execution.command_run".to_string(),
            payload: json!({
                "session_id": "disconnect-session",
                "runtime_id": "disconnect-runtime",
                "session_directory": workspace.path().display().to_string(),
                "arguments": {
                    "commands": [{
                        "command": "shell_command",
                        "command_line": json!({
                            "command": disconnect_survival_command(),
                            "timeout_ms": 5000
                        }).to_string()
                    }]
                }
            }),
            deadline_ms: None,
        };
        client
            .write_all(format!("{}\n", serde_json::to_string(&request)?).as_bytes())
            .await?;
        client.flush().await?;

        wait_for_path(&started, std::time::Duration::from_secs(2)).await?;
        drop(client);

        wait_for_path(&done, std::time::Duration::from_secs(4)).await?;
        let connection_task = server.await??;
        connection_task.await?;
        wait_for_active_command_runs(&state, 0, std::time::Duration::from_secs(2)).await?;
        Ok(())
    }

    async fn wait_for_path(
        path: &std::path::Path,
        timeout: std::time::Duration,
    ) -> anyhow::Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if path.exists() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        anyhow::bail!("timed out waiting for {}", path.display())
    }

    async fn wait_for_active_command_runs(
        state: &crate::app::AppState,
        expected: usize,
        timeout: std::time::Duration,
    ) -> anyhow::Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if state.command_run.active_count() == expected {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        anyhow::bail!(
            "timed out waiting for active command_run count {expected}; got {}",
            state.command_run.active_count()
        )
    }

    fn disconnect_survival_command() -> &'static str {
        if cfg!(windows) {
            "$ErrorActionPreference='Stop'; Set-Content -LiteralPath 'started.txt' -Value 'started'; Start-Sleep -Milliseconds 800; Set-Content -LiteralPath 'done.txt' -Value 'done'"
        } else {
            "set -eu; printf started > started.txt; sleep 0.8; printf done > done.txt"
        }
    }
}
