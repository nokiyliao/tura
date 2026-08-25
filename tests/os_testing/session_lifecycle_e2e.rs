use anyhow::{bail, Context, Result};
use session_lifecycle::{
    checkpoint_identity_from_path, AckOutcome, CheckpointEventKind, IntakeOutcome, LifecycleConfig,
    LiveEffectEvidence, NativeCheckpointEventAdapter, NativeEventRead, ReceiptWriteOutcome,
    ReclaimOutcome, SessionLifecycleStore, TerminalReceipt, TerminalReceiptIdentity, TerminalState,
};
use std::time::{Duration, Instant};

#[test]
fn session_lifecycle_native_event_receipt_replay_and_slot_release_e2e() -> Result<()> {
    let root = tempfile::tempdir().context("create isolated lifecycle root")?;
    let commander_id = "commander-e2e";
    let store = SessionLifecycleStore::open(
        root.path(),
        commander_id,
        LifecycleConfig {
            watchdog_interval_secs: 1_800,
            event_channel_capacity: 16,
        },
    )
    .context("open lifecycle store")?;
    let checkpoint_dir = root.path().join("checkpoints");
    let adapter = NativeCheckpointEventAdapter::watch(&checkpoint_dir, 16)
        .context("watch native checkpoint events")?;

    let checkpoint_path = store
        .publish_checkpoint_evidence(
            "child-e2e",
            "auto_compact_context",
            12,
            4,
            br#"{"delta":"native-e2e"}"#,
        )
        .context("publish checkpoint evidence")?;
    wait_for_native_checkpoint_event(&adapter)?;

    let first = checkpoint_identity_from_path(&checkpoint_path, CheckpointEventKind::CloseWrite)
        .context("read first checkpoint identity")?;
    let stable = checkpoint_identity_from_path(&checkpoint_path, CheckpointEventKind::CloseWrite)
        .context("read stable checkpoint identity")?;
    let admitted = store
        .admit_checkpoint(&first, &stable)
        .context("admit stable native checkpoint")?;
    assert_eq!(admitted.commander_session_id, commander_id);
    assert_eq!(admitted.source_path, checkpoint_path);

    let receipt = TerminalReceipt::new(
        TerminalReceiptIdentity::new(
            "transaction-e2e",
            "event-e2e",
            0,
            commander_id,
            "child-e2e",
            "runtime-e2e",
            "lease-e2e",
        ),
        TerminalState::Interrupted,
        1_786_867_200_000,
    );
    assert!(matches!(
        store
            .write_terminal_receipt(&receipt)
            .context("persist terminal receipt")?,
        ReceiptWriteOutcome::Written(_)
    ));
    drop(store);

    let restarted =
        SessionLifecycleStore::open(root.path(), commander_id, LifecycleConfig::default())
            .context("restart lifecycle store")?;
    let replay = restarted
        .replay_pending()
        .context("replay pending receipt")?;
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].2, IntakeOutcome::Applied { event_seq: 0 });

    let retained = restarted
        .reclaim_terminal_slot(
            "transaction-e2e",
            "event-e2e",
            LiveEffectEvidence {
                active_tool_calls: 1,
                ..Default::default()
            },
        )
        .context("retain slot while tool call is live")?;
    assert!(matches!(
        retained,
        ReclaimOutcome::Retained { blocker } if blocker.code == "LIVE_TOOL_CALL_REMAINS"
    ));
    assert_eq!(
        restarted
            .reclaim_terminal_slot(
                "transaction-e2e",
                "event-e2e",
                LiveEffectEvidence::default(),
            )
            .context("release quiescent terminal slot")?,
        ReclaimOutcome::Released
    );
    assert_eq!(
        restarted
            .reclaim_terminal_slot(
                "transaction-e2e",
                "event-e2e",
                LiveEffectEvidence::default(),
            )
            .context("repeat terminal slot release")?,
        ReclaimOutcome::AlreadyReleased
    );
    assert_eq!(
        restarted
            .acknowledge("transaction-e2e", "event-e2e", "delivery-e2e")
            .context("acknowledge terminal callback")?,
        AckOutcome::Acknowledged
    );

    let readback = restarted.readback().context("read lifecycle state")?;
    assert_eq!(readback.commander_session_id, commander_id);
    assert_eq!(readback.primary_trigger, "native_checkpoint_event");
    assert_eq!(readback.pending_receipts, 0);
    assert_eq!(readback.applied_receipts, 1);
    assert_eq!(readback.acknowledged_receipts, 1);
    assert_eq!(readback.released_slots, 1);
    assert_eq!(
        readback
            .last_blocker
            .as_ref()
            .and_then(|value| value["code"].as_str()),
        Some("LIVE_TOOL_CALL_REMAINS")
    );
    Ok(())
}

fn wait_for_native_checkpoint_event(adapter: &NativeCheckpointEventAdapter) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match adapter.receive(Duration::from_millis(200)) {
            NativeEventRead::Event(_) => return Ok(()),
            NativeEventRead::Ignored | NativeEventRead::ChannelFull => {}
            NativeEventRead::Disconnected => bail!("native checkpoint event adapter disconnected"),
        }
    }
    bail!("native checkpoint event was not observed before the bounded deadline")
}
