use super::helpers::*;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

fn admission_request() -> Value {
    json!({
        "parent_session_id": "parent-1",
        "parent_mission_revision_sha256": "a".repeat(64),
        "commander_thread_id": "commander-thread-1",
        "child_session_id": "child-1",
        "child_runtime_id": "runtime-1",
        "child_transaction_id": "callback-1",
        "child_lease_id": "lease-1",
        "callback_request_id": "callback-1",
        "effect_id": "runtime-1.message",
        "delegated_input_sha256": "b".repeat(64),
        "session_directory": "/tmp/child-1",
        "session_name": "delegated child",
        "created_at_ms": 1_788_000_000_000_i64,
        "execution_payload": {"prompt": "perform delegated work"}
    })
}

async fn post_admission(parent_id: &str, payload: &Value) -> Result<(StatusCode, Value)> {
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/session/{parent_id}/children"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(payload)?))?;
    let response = gateway::web::build_router().oneshot(request).await?;
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

#[tokio::test]
async fn public_gateway_child_route_forwards_exact_contract_and_replay() -> Result<()> {
    let _guard = ENV_LOCK.lock().await;
    let root = tempfile::tempdir().context("temp root")?;
    let home = root.path().join("home");
    std::fs::create_dir_all(&home)?;
    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let _env = EnvGuard::new(&home, &source_root);
    let response = json!({
        "outcome": "admitted",
        "parent_session_id": "parent-1",
        "child_session_id": "child-1",
        "child_runtime_id": "runtime-1",
        "child_transaction_id": "callback-1",
        "callback_request_id": "callback-1",
        "effect_id": "runtime-1.message"
    });
    let mut replay_response = response.clone();
    replay_response["outcome"] = Value::String("already_admitted".to_string());
    let router = FakeRouter::start(
        &home,
        vec![
            RouterReply::Payload(response.clone()),
            RouterReply::Payload(replay_response.clone()),
        ],
    )?;
    let payload = admission_request();

    let (status, first_body) = post_admission("parent-1", &payload).await?;
    assert_eq!(status, StatusCode::OK, "{first_body}");
    assert_eq!(first_body, response);
    let first = router.next_request(Duration::from_secs(10))?;
    assert_eq!(
        first["method"],
        router_contract::METHOD_REGISTER_CHILD_SESSION
    );
    assert_eq!(first["payload"], payload);

    let (status, second_body) = post_admission("parent-1", &payload).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second_body, replay_response);
    let second = router.next_request(Duration::from_secs(10))?;
    assert_eq!(second["method"], first["method"]);
    assert_eq!(second["payload"], first["payload"]);

    let (status, _) = post_admission("different-parent", &payload).await?;
    assert_eq!(status, StatusCode::CONFLICT);
    let mut missing_effect = payload;
    missing_effect["effect_id"] = Value::String(String::new());
    let (status, _) = post_admission("parent-1", &missing_effect).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let mut missing_commander_thread = admission_request();
    missing_commander_thread
        .as_object_mut()
        .expect("admission object")
        .remove("commander_thread_id");
    let (status, body) = post_admission("parent-1", &missing_commander_thread).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let mut blank_commander_thread = admission_request();
    blank_commander_thread["commander_thread_id"] = Value::String("   ".to_string());
    let (status, body) = post_admission("parent-1", &blank_commander_thread).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    drop(router);
    Ok(())
}
