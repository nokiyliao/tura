import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { startBackendStressEnvironment } from "./full_chain_backend_fixture.mjs";

const marker = "P4-ACTIVE-CHILD-REPLAY-GATE";
const backend = await startBackendStressEnvironment({
  runIdPrefix: "child-callback-replay",
  providerGateMarker: marker,
  config: {
    workspaces: 1,
    tasksPerWorkspace: 1,
    turnsPerSession: 1,
    liveSessionTarget: 1,
    ensureBuilds: true,
    totalTimeoutMs: 180_000,
  },
});

try {
  const parent = backend.targetSession;
  const suffix = backend.runId.replace(/[^a-zA-Z0-9_.:-]/g, "-");
  const runtimeId = `runtime-child-${suffix}`;
  const transactionId = `transaction-child-${suffix}`;
  const delegatedPrompt = `${marker} complete delegated work`;
  const payload = {
    parent_session_id: parent.sessionId,
    parent_mission_revision_sha256: "a".repeat(64),
    child_session_id: `child-${suffix}`,
    child_runtime_id: runtimeId,
    child_transaction_id: transactionId,
    child_lease_id: `lease-child-${suffix}`,
    callback_request_id: transactionId,
    effect_id: `${runtimeId}.message`,
    delegated_input_sha256: crypto.createHash("sha256").update(JSON.stringify(delegatedPrompt)).digest("hex"),
    session_directory: parent.workspace,
    session_name: "P4 active child replay",
    created_at_ms: Date.now(),
    execution_payload: {
      prompt: delegatedPrompt,
      directory: parent.workspace,
      model: "openai/mock-coder",
      agent: "direct-text-only",
    },
  };
  const childPath = `/session/${encodeURIComponent(parent.sessionId)}/children`;
  const firstController = new AbortController();
  const firstRequest = fetch(`${backend.gateway.url}${childPath}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-opencode-directory": encodeURIComponent(parent.workspace),
    },
    body: JSON.stringify(payload),
    signal: firstController.signal,
  }).then(async (response) => ({ status: response.status, body: await response.text() }));
  await backend.waitForProviderGate();
  const providerBaseline = backend.providerRequests.length;
  firstController.abort();
  await firstRequest.catch((error) => {
    assert.equal(error.name, "AbortError");
  });

  const replayResponse = await Promise.race([
    backend.requestJson(
      backend.gateway.url,
      "POST",
      childPath,
      payload,
      parent.workspace,
      5_000,
    ),
    new Promise((_, reject) => setTimeout(() => reject(new Error("public child replay exceeded 5s")), 5_000)),
  ]);
  assert.equal(replayResponse.outcome, "already_admitted");
  assert.equal(backend.providerRequests.length, providerBaseline, "replay must not enqueue a provider turn");

  backend.releaseProviderGate();
  const lifecycleRoot = await waitForAck(backend.turaHome, parent.sessionId, 30_000);
  assert.equal(await jsonCount(path.join(lifecycleRoot, "callbacks", "pending")), 0);
  assert.equal(await jsonCount(path.join(lifecycleRoot, "callbacks", "acknowledged")), 1);
  assert.equal(await continuationStateCount(path.join(lifecycleRoot, "continuations"), "acknowledged"), 1);

  const childCalls = backend.providerRequests.filter((entry) => entry.promptText.includes(marker));
  assert.equal(childCalls.length, 1, "child effect must execute once");
  const runtime = await backend.callSessionDb({ command: "get_runtime_lease", runtime_id: runtimeId });
  assert.equal(runtime.kind, "runtime_lease_read");
  assert.equal(runtime.runtime.lease_active, false);
  assert.equal(runtime.runtime.terminal, true);
  console.log(JSON.stringify({
    status: "PASS_P4_ACTUAL_DAEMON_CHILD_REPLAY_FORWARDER_ACCEPTANCE",
    child_provider_calls: childCalls.length,
    replay_outcome: replayResponse.outcome,
    callback_count: 1,
    continuation_ack_count: 1,
    callback_ack_count: 1,
  }));
} finally {
  backend.releaseProviderGate();
  await backend.cleanup();
}

async function waitForAck(turaHome, commanderSessionId, timeoutMs) {
  const digest = crypto.createHash("sha256").update(commanderSessionId).digest("hex");
  const root = path.join(turaHome, "db", "session_log", "session_lifecycle_v1", digest);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await jsonCount(path.join(root, "callbacks", "acknowledged"))) === 1) return root;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`timed out waiting for callback ACK under ${root}`);
}

async function jsonCount(directory) {
  if (!fs.existsSync(directory)) return 0;
  return (await fsp.readdir(directory)).filter((name) => name.endsWith(".json")).length;
}

async function continuationStateCount(directory, state) {
  if (!fs.existsSync(directory)) return 0;
  const files = (await fsp.readdir(directory)).filter((name) => name.endsWith(".json"));
  const records = await Promise.all(
    files.map(async (name) => JSON.parse(await fsp.readFile(path.join(directory, name), "utf8"))),
  );
  return records.filter((record) => record.state === state).length;
}
