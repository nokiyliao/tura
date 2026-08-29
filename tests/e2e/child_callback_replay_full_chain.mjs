import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import net from "node:net";
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
  const request = {
    request_id: transactionId,
    kind: "call",
    method: "execution.register_child_session",
    payload,
  };
  const routerEndpoint = JSON.parse(
    await fsp.readFile(path.join(backend.turaHome, "db", "session_log", "router.addr"), "utf8"),
  );

  const first = connectRouter(routerEndpoint.addr, request);
  const firstAdmission = await Promise.race([
    backend.waitForProviderGate().then(() => ({ gated: true })),
    first.next((value) => value.request_id === transactionId, 10_000).then((value) => ({ value })),
  ]);
  assert.equal(firstAdmission.gated, true, `child failed before provider gate: ${JSON.stringify(firstAdmission.value)}`);
  const providerBaseline = backend.providerRequests.length;
  first.socket.destroy();

  const replay = connectRouter(routerEndpoint.addr, request);
  const replayResponse = await replay.next(
    (value) => value.request_id === transactionId && value.ok === true && value.payload?.outcome,
    5_000,
  );
  assert.equal(replayResponse.payload.outcome, "already_admitted");
  assert.equal(backend.providerRequests.length, providerBaseline, "replay must not enqueue a provider turn");

  backend.releaseProviderGate();
  const callback = await replay.next(
    (value) => value.kind === "gateway.callback" && value.payload?.runtime_id === runtimeId,
    30_000,
  );
  assert.equal(callback.method, "session.agent_message");
  assert.equal(callback.payload.session_id, payload.child_session_id);
  const lifecycleRoot = await waitForAck(backend.turaHome, parent.sessionId, 30_000);
  assert.equal(replay.values.filter((value) => value.kind === "gateway.callback").length, 1);
  assert.equal(await jsonCount(path.join(lifecycleRoot, "callbacks", "pending")), 0);
  assert.equal(await jsonCount(path.join(lifecycleRoot, "callbacks", "acknowledged")), 1);
  assert.equal(await continuationStateCount(path.join(lifecycleRoot, "continuations"), "acknowledged"), 1);

  const childCalls = backend.providerRequests.filter((entry) => entry.promptText.includes(marker));
  assert.equal(childCalls.length, 1, "child effect must execute once");
  const runtime = await backend.callSessionDb({ command: "get_runtime_lease", runtime_id: runtimeId });
  assert.equal(runtime.kind, "runtime_lease_read");
  assert.equal(runtime.runtime.lease_active, false);
  assert.equal(runtime.runtime.terminal, true);
  replay.socket.end();
  console.log(JSON.stringify({
    status: "PASS_P4_ACTUAL_DAEMON_CHILD_REPLAY_FORWARDER_ACCEPTANCE",
    child_provider_calls: childCalls.length,
    replay_outcome: replayResponse.payload.outcome,
    callback_count: 1,
    continuation_ack_count: 1,
    callback_ack_count: 1,
  }));
} finally {
  backend.releaseProviderGate();
  await backend.cleanup();
}

function connectRouter(addr, request) {
  const [host, portText] = addr.split(":");
  const socket = net.createConnection({ host, port: Number(portText) });
  socket.setEncoding("utf8");
  let buffer = "";
  const values = [];
  const waiters = [];
  socket.on("data", (chunk) => {
    buffer += chunk;
    while (buffer.includes("\n")) {
      const index = buffer.indexOf("\n");
      const line = buffer.slice(0, index).trim();
      buffer = buffer.slice(index + 1);
      if (!line) continue;
      const value = JSON.parse(line);
      values.push(value);
      for (const waiter of [...waiters]) {
        if (!waiter.predicate(value)) continue;
        waiters.splice(waiters.indexOf(waiter), 1);
        clearTimeout(waiter.timer);
        waiter.resolve(value);
      }
    }
  });
  socket.on("connect", () => socket.write(`${JSON.stringify(request)}\n`));
  return {
    socket,
    values,
    next(predicate, timeoutMs) {
      const existing = values.find(predicate);
      if (existing) return Promise.resolve(existing);
      return new Promise((resolve, reject) => {
        const waiter = { predicate, resolve, timer: undefined };
        waiter.timer = setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index >= 0) waiters.splice(index, 1);
          reject(new Error(`router response timed out; saw ${JSON.stringify(values)}`));
        }, timeoutMs);
        waiters.push(waiter);
      });
    },
  };
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
