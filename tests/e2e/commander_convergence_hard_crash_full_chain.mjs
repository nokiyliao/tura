import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { startBackendStressEnvironment } from "./full_chain_backend_fixture.mjs";

if (process.env.TURA_P5_HARD_CRASH_E2E !== "1") throw new Error("TURA_P5_HARD_CRASH_E2E=1 is required");
const fake = path.join(import.meta.dirname, "fake_official_codex_commander.mjs");
const backend = await startBackendStressEnvironment({ runIdPrefix: "commander-convergence-hard-crash-v6", officialCodexAppServer: fake, sessionModel: "official_codex_app_server/gpt-5.6-sol", config: { workspaces: 1, tasksPerWorkspace: 1, turnsPerSession: 1, liveSessionTarget: 0, ensureBuilds: true, totalTimeoutMs: 240_000 } });
try {
  const parent = backend.targetSession;
  const statePath = path.join(parent.workspace, ".tura", "p5-fake-commander-state.json");
  await fsp.mkdir(path.dirname(statePath), { recursive: true });
  await fsp.writeFile(statePath, JSON.stringify({ schema_version: "p5_fake_commander_state_v1", thread_id: "thread-p5-global-commander", codex_session_id: "codex-session-p5-global-commander", turn_start_count: 0, turns: [{ id: "turn-p5-existing-commander", status: "completed", items: [] }], child_turns: [] }));
  const watcher = spawn(process.execPath, [fake, "--watch-ledger-and-kill", parent.workspace], { detached: true, stdio: "ignore", env: { ...process.env, TURA_P5_HARD_CRASH_E2E: "1" } });
  watcher.unref();
  const armed = await waitJson(path.join(parent.workspace, ".tura", "p5-watcher-armed.json"), 10_000);
  assert.equal(armed.armed, true);
  const initial = JSON.parse(await fsp.readFile(statePath, "utf8"));
  const preRevision = revision(initial.thread_id, initial.turns.map((turn) => turn.id));
  const suffix = backend.runId.replace(/[^a-zA-Z0-9_.:-]/g, "-");
  const runtimeId = `runtime-p5-child-${suffix}`;
  const transactionId = `transaction-p5-child-${suffix}`;
  const prompt = "P5 child produces one deterministic callback";
  const controller = new AbortController();
  const request = fetch(`${backend.gateway.url}/session/${encodeURIComponent(parent.sessionId)}/children`, { method: "POST", headers: { "content-type": "application/json", "x-opencode-directory": encodeURIComponent(parent.workspace) }, body: JSON.stringify({ parent_session_id: parent.sessionId, parent_mission_revision_sha256: preRevision, commander_thread_id: initial.thread_id, child_session_id: `child-p5-${suffix}`, child_runtime_id: runtimeId, child_transaction_id: transactionId, child_lease_id: `lease-p5-child-${suffix}`, callback_request_id: transactionId, effect_id: `${runtimeId}.message`, delegated_input_sha256: sha256(JSON.stringify(prompt)), session_directory: parent.workspace, session_name: "P5 Commander convergence hard crash", created_at_ms: Date.now(), execution_payload: { prompt, directory: parent.workspace, model: "official_codex_app_server/gpt-5.6-sol", agent: "direct-text-only" } }), signal: controller.signal }).then(async (response) => { const text = await response.text(); if (!response.ok) throw new Error(`${response.status}:${text}`); return text; });
  const crash = await waitJson(path.join(parent.workspace, ".tura", "p5-hard-crash-observed.json"), 45_000);
  assert.ok(crash.ledger);
  const routerBefore = await routerEndpoint(backend.turaHome);
  process.kill(routerBefore.pid, "SIGKILL");
  controller.abort();
  await request.catch(() => undefined);
  await waitRouter(backend, routerBefore.pid, 30_000);
  const lifecycleRoot = await waitAck(backend.turaHome, parent.sessionId, 45_000);
  assert.equal(await count(path.join(lifecycleRoot, "callbacks", "intaken")), 1);
  assert.equal(await count(path.join(lifecycleRoot, "callbacks", "acknowledged")), 1);
  assert.equal((await records(path.join(lifecycleRoot, "continuations"))).filter((r) => r.state === "acknowledged").length, 1);
  assert.equal(await count(path.join(lifecycleRoot, "receipts", "pending")), 0);
  const state = JSON.parse(await fsp.readFile(statePath, "utf8"));
  assert.equal(state.turn_start_count, 1);
  const session = await backend.callSessionDb({ command: "get_session", session_id: parent.sessionId });
  const ids = session.session.lifecycle_projection.runtime_ids.filter((id) => id.startsWith("callback-continuation-"));
  assert.equal(ids.length, 2);
  const aggregates = await Promise.all(ids.map(async (id) => (await backend.callSessionDb({ command: "replay_runtime", runtime_id: id })).runtime.aggregate));
  const fallback = aggregates.find((r) => r.fallback_from_id);
  assert.ok(fallback);
  const lease = await backend.callSessionDb({ command: "get_runtime_lease", runtime_id: fallback.runtime_id });
  assert.equal(lease.runtime.terminal, true); assert.equal(lease.runtime.lease_active, false); assert.equal(fallback.effects?.length || 0, 0);
  const receipts = (await records(path.join(lifecycleRoot, "receipts", "applied"))).map((r) => r.receipt).filter((r) => r?.transaction_id?.startsWith("callback-continuation-request-"));
  assert.ok(receipts.some((r) => r.event_seq === 0 && r.terminal_state === "failed"));
  assert.ok(receipts.some((r) => r.event_seq === 1 && r.terminal_state === "completed"));
  console.log(JSON.stringify({ status: "PASS_P5_V5", run_root: backend.runRoot, watcher_armed: true, hard_crash_observed: true, target_turn_start_count: 1, fallback_count: 1, pending: 0, callbacks: 1, ack: 1, effects: 0 }));
} finally { await backend.cleanup(); }

function sha256(v) { return crypto.createHash("sha256").update(v).digest("hex"); }
function revision(thread_id, ordered_turn_ids) { return sha256(JSON.stringify({ ordered_turn_ids, thread_id })); }
async function waitJson(file, ms) { const end = Date.now() + ms; while (Date.now() < end) { try { return JSON.parse(await fsp.readFile(file, "utf8")); } catch {} await new Promise((r) => setTimeout(r, 25)); } throw new Error(`timeout ${file}`); }
async function routerEndpoint(home) { return JSON.parse(await fsp.readFile(path.join(home, "db", "session_log", "router.addr"), "utf8")); }
async function waitRouter(backend, pid, ms) { const end = Date.now() + ms; while (Date.now() < end) { await fetch(`${backend.gateway.url}/global/health`).then((r) => r.body?.cancel()).catch(() => {}); try { const e = await routerEndpoint(backend.turaHome); if (e.pid && e.pid !== pid) return e; } catch {} await new Promise((r) => setTimeout(r, 100)); } throw new Error("router replacement timeout"); }
async function count(dir) { return fs.existsSync(dir) ? (await fsp.readdir(dir)).filter((n) => n.endsWith(".json")).length : 0; }
async function records(dir) { if (!fs.existsSync(dir)) return []; return Promise.all((await fsp.readdir(dir)).filter((n) => n.endsWith(".json")).map(async (n) => JSON.parse(await fsp.readFile(path.join(dir, n), "utf8")))); }
async function waitAck(home, session, ms) { const root = path.join(home, "db", "session_log", "session_lifecycle_v1", sha256(session)); const end = Date.now() + ms; while (Date.now() < end) { if (await count(path.join(root, "callbacks", "acknowledged")) === 1) return root; await new Promise((r) => setTimeout(r, 100)); } throw new Error("ack timeout"); }
