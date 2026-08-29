#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const args = process.argv.slice(2);
if (args[0] === "--watch-ledger-and-kill") {
  await watchLedgerAndKill(args[1]);
  process.exit(0);
}
if (args.includes("--version")) {
  process.stdout.write("codex-cli 9.9.9-p5-e2e\n");
  process.exit(0);
}
if (!args.includes("app-server")) throw new Error(`unsupported invocation: ${args.join(" ")}`);
if (process.env.TURA_P5_HARD_CRASH_E2E !== "1") throw new Error("P5 hard-crash fault is not armed");

const workspace = process.env.TURA_CWD || process.cwd();
const turaDir = path.join(workspace, ".tura");
const statePath = path.join(turaDir, "p5-fake-commander-state.json");
fs.mkdirSync(turaDir, { recursive: true });
fs.writeFileSync(path.join(turaDir, "p5-runtime-parent.json"), JSON.stringify({ runtime_pid: process.ppid }));
const state = fs.existsSync(statePath) ? JSON.parse(fs.readFileSync(statePath, "utf8")) : defaultState();
const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (!line.trim()) continue;
  const message = JSON.parse(line);
  const id = message.id;
  if (message.method === "initialize") respond(id, { userAgent: "p5-e2e", platformFamily: "unix", platformOs: "test" });
  else if (message.method === "initialized") continue;
  else if (message.method === "thread/start") respond(id, { thread: { id: "thread-p5-child", sessionId: "codex-session-p5-child" } });
  else if (message.method === "thread/resume") respond(id, { thread: { id: state.thread_id, sessionId: state.codex_session_id } });
  else if (message.method === "thread/read") {
    const commander = message.params?.threadId === state.thread_id;
    respond(id, { thread: { id: commander ? state.thread_id : "thread-p5-child", turns: commander ? state.turns : state.child_turns } });
  } else if (message.method === "turn/start") {
    const commander = message.params?.threadId === state.thread_id;
    const turns = commander ? state.turns : state.child_turns;
    const turnId = commander ? `turn-p5-${state.turn_start_count + 1}` : `turn-p5-child-${turns.length + 1}`;
    const text = commander ? JSON.stringify({ schema_version: "tura_commander_convergence_result_v1", requested_action: "MISSION_VERIFICATION", disposition: "route_selected", first_false_predicate: "P6_CONTINUATION_RECOVERY_NO_BLIND_RETRY", selected_route: "P6_CONTINUATION_RECOVERY_FAULT_MATRIX" }) : "P5 child completed";
    if (commander) state.turn_start_count += 1;
    turns.push({ id: turnId, status: "completed", items: [{ type: "agentMessage", id: `item-${turnId}`, text, phase: "final_answer" }] });
    writeState();
    respond(id, { turn: { id: turnId, status: "inProgress", items: [] } });
    notify("item/completed", { threadId: commander ? state.thread_id : "thread-p5-child", turnId, item: { type: "agentMessage", id: `item-${turnId}`, text, phase: "final_answer" } });
    notify("turn/completed", { threadId: commander ? state.thread_id : "thread-p5-child", turn: turns.at(-1) });
  } else throw new Error(`unexpected method ${message.method}`);
}

function defaultState() { return { schema_version: "p5_fake_commander_state_v1", thread_id: "thread-p5-global-commander", codex_session_id: "codex-session-p5-global-commander", turn_start_count: 0, turns: [], child_turns: [] }; }
function writeState() { const tmp = `${statePath}.${crypto.randomUUID()}.tmp`; fs.writeFileSync(tmp, JSON.stringify(state)); fs.renameSync(tmp, statePath); }
function respond(id, result) { process.stdout.write(`${JSON.stringify({ id, result })}\n`); }
function notify(method, params) { process.stdout.write(`${JSON.stringify({ method, params })}\n`); }

async function watchLedgerAndKill(root) {
  const marker = path.join(root, ".tura", "p5-watcher-armed.json");
  fs.mkdirSync(path.dirname(marker), { recursive: true });
  fs.writeFileSync(marker, JSON.stringify({ armed: true, watcher_pid: process.pid }));
  const deadline = Date.now() + 45_000;
  while (Date.now() < deadline) {
    const pidPath = path.join(root, ".tura", "p5-runtime-parent.json");
    const ledgerRoot = path.join(root, ".tura", "run", "effect_ledgers");
    if (fs.existsSync(pidPath) && fs.existsSync(ledgerRoot)) {
      const runtimePid = JSON.parse(fs.readFileSync(pidPath, "utf8")).runtime_pid;
      for (const name of fs.readdirSync(ledgerRoot)) {
        const file = path.join(ledgerRoot, name);
        if (!name.endsWith(".json") || !fs.readFileSync(file, "utf8").includes('"commander_convergence_proof"')) continue;
        fs.writeFileSync(path.join(root, ".tura", "p5-hard-crash-observed.json"), JSON.stringify({ ledger: name, runtime_pid: runtimePid }));
        process.kill(runtimePid, "SIGKILL");
        return;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  process.exitCode = 2;
}
