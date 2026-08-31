import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promptPayload } from "../../src/commands/run.js";
import { commandRunShellForCommand, main, parseRun } from "../../src/cli.js";
import type { RegisterChildSessionRequest } from "../../src/types/session.js";

test("run shell flags override the command-run surface", () => {
  assert.equal(parseRun(["--bash", "inspect"], false).commandRunShell, "bash");
  assert.equal(parseRun(["--zsh", "inspect"], false).commandRunShell, "zsh");
  assert.equal(parseRun(["--shel", "inspect"], false).commandRunShell, "shell_command");
  assert.equal(parseRun(["-c", "command_run_shell=zsh", "inspect"], false).commandRunShell, "zsh");
  assert.throws(() => parseRun(["-c", "command_run_shell=zash", "inspect"], false), /bash/);
});

test("run defaults to balanced with priority routing off", () => {
  const parsed = parseRun(["hello"], false);

  assert.equal(parsed.agent, "balanced");
  assert.equal(parsed.modelVariant, "high");
  assert.equal(parsed.modelAccelerationEnabled, false);
  assert.equal(parsed.disablePermissionRestrictions, true);
});

test("run keeps explicit priority routing opt-in", () => {
  assert.equal(parseRun(["--priority", "hello"], false).modelAccelerationEnabled, true);
  assert.equal(parseRun(["--model-acceleration", "hello"], false).modelAccelerationEnabled, true);
  assert.equal(
    parseRun(["--no-model-acceleration", "hello"], false).modelAccelerationEnabled,
    false,
  );
});

test("run forwards request-scoped permission restriction overrides", () => {
  assert.equal(
    parseRun(["-c", "disable_permission_restrictions=true", "install"], false)
      .disablePermissionRestrictions,
    true,
  );
  assert.equal(
    parseRun(["--config=disable_permission_restrictions=false", "inspect"], false)
      .disablePermissionRestrictions,
    false,
  );
});

test("run loads immutable task-context and J-Space JSON inputs", () => {
  const directory = mkdtempSync(join(tmpdir(), "tura-context-cli-"));
  const jspacePath = join(directory, "jspace.json");
  const capsulePath = join(directory, "capsule.json");
  writeFileSync(jspacePath, JSON.stringify({ semantic_sha256: "a".repeat(64) }));
  writeFileSync(capsulePath, JSON.stringify({ semantic_sha256: "b".repeat(64) }));

  const parsed = parseRun(
    ["--jspace-contract", jspacePath, "--task-context-capsule", capsulePath, "execute"],
    false,
  );

  assert.deepEqual(parsed.jspaceContract, { semantic_sha256: "a".repeat(64) });
  assert.deepEqual(parsed.taskContextCapsule, { semantic_sha256: "b".repeat(64) });
});

test("child run loads the exact wire request without normal run defaults", () => {
  const directory = mkdtempSync(join(tmpdir(), "tura-child-cli-"));
  const requestPath = join(directory, "child.json");
  const request = childRequest();
  writeFileSync(requestPath, JSON.stringify(request));

  const parsed = parseRun(["--child-request", requestPath, "--json", "--no-stream"], false);

  assert.deepEqual(parsed.childRequest, request);
  assert.equal(parsed.prompt, undefined);
  assert.equal(parsed.sessionID, undefined);
  assert.equal(parsed.model, undefined);
  assert.equal(parsed.agent, undefined);
  assert.equal(parsed.jspaceContract, undefined);
  assert.equal(parsed.taskContextCapsule, undefined);
  assert.equal(parsed.output, "json");
  assert.equal(parsed.stream, false);
});

test("child run rejects every duplicate execution authority before gateway setup", () => {
  const directory = mkdtempSync(join(tmpdir(), "tura-child-conflict-"));
  const requestPath = join(directory, "child.json");
  writeFileSync(requestPath, JSON.stringify(childRequest()));
  const conflicts = [
    ["duplicate prompt"],
    ["--session", "other"],
    ["--model", "other-model"],
    ["--agent", "other-agent"],
    ["--jspace-contract", requestPath],
    ["--task-context-capsule", requestPath],
    ["--config", "model=other-model"],
    ["--priority"],
    ["--bash"],
  ];

  for (const conflict of conflicts) {
    assert.throws(
      () => parseRun(["--child-request", requestPath, ...conflict], false),
      /TURA_CHILD_REQUEST_AUTHORITY_CONFLICT/,
      conflict.join(" "),
    );
  }
});

test("child run rejects malformed JSON and missing parent or child identity", () => {
  const directory = mkdtempSync(join(tmpdir(), "tura-child-invalid-"));
  const requestPath = join(directory, "child.json");
  writeFileSync(requestPath, "{");
  assert.throws(
    () => parseRun(["--child-request", requestPath], false),
    /TURA_CHILD_REQUEST_INVALID:json_file/,
  );

  for (const field of ["parent_session_id", "child_session_id"] as const) {
    const request = childRequest();
    request[field] = "";
    writeFileSync(requestPath, JSON.stringify(request));
    assert.throws(
      () => parseRun(["--child-request", requestPath], false),
      new RegExp(`TURA_CHILD_REQUEST_INVALID:identity_missing:${field}`),
    );
  }
});

test("invalid or mixed child input fails before any gateway request", async () => {
  const directory = mkdtempSync(join(tmpdir(), "tura-child-presubmit-"));
  const malformedPath = join(directory, "malformed.json");
  const requestPath = join(directory, "child.json");
  writeFileSync(malformedPath, "{");
  writeFileSync(requestPath, JSON.stringify(childRequest()));
  const originalFetch = globalThis.fetch;
  const originalWrite = process.stdout.write;
  let sendCount = 0;
  globalThis.fetch = (() => {
    sendCount += 1;
    throw new Error("gateway request must not occur");
  }) as typeof fetch;
  process.stdout.write = (() => true) as typeof process.stdout.write;

  try {
    await assert.rejects(
      main(["--gateway-url", "http://127.0.0.1:1", "run", "--child-request", malformedPath]),
      /TURA_CHILD_REQUEST_INVALID:json_file/,
    );
    await assert.rejects(
      main([
        "--gateway-url",
        "http://127.0.0.1:1",
        "run",
        "--child-request",
        requestPath,
        "duplicate prompt",
      ]),
      /TURA_CHILD_REQUEST_AUTHORITY_CONFLICT/,
    );
  } finally {
    globalThis.fetch = originalFetch;
    process.stdout.write = originalWrite;
  }
  assert.equal(sendCount, 0);
});

test("top-level shell commands cover only the documented surfaces", () => {
  assert.equal(commandRunShellForCommand("bash"), "bash");
  assert.equal(commandRunShellForCommand("zsh"), "zsh");
  assert.equal(commandRunShellForCommand("shel"), "shell_command");
  assert.equal(commandRunShellForCommand("shll"), undefined);
  assert.equal(commandRunShellForCommand("zash"), undefined);
});

test("prompt payload forwards the command-run shell override to the gateway", () => {
  const payload = promptPayload("inspect", { source: "cli", commandRunShell: "zsh" });

  assert.equal(payload.command_run_shell, "zsh");
});

function childRequest(): RegisterChildSessionRequest {
  return {
    parent_session_id: "parent/session",
    parent_mission_revision_sha256: "a".repeat(64),
    commander_thread_id: "thread-1",
    child_session_id: "child-1",
    child_runtime_id: "runtime-1",
    child_transaction_id: "transaction-1",
    child_lease_id: "lease-1",
    callback_request_id: "transaction-1",
    effect_id: "runtime-1.message",
    delegated_input_sha256: "b".repeat(64),
    session_directory: "/workspace",
    session_name: "Delegated child",
    created_at_ms: 1,
    execution_payload: {
      prompt: "delegated work",
      model: "openai/gpt-test",
      agent: "balanced",
      jspace_contract: { semantic_sha256: "c".repeat(64) },
      task_context_capsule: { semantic_sha256: "d".repeat(64) },
    },
  };
}
