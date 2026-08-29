import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promptPayload } from "../../src/commands/run.js";
import { commandRunShellForCommand, parseRun } from "../../src/cli.js";

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
    [
      "--jspace-contract",
      jspacePath,
      "--task-context-capsule",
      capsulePath,
      "execute",
    ],
    false,
  );

  assert.deepEqual(parsed.jspaceContract, { semantic_sha256: "a".repeat(64) });
  assert.deepEqual(parsed.taskContextCapsule, { semantic_sha256: "b".repeat(64) });
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
