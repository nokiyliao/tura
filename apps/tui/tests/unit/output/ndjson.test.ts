import assert from "node:assert/strict";
import test from "node:test";
import { NdjsonOutput } from "../../../src/output/ndjson.js";
import type { ChildAdmissionReceipt } from "../../../src/types/session.js";

test("NDJSON child admission fixture preserves machine-binding identities", () => {
  const receipt: ChildAdmissionReceipt = {
    outcome: "admitted",
    parent_session_id: "parent-1",
    parent_mission_revision_sha256: "a".repeat(64),
    commander_thread_id: "thread-1",
    child_session_id: "child-1",
    child_runtime_id: "runtime-1",
    child_lease_id: "lease-1",
    child_transaction_id: "transaction-1",
    callback_request_id: "transaction-1",
    effect_id: "runtime-1.message",
    delegated_input_sha256: "b".repeat(64),
  };
  const original = process.stdout.write;
  let output = "";
  process.stdout.write = ((chunk: Uint8Array | string) => {
    output += chunk.toString();
    return true;
  }) as typeof process.stdout.write;

  try {
    new NdjsonOutput().childAdmitted(receipt);
  } finally {
    process.stdout.write = original;
  }

  assert.deepEqual(JSON.parse(output), {
    type: "cli.child_admitted",
    childAdmission: receipt,
  });
});
