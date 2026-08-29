import assert from "node:assert/strict";
import test from "node:test";
import type { GatewayClient } from "../../../src/gateway/client.js";
import {
  resolveExistingRunSession,
  throwIfCliRunFailed,
  waitByPolling,
  waitWithEvents,
} from "../../../src/commands/run.js";
import type { Message, RunResult, Session } from "../../../src/types/session.js";

function runResult(status: RunResult["status"]): RunResult {
  return {
    sessionID: "terminal-session",
    status,
    finalText: "",
    messages: [],
    usage: null,
    metadata: {
      input_token_usage: 0,
      input_token_cache: 0,
      provider_time_ms: 0,
      total_time_ms: 0,
      commands: 0,
      failed_commands: 0,
      tps: 0,
      turns: 0,
    },
  };
}

test("existing run applies an explicit permission override before the prompt", async () => {
  const updates: Array<{ sessionID: string; payload: Partial<Session> }> = [];
  const client = {
    async getSession(): Promise<Session> {
      throw new Error("explicit override must not reuse stale session management");
    },
    async updateSession(sessionID: string, payload: Partial<Session>): Promise<Session> {
      updates.push({ sessionID, payload });
      return { id: sessionID, ...payload };
    },
  } as Pick<GatewayClient, "getSession" | "updateSession">;

  const session = await resolveExistingRunSession(client, "session-1", true);

  assert.equal(session.disable_permission_restrictions, true);
  assert.deepEqual(updates, [
    { sessionID: "session-1", payload: { disable_permission_restrictions: true } },
  ]);
});

test("existing run without a permission override preserves session management", async () => {
  let updateCount = 0;
  const client = {
    async getSession(sessionID: string): Promise<Session> {
      return { id: sessionID, disable_permission_restrictions: false };
    },
    async updateSession(): Promise<Session> {
      updateCount += 1;
      throw new Error("unexpected session mutation");
    },
  } as Pick<GatewayClient, "getSession" | "updateSession">;

  const session = await resolveExistingRunSession(client, "session-2", undefined);

  assert.equal(session.disable_permission_restrictions, false);
  assert.equal(updateCount, 0);
});

test("existing run applies the CLI default permission setting", async () => {
  const updates: Array<{ sessionID: string; payload: Partial<Session> }> = [];
  const client = {
    async getSession(): Promise<Session> {
      throw new Error("CLI default must update the existing session");
    },
    async updateSession(sessionID: string, payload: Partial<Session>): Promise<Session> {
      updates.push({ sessionID, payload });
      return { id: sessionID, ...payload };
    },
  } as Pick<GatewayClient, "getSession" | "updateSession">;

  const session = await resolveExistingRunSession(client, "session-default", true);

  assert.equal(session.disable_permission_restrictions, true);
  assert.deepEqual(updates, [
    { sessionID: "session-default", payload: { disable_permission_restrictions: true } },
  ]);
});

test("polling transport detaches without aborting an accepted busy execution", async () => {
  let abortCount = 0;
  const messages: Message[] = [
    { id: "user-1", role: "user", parts: [{ id: "part-1", type: "text", text: "work" }] },
    {
      id: "assistant-progress",
      role: "assistant",
      parts: [{ id: "part-2", type: "text", text: "still working" }],
    },
  ];
  const session: Session = { id: "session-1", status: "busy" };
  const client = {
    async getSession(): Promise<Session> {
      return session;
    },
    async listMessages(): Promise<Message[]> {
      return messages;
    },
    async abort(): Promise<void> {
      abortCount += 1;
    },
  } as unknown as GatewayClient;

  const result = await waitByPolling(client, session, 0, 0);

  assert.equal(result.status, "detached");
  assert.equal(result.sessionID, session.id);
  assert.equal(result.finalText, "still working");
  assert.equal(abortCount, 0);
});

test("stream transport detaches without aborting the accepted execution", async () => {
  let abortCount = 0;
  let streamReturnCount = 0;
  const messages: Message[] = [
    {
      id: "assistant-progress",
      role: "assistant",
      parts: [{ id: "part-1", type: "text", text: "still working" }],
    },
  ];
  const session: Session = { id: "session-2", status: "busy" };
  const iterator = {
    async next(): Promise<IteratorResult<never>> {
      return { done: true, value: undefined };
    },
    async return(): Promise<IteratorResult<never>> {
      streamReturnCount += 1;
      return { done: true, value: undefined };
    },
  };
  const client = {
    directory: "/workspace",
    streamEvents(): AsyncIterable<never> {
      return { [Symbol.asyncIterator]: () => iterator };
    },
    async listMessages(): Promise<Message[]> {
      return messages;
    },
    async abort(): Promise<void> {
      abortCount += 1;
    },
  } as unknown as GatewayClient;

  const result = await waitWithEvents(client, session, 0, 0, undefined, undefined);

  assert.equal(result.status, "detached");
  assert.equal(result.sessionID, session.id);
  assert.equal(abortCount, 0);
  assert.equal(streamReturnCount, 1);
});

test("stream transport returns a failed cancellation before assistant output", async () => {
  let streamReturnCount = 0;
  const messages: Message[] = [
    { id: "user-1", role: "user", parts: [{ id: "part-1", type: "text", text: "work" }] },
  ];
  const session: Session = { id: "session-cancelled", status: "error" };
  const iterator = {
    next(): Promise<IteratorResult<never>> {
      return new Promise(() => undefined);
    },
    async return(): Promise<IteratorResult<never>> {
      streamReturnCount += 1;
      return { done: true, value: undefined };
    },
  };
  const client = {
    directory: "/workspace",
    streamEvents(): AsyncIterable<never> {
      return { [Symbol.asyncIterator]: () => iterator };
    },
    async getSession(): Promise<Session> {
      return session;
    },
    async listMessages(): Promise<Message[]> {
      return messages;
    },
  } as unknown as GatewayClient;

  const result = await waitWithEvents(client, session, messages.length, 3, undefined, undefined);

  assert.equal(result.status, "failed");
  assert.equal(result.sessionID, session.id);
  assert.equal(result.finalText, "");
  assert.equal(streamReturnCount, 1);
});

test("CLI surfaces durable terminal failures as a typed nonzero result", () => {
  assert.throws(
    () => throwIfCliRunFailed(runResult("failed"), "cli"),
    (error: unknown) =>
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      "exitCode" in error &&
      error.code === "TURA_RUNTIME_TERMINAL_FAILURE" &&
      error.exitCode === 1,
  );
  assert.doesNotThrow(() => throwIfCliRunFailed(runResult("completed"), "cli"));
  assert.doesNotThrow(() => throwIfCliRunFailed(runResult("detached"), "cli"));
  assert.doesNotThrow(() => throwIfCliRunFailed(runResult("failed"), "tui"));
});
