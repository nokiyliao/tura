import assert from "node:assert/strict";
import http from "node:http";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { GatewayClient } from "../../../src/gateway/client.js";
import {
  childAdmissionReceipt,
  resolveExistingRunSession,
  runPrompt,
  throwIfCliRunFailed,
  waitByPolling,
  waitWithEvents,
} from "../../../src/commands/run.js";
import type { CliContext } from "../../../src/types/common.js";
import type {
  Message,
  RegisterChildSessionRequest,
  RegisterChildSessionResponse,
  RunResult,
  Session,
} from "../../../src/types/session.js";

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

test("child run performs one admission mutation and preserves terminal JSON semantics", async () => {
  const request = childRequest();
  const response = childResponse(request);
  const mutations = { child: 0, create: 0, fork: 0, prompt: 0 };
  const bodies: unknown[] = [];
  const directory = await mkdtemp(join(tmpdir(), "tura-child-run-"));
  const lastMessageFile = join(directory, "last.txt");

  await withServer(
    async (req, res) => {
      const path = new URL(req.url ?? "/", "http://gateway").pathname;
      if (req.method === "GET" && path === "/global/health") {
        return sendJson(res, { healthy: true, version: "test" });
      }
      if (req.method === "GET" && path === "/project/current") return sendJson(res, {});
      if (req.method === "POST" && path === "/session/parent%2Fsession/children") {
        mutations.child += 1;
        bodies.push(await readBody(req));
        return sendJson(res, response);
      }
      if (req.method === "POST" && path === "/session") mutations.create += 1;
      if (req.method === "POST" && path.endsWith("/fork")) mutations.fork += 1;
      if (req.method === "POST" && path.endsWith("/prompt_async")) mutations.prompt += 1;
      if (req.method === "GET" && path === "/session/child-1") {
        return sendJson(res, {
          id: "child-1",
          status: "idle",
          directory: request.session_directory,
        });
      }
      if (req.method === "GET" && path === "/session/child-1/message") {
        return sendJson(res, [
          {
            id: "assistant-final",
            role: "assistant",
            updated_at: 2,
            parts: [{ id: "part-final", type: "text", text: "CHILD_FINAL" }],
          },
        ]);
      }
      sendJson(res, { unexpected: `${req.method} ${path}` }, 404);
    },
    async (baseUrl) => {
      const output = await captureStdout(async () =>
        runPrompt(cliContext(baseUrl), {
          childRequest: request,
          output: "json",
          stream: false,
          timeoutSec: 3,
          lastMessageFile,
          source: "cli",
        }),
      );
      const result = JSON.parse(output) as RunResult;
      assert.equal(result.status, "completed");
      assert.equal(result.sessionID, request.child_session_id);
      assert.equal(result.finalText, "CHILD_FINAL");
      assert.deepEqual(result.childAdmission, childAdmissionReceipt(request, response));
    },
  );

  assert.deepEqual(mutations, { child: 1, create: 0, fork: 0, prompt: 0 });
  assert.deepEqual(bodies, [request]);
  assert.equal(await readFile(lastMessageFile, "utf8"), "CHILD_FINAL");
});

test("child admission response identity drift fails closed", () => {
  const request = childRequest();
  for (const field of [
    "parent_session_id",
    "child_session_id",
    "child_runtime_id",
    "child_transaction_id",
    "callback_request_id",
    "effect_id",
  ] as const) {
    const response = childResponse(request);
    response[field] = "drift";
    assert.throws(
      () => childAdmissionReceipt(request, response),
      new RegExp(`TURA_CHILD_ADMISSION_IDENTITY_MISMATCH:${field}`),
    );
  }
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

function childResponse(request: RegisterChildSessionRequest): RegisterChildSessionResponse {
  return {
    outcome: "admitted",
    parent_session_id: request.parent_session_id,
    child_session_id: request.child_session_id,
    child_runtime_id: request.child_runtime_id,
    child_transaction_id: request.child_transaction_id,
    callback_request_id: request.callback_request_id,
    effect_id: request.effect_id,
  };
}

function cliContext(gatewayUrl: string): CliContext {
  return {
    gatewayUrl,
    gatewayUrlExplicit: true,
    cwd: "/ignored-by-child-mode",
    json: false,
    color: "never",
    display: "plain",
    verbose: false,
    mock: false,
    dev: false,
  };
}

async function withServer(
  handler: (req: http.IncomingMessage, res: http.ServerResponse) => void | Promise<void>,
  callback: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const server = http.createServer((req, res) => void handler(req, res));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  try {
    await callback(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}

function sendJson(res: http.ServerResponse, value: unknown, status = 200): void {
  const body = JSON.stringify(value);
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
  });
  res.end(body);
}

function readBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("error", reject);
    req.on("end", () => resolve(body ? JSON.parse(body) : undefined));
  });
}

async function captureStdout(callback: () => Promise<unknown>): Promise<string> {
  const original = process.stdout.write;
  let output = "";
  process.stdout.write = ((chunk: Uint8Array | string) => {
    output += chunk.toString();
    return true;
  }) as typeof process.stdout.write;
  try {
    await callback();
    return output;
  } finally {
    process.stdout.write = original;
  }
}
