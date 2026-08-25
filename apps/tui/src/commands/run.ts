import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { GatewayClient } from "../gateway/client.js";
import { sameDirectory } from "../gateway/directory.js";
import { normalizeEvent } from "../gateway/events.js";
import { GatewayUnavailableError, type CliContext, type OutputMode } from "../types/common.js";
import {
  hasUserFacingAssistantText,
  sessionStatusText,
  type PromptPayload,
  type RunResult,
  type Session,
} from "../types/session.js";
import { buildRunResult, writeLastMessage } from "../output/final-result.js";
import { HumanOutput } from "../output/human.js";
import { printRunJson } from "../output/json.js";
import { NdjsonOutput } from "../output/ndjson.js";
import { userFacingError } from "../gateway/errors.js";
import type { CommandRunShell } from "./config-values.js";

const RUN_COMPLETION_STABLE_MS = 1000;

export interface RunOptions {
  prompt: string;
  sessionID?: string;
  model?: string;
  agent?: string;
  sessionType?: string;
  modelVariant?: string;
  modelAccelerationEnabled?: boolean;
  killProcessesOnStart?: boolean;
  validatorEnabled?: boolean;
  disablePermissionRestrictions?: boolean;
  commandRunShell?: CommandRunShell;
  jspaceContract?: unknown;
  taskContextCapsule?: unknown;
  output: OutputMode;
  stream: boolean;
  timeoutSec: number;
  lastMessageFile?: string;
  source: "cli" | "tui";
}

export async function runPrompt(context: CliContext, options: RunOptions): Promise<RunResult> {
  return withCommandRunShellEnv(options.commandRunShell, () =>
    runPromptWithShellEnv(context, options),
  );
}

async function runPromptWithShellEnv(context: CliContext, options: RunOptions): Promise<RunResult> {
  const client = new GatewayClient({
    baseUrl: context.gatewayUrl,
    directory: context.cwd,
    verbose: context.verbose,
  });
  try {
    await client.health();
    await client.syncWorkspace();
  } catch (error) {
    throw new GatewayUnavailableError(userFacingError(error));
  }

  const session = options.sessionID
    ? await resolveExistingRunSession(
        client,
        options.sessionID,
        options.disablePermissionRestrictions,
      )
    : await client.createSession({
        directory: context.cwd,
        model: options.model,
        agent: options.agent,
        session_type: options.sessionType,
        model_variant: options.modelVariant,
        model_acceleration_enabled: options.modelAccelerationEnabled,
        kill_processes_on_start: options.killProcessesOnStart,
        validator_enabled: options.validatorEnabled,
        disable_permission_restrictions: options.disablePermissionRestrictions,
      });
  const initialMessages = await client.listMessages(session.id).catch(() => []);
  const initialCount = initialMessages.length;
  const payload = promptPayload(options.prompt, {
    source: options.source,
    model: options.model ?? session.model ?? undefined,
    agent: options.agent ?? session.agent ?? undefined,
    modelVariant: options.modelVariant ?? session.model_variant ?? undefined,
    modelAccelerationEnabled:
      options.modelAccelerationEnabled ?? session.model_acceleration_enabled,
    commandRunShell: options.commandRunShell,
    jspaceContract: options.jspaceContract,
    taskContextCapsule: options.taskContextCapsule,
  });

  const human = options.output === "text" ? new HumanOutput(context.color) : undefined;
  const ndjson = options.output === "ndjson" ? new NdjsonOutput() : undefined;
  human?.header(session, context.cwd);
  ndjson?.started({ sessionID: session.id, prompt: options.prompt });
  await client.sendPromptAsync(session.id, payload);

  let result: RunResult;
  try {
    result = options.stream
      ? await waitWithEvents(client, session, initialCount, options.timeoutSec, human, ndjson)
      : await waitByPolling(client, session, initialCount, options.timeoutSec);
  } catch (error) {
    ndjson?.failed(session.id, error);
    throw error;
  }

  await writeLastMessage(options.lastMessageFile, result.finalText);
  if (options.output === "json") printRunJson(result);
  if (options.output === "ndjson") ndjson?.completed(result);
  if (options.output === "text") human?.final(result);
  return result;
}

export async function resolveExistingRunSession(
  client: Pick<GatewayClient, "getSession" | "updateSession">,
  sessionID: string,
  disablePermissionRestrictions: boolean | undefined,
): Promise<Session> {
  if (disablePermissionRestrictions === undefined) return client.getSession(sessionID);
  return client.updateSession(sessionID, {
    disable_permission_restrictions: disablePermissionRestrictions,
  });
}

async function withCommandRunShellEnv<T>(
  shell: CommandRunShell | undefined,
  callback: () => Promise<T>,
): Promise<T> {
  if (!shell) return callback();
  const previous = process.env.TURA_COMMAND_RUN_SHELL;
  process.env.TURA_COMMAND_RUN_SHELL = shell;
  try {
    return await callback();
  } finally {
    if (previous === undefined) delete process.env.TURA_COMMAND_RUN_SHELL;
    else process.env.TURA_COMMAND_RUN_SHELL = previous;
  }
}

export function promptPayload(
  prompt: string,
  options: Pick<
    RunOptions,
    | "model"
    | "agent"
    | "source"
    | "modelVariant"
    | "modelAccelerationEnabled"
    | "commandRunShell"
    | "jspaceContract"
    | "taskContextCapsule"
  >,
): PromptPayload {
  const messageID = `msg_${options.source}_${randomUUID()}`;
  return {
    messageID,
    parts: [{ id: `part_${options.source}_${randomUUID()}`, type: "text", text: prompt }],
    ...(options.model ? { model: options.model } : {}),
    ...(options.agent ? { agent: options.agent } : {}),
    ...(options.modelVariant
      ? { variant: options.modelVariant, model_variant: options.modelVariant }
      : {}),
    ...(options.modelAccelerationEnabled !== undefined
      ? { model_acceleration_enabled: options.modelAccelerationEnabled }
      : {}),
    ...(options.commandRunShell ? { command_run_shell: options.commandRunShell } : {}),
    ...(options.jspaceContract ? { jspace_contract: options.jspaceContract } : {}),
    ...(options.taskContextCapsule
      ? { task_context_capsule: options.taskContextCapsule }
      : {}),
    source: options.source,
  };
}

export async function waitWithEvents(
  client: GatewayClient,
  session: Session,
  initialCount: number,
  timeoutSec: number,
  human: HumanOutput | undefined,
  ndjson: NdjsonOutput | undefined,
): Promise<RunResult> {
  const controller = new AbortController();
  const idleTimeoutMs = timeoutSec * 1000;
  let deadline = Date.now() + idleTimeoutMs;
  const stream = client.streamEvents(controller.signal)[Symbol.asyncIterator]();
  let candidate: { result: RunResult; signature: string; since: number } | undefined;
  let lastRelevantEventAt = Date.now();
  let lastProgressSignature = "";
  const eventTexts = new Map<string, string>();
  let latestEventText = "";
  try {
    while (Date.now() < deadline) {
      const remaining = Math.max(1, Math.min(1000, deadline - Date.now()));
      const event = await Promise.race([stream.next(), delay(remaining).then(() => undefined)]);
      if (event && !event.done) {
        const normalized = normalizeEvent(event.value);
        const directoryMatches =
          normalized.directory === "global" ||
          sameDirectory(normalized.directory, client.directory);
        if (directoryMatches && (!normalized.sessionID || normalized.sessionID === session.id)) {
          lastRelevantEventAt = Date.now();
          deadline = lastRelevantEventAt + idleTimeoutMs;
          latestEventText = updateEventText(eventTexts, latestEventText, normalized);
          human?.event(normalized);
          ndjson?.event(normalized);
        }
      }
      const completed = await completionResult(client, session.id, initialCount, (signature) => {
        if (signature === lastProgressSignature) return;
        lastProgressSignature = signature;
        deadline = Date.now() + idleTimeoutMs;
      });
      candidate = stableCompletionCandidate(candidate, completed);
      const stableSince = Math.max(candidate?.since ?? 0, lastRelevantEventAt);
      if (candidate && Date.now() - stableSince >= RUN_COMPLETION_STABLE_MS) {
        return resultWithEventText(candidate.result, latestEventText);
      }
    }
  } finally {
    controller.abort();
    await stream.return?.(undefined);
  }
  return detachedResult(client, session.id);
}

function updateEventText(
  texts: Map<string, string>,
  latest: string,
  event: ReturnType<typeof normalizeEvent>,
): string {
  if (event.text === undefined) return latest;
  if (event.type === "message.part.delta") {
    const key = event.messageID && event.partID ? `${event.messageID}\u0000${event.partID}` : "";
    if (!key) return latest;
    const text = `${texts.get(key) ?? ""}${event.text}`;
    texts.set(key, text);
    return text.trim() ? text : latest;
  }
  if (event.type === "message.updated") {
    const key = event.messageID ?? "assistant";
    texts.set(key, event.text);
    return event.text.trim() ? event.text : latest;
  }
  return latest;
}

function resultWithEventText(result: RunResult, eventText: string): RunResult {
  const text = eventText.trim();
  if (!text || text.length < result.finalText.trim().length) return result;
  return { ...result, finalText: text };
}

export async function waitByPolling(
  client: GatewayClient,
  session: Session,
  initialCount: number,
  timeoutSec: number,
): Promise<RunResult> {
  const idleTimeoutMs = timeoutSec * 1000;
  let deadline = Date.now() + idleTimeoutMs;
  let candidate: { result: RunResult; signature: string; since: number } | undefined;
  let lastProgressSignature = "";
  while (Date.now() < deadline) {
    const completed = await completionResult(client, session.id, initialCount, (signature) => {
      if (signature === lastProgressSignature) return;
      lastProgressSignature = signature;
      deadline = Date.now() + idleTimeoutMs;
    });
    candidate = stableCompletionCandidate(candidate, completed);
    if (candidate && Date.now() - candidate.since >= RUN_COMPLETION_STABLE_MS)
      return candidate.result;
    await delay(1000);
  }
  return detachedResult(client, session.id);
}

async function completionResult(
  client: GatewayClient,
  sessionID: string,
  initialCount: number,
  observeProgress?: (signature: string) => void,
): Promise<RunResult | undefined> {
  const session = await client.getSession(sessionID).catch(() => undefined);
  const messages = await client.listMessages(sessionID);
  observeProgress?.(
    JSON.stringify({
      status: sessionStatusText(session?.status),
      count: messages.length,
      lastID: messages.at(-1)?.id,
      lastUpdated: messages.at(-1)?.updated_at ?? messages.at(-1)?.time?.updated,
    }),
  );
  const hasNewAssistant = hasUserFacingAssistantText(messages, initialCount);
  const status = sessionStatusText(session?.status);
  if (status === "busy") return undefined;
  if (status === "error" && hasNewAssistant) {
    return buildRunResult(sessionID, messages, "failed");
  }
  if (status === "idle" && hasNewAssistant) {
    return buildRunResult(sessionID, messages, "completed");
  }
  return undefined;
}

async function detachedResult(client: GatewayClient, sessionID: string): Promise<RunResult> {
  const messages = await client.listMessages(sessionID).catch(() => []);
  return buildRunResult(sessionID, messages, "detached");
}

function stableCompletionCandidate(
  previous: { result: RunResult; signature: string; since: number } | undefined,
  result: RunResult | undefined,
): { result: RunResult; signature: string; since: number } | undefined {
  if (!result) return undefined;
  const signature = JSON.stringify({
    status: result.status,
    finalText: result.finalText,
    count: result.messages.length,
    lastID: result.messages.at(-1)?.id,
    lastUpdated: result.messages.at(-1)?.updated_at,
  });
  if (previous?.signature === signature) return previous;
  return { result, signature, since: Date.now() };
}
