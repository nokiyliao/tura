import type { NormalizedEvent } from "../types/event.js";
import type { ChildAdmissionReceipt, RunResult } from "../types/session.js";

export class NdjsonOutput {
  started(value: { sessionID: string; prompt: string }): void {
    process.stdout.write(`${JSON.stringify({ type: "cli.started", ...value })}\n`);
  }

  event(event: NormalizedEvent): void {
    process.stdout.write(
      `${JSON.stringify({ type: event.type, sessionID: event.sessionID, messageID: event.messageID, status: event.status, text: event.text, raw: event.raw })}\n`,
    );
  }

  childAdmitted(receipt: ChildAdmissionReceipt): void {
    process.stdout.write(
      `${JSON.stringify({ type: "cli.child_admitted", childAdmission: receipt })}\n`,
    );
  }

  completed(result: RunResult): void {
    const type = result.status === "detached" ? "cli.detached" : "cli.completed";
    process.stdout.write(`${JSON.stringify({ type, ...result })}\n`);
  }

  failed(sessionID: string | undefined, error: unknown): void {
    const message = error instanceof Error ? error.message : String(error);
    process.stdout.write(`${JSON.stringify({ type: "cli.failed", sessionID, error: message })}\n`);
  }
}
