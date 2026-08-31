export type JsonObject = Record<string, unknown>;

export interface CliContext {
  gatewayUrl: string;
  /** True when the gateway URL was explicitly chosen (flag or TURA_GATEWAY_URL),
   * so a reachable Tura gateway there is reused without the root identity check. */
  gatewayUrlExplicit: boolean;
  cwd: string;
  json: boolean;
  color: ColorMode;
  display: DisplayMode;
  language?: "zh-CN" | "en";
  initialSessionId?: string;
  verbose: boolean;
  mock: boolean;
  dev: boolean;
}

export type ColorMode = "auto" | "always" | "never";
export type DisplayMode = "auto" | "plain" | "rich";

export type OutputMode = "text" | "json" | "ndjson";

export class CliUsageError extends Error {
  exitCode = 2;
}

export class GatewayUnavailableError extends Error {
  exitCode = 5;
}

export class TimeoutError extends Error {
  exitCode = 4;
}

export class RuntimeTerminalizationError extends Error {
  code = "TURA_RUNTIME_TERMINAL_FAILURE";
  exitCode = 1;

  constructor(sessionID: string) {
    super(`TURA_RUNTIME_TERMINAL_FAILURE: session=${sessionID}`);
  }
}

export class ChildRequestValidationError extends CliUsageError {
  code = "TURA_CHILD_REQUEST_INVALID";

  constructor(detail: string) {
    super(
      `${detail.startsWith("TURA_CHILD_REQUEST_INVALID") ? detail : `TURA_CHILD_REQUEST_INVALID:${detail}`}`,
    );
  }
}

export class ChildRequestAuthorityError extends CliUsageError {
  code = "TURA_CHILD_REQUEST_AUTHORITY_CONFLICT";

  constructor(source: string) {
    super(`TURA_CHILD_REQUEST_AUTHORITY_CONFLICT:${source}`);
  }
}

export class ChildAdmissionIdentityError extends Error {
  code = "TURA_CHILD_ADMISSION_IDENTITY_MISMATCH";
  exitCode = 1;

  constructor(field: string, expected: string, actual: unknown) {
    super(
      `TURA_CHILD_ADMISSION_IDENTITY_MISMATCH:${field}:expected=${expected},actual=${String(actual)}`,
    );
  }
}
