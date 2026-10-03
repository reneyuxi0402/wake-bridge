import { readFileSync } from "node:fs";
import { BridgeError } from "./core.js";
import type { HostBootstrapCredential } from "./host-adapter-registry.js";
import { MIN_SECRET_LENGTH } from "./validation.js";
import {
  HOST_ADAPTER_CONTRACT_VERSION,
  LOCAL_HOST_PROTOCOL_VERSION,
  loopbackHttpOrigin,
  routeFor,
  stringAddress,
  validateHostAdapterManifest,
  type HostAdapter,
  type HostAdapterManifest,
  type TransportReceiptUpperBound,
} from "./transport-sdk.js";
import type { EndpointCapabilities, EndpointRegistration, TransportContext, TransportResult } from "./types.js";

const IDENTIFIER = /^[a-z][a-z0-9._-]{0,63}$/u;
const VERSION = /^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$/u;
const TOKEN_ENV = /^[A-Z_][A-Z0-9_]{0,127}$/u;
const PRINTABLE = /^[^\u0000-\u001f\u007f]{1,256}$/u;
const MAX_DECLARATIONS = 32;
const RECEIPT_UPPER_BOUNDS: readonly string[] = ["accepted_to_live_pipe", "host_accepted"];

export interface OutOfProcessHostReference {
  id: string;
  adapter_kind: string;
  adapter_version: string;
  host_kind: string;
  tested_host_versions: string[];
  token_env: string;
  attention_channels: string[];
  capabilities: EndpointCapabilities;
  receipt_upper_bound: Exclude<TransportReceiptUpperBound, "agent_completed">;
  timeout_ms?: number;
}

export interface OutOfProcessHostFile {
  version: 1;
  adapters: OutOfProcessHostReference[];
}

export interface LoadedOutOfProcessHosts {
  adapters: LocalHttpHostAdapter[];
  credentials: HostBootstrapCredential[];
}

function boundedPrintable(values: unknown, field: string, required = false): string[] {
  if (!Array.isArray(values)) {
    throw new BridgeError(`out-of-process host ${field} is missing or not an array of strings`, "invalid_host_adapter_file", 400);
  }
  if (required && values.length === 0) {
    throw new BridgeError(`out-of-process host ${field} must list at least one entry`, "invalid_host_adapter_file", 400);
  }
  if (values.length > MAX_DECLARATIONS) {
    throw new BridgeError(`out-of-process host ${field} must list at most ${MAX_DECLARATIONS} entries`, "invalid_host_adapter_file", 400);
  }
  if (values.some((value) => typeof value !== "string" || !PRINTABLE.test(value))) {
    throw new BridgeError(`out-of-process host ${field} entries must be printable strings of 1 to 256 characters`, "invalid_host_adapter_file", 400);
  }
  return [...values];
}

function requirePattern(value: unknown, field: string, pattern: RegExp): void {
  if (typeof value !== "string" || !pattern.test(value)) {
    throw new BridgeError(`out-of-process host ${field} is missing or does not match ${pattern.source}`, "invalid_host_adapter_file", 400);
  }
}

function normalizedManifest(reference: OutOfProcessHostReference): HostAdapterManifest {
  requirePattern(reference.adapter_kind, "adapter_kind", IDENTIFIER);
  requirePattern(reference.adapter_version, "adapter_version", VERSION);
  requirePattern(reference.host_kind, "host_kind", IDENTIFIER);
  if (!reference.capabilities || typeof reference.capabilities !== "object" || Array.isArray(reference.capabilities)
    || reference.capabilities.exact_live_route !== true || reference.capabilities.requires_live_binding !== true) {
    throw new BridgeError("out-of-process host must declare exact_live_route and requires_live_binding", "invalid_host_adapter_file", 400);
  }
  const receiptUpperBound: unknown = reference.receipt_upper_bound;
  if (typeof receiptUpperBound !== "string" || !RECEIPT_UPPER_BOUNDS.includes(receiptUpperBound)) {
    // HTTP 202 only proves the host accepted the wake, so agent_completed is never offered here.
    const problem = receiptUpperBound === undefined ? "is required"
      : receiptUpperBound === "agent_completed" ? "cannot be agent_completed for an out-of-process host"
        : "is unsupported";
    throw new BridgeError(
      `out-of-process host receipt_upper_bound ${problem}; use accepted_to_live_pipe or host_accepted`,
      "invalid_host_adapter_file",
      400,
    );
  }
  const manifest: HostAdapterManifest = {
    contract_version: HOST_ADAPTER_CONTRACT_VERSION,
    adapter_kind: reference.adapter_kind,
    adapter_version: reference.adapter_version,
    host_kinds: [reference.host_kind],
    support_tier: "experimental",
    tested_host_versions: boundedPrintable(reference.tested_host_versions, "tested_host_versions"),
    capabilities: {
      ...reference.capabilities,
      exact_live_route: true,
      requires_live_binding: true,
      receipt_stages: ["transport_accepted"],
      receipt_upper_bound: reference.receipt_upper_bound,
      support_tier: "experimental",
    },
    receipt_upper_bound: reference.receipt_upper_bound,
  };
  validateHostAdapterManifest(manifest);
  return manifest;
}

export class LocalHttpHostAdapter implements HostAdapter {
  readonly kind: string;
  readonly manifest: HostAdapterManifest;
  readonly capabilities: EndpointCapabilities;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;

  constructor(manifest: HostAdapterManifest, options: { fetch?: typeof fetch; timeout_ms?: number } = {}) {
    this.manifest = validateHostAdapterManifest(manifest);
    this.kind = manifest.adapter_kind;
    this.capabilities = manifest.capabilities;
    const timeout = options.timeout_ms ?? 5_000;
    if (!Number.isSafeInteger(timeout) || timeout < 100 || timeout > 30_000) {
      throw new BridgeError("out-of-process host timeout_ms must be an integer from 100 to 30000", "invalid_host_adapter", 400);
    }
    this.timeoutMs = timeout;
    this.fetchImpl = options.fetch ?? fetch;
  }

  validateEndpoint(registration: EndpointRegistration): void {
    if (!this.manifest.host_kinds.includes(registration.host_kind)) {
      throw new Error("host kind is not declared by the adapter");
    }
    const route = (registration.routes ?? []).find((candidate) => candidate.kind === this.kind);
    if (!route) throw new Error("local host route is missing");
    if (Object.keys(route.address).sort().join(",") !== "base_url,token") {
      throw new Error("local host route accepts only base_url and token");
    }
    loopbackHttpOrigin(stringAddress(route, "base_url"), "local host delivery route");
    if (stringAddress(route, "token").length < MIN_SECRET_LENGTH) throw new Error(`local host route token must contain at least ${MIN_SECRET_LENGTH} characters`);
  }

  async dispatch(context: TransportContext): Promise<TransportResult> {
    if (!this.manifest.host_kinds.includes(context.endpoint.host_kind)) {
      return { accepted: false, retryable: false, error_class: "invalid_endpoint", error_message: "host kind is not declared by the adapter" };
    }
    const route = routeFor(context.endpoint, this.kind);
    if (!route) {
      return { accepted: false, retryable: false, error_class: "invalid_endpoint", error_message: "local host route is missing" };
    }
    let origin: URL;
    let token: string;
    try {
      this.validateEndpoint({ host_kind: context.endpoint.host_kind, session_ref: context.endpoint.session_ref, routes: [route] });
      origin = loopbackHttpOrigin(stringAddress(route, "base_url"), "local host delivery route");
      token = stringAddress(route, "token");
    } catch (error) {
      return { accepted: false, retryable: false, error_class: "invalid_endpoint", error_message: String(error) };
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    timeout.unref();
    try {
      const response = await this.fetchImpl(new URL("v1/wakes", origin), {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
          "x-wakebridge-delivery-nonce": context.delivery_nonce,
        },
        body: JSON.stringify({
          protocol_version: LOCAL_HOST_PROTOCOL_VERSION,
          attempt_id: context.attempt_id,
          delivery_nonce: context.delivery_nonce,
          wake: context.payload,
        }),
        redirect: "error",
        signal: controller.signal,
      });
      const status = response.status;
      try { await response.body?.cancel(); } catch { /* response bodies are deliberately ignored */ }
      if (status === 202) {
        return {
          accepted: true,
          transport_kind: this.kind,
          receipt_details: {
            protocol_version: LOCAL_HOST_PROTOCOL_VERSION,
            receipt_upper_bound: this.manifest.receipt_upper_bound,
            host_status: status,
          },
        };
      }
      const retryable = status === 408 || status === 425 || status === 429 || status >= 500;
      return {
        accepted: false,
        retryable,
        error_class: retryable ? "host_temporarily_unavailable" : "host_rejected",
        error_message: `local host returned HTTP ${status}`,
      };
    } catch (error) {
      return {
        accepted: false,
        retryable: true,
        error_class: error instanceof Error && error.name === "AbortError" ? "host_timeout" : "host_unreachable",
        error_message: String(error),
      };
    } finally {
      clearTimeout(timeout);
    }
  }
}

export function loadOutOfProcessHostFile(
  path: string,
  environment: NodeJS.ProcessEnv = process.env,
): LoadedOutOfProcessHosts {
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new BridgeError(`out-of-process host file is unreadable: ${String(error)}`, "invalid_host_adapter_file", 400);
  }
  const file = parsed as Partial<OutOfProcessHostFile>;
  if (file?.version !== 1 || !Array.isArray(file.adapters) || !file.adapters.length || file.adapters.length > MAX_DECLARATIONS) {
    throw new BridgeError("out-of-process host file must use version 1 and contain adapters", "invalid_host_adapter_file", 400);
  }
  const ids = new Set<string>();
  const kinds = new Set<string>();
  const adapters: LocalHttpHostAdapter[] = [];
  const declarations: Array<{ reference: OutOfProcessHostReference; attention_channels: string[] }> = [];
  for (const reference of file.adapters) {
    if (!reference || typeof reference !== "object" || Array.isArray(reference)) {
      throw new BridgeError("out-of-process host adapters entries must be objects", "invalid_host_adapter_file", 400);
    }
    requirePattern(reference.id, "id", IDENTIFIER);
    requirePattern(reference.token_env, "token_env", TOKEN_ENV);
    if (ids.has(reference.id) || kinds.has(reference.adapter_kind)) {
      throw new BridgeError("out-of-process host ids and adapter kinds must be unique", "invalid_host_adapter_file", 400);
    }
    ids.add(reference.id);
    kinds.add(reference.adapter_kind);
    const attentionChannels = boundedPrintable(reference.attention_channels, "attention_channels", true);
    adapters.push(new LocalHttpHostAdapter(normalizedManifest(reference), { timeout_ms: reference.timeout_ms }));
    declarations.push({ reference, attention_channels: attentionChannels });
  }
  // Tokens are checked only after the whole file is valid, so doctor reports file
  // mistakes even before the daemon environment file has been loaded.
  const credentials = declarations.map(({ reference, attention_channels }): HostBootstrapCredential => {
    const token = environment[reference.token_env];
    if (!token || token.length < MIN_SECRET_LENGTH) {
      throw new BridgeError(`host credential environment variable is missing or too short: ${reference.token_env}`, "host_credential_unavailable", 400);
    }
    return { id: reference.id, token, adapter_kind: reference.adapter_kind, host_kind: reference.host_kind, attention_channels };
  });
  return { adapters, credentials };
}
