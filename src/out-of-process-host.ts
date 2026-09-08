import { readFileSync } from "node:fs";
import { BridgeError } from "./core.js";
import type { HostBootstrapCredential } from "./host-adapter-registry.js";
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
  if (!Array.isArray(values) || values.length > MAX_DECLARATIONS || (required && values.length === 0)
    || values.some((value) => typeof value !== "string" || !PRINTABLE.test(value))) {
    throw new BridgeError(`out-of-process host ${field} are invalid`, "invalid_host_adapter_file", 400);
  }
  return [...values];
}

function normalizedManifest(reference: OutOfProcessHostReference): HostAdapterManifest {
  if (!IDENTIFIER.test(reference.adapter_kind || "") || !VERSION.test(reference.adapter_version || "")
    || !IDENTIFIER.test(reference.host_kind || "")) {
    throw new BridgeError("out-of-process host adapter identity is invalid", "invalid_host_adapter_file", 400);
  }
  if (!reference.capabilities || typeof reference.capabilities !== "object" || Array.isArray(reference.capabilities)
    || reference.capabilities.exact_live_route !== true || reference.capabilities.requires_live_binding !== true) {
    throw new BridgeError("out-of-process host must declare exact_live_route and requires_live_binding", "invalid_host_adapter_file", 400);
  }
  if (!["accepted_to_live_pipe", "host_accepted"].includes(reference.receipt_upper_bound)) {
    throw new BridgeError("out-of-process host receipt upper bound is unsupported", "invalid_host_adapter_file", 400);
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
    if (stringAddress(route, "token").length < 32) throw new Error("local host route token must contain at least 32 characters");
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
  const credentials: HostBootstrapCredential[] = [];
  for (const reference of file.adapters) {
    if (!reference || !IDENTIFIER.test(reference.id || "") || !TOKEN_ENV.test(reference.token_env || "")) {
      throw new BridgeError("out-of-process host credential reference is invalid", "invalid_host_adapter_file", 400);
    }
    if (ids.has(reference.id) || kinds.has(reference.adapter_kind)) {
      throw new BridgeError("out-of-process host ids and adapter kinds must be unique", "invalid_host_adapter_file", 400);
    }
    ids.add(reference.id);
    kinds.add(reference.adapter_kind);
    const token = environment[reference.token_env];
    if (!token || token.length < 32) {
      throw new BridgeError(`host credential environment variable is missing or too short: ${reference.token_env}`, "host_credential_unavailable", 400);
    }
    const attentionChannels = boundedPrintable(reference.attention_channels, "attention_channels", true);
    const manifest = normalizedManifest(reference);
    adapters.push(new LocalHttpHostAdapter(manifest, { timeout_ms: reference.timeout_ms }));
    credentials.push({
      id: reference.id,
      token,
      adapter_kind: reference.adapter_kind,
      host_kind: reference.host_kind,
      attention_channels: attentionChannels,
    });
  }
  return { adapters, credentials };
}
