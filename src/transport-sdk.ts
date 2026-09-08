import { WakeBridgeSdkError } from "./sdk-error.js";
import type {
  ActivityKind,
  ActivityObservationResult,
  Binding,
  Endpoint,
  EndpointCapabilities,
  EndpointRegistration,
  EndpointRoute,
  JsonValue,
  PresenceLease,
  WakePayload,
  WakeTransport,
} from "./types.js";

export const HOST_ADAPTER_CONTRACT_VERSION = 1;
export const SUPPORTED_HOST_ADAPTER_CONTRACT_VERSIONS = [HOST_ADAPTER_CONTRACT_VERSION] as const;
export const LOCAL_HOST_PROTOCOL_VERSION = 1 as const;

export type HostAdapterSupportTier = "mock" | "experimental" | "supported";
export type TransportReceiptUpperBound = "accepted_to_live_pipe" | "host_accepted" | "agent_completed";

export interface HostAdapterManifest {
  contract_version: typeof HOST_ADAPTER_CONTRACT_VERSION;
  adapter_kind: string;
  adapter_version: string;
  host_kinds: string[];
  support_tier: HostAdapterSupportTier;
  tested_host_versions: string[];
  capabilities: EndpointCapabilities;
  receipt_upper_bound: TransportReceiptUpperBound;
}

export interface HostAdapterLifecycleContext {
  /** Non-secret instance fence. No Core, database, or owner-control object is exposed. */
  instance_id: string;
  daemon_origin: string | null;
}

export interface HostAdapter extends WakeTransport {
  readonly manifest: HostAdapterManifest;
  validateEndpoint?(registration: EndpointRegistration): void;
  start?(context: HostAdapterLifecycleContext): Promise<void> | void;
  stop?(): Promise<void> | void;
}

const IDENTIFIER = /^[a-z][a-z0-9._-]{0,63}$/u;
const PRINTABLE_REF = /^[^\u0000-\u001f\u007f]{1,256}$/u;
const TOKEN_MIN_LENGTH = 32;
const MAX_RESPONSE_BYTES = 65_536;

function nonEmptyStrings(values: unknown): values is string[] {
  return Array.isArray(values) && values.length > 0 && values.every((value) => typeof value === "string" && value.trim());
}

export function validateHostAdapterManifest(manifest: HostAdapterManifest): HostAdapterManifest {
  if (manifest.contract_version !== HOST_ADAPTER_CONTRACT_VERSION) {
    throw new WakeBridgeSdkError("unsupported host adapter contract version", "invalid_host_adapter", 400);
  }
  if (!IDENTIFIER.test(manifest.adapter_kind) || !manifest.adapter_version.trim()) {
    throw new WakeBridgeSdkError("host adapter kind or version is invalid", "invalid_host_adapter", 400);
  }
  if (!nonEmptyStrings(manifest.host_kinds)) {
    throw new WakeBridgeSdkError("host adapter must declare host_kinds", "invalid_host_adapter", 400);
  }
  if (!["mock", "experimental", "supported"].includes(manifest.support_tier)) {
    throw new WakeBridgeSdkError("host adapter support tier is invalid", "invalid_host_adapter", 400);
  }
  if (!Array.isArray(manifest.tested_host_versions)) {
    throw new WakeBridgeSdkError("tested_host_versions must be an array", "invalid_host_adapter", 400);
  }
  if (!manifest.capabilities || typeof manifest.capabilities !== "object") {
    throw new WakeBridgeSdkError("host adapter capabilities are required", "invalid_host_adapter", 400);
  }
  if (!["accepted_to_live_pipe", "host_accepted", "agent_completed"].includes(manifest.receipt_upper_bound)) {
    throw new WakeBridgeSdkError("host adapter receipt upper bound is invalid", "invalid_host_adapter", 400);
  }
  return manifest;
}

export function routeFor(endpoint: { routes: EndpointRoute[] }, kind: string): EndpointRoute | null {
  return [...endpoint.routes]
    .sort((left, right) => (right.priority ?? 0) - (left.priority ?? 0))
    .find((route) => route.kind === kind) ?? null;
}

export function stringAddress(route: EndpointRoute, key: string): string {
  const value = route.address[key] as JsonValue | undefined;
  if (typeof value !== "string" || !value.trim()) {
    throw new WakeBridgeSdkError(`transport route ${key} is required`, "invalid_endpoint", 400);
  }
  return value;
}

export function loopbackHttpOrigin(raw: string, label = "host route"): URL {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new WakeBridgeSdkError(`${label} origin is invalid`, "invalid_host_origin", 400);
  }
  if (url.protocol !== "http:" || !["127.0.0.1", "[::1]"].includes(url.hostname) || url.username || url.password
    || url.pathname !== "/" || url.search || url.hash) {
    throw new WakeBridgeSdkError(`${label} must be a credential-free loopback HTTP origin`, "invalid_host_origin", 400);
  }
  return url;
}

export interface LocalHostDeliveryRequest {
  protocol_version: typeof LOCAL_HOST_PROTOCOL_VERSION;
  attempt_id: string;
  delivery_nonce: string;
  wake: WakePayload;
}

export function validateLocalHostDeliveryRequest(value: unknown): LocalHostDeliveryRequest {
  const request = value as Partial<LocalHostDeliveryRequest>;
  if (request?.protocol_version !== LOCAL_HOST_PROTOCOL_VERSION
    || typeof request.attempt_id !== "string" || !request.attempt_id || request.attempt_id.length > 200
    || typeof request.delivery_nonce !== "string" || request.delivery_nonce.length < TOKEN_MIN_LENGTH || request.delivery_nonce.length > 256
    || !request.wake || request.wake.schema_version !== 1
    || typeof request.wake.wake_batch_id !== "string" || !request.wake.wake_batch_id
    || typeof request.wake.attention_channel !== "string" || !request.wake.attention_channel
    || !Number.isSafeInteger(request.wake.binding_generation) || request.wake.binding_generation < 1
    || !Array.isArray(request.wake.claim_refs)) {
    throw new WakeBridgeSdkError("local host delivery request is invalid", "invalid_host_delivery", 400);
  }
  return request as LocalHostDeliveryRequest;
}

export interface HostSessionRegistration {
  endpoint: Endpoint & { lease_token: string };
  binding: Binding;
}

export interface HostSessionClientOptions {
  base_url: string;
  host_token: string;
  adapter_kind: string;
  fetch?: typeof fetch;
  timeout_ms?: number;
}

export interface HostSessionOpenInput {
  session_ref: string;
  attention_channel: string;
  route_origin: string;
  route_token: string;
}

export interface HostSessionLease {
  endpoint_id: string;
  lease_token: string;
  attention_channel: string;
  generation: number;
}

export interface HostActivityInput {
  observation_id: string;
  kind: ActivityKind;
  observed_at?: string;
  ttl_ms?: number;
}

export interface HostPresenceInput {
  observed_by: string;
  observation?: string;
  ttl_ms?: number;
}

export interface HostWakeEchoInput {
  observation_id: string;
  delivery_nonce: string;
  observed_at?: string;
  ttl_ms?: number;
}

/** Narrow loopback client for an out-of-process host bridge. It has no owner-control methods. */
export class HostSessionClient {
  private readonly origin: URL;
  private readonly hostToken: string;
  private readonly adapterKind: string;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;

  constructor(options: HostSessionClientOptions) {
    this.origin = loopbackHttpOrigin(options.base_url, "Wake Bridge daemon");
    if (typeof options.host_token !== "string" || options.host_token.length < TOKEN_MIN_LENGTH) {
      throw new WakeBridgeSdkError("host bootstrap token must contain at least 32 characters", "invalid_host_credential", 400);
    }
    if (!IDENTIFIER.test(options.adapter_kind)) {
      throw new WakeBridgeSdkError("adapter_kind is invalid", "invalid_host_adapter", 400);
    }
    const timeout = options.timeout_ms ?? 5_000;
    if (!Number.isSafeInteger(timeout) || timeout < 100 || timeout > 30_000) {
      throw new WakeBridgeSdkError("timeout_ms must be an integer from 100 to 30000", "invalid_arguments", 400);
    }
    this.hostToken = options.host_token;
    this.adapterKind = options.adapter_kind;
    this.fetchImpl = options.fetch ?? fetch;
    this.timeoutMs = timeout;
  }

  async open(input: HostSessionOpenInput): Promise<HostSessionRegistration> {
    if (!PRINTABLE_REF.test(input.session_ref) || !PRINTABLE_REF.test(input.attention_channel)) {
      throw new WakeBridgeSdkError("host session_ref or attention_channel is invalid", "invalid_host_session", 400);
    }
    const routeOrigin = loopbackHttpOrigin(input.route_origin, "host delivery route").origin;
    if (typeof input.route_token !== "string" || input.route_token.length < TOKEN_MIN_LENGTH) {
      throw new WakeBridgeSdkError("host delivery route token must contain at least 32 characters", "invalid_host_session", 400);
    }
    if (input.route_token === this.hostToken) {
      throw new WakeBridgeSdkError("credential_scope_collision: host bootstrap and delivery route tokens must be distinct", "credential_scope_collision", 400);
    }
    const value = await this.post("v1/host-sessions/open", this.hostToken, {
      adapter_kind: this.adapterKind,
      session_ref: input.session_ref,
      attention_channel: input.attention_channel,
      route_address: { base_url: `${routeOrigin}/`, token: input.route_token },
    }) as HostSessionRegistration;
    if (!value?.endpoint?.id || !value.endpoint.lease_token || !Number.isSafeInteger(value?.binding?.generation)) {
      throw new WakeBridgeSdkError("Wake Bridge returned an invalid host session", "invalid_host_session_response", 502);
    }
    return value;
  }

  lease(registration: HostSessionRegistration): HostSessionLease {
    return {
      endpoint_id: registration.endpoint.id,
      lease_token: registration.endpoint.lease_token,
      attention_channel: registration.binding.attention_channel,
      generation: registration.binding.generation,
    };
  }

  async renew(lease: HostSessionLease): Promise<Endpoint> {
    const value = await this.post(`v1/host-sessions/${encodeURIComponent(lease.endpoint_id)}/renew`, lease.lease_token, {});
    return (value as { endpoint: Endpoint }).endpoint;
  }

  async close(lease: HostSessionLease): Promise<Endpoint> {
    const value = await this.post(`v1/host-sessions/${encodeURIComponent(lease.endpoint_id)}/close`, lease.lease_token, {});
    return (value as { endpoint: Endpoint }).endpoint;
  }

  async renewPresence(lease: HostSessionLease, input: HostPresenceInput): Promise<PresenceLease> {
    const value = await this.post(`v1/host-sessions/${encodeURIComponent(lease.endpoint_id)}/presence`, lease.lease_token, {
      attention_channel: lease.attention_channel,
      generation: lease.generation,
      observed_by: input.observed_by,
      observation: input.observation,
      ttl_ms: input.ttl_ms,
    });
    return (value as { presence: PresenceLease }).presence;
  }

  async observeActivity(lease: HostSessionLease, input: HostActivityInput): Promise<ActivityObservationResult> {
    const value = await this.post(`v1/host-sessions/${encodeURIComponent(lease.endpoint_id)}/activity`, lease.lease_token, {
      attention_channel: lease.attention_channel,
      generation: lease.generation,
      observation_id: input.observation_id,
      observed_at: input.observed_at,
      ttl_ms: input.ttl_ms,
      classification: "activity",
      kind: input.kind,
    });
    return (value as { activity: ActivityObservationResult }).activity;
  }

  async consumeWakeEcho(lease: HostSessionLease, input: HostWakeEchoInput): Promise<ActivityObservationResult> {
    const value = await this.post(`v1/host-sessions/${encodeURIComponent(lease.endpoint_id)}/activity`, lease.lease_token, {
      attention_channel: lease.attention_channel,
      generation: lease.generation,
      observation_id: input.observation_id,
      observed_at: input.observed_at,
      ttl_ms: input.ttl_ms,
      classification: "wake_echo",
      delivery_nonce: input.delivery_nonce,
    });
    return (value as { activity: ActivityObservationResult }).activity;
  }

  private async post(path: string, token: string, body: unknown): Promise<unknown> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    timeout.unref();
    try {
      const response = await this.fetchImpl(new URL(path, this.origin), {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify(body),
        redirect: "error",
        signal: controller.signal,
      });
      const raw = await response.text();
      if (Buffer.byteLength(raw) > MAX_RESPONSE_BYTES) {
        throw new WakeBridgeSdkError("Wake Bridge host response is too large", "oversized_host_response", 502);
      }
      let value: unknown = {};
      try { value = raw ? JSON.parse(raw) : {}; } catch { /* handled below */ }
      if (!response.ok) {
        const message = value && typeof value === "object" && "code" in value ? String((value as { code: unknown }).code) : `HTTP ${response.status}`;
        throw new WakeBridgeSdkError(`Wake Bridge host request failed: ${message}`, "host_session_request_failed", response.status);
      }
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw new WakeBridgeSdkError("Wake Bridge host response is invalid", "invalid_host_session_response", 502);
      }
      return value;
    } catch (error) {
      if (error instanceof WakeBridgeSdkError) throw error;
      throw new WakeBridgeSdkError(
        error instanceof Error && error.name === "AbortError" ? "Wake Bridge host request timed out" : "Wake Bridge host request failed",
        error instanceof Error && error.name === "AbortError" ? "host_session_timeout" : "host_session_unreachable",
        503,
      );
    } finally {
      clearTimeout(timeout);
    }
  }
}

export function validateEndpointForAdapter(registration: EndpointRegistration, manifest: HostAdapterManifest): void {
  validateHostAdapterManifest(manifest);
  if (!manifest.host_kinds.includes(registration.host_kind)) {
    throw new WakeBridgeSdkError("endpoint host kind is not supported by adapter", "invalid_endpoint", 400);
  }
  if (!(registration.routes ?? []).some((route) => route.kind === manifest.adapter_kind)) {
    throw new WakeBridgeSdkError("endpoint has no route for adapter", "invalid_endpoint", 400);
  }
}

export type {
  ActivityKind,
  ActivityObservationResult,
  Binding,
  Endpoint,
  EndpointCapabilities,
  EndpointRegistration,
  EndpointRoute,
  JsonPrimitive,
  JsonValue,
  TransportContext,
  TransportResult,
  WakePayload,
  WakeTransport,
} from "./types.js";
export { WakeBridgeSdkError } from "./sdk-error.js";
