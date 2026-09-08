import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { createHash, timingSafeEqual } from "node:crypto";
import { WakeBridge, BridgeError } from "./core.js";
import type { BridgeConfig, JsonValue, SourceConnectorConfig, SourceSupervisorStatus } from "./types.js";
import {
  validateEndpointForAdapter,
  type HostAdapter,
  type HostAdapterManifest,
} from "./transport-sdk.js";
import { HostAdapterRegistry, type HostBootstrapCredential } from "./host-adapter-registry.js";
import { WakeBridgeSdkError } from "./sdk-error.js";
import { SourceSupervisor } from "./source-connector.js";
import { validateSourceIngestCredential, type SourceIngestCredential } from "./source-ingress.js";
import { validatePolicyFile } from "./policy-control.js";
import { operatorStatus, retryDeadLetter } from "./operator-control.js";

export interface DaemonOptions {
  host?: string;
  port?: number;
  bridge?: WakeBridge;
  host_adapters?: HostAdapter[];
  host_credentials?: HostBootstrapCredential[];
  /** Set to 0 to disable the scheduler loop (useful for embedding/tests). */
  scheduler_interval_ms?: number;
  source_connectors?: SourceConnectorConfig[];
  source_environment?: NodeJS.ProcessEnv;
  source_fetch?: typeof fetch;
  source_credentials?: SourceIngestCredential[];
  /** Explicit development escape hatch. Only accepted on a loopback bind. */
  unsafe_no_auth?: boolean;
}

function jsonResponse(response: ServerResponse, status: number, value: unknown): void {
  const body = JSON.stringify(value);
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("content-length", Buffer.byteLength(body));
  response.end(body);
}

async function readJson(request: IncomingMessage): Promise<Record<string, any>> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.from(chunk as Uint8Array);
    bytes += buffer.length;
    if (bytes > 1_048_576) throw new BridgeError("request body is too large", "oversized_request", 413);
    chunks.push(buffer);
  }
  if (!chunks.length) return {};
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("JSON object required");
    return value as Record<string, any>;
  } catch (error) {
    throw new BridgeError(`invalid JSON body: ${String(error)}`, "invalid_json", 400);
  }
}

function authorized(request: IncomingMessage, bridge: WakeBridge): boolean {
  const header = request.headers.authorization || request.headers["x-wakebridge-token"];
  const value = Array.isArray(header) ? header[0] : header;
  if (!value || !bridge.config.admin_token) return false;
  const token = value.startsWith("Bearer ") ? value.slice(7) : value;
  return secretEqual(token, bridge.config.admin_token);
}

function bearerValue(request: IncomingMessage): string {
  const header = request.headers.authorization;
  const value = Array.isArray(header) ? header[0] : header;
  return typeof value === "string" && value.startsWith("Bearer ") ? value.slice(7) : "";
}

function secretEqual(actual: string, expected: string): boolean {
  const left = Buffer.from(actual, "utf8");
  const right = Buffer.from(expected, "utf8");
  return left.length > 0 && left.length === right.length && timingSafeEqual(left, right);
}

function hostCredential(request: IncomingMessage, credentials: HostBootstrapCredential[], adapterKind: string): HostBootstrapCredential | null {
  const token = bearerValue(request);
  return credentials.find((credential) => credential.adapter_kind === adapterKind && secretEqual(token, credential.token)) ?? null;
}

function sourceCredential(request: IncomingMessage, credentials: SourceIngestCredential[], source: string): SourceIngestCredential | null {
  const token = bearerValue(request);
  return credentials.find((credential) => credential.source === source && secretEqual(token, credential.token)) ?? null;
}

function loopbackHost(host: string): boolean {
  return host === "127.0.0.1" || host === "::1" || host === "localhost";
}

function validateCredentialSeparation(adminToken: string | undefined, hosts: HostBootstrapCredential[], sources: SourceIngestCredential[]): void {
  const credentials = [
    ...(adminToken ? [{ kind: "owner", id: "admin", token: adminToken }] : []),
    ...hosts.map((credential) => ({ kind: "host", id: credential.id, token: credential.token })),
    ...sources.map((credential) => ({ kind: "source", id: credential.id, token: credential.token })),
  ];
  for (let left = 0; left < credentials.length; left += 1) {
    for (let right = left + 1; right < credentials.length; right += 1) {
      if (secretEqual(credentials[left].token, credentials[right].token)) {
        throw new BridgeError(
          `credential token is reused across credential scopes: ${credentials[left].kind}/${credentials[left].id} and ${credentials[right].kind}/${credentials[right].id}`,
          "credential_scope_collision",
          400,
        );
      }
    }
  }
}

function endpointId(instanceId: string, hostKind: string, sessionRef: string): string {
  return `ep_${createHash("sha256").update(`${instanceId}\0${hostKind}\0${sessionRef}`).digest("hex").slice(0, 32)}`;
}

function hostSessionEndpoint(endpoint: ReturnType<WakeBridge["registerEndpoint"]>): ReturnType<WakeBridge["registerEndpoint"]> {
  return {
    ...endpoint,
    routes: endpoint.routes.map((route) => ({
      ...route,
      address: Object.fromEntries(Object.entries(route.address).map(([key, value]) => [
        key,
        /token|secret|authorization|bearer/i.test(key) ? "[redacted]" : value,
      ])),
    })),
  };
}

function pathParts(url: string): string[] {
  return url.split("?")[0].split("/").filter(Boolean).map((part) => decodeURIComponent(part));
}

/** Start the local management API. It binds loopback by default. */
export async function startDaemon(config: BridgeConfig, options: DaemonOptions = {}): Promise<{ bridge: WakeBridge; server: ReturnType<typeof createServer>; address: AddressInfo | string | null; sources: () => SourceSupervisorStatus[]; adapters: () => HostAdapterManifest[]; close: () => Promise<void> }> {
  const host = options.host ?? "127.0.0.1";
  if (options.unsafe_no_auth && !loopbackHost(host)) {
    throw new BridgeError("unsafe_no_auth is restricted to an explicit loopback bind", "unsafe_daemon_bind", 400);
  }
  if (!config.admin_token && !options.unsafe_no_auth) {
    throw new BridgeError("admin_token is required; use unsafe_no_auth only for an explicit loopback development daemon", "admin_token_required", 400);
  }
  if (config.admin_token && config.admin_token.length < 32) {
    throw new BridgeError("admin_token must contain at least 32 characters", "invalid_admin_token", 400);
  }
  const adapterRegistry = new HostAdapterRegistry(options.host_adapters ?? []);
  const hostCredentials = options.host_credentials ?? [];
  const sourceCredentials = options.source_credentials ?? [];
  if (hostCredentials.length && !config.admin_token) {
    throw new BridgeError("admin_token is required when host bootstrap credentials are enabled", "invalid_host_credential", 400);
  }
  const hostCredentialIds = new Set<string>();
  for (const credential of hostCredentials) {
    if (!credential.id || credential.token.length < 32 || !credential.adapter_kind || !credential.host_kind
      || !Array.isArray(credential.attention_channels) || !credential.attention_channels.length) {
      throw new BridgeError("host bootstrap credential is invalid", "invalid_host_credential", 400);
    }
    if (hostCredentialIds.has(credential.id)) {
      throw new BridgeError(`duplicate host credential id: ${credential.id}`, "invalid_host_credential", 400);
    }
    hostCredentialIds.add(credential.id);
    if (!adapterRegistry.get(credential.adapter_kind)) {
      throw new BridgeError(`host credential references unknown adapter: ${credential.adapter_kind}`, "invalid_host_credential", 400);
    }
  }
  const sourceCredentialIds = new Set<string>();
  for (const credential of sourceCredentials) {
    validateSourceIngestCredential(credential);
    if (sourceCredentialIds.has(credential.id)) {
      throw new BridgeError(`duplicate source credential id: ${credential.id}`, "invalid_source_credential", 400);
    }
    sourceCredentialIds.add(credential.id);
  }
  validateCredentialSeparation(config.admin_token, hostCredentials, sourceCredentials);
  const bridge = options.bridge ?? new WakeBridge(config);
  if (bridge.config.instance_id !== config.instance_id || bridge.config.owner_id !== config.owner_id
    || bridge.config.db_path !== config.db_path || bridge.config.admin_token !== config.admin_token) {
    throw new BridgeError("embedded bridge identity or owner credential does not match daemon config", "daemon_bridge_mismatch", 400);
  }
  const sourceSupervisor = new SourceSupervisor(bridge, options.source_connectors ?? [], options.source_environment, options.source_fetch);
  const server = createServer(async (request, response) => {
    try {
      const method = request.method || "GET";
      const parts = pathParts(request.url || "/");
      const hostSessionRoute = parts[0] === "v1" && parts[1] === "host-sessions";
      if (method === "POST" && hostSessionRoute && parts[2] === "open") {
        const body = await readJson(request);
        const adapterKind = typeof body.adapter_kind === "string" ? body.adapter_kind : "";
        const credential = hostCredential(request, hostCredentials, adapterKind);
        if (!credential) {
          jsonResponse(response, 401, { error: "unauthorized" });
          return;
        }
        const adapter = adapterRegistry.get(adapterKind);
        if (!adapter || !adapter.manifest.host_kinds.includes(credential.host_kind)) {
          throw new BridgeError("host credential does not match adapter manifest", "invalid_host_credential", 400);
        }
        const sessionRef = typeof body.session_ref === "string" ? body.session_ref.trim() : "";
        const attentionChannel = typeof body.attention_channel === "string" ? body.attention_channel.trim() : "";
        const routeAddress = body.route_address;
        if (!sessionRef || sessionRef.length > 256 || !attentionChannel || !credential.attention_channels.includes(attentionChannel)
          || !routeAddress || typeof routeAddress !== "object" || Array.isArray(routeAddress)) {
          throw new BridgeError("host session registration is invalid or outside credential scope", "invalid_host_session", 400);
        }
        const routeToken = typeof routeAddress.token === "string" ? routeAddress.token : "";
        const reservedTokens = [bridge.config.admin_token, ...hostCredentials.map((item) => item.token), ...sourceCredentials.map((item) => item.token)]
          .filter((token): token is string => Boolean(token));
        if (routeToken && reservedTokens.some((token) => secretEqual(routeToken, token))) {
          throw new BridgeError("host delivery route credential must be distinct from owner, host, and source credentials", "credential_scope_collision", 400);
        }
        const registration = {
          id: endpointId(bridge.config.instance_id, credential.host_kind, sessionRef),
          host_kind: credential.host_kind,
          session_ref: sessionRef,
          capabilities: adapter.manifest.capabilities,
          routes: [{ kind: adapter.kind, priority: 100, address: routeAddress }],
        };
        validateEndpointForAdapter(registration, adapter.manifest);
        try {
          adapter.validateEndpoint?.(registration);
        } catch (error) {
          throw new BridgeError(`host session route is invalid: ${String(error)}`, "invalid_host_session", 400);
        }
        const currentGeneration = bridge.getBinding(attentionChannel)?.generation ?? 0;
        const endpoint = bridge.registerEndpoint(registration);
        const binding = bridge.takeover(attentionChannel, endpoint.id, currentGeneration);
        jsonResponse(response, 201, { endpoint: hostSessionEndpoint(endpoint), binding });
        return;
      }
      if (method === "POST" && hostSessionRoute && parts[2] === "close" && parts.length === 3) {
        const body = await readJson(request);
        const adapterKind = typeof body.adapter_kind === "string" ? body.adapter_kind : "";
        const credential = hostCredential(request, hostCredentials, adapterKind);
        if (!credential) {
          jsonResponse(response, 401, { error: "unauthorized" });
          return;
        }
        const sessionRef = typeof body.session_ref === "string" ? body.session_ref.trim() : "";
        const attentionChannel = typeof body.attention_channel === "string" ? body.attention_channel.trim() : "";
        const expectedGeneration = Number(body.expected_generation);
        if (!sessionRef || sessionRef.length > 256 || !credential.attention_channels.includes(attentionChannel)
          || !Number.isSafeInteger(expectedGeneration) || expectedGeneration < 1) {
          throw new BridgeError("host session close is invalid or outside credential scope", "invalid_host_session", 400);
        }
        const id = endpointId(bridge.config.instance_id, credential.host_kind, sessionRef);
        const endpoint = bridge.getEndpoint(id);
        if (!endpoint || endpoint.host_kind !== credential.host_kind || endpoint.session_ref !== sessionRef) {
          throw new BridgeError("host session endpoint is unknown", "endpoint_not_found", 404);
        }
        const revoked = bridge.revokeBoundEndpoint(id, attentionChannel, expectedGeneration);
        jsonResponse(response, 200, { endpoint: hostSessionEndpoint(revoked) });
        return;
      }
      if (method === "POST" && hostSessionRoute && parts[2] === "renew" && parts.length === 3) {
        const body = await readJson(request);
        const adapterKind = typeof body.adapter_kind === "string" ? body.adapter_kind : "";
        const credential = hostCredential(request, hostCredentials, adapterKind);
        if (!credential) {
          jsonResponse(response, 401, { error: "unauthorized" });
          return;
        }
        const sessionRef = typeof body.session_ref === "string" ? body.session_ref.trim() : "";
        const attentionChannel = typeof body.attention_channel === "string" ? body.attention_channel.trim() : "";
        const expectedGeneration = Number(body.expected_generation);
        if (!sessionRef || sessionRef.length > 256 || !credential.attention_channels.includes(attentionChannel)
          || !Number.isSafeInteger(expectedGeneration) || expectedGeneration < 1) {
          throw new BridgeError("host session renewal is invalid or outside credential scope", "invalid_host_session", 400);
        }
        const id = endpointId(bridge.config.instance_id, credential.host_kind, sessionRef);
        const endpoint = bridge.getEndpoint(id);
        if (!endpoint || endpoint.host_kind !== credential.host_kind || endpoint.session_ref !== sessionRef) {
          throw new BridgeError("host session endpoint is unknown", "endpoint_not_found", 404);
        }
        const renewed = bridge.renewBoundEndpoint(id, attentionChannel, expectedGeneration);
        jsonResponse(response, 200, { endpoint: hostSessionEndpoint(renewed), binding: bridge.getBinding(attentionChannel) });
        return;
      }
      if (method === "POST" && hostSessionRoute && parts.length === 4 && ["renew", "close", "presence", "activity"].includes(parts[3])) {
        const body = await readJson(request);
        const leaseToken = bearerValue(request);
        if (!leaseToken) {
          jsonResponse(response, 401, { error: "unauthorized" });
          return;
        }
        if (parts[3] === "renew") {
          jsonResponse(response, 200, { endpoint: hostSessionEndpoint(bridge.renewEndpoint(parts[2], leaseToken)) });
          return;
        }
        if (parts[3] === "close") {
          jsonResponse(response, 200, { endpoint: hostSessionEndpoint(bridge.revokeEndpoint(parts[2], leaseToken)) });
          return;
        }
        if (parts[3] === "presence") {
          jsonResponse(response, 200, { presence: bridge.renewPresence({
            attention_channel: String(body.attention_channel || ""),
            endpoint_id: parts[2],
            generation: Number(body.generation),
            lease_token: leaseToken,
            ttl_ms: body.ttl_ms,
            observed_by: String(body.observed_by || ""),
            observation: String(body.observation || "user_message_accepted"),
          }) });
          return;
        }
        const common = {
          attention_channel: String(body.attention_channel || ""),
          endpoint_id: parts[2],
          generation: Number(body.generation),
          lease_token: leaseToken,
          observation_id: String(body.observation_id || ""),
          observed_at: body.observed_at,
          ttl_ms: body.ttl_ms,
        };
        const activity = body.classification === "wake_echo"
          ? bridge.consumeWakeEcho({ ...common, delivery_nonce: String(body.delivery_nonce || "") })
          : bridge.observeActivity({ ...common, kind: body.kind });
        jsonResponse(response, 200, { activity });
        return;
      }
      const scopedSourceIngress = method === "POST" && parts[0] === "v1" && parts[1] === "ingress" && parts[3] === "events" && parts.length === 4;
      if (scopedSourceIngress) {
        const source = parts[2];
        if (!sourceCredential(request, sourceCredentials, source)) {
          jsonResponse(response, 401, { error: "unauthorized" });
          return;
        }
        const body = await readJson(request);
        const event = (body.event && typeof body.event === "object" && !Array.isArray(body.event) ? body.event : body) as Record<string, any>;
        if (Object.prototype.hasOwnProperty.call(body, "source") || Object.prototype.hasOwnProperty.call(event, "source")) {
          throw new BridgeError("source identity is fixed by the ingest credential and path", "source_identity_override", 400);
        }
        jsonResponse(response, 201, bridge.emitEvent(source, event as any));
        return;
      }
      if (!options.unsafe_no_auth && !authorized(request, bridge)) {
        jsonResponse(response, 401, { error: "unauthorized" });
        return;
      }
      const body = method === "GET" || method === "HEAD" ? {} : await readJson(request);
      if (method === "GET" && parts.join("/") === "health") {
        jsonResponse(response, 200, {
          ok: true,
          instance_id: bridge.config.instance_id,
          owner_id: bridge.config.owner_id,
          now: bridge.nowIso(),
          host_adapters: adapterRegistry.list(),
        });
        return;
      }
      if (method === "GET" && (parts.join("/") === "v1/inspect" || parts.join("/") === "inspect")) {
        jsonResponse(response, 200, bridge.inspect());
        return;
      }
      if (method === "GET" && parts.join("/") === "v1/status") {
        jsonResponse(response, 200, operatorStatus(bridge, {
          sources: sourceSupervisor.list(),
          host_adapters: adapterRegistry.list(),
        }));
        return;
      }
      if (method === "GET" && parts.join("/") === "v1/sources") {
        jsonResponse(response, 200, { sources: sourceSupervisor.list() });
        return;
      }
      if (method === "GET" && parts.join("/") === "v1/policies") {
        jsonResponse(response, 200, { policies: bridge.listPolicies(), active: bridge.listActivePolicies(), status: bridge.listPolicyStatuses() });
        return;
      }
      if (method === "POST" && parts.join("/") === "v1/policies/install") {
        const file = validatePolicyFile(body);
        bridge.installPolicies(file.policies);
        jsonResponse(response, 200, { ok: true, installed: file.policies.map((policy) => ({ id: policy.id, version: policy.version })), active: bridge.listActivePolicies(), status: bridge.listPolicyStatuses() });
        return;
      }
      if (method === "POST" && (parts.join("/") === "v1/policies/test" || parts.join("/") === "v1/policies/preview")) {
        const source = typeof body.source === "string" ? body.source : "";
        const event = body.event;
        if (!source || !event || typeof event !== "object" || Array.isArray(event)) {
          throw new BridgeError("policy evaluation requires source and an event object", "invalid_arguments", 400);
        }
        const candidates = body.policy_file == null ? undefined : validatePolicyFile(body.policy_file).policies;
        jsonResponse(response, 200, parts[2] === "test"
          ? bridge.testPolicy(source, event, candidates)
          : bridge.previewPolicy(source, event, candidates));
        return;
      }
      if (method === "POST" && parts[0] === "v1" && parts[1] === "sources" && parts[3] === "verify") {
        jsonResponse(response, 200, { verification: await sourceSupervisor.verify(parts[2]) });
        return;
      }
      if (method === "POST" && parts[0] === "v1" && parts[1] === "sources" && parts[3] === "bootstrap") {
        if (body.mode !== "from-now") throw new BridgeError("source bootstrap mode must be from-now", "invalid_arguments", 400);
        jsonResponse(response, 200, { verification: await sourceSupervisor.bootstrap(parts[2], {
          subject_ref: String(body.expected_subject_ref || ""),
          binding_fingerprint: String(body.expected_binding_fingerprint || ""),
        }) });
        return;
      }
      if (method === "POST" && parts[0] === "v1" && parts[1] === "sources" && parts[3] === "rebind") {
        const revision = Number(body.expected_checkpoint_revision);
        if (!Number.isSafeInteger(revision) || revision < 1) throw new BridgeError("expected_checkpoint_revision is invalid", "invalid_arguments", 400);
        const reason = typeof body.reason === "string" ? body.reason : "";
        jsonResponse(response, 200, { verification: await sourceSupervisor.rebind(parts[2], revision, {
          subject_ref: String(body.expected_subject_ref || ""),
          binding_fingerprint: String(body.expected_binding_fingerprint || ""),
        }, reason) });
        return;
      }
      if (method === "POST" && parts[0] === "v1" && parts[1] === "sources" && parts[3] === "enable") {
        jsonResponse(response, 200, { source: await sourceSupervisor.enable(parts[2]) });
        return;
      }
      if (method === "POST" && parts[0] === "v1" && parts[1] === "sources" && parts[3] === "disable") {
        jsonResponse(response, 200, { source: await sourceSupervisor.disable(parts[2]) });
        return;
      }
      if (method === "POST" && parts[0] === "v1" && parts[1] === "sources" && parts[3] === "poll") {
        jsonResponse(response, 200, { source: await sourceSupervisor.runOnce(parts[2]) });
        return;
      }
      const collection = parts[0] === "v1" ? parts[1] : parts[0];
      if (method === "GET" && collection === "events") {
        jsonResponse(response, 200, { events: bridge.listEvents({ limit: Number(new URL(request.url || "/", "http://localhost").searchParams.get("limit") || 100) }) });
        return;
      }
      if (method === "GET" && collection === "claims") {
        jsonResponse(response, 200, { claims: bridge.listClaims({ state: new URL(request.url || "/", "http://localhost").searchParams.get("state") as any || undefined, channel: new URL(request.url || "/", "http://localhost").searchParams.get("channel") || undefined }) });
        return;
      }
      if (method === "GET" && collection === "batches") {
        jsonResponse(response, 200, { batches: bridge.listBatches({ channel: new URL(request.url || "/", "http://localhost").searchParams.get("channel") || undefined }) });
        return;
      }
      if (method === "GET" && collection === "receipts") {
        jsonResponse(response, 200, { receipts: bridge.listReceipts(parts[2]) });
        return;
      }
      if (method === "GET" && collection === "bindings") {
        jsonResponse(response, 200, { bindings: bridge.listBindings() });
        return;
      }
      if (method === "GET" && collection === "endpoints") {
        jsonResponse(response, 200, { endpoints: bridge.listEndpoints() });
        return;
      }
      if (method === "GET" && collection === "presence") {
        jsonResponse(response, 200, { presence: bridge.listPresence() });
        return;
      }
      if (method === "GET" && collection === "activity-watches") {
        jsonResponse(response, 200, { activity_watches: bridge.listActivityWatches() });
        return;
      }
      if (method === "GET" && collection === "delivery-correlations") {
        jsonResponse(response, 200, { delivery_correlations: bridge.listDeliveryCorrelations() });
        return;
      }
      if (method === "POST" && collection === "events") {
        const source = String(body.source || "manual");
        const event = (body.event && typeof body.event === "object" ? body.event : body) as any;
        delete event.source;
        jsonResponse(response, 201, bridge.emitEvent(source, event));
        return;
      }
      if (method === "POST" && collection === "tick") {
        jsonResponse(response, 200, bridge.tick());
        return;
      }
      if (method === "POST" && collection === "dispatch") {
        jsonResponse(response, 200, { results: await bridge.dispatchDue() });
        return;
      }
      if (method === "POST" && collection === "batches" && parts[3] === "retry") {
        const expectedAttempt = Number(body.expected_attempt);
        if (!Number.isSafeInteger(expectedAttempt) || expectedAttempt < 1) {
          throw new BridgeError("expected_attempt is invalid", "invalid_arguments", 400);
        }
        jsonResponse(response, 200, retryDeadLetter(bridge, {
          batch_id: parts[2],
          expected_attempt: expectedAttempt,
          reason: typeof body.reason === "string" ? body.reason : "",
        }));
        return;
      }
      if (method === "POST" && collection === "claims" && parts[2] === "schedule") {
        jsonResponse(response, 201, bridge.scheduleClaim(body as any));
        return;
      }
      if (method === "POST" && collection === "claims" && parts[3] === "snooze") {
        jsonResponse(response, 200, { claim: bridge.snoozeClaim(parts[2], String(body.until)) });
        return;
      }
      if (method === "POST" && collection === "claims" && parts[3] === "dismiss") {
        jsonResponse(response, 200, { claim: bridge.dismissClaim(parts[2], String(body.reason || "dismissed")) });
        return;
      }
      if (method === "POST" && collection === "claims" && parts[3] === "consume") {
        jsonResponse(response, 200, { claim: bridge.consumeClaim(parts[2], body.result as JsonValue) });
        return;
      }
      if (method === "POST" && collection === "endpoints" && parts[2] === "register") {
        jsonResponse(response, 201, { endpoint: hostSessionEndpoint(bridge.registerEndpoint(body as any)) });
        return;
      }
      if (method === "POST" && collection === "endpoints" && parts[3] === "renew") {
        jsonResponse(response, 200, { endpoint: hostSessionEndpoint(bridge.renewEndpoint(parts[2], String(body.lease_token), body.lease_ms)) });
        return;
      }
      if (method === "POST" && collection === "takeover") {
        jsonResponse(response, 200, { binding: bridge.takeover(String(body.attention_channel), String(body.endpoint_id), body.expected_generation == null ? undefined : Number(body.expected_generation)) });
        return;
      }
      if (method === "POST" && collection === "presence" && parts[2] === "renew") {
        jsonResponse(response, 200, { presence: bridge.renewPresence(body as any) });
        return;
      }
      if (method === "POST" && collection === "activity" && parts[2] === "observe") {
        jsonResponse(response, 200, { activity: bridge.observeActivity(body as any) });
        return;
      }
      if (method === "POST" && collection === "activity-watches" && parts[2] === "configure") {
        jsonResponse(response, 200, { activity_watch: bridge.configureActivityWatch(body as any) });
        return;
      }
      if (method === "POST" && collection === "receipts" && parts[2] === "record") {
        jsonResponse(response, 201, { receipt: bridge.recordReceipt(String(body.batch_id), body.stage, body.details as any, body.endpoint_id, body.generation == null ? undefined : Number(body.generation)) });
        return;
      }
      jsonResponse(response, 404, { error: "not_found" });
    } catch (error) {
      const contractError = error instanceof WakeBridgeSdkError;
      const status = contractError ? error.status : 500;
      jsonResponse(response, status, { error: error instanceof Error ? error.message : String(error), code: contractError ? error.code : "internal_error" });
    }
  });
  const port = options.port ?? 4311;
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => { server.off("listening", onListening); reject(error); };
    const onListening = () => { server.off("error", onError); resolve(); };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(port, host);
  });
  const address = server.address();
  const daemonOrigin = typeof address === "object" && address
    ? `http://${host.includes(":") ? `[${host}]` : host}:${address.port}`
    : null;
  try {
    await adapterRegistry.start(bridge, { instance_id: bridge.config.instance_id, daemon_origin: daemonOrigin });
  } catch (error) {
    try {
      await adapterRegistry.stop();
    } catch (stopError) {
      process.emitWarning(`wakebridge host adapter cleanup failed: ${String(stopError)}`);
    }
    await new Promise<void>((resolve) => server.close(() => resolve()));
    throw error;
  }
  sourceSupervisor.start();
  const schedulerInterval = options.scheduler_interval_ms ?? 1_000;
  let schedulerTimer: NodeJS.Timeout | undefined;
  let schedulerPromise: Promise<void> | undefined;
  const runScheduler = async (): Promise<void> => {
    bridge.tick();
    await bridge.dispatchDue();
  };
  const triggerScheduler = () => {
    // Keep the one real pass promise authoritative.  An interval tick while
    // dispatch is awaiting a transport must not replace it with an already
    // resolved "skipped" promise, or close() could return while that worker
    // still owns a durable lease.
    if (schedulerPromise) return;
    const pass = runScheduler().catch((error) => {
      // Keep the daemon alive; the durable event/claim/outbox state remains
      // inspectable and the next interval retries the scheduler pass.
      process.emitWarning(`wakebridge scheduler pass failed: ${String(error)}`);
    });
    schedulerPromise = pass;
    void pass.finally(() => {
      if (schedulerPromise === pass) schedulerPromise = undefined;
    });
  };
  if (schedulerInterval > 0) {
    schedulerTimer = setInterval(triggerScheduler, schedulerInterval);
    // The HTTP server itself owns daemon lifetime; this timer must not keep an
    // embedded caller alive after it closes the server.
    schedulerTimer.unref();
    triggerScheduler();
  }
  return {
    bridge,
    server,
    address,
    sources: () => sourceSupervisor.list(),
    adapters: () => adapterRegistry.list(),
    close: async () => {
      let firstError: unknown;
      try {
        await adapterRegistry.stop();
      } catch (error) {
        firstError = error;
      }
      try {
        await sourceSupervisor.stop();
      } catch (error) {
        firstError ??= error;
      }
      if (schedulerTimer) clearInterval(schedulerTimer);
      schedulerTimer = undefined;
      try {
        await schedulerPromise;
      } catch (error) {
        firstError ??= error;
      }
      try {
        await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
      } catch (error) {
        firstError ??= error;
      }
      if (firstError) throw firstError;
    },
  };
}
