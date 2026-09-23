import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  HostSessionClient,
  loopbackHttpOrigin,
} from "wake-bridge/transport";
import {
  ADAPTER_KIND,
  CHANNEL_SOURCE,
  readChannelState,
} from "./channel.mjs";

export const MAX_HOOK_INPUT_BYTES = 65_536;
export const MAX_HOOK_RESPONSE_BYTES = 65_536;
export const SESSION_START_DEADLINE_MS = 10_000;

const MIN_TOKEN_LENGTH = 32;
const SAFE_TEXT = /^[^\u0000-\u001f\u007f]{1,256}$/u;

class HookError extends Error {
  constructor(message, status = 503) {
    super(message);
    this.name = "HookError";
    this.status = status;
  }
}

function token(value, name, required = true) {
  if (typeof value !== "string" || (required && value.length < MIN_TOKEN_LENGTH)) {
    if (!required && !value) return "";
    throw new Error(`${name} is invalid`);
  }
  return value;
}

function port(value) {
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result < 1 || result > 65_535) throw new Error("channel port is invalid");
  return result;
}

function loopbackOrigin(raw) {
  return loopbackHttpOrigin(raw, "daemon").origin;
}

function safeString(value, max = 256) {
  return typeof value === "string" && value.length > 0 && value.length <= max && SAFE_TEXT.test(value) ? value : null;
}

function redactedError(error) {
  if (error && typeof error === "object") {
    const status = Number(error.status);
    if (Number.isSafeInteger(status) && status >= 400 && status <= 599) return `status=${status}`;
    const code = typeof error.code === "string" && /^[a-z0-9_.-]{1,64}$/u.test(error.code) ? error.code : "error";
    return `code=${code}`;
  }
  return "error";
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
    // This is the only work keeping an independent SessionStart hook alive;
    // an unref'ed timer would let Node exit before the bounded retry window.
  });
}

async function readInput(stream, maxBytes = MAX_HOOK_INPUT_BYTES) {
  let raw = "";
  for await (const chunk of stream) {
    raw += Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk);
    if (Buffer.byteLength(raw) > maxBytes) throw new HookError("hook input is too large", 400);
  }
  return raw;
}

export function parseHookPayload(raw) {
  try {
    const payload = JSON.parse(raw);
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Error("invalid");
    return payload;
  } catch {
    throw new HookError("hook input is invalid", 400);
  }
}

function decodeAttribute(value) {
  return value.replaceAll("&quot;", "\"").replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&");
}

/** Read only the opening channel tag; never forward the prompt itself. */
export function parseChannelTag(prompt) {
  if (typeof prompt !== "string" || !prompt) return null;
  const opening = prompt.match(/^\s*<channel\b([^>]*)>/u);
  if (!opening) return null;
  const attributes = opening[1];
  const source = attributes.match(/\bsource\s*=\s*(["'])(.*?)\1/u)?.[2];
  const deliveryNonce = attributes.match(/\bdelivery_nonce\s*=\s*(["'])(.*?)\1/u)?.[2]
    ?? attributes.match(/\bevent_id\s*=\s*(["'])(.*?)\1/u)?.[2]
    ?? null;
  return {
    source: source == null ? null : decodeAttribute(source),
    delivery_nonce: deliveryNonce == null ? null : decodeAttribute(deliveryNonce),
  };
}

export function observationFromPayload(payload, now = new Date()) {
  const event = safeString(payload?.hook_event_name, 64);
  const sessionId = safeString(payload?.session_id, 256);
  if (!event || !sessionId) throw new HookError("hook event or session is invalid", 400);
  const prompt = typeof payload.prompt === "string" ? payload.prompt : "";
  const tag = parseChannelTag(prompt);
  return {
    observation_id: safeString(payload.observation_id, 256) ?? `${sessionId}:${event}:${randomUUID()}`,
    observed_at: typeof payload.observed_at === "string" ? payload.observed_at : now.toISOString(),
    hook_event_name: event,
    session_id: sessionId,
    lifecycle_source: safeString(payload.source, 128),
    reason: safeString(payload.reason, 256),
    prompt_origin_candidate: tag ? "channel" : prompt ? "human_or_remote" : null,
    channel_source: tag?.source ?? null,
    delivery_nonce: safeString(tag?.delivery_nonce, 256),
    prompt_bytes: prompt ? Buffer.byteLength(prompt) : 0,
  };
}

function responseStatus(error) {
  const status = Number(error?.status);
  return Number.isSafeInteger(status) && status >= 400 && status <= 599 ? status : 503;
}

function sameState(left, right) {
  return left?.session_id === right?.session_id
    && left?.endpoint_id === right?.endpoint_id
    && left?.generation === right?.generation
    && left?.lease_token === right?.lease_token;
}

function removeStateIfSame(path, expected) {
  const current = readChannelState(path);
  if (!sameState(current, expected)) return false;
  try { unlinkSync(path); return true; } catch { return false; }
}

function leaseFromState(state) {
  if (!state || typeof state !== "object"
    || !safeString(state.session_id, 256) || !safeString(state.endpoint_id, 256)
    || !safeString(state.attention_channel, 256) || !Number.isSafeInteger(state.generation) || state.generation < 1
    || typeof state.lease_token !== "string" || state.lease_token.length < MIN_TOKEN_LENGTH) return null;
  return {
    endpoint_id: state.endpoint_id,
    lease_token: state.lease_token,
    attention_channel: state.attention_channel,
    generation: state.generation,
  };
}

function channelRoute(portNumber) {
  return `http://127.0.0.1:${portNumber}`;
}

async function fetchWithTimeout(fetchImpl, url, init, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetchImpl(url, { ...init, redirect: "error", signal: controller.signal });
    const raw = await response.text();
    if (Buffer.byteLength(raw) > MAX_HOOK_RESPONSE_BYTES) throw new HookError("hook response is too large", 502);
    return { response, raw };
  } catch (error) {
    if (error?.name === "AbortError") throw new HookError("hook channel request timed out", 503);
    if (error instanceof HookError) throw error;
    throw new HookError("hook channel request failed", 503);
  } finally {
    clearTimeout(timer);
  }
}

function parseResponseBody(raw) {
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function postHookObservation({ fetchImpl, routeOrigin, routeToken, observation, timeoutMs }) {
  const { response, raw } = await fetchWithTimeout(fetchImpl, `${routeOrigin}/v1/hooks`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${routeToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(observation),
  }, timeoutMs);
  const value = parseResponseBody(raw);
  if (!response.ok) throw new HookError("hook channel rejected observation", response.status);
  return value;
}

async function closeFromState({ statePath, state, client, sessionId }) {
  if (!state || state.session_id !== sessionId) return { closed: false, stale: false, missing: true };
  const lease = leaseFromState(state);
  if (!lease) throw new HookError("saved session state is invalid", 503);
  try {
    await client.close(lease);
    removeStateIfSame(statePath, state);
    return { closed: true, stale: false };
  } catch (error) {
    const status = responseStatus(error);
    // 401/409 means this saved lease is already fenced/expired. Never turn
    // that into a bootstrap close that might revoke a replacement session.
    if (status === 401 || status === 409) return { closed: false, stale: true };
    throw error;
  }
}

async function sessionEnd(options, observation, config) {
  const state = readChannelState(config.statePath);
  // Prefer the live channel so it can clear its in-memory lease and renewal
  // timer. A refused connection means the channel exited; use exact state.
  if (config.routeToken) {
    try {
      const value = await postHookObservation({
        fetchImpl: config.fetchImpl,
        routeOrigin: config.routeOrigin,
        routeToken: config.routeToken,
        observation,
        timeoutMs: config.timeoutMs,
      });
      return { routed: true, response: value };
    } catch (error) {
      // A live channel can reject a stale/nonmatching SessionEnd. Only the
      // exact state snapshot may be used for fallback; otherwise do nothing.
      if (responseStatus(error) === 400) throw error;
    }
  }
  if (!state || state.session_id !== observation.session_id) return { routed: false, missing: true };
  const result = await closeFromState({
    statePath: config.statePath,
    state,
    client: config.client,
    sessionId: observation.session_id,
  });
  return { routed: false, ...result };
}

function makeConfig(options, environment) {
  const routeToken = options.route_token ?? options.routeToken ?? environment.WAKEBRIDGE_CLAUDE_ROUTE_TOKEN
    ?? environment.WB_CLAUDE_CHANNEL_TOKEN ?? "";
  const hostToken = options.host_token ?? options.hostToken ?? environment.WAKEBRIDGE_CLAUDE_HOST_TOKEN ?? "";
  const daemonOrigin = loopbackOrigin(options.daemon_origin ?? options.daemonOrigin
    ?? environment.WAKEBRIDGE_DAEMON_ORIGIN ?? "http://127.0.0.1:4311");
  const channelPort = port(options.channel_port ?? options.channelPort ?? environment.WB_CLAUDE_CHANNEL_PORT ?? "");
  const stateDir = options.state_dir ?? options.stateDir ?? environment.WB_CLAUDE_SESSION_STATE_DIR ?? "";
  if (typeof stateDir !== "string" || !stateDir) throw new Error("WB_CLAUDE_SESSION_STATE_DIR is required");
  const timeoutMs = Number(options.timeout_ms ?? options.timeoutMs ?? 5_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 30_000) throw new Error("timeout_ms is invalid");
  const deadline = Math.min(SESSION_START_DEADLINE_MS, Math.max(100, Number(options.session_start_deadline_ms ?? options.sessionStartDeadlineMs ?? SESSION_START_DEADLINE_MS)));
  const retryDelay = Math.min(1_000, Math.max(1, Number(options.retry_delay_ms ?? options.retryDelayMs ?? 100)));
  if (!Number.isSafeInteger(deadline) || !Number.isSafeInteger(retryDelay)) throw new Error("hook retry timing is invalid");
  if (routeToken) token(routeToken, "WAKEBRIDGE_CLAUDE_ROUTE_TOKEN");
  if (!options.client && !hostToken) token(hostToken, "WAKEBRIDGE_CLAUDE_HOST_TOKEN");
  const fetchImpl = options.fetch ?? fetch;
  const client = options.client ?? new HostSessionClient({ base_url: daemonOrigin, host_token: hostToken, adapter_kind: ADAPTER_KIND, fetch: fetchImpl, timeout_ms: timeoutMs });
  return {
    routeToken,
    hostToken,
    daemonOrigin,
    routeOrigin: channelRoute(channelPort),
    channelPort,
    statePath: join(stateDir, "session.json"),
    timeoutMs,
    deadline,
    retryDelay,
    fetchImpl,
    client,
  };
}

/** Execute one Claude hook JSON document without logging its prompt or tokens. */
export async function handleHookPayload(payload, options = {}) {
  const environment = options.environment ?? process.env;
  const config = makeConfig(options, environment);
  const observation = observationFromPayload(payload);
  if (observation.hook_event_name === "SessionEnd") {
    return sessionEnd(options, observation, config);
  }
  if (!config.routeToken) throw new Error("WAKEBRIDGE_CLAUDE_ROUTE_TOKEN is required");
  const isSessionStart = observation.hook_event_name === "SessionStart";
  const deadline = Date.now() + (isSessionStart ? config.deadline : config.timeoutMs);
  let lastError;
  for (;;) {
    const remaining = Math.max(1, deadline - Date.now());
    try {
      const value = await postHookObservation({
        fetchImpl: config.fetchImpl,
        routeOrigin: config.routeOrigin,
        routeToken: config.routeToken,
        observation,
        timeoutMs: Math.min(config.timeoutMs, remaining),
      });
      if (isSessionStart && (!value || typeof value.endpoint_id !== "string" || !Number.isSafeInteger(value.generation))) {
        throw new HookError("SessionStart response is invalid", 502);
      }
      return value;
    } catch (error) {
      lastError = error;
      if (!isSessionStart || Date.now() >= deadline) throw lastError;
      await sleep(Math.min(config.retryDelay, Math.max(1, deadline - Date.now())));
    }
  }
}

/** Read stdin with a hard bound, then execute the hook event. */
export async function runHook(options = {}) {
  const stdin = options.stdin ?? process.stdin;
  const raw = await readInput(stdin, options.max_input_bytes ?? MAX_HOOK_INPUT_BYTES);
  const payload = parseHookPayload(raw);
  return handleHookPayload(payload, options);
}

async function main() {
  await runHook();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    // Never echo payloads, bearer values, or upstream response bodies.
    try { process.stderr.write(`wakebridge Claude hook failed (${redactedError(error)})\n`); } catch { /* best effort */ }
    process.exitCode = 1;
  });
}
