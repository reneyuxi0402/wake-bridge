import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, statSync, writeFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Writable } from "node:stream";
import { afterEach, test } from "node:test";
import { handleHookPayload } from "../src/hook.mjs";
import { startChannel } from "../src/channel.mjs";

const ROUTE_TOKEN = "route-token-0123456789abcdef0123456789";
const LEASE_TOKEN = "lease-token-0123456789abcdef0123456789";
const HOST_TOKEN = "host-token-0123456789abcdef0123456789";

const resources = new Set();
const stateDirs = new Set();

function outputSink({ delay = 0, error = null } = {}) {
  const messages = [];
  const stream = new Writable({
    write(chunk, encoding, callback) {
      try {
        if (error) return callback(error);
        messages.push(JSON.parse(String(chunk)));
        if (delay) return setTimeout(callback, delay);
        callback();
      } catch (caught) {
        callback(caught);
      }
    },
  });
  return { stream, messages };
}

function textSink() {
  const chunks = [];
  const stream = new Writable({
    write(chunk, encoding, callback) {
      chunks.push(String(chunk));
      callback();
    },
  });
  return { stream, text: () => chunks.join("") };
}

function makeClient({ generation = 1, renew = null, openError = null } = {}) {
  const calls = [];
  const client = {
    async open(input) {
      calls.push({ method: "open", input });
      if (openError) throw openError;
      return {
        endpoint: {
          id: `ep-${input.session_ref}`,
          lease_token: LEASE_TOKEN,
          lease_expires_at: new Date(Date.now() + 60_000).toISOString(),
        },
        binding: { attention_channel: input.attention_channel, generation },
      };
    },
    lease(registration) {
      return {
        endpoint_id: registration.endpoint.id,
        lease_token: registration.endpoint.lease_token,
        attention_channel: registration.binding.attention_channel,
        generation: registration.binding.generation,
      };
    },
    async renew(lease) {
      calls.push({ method: "renew", lease });
      if (renew) return renew(lease);
      return { id: lease.endpoint_id, lease_expires_at: new Date(Date.now() + 60_000).toISOString() };
    },
    async close(lease) { calls.push({ method: "close", lease }); },
    async renewPresence(lease, input) { calls.push({ method: "presence", lease, input }); },
    async observeActivity(lease, input) { calls.push({ method: "activity", lease, input }); },
    async consumeWakeEcho(lease, input) { calls.push({ method: "echo", lease, input }); },
  };
  return { client, calls };
}

async function createChannel(options = {}) {
  const stateDir = options.state_dir ?? mkdtempSync(join(tmpdir(), "wakebridge-channel-test-"));
  stateDirs.add(stateDir);
  const sink = options.sink ?? outputSink();
  const stderr = textSink();
  const channel = await startChannel({
    port: 0,
    route_token: ROUTE_TOKEN,
    attention_channel: "life",
    state_dir: stateDir,
    stdout: sink.stream,
    stderr: stderr.stream,
    client: options.client,
    renew_interval_ms: options.renew_interval_ms,
    timeout_ms: options.timeout_ms,
  });
  resources.add(channel);
  return { channel, stateDir, sink, stderr };
}

async function request(channel, path, body, headers = {}) {
  const response = await fetch(`${channel.routeOrigin}${path}`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${ROUTE_TOKEN}`,
      "content-type": "application/json",
      ...headers,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  await response.arrayBuffer();
  return response;
}

function delivery(generation = 1, attempt = "attempt-one", nonce = "n".repeat(40)) {
  return {
    protocol_version: 1,
    attempt_id: attempt,
    delivery_nonce: nonce,
    wake: {
      schema_version: 1,
      instance_id: "instance",
      wake_batch_id: `batch-${attempt}`,
      attention_channel: "life",
      claim_refs: [],
      binding_generation: generation,
    },
  };
}

async function initialize(channel) {
  await channel._handleRpc({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-06-18" } });
  await channel._handleRpc({ jsonrpc: "2.0", method: "notifications/initialized" });
}

test("channel negotiates only its implemented protocol revision", async () => {
  const { client } = makeClient();
  const { channel, sink } = await createChannel({ client });
  await channel._handleRpc({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2026-07-28" } });
  assert.equal(sink.messages[0].result.protocolVersion, "2025-06-18");
});

afterEach(async () => {
  await Promise.all([...resources].map((channel) => channel.close()));
  resources.clear();
  for (const directory of stateDirs) rmSync(directory, { recursive: true, force: true });
  stateDirs.clear();
});

test("channel fences readiness/auth/body/generation and writes concurrent duplicate delivery once", async () => {
  const { client } = makeClient();
  const sink = outputSink({ delay: 10 });
  const { channel, stateDir, stderr } = await createChannel({ client, sink });
  const body = delivery();
  const nonceHeader = { "x-wakebridge-delivery-nonce": body.delivery_nonce };

  assert.equal((await request(channel, "/v1/wakes", body, nonceHeader)).status, 425);
  assert.equal((await fetch(`${channel.routeOrigin}/v1/wakes`, { method: "POST", headers: { authorization: "Bearer wrong" }, body: "{}" })).status, 401);
  await channel._handleRpc({ jsonrpc: "2.0", id: 2, method: "initialize", params: {} });
  assert.equal((await request(channel, "/v1/wakes", body, nonceHeader)).status, 425);
  await channel._handleRpc({ jsonrpc: "2.0", method: "notifications/initialized" });
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "session-one" })).status, 200);

  const state = JSON.parse(readFileSync(join(stateDir, "session.json"), "utf8"));
  assert.equal(state.session_id, "session-one");
  assert.equal(statSync(join(stateDir, "session.json")).mode & 0o777, 0o600);
  const stale = delivery(2, "stale-generation", "x".repeat(40));
  assert.equal((await request(channel, "/v1/wakes", stale, { "x-wakebridge-delivery-nonce": stale.delivery_nonce })).status, 409);
  assert.equal((await request(channel, "/v1/wakes", body, nonceHeader)).status, 202);
  const duplicate = await Promise.all([
    request(channel, "/v1/wakes", body, nonceHeader),
    request(channel, "/v1/wakes", body, nonceHeader),
  ]);
  assert.deepEqual(duplicate.map((response) => response.status), [202, 202]);
  assert.equal(sink.messages.filter((value) => value.method === "notifications/claude/channel").length, 1);
  const notification = sink.messages.find((value) => value.method === "notifications/claude/channel");
  assert.equal(notification.params.meta.binding_generation, "1");
  assert.ok(Object.values(notification.params.meta).every((value) => typeof value === "string"));
  assert.equal(notification.params.content.startsWith("<channel"), false);
  assert.doesNotMatch(stderr.text(), new RegExp(LEASE_TOKEN));
});

test("channel distinguishes verified wake echo from channel-shaped spoof and settles only known turns", async () => {
  const { client, calls } = makeClient();
  const { channel } = await createChannel({ client });
  await initialize(channel);
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "session-two" })).status, 200);

  const wake = delivery(1, "echo-attempt", "e".repeat(40));
  assert.equal((await request(channel, "/v1/wakes", wake, { "x-wakebridge-delivery-nonce": wake.delivery_nonce })).status, 202);
  assert.equal((await request(channel, "/v1/hooks", {
    hook_event_name: "UserPromptSubmit", session_id: "session-two", prompt_origin_candidate: "channel",
    channel_source: "other-channel", delivery_nonce: wake.delivery_nonce,
  })).status, 204);
  assert.equal(calls.filter((call) => call.method === "presence").length, 0);
  assert.equal((await request(channel, "/v1/hooks", {
    hook_event_name: "UserPromptSubmit", session_id: "session-two", prompt_origin_candidate: "channel",
    channel_source: "wakebridge_channel", delivery_nonce: wake.delivery_nonce,
  })).status, 204);
  assert.equal(calls.filter((call) => call.method === "echo").length, 1);
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "Stop", session_id: "session-two" })).status, 204);
  assert.equal(calls.find((call) => call.method === "activity")?.input.kind, "wake_settled");
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "Stop", session_id: "session-two" })).status, 204);
  assert.equal(calls.filter((call) => call.method === "activity").length, 1);
});

test("failed renewal disables delivery", async () => {
  const { client } = makeClient({ renew: async () => { throw Object.assign(new Error("secret"), { status: 409 }); } });
  const { channel } = await createChannel({ client, renew_interval_ms: 100 });
  await initialize(channel);
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "session-three" })).status, 200);
  await new Promise((resolve) => setTimeout(resolve, 150));
  const wake = delivery();
  assert.equal((await request(channel, "/v1/wakes", wake, { "x-wakebridge-delivery-nonce": wake.delivery_nonce })).status, 425);

  const failing = outputSink({ error: new Error(LEASE_TOKEN) });
  channel.stdout = failing.stream;
  const wakeTwo = delivery(1, "disconnected", "d".repeat(40));
  assert.equal((await request(channel, "/v1/wakes", wakeTwo, { "x-wakebridge-delivery-nonce": wakeTwo.delivery_nonce })).status, 425);
  assert.equal(failing.messages.length, 0);
});

test("stdout callback errors and a callback that never resolves return bounded 503", async () => {
  const failing = outputSink({ error: new Error(LEASE_TOKEN) });
  const first = makeClient();
  const running = await createChannel({ client: first.client, timeout_ms: 100 });
  await initialize(running.channel);
  running.channel.stdout = failing.stream;
  assert.equal((await request(running.channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "stdout-error" })).status, 200);
  const firstWake = delivery(1, "stdout-error", "q".repeat(40));
  assert.equal((await request(running.channel, "/v1/wakes", firstWake, { "x-wakebridge-delivery-nonce": firstWake.delivery_nonce })).status, 503);

  const never = new Writable({ write() { /* simulate a host pipe that never invokes its callback */ } });
  const second = makeClient();
  const blocked = await createChannel({ client: second.client, timeout_ms: 100 });
  await initialize(blocked.channel);
  blocked.channel.stdout = never;
  assert.equal((await request(blocked.channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "stdout-timeout" })).status, 200);
  const secondWake = delivery(1, "stdout-timeout", "t".repeat(40));
  const started = Date.now();
  assert.equal((await request(blocked.channel, "/v1/wakes", secondWake, { "x-wakebridge-delivery-nonce": secondWake.delivery_nonce })).status, 503);
  assert.ok(Date.now() - started < 1_000);
  // A delayed pipe error after the timeout is still handled and redacted.
  never.destroy(new Error(LEASE_TOKEN));
  await new Promise(resolve => setImmediate(resolve));
  assert.ok(!running.stderr.text().includes(LEASE_TOKEN));
  assert.ok(!blocked.stderr.text().includes(LEASE_TOKEN));
});

test("a fenced process cannot re-register after takeover/lease loss", async () => {
  let renewCalls = 0;
  const { client, calls } = makeClient({
    renew: async () => {
      renewCalls += 1;
      throw Object.assign(new Error("fenced"), { status: 409 });
    },
  });
  const { channel } = await createChannel({ client, renew_interval_ms: 100 });
  await initialize(channel);
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "old-session" })).status, 200);
  await new Promise((resolve) => setTimeout(resolve, 150));
  assert.equal(renewCalls, 1);
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "old-session" })).status, 409);
  assert.equal((await request(channel, "/v1/hooks", { hook_event_name: "SessionStart", session_id: "new-session" })).status, 409);
  assert.equal(calls.filter((call) => call.method === "open").length, 1);
});

test("SessionEnd fallback closes the exact saved lease and leaves stale state fenced", async () => {
  const stateDir = mkdtempSync(join(tmpdir(), "wakebridge-hook-state-"));
  stateDirs.add(stateDir);
  const statePath = join(stateDir, "session.json");
  const state = {
    schema_version: 1,
    session_id: "fallback-session",
    endpoint_id: "ep-fallback",
    attention_channel: "life",
    generation: 7,
    lease_expires_at: new Date(Date.now() + 60_000).toISOString(),
    lease_token: LEASE_TOKEN,
  };
  writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
  let closed;
  const client = { async close(lease) { closed = lease; } };
  const failedFetch = async () => { throw new Error(HOST_TOKEN); };
  await handleHookPayload({ hook_event_name: "SessionEnd", session_id: "fallback-session" }, {
    client,
    fetch: failedFetch,
    route_token: ROUTE_TOKEN,
    host_token: HOST_TOKEN,
    daemon_origin: "http://127.0.0.1:4311",
    channel_port: 43991,
    state_dir: stateDir,
  });
  assert.deepEqual(closed, {
    endpoint_id: "ep-fallback", lease_token: LEASE_TOKEN, attention_channel: "life", generation: 7,
  });
  assert.equal(existsSync(statePath), false);

  writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
  const staleClient = { async close() { throw Object.assign(new Error(HOST_TOKEN), { status: 409 }); } };
  const result = await handleHookPayload({ hook_event_name: "SessionEnd", session_id: "fallback-session" }, {
    client: staleClient, fetch: failedFetch, route_token: ROUTE_TOKEN, host_token: HOST_TOKEN,
    daemon_origin: "http://127.0.0.1:4311", channel_port: 43991, state_dir: stateDir,
  });
  assert.equal(result.stale, true);
  assert.equal(existsSync(statePath), true);
});
