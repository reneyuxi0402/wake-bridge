import { createServer } from "node:http";
import { Readable } from "node:stream";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ReferenceHostAdapter } from "../examples/reference-host-adapter/adapter.mjs";
import { WakeBridgeSdkError } from "wake-bridge/transport";

const HOST_TOKEN = "reference-host-bootstrap-token-0123456789abcdef";
const ROUTE_TOKEN = "reference-host-route-token-0123456789abcdef";
const LEASE_TOKEN = "reference-host-lease-token-0123456789abcdef";
const servers = new Set();
const adapters = new Set();

afterEach(async () => {
  await Promise.all([...adapters].map((adapter) => adapter.close()));
  adapters.clear();
  await Promise.all([...servers].map((server) => new Promise((resolve) => server.close(resolve))));
  servers.clear();
});

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return `http://127.0.0.1:${server.address().port}`;
}

function isolatedAdapter(acceptWake) {
  const adapter = new ReferenceHostAdapter({
    daemonOrigin: "http://127.0.0.1:4311",
    hostToken: HOST_TOKEN,
    routeToken: ROUTE_TOKEN,
    sessionRef: "exact-session-one",
    attentionChannel: "life",
    acceptWake,
  });
  adapter.registration = {
    endpoint: { id: "ep_reference", instance_id: "reference-instance", lease_token: LEASE_TOKEN },
    binding: { attention_channel: "life", generation: 1 },
  };
  return adapter;
}

function deliver(adapter, attemptId = "attempt-one", wakeOverrides = {}) {
  const delivery = {
    protocol_version: 1,
    attempt_id: attemptId,
    delivery_nonce: "reference-delivery-nonce-0123456789abcdef",
    wake: {
      schema_version: 1, instance_id: "reference-instance", wake_batch_id: "wb_reference",
      attention_channel: "life", claim_refs: [], binding_generation: 1,
      ...wakeOverrides,
    },
  };
  const request = Readable.from([Buffer.from(JSON.stringify(delivery))]);
  Object.assign(request, {
    method: "POST", url: "/v1/wakes",
    headers: { authorization: `Bearer ${ROUTE_TOKEN}`, "x-wakebridge-delivery-nonce": delivery.delivery_nonce },
  });
  const statuses = [];
  const response = { writeHead(status) { statuses.push(status); return this; }, end() {} };
  return { statuses, done: adapter.handleRequest(request, response) };
}

function sharedAdapter(acceptWake = vi.fn()) {
  const adapter = isolatedAdapter(acceptWake);
  adapter.registration = {
    endpoint: adapter.registration.endpoint,
    bindings: [
      { attention_channel: "life", generation: 1 },
      { attention_channel: "work", generation: 7 },
    ],
  };
  adapter.leases = adapter.client.leases(adapter.registration);
  adapter.lease = adapter.leases[0];
  adapter.client.open = vi.fn();
  adapter.client.openChannels = vi.fn();
  adapter.client.renew = vi.fn().mockResolvedValue({});
  adapter.client.close = vi.fn().mockResolvedValue({});
  return adapter;
}

function channelFailure(remoteCode) {
  const failure = new WakeBridgeSdkError("request failed", "host_session_request_failed", 409);
  failure.remote_code = remoteCode;
  return failure;
}

describe("reference Host Adapter example", () => {
  it.each([
    ["presence", "stale_generation"], ["activity", "stale_generation"],
    ["presence", "channel_deleted"], ["activity", "channel_deleted"],
  ])("retires only the failed channel after %s returns %s", async (operation, remoteCode) => {
    const accepted = vi.fn();
    const adapter = sharedAdapter(accepted);
    const failure = channelFailure(remoteCode);
    adapter.client.renewPresence = vi.fn().mockResolvedValue({});
    adapter.client.observeActivity = vi.fn().mockResolvedValue({});
    const method = operation === "presence" ? "renewPresence" : "observeActivity";
    adapter.client[method].mockRejectedValueOnce(failure);
    const logging = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      const failed = operation === "presence" ? adapter.noteUserPresence("old-life", "life")
        : adapter.noteSessionActivity("old-life", "activity_started", "life");
      await expect(failed).rejects.toBe(failure);
      expect(adapter.leases.map((lease) => lease.attention_channel)).toEqual(["work"]);
      expect(adapter.lease).toBe(adapter.leases[0]);
      expect(adapter.leaseLost).toBe(false);
      await expect(adapter.noteUserPresence("later-life", "life")).rejects.toThrow("not registered");
      await expect(adapter.noteSessionActivity("later-life", "activity_started", "life")).rejects.toThrow("not registered");
      const rejected = deliver(adapter);
      await rejected.done;
      expect(rejected.statuses).toEqual([409]);
      await adapter.noteUserPresence("user-work", "work");
      await adapter.noteSessionActivity("turn-work", "activity_started", "work");
      await adapter.renewLease();
      expect(adapter.client.renew).toHaveBeenCalledWith(adapter.leases[0]);
      const delivered = deliver(adapter, "attempt-work", { attention_channel: "work", binding_generation: 7 });
      await delivered.done;
      expect(delivered.statuses).toEqual([202]);
      expect(accepted).toHaveBeenCalledTimes(1);
      expect(adapter.client.open).not.toHaveBeenCalled();
      expect(adapter.client.openChannels).not.toHaveBeenCalled();
      await adapter.close();
      expect(adapter.client.close).toHaveBeenCalledWith(expect.objectContaining({ attention_channel: "work" }));
      expect(logging).toHaveBeenCalledWith("[reference-host-adapter] channel registration lost; presence, activity and delivery stopped for that channel. Explicit registration is required to restore it.\n");
    } finally { logging.mockRestore(); }
  });

  it.each([1, 2])("ignores a late channel failure for a replaced handle at generation %s", async (generation) => {
    const adapter = sharedAdapter();
    let rejectOld;
    adapter.client.renewPresence = vi.fn().mockImplementationOnce(() => new Promise((_resolve, reject) => { rejectOld = reject; }));
    const pending = adapter.noteUserPresence("old-life", "life");
    const replacement = { ...adapter.leases[0], generation };
    adapter.leases[0] = replacement;
    adapter.lease = replacement;
    adapter.registration.bindings[0] = { attention_channel: "life", generation };
    const failure = channelFailure("stale_generation");
    rejectOld(failure);
    await expect(pending).rejects.toBe(failure);
    expect(adapter.leases[0]).toBe(replacement);
    expect(adapter.registration.bindings[0].generation).toBe(generation);
    const delivered = deliver(adapter, "new-life", { binding_generation: generation });
    await delivered.done;
    expect(delivered.statuses).toEqual([202]);
  });

  it("preserves the original late observation error after close", async () => {
    const adapter = sharedAdapter();
    let rejectOld;
    adapter.client.observeActivity = vi.fn().mockImplementationOnce(() => new Promise((_resolve, reject) => { rejectOld = reject; }));
    const pending = adapter.noteSessionActivity("old-life", "activity_started", "life");
    await adapter.close();
    const failure = channelFailure("stale_generation");
    rejectOld(failure);
    await expect(pending).rejects.toBe(failure);
    expect(adapter.registration).toBeUndefined();
    expect(adapter.leases).toEqual([]);
  });

  it("also retires a single-channel registration binding", async () => {
    const accepted = vi.fn();
    const adapter = isolatedAdapter(accepted);
    adapter.lease = adapter.client.lease(adapter.registration);
    adapter.leases = [adapter.lease];
    const failure = channelFailure("stale_generation");
    adapter.client.observeActivity = vi.fn().mockRejectedValue(failure);
    const logging = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      await expect(adapter.noteSessionActivity("old-life")).rejects.toBe(failure);
      expect(adapter.registration.bindings).toEqual([]);
      expect(adapter.registration.binding).toBeUndefined();
      const rejected = deliver(adapter);
      await rejected.done;
      expect(rejected.statuses).toEqual([409]);
      expect(accepted).not.toHaveBeenCalled();
    } finally { logging.mockRestore(); }
  });

  it("does not retire channel handles for unconfirmed observation failures", async () => {
    const adapter = sharedAdapter();
    const registration = adapter.registration;
    const leases = adapter.leases;
    adapter.client.renewPresence = vi.fn().mockRejectedValue(new WakeBridgeSdkError("unreachable", "host_session_unreachable", 503));
    adapter.client.observeActivity = vi.fn().mockRejectedValue(channelFailure("other_conflict"));
    await expect(adapter.noteUserPresence("retry", "life")).rejects.toThrow("unreachable");
    await expect(adapter.noteSessionActivity("retry", "activity_started", "life")).rejects.toHaveProperty("remote_code", "other_conflict");
    expect(adapter.registration).toBe(registration);
    expect(adapter.leases).toBe(leases);
  });

  it("rejects all delivery and stops renewing when no channels remain, without closing or reopening the endpoint", async () => {
    const adapter = sharedAdapter();
    adapter.client.renewPresence = vi.fn().mockRejectedValue(channelFailure("stale_generation"));
    const logging = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      for (const channel of ["life", "work"]) {
        await expect(adapter.noteUserPresence("gone", channel)).rejects.toHaveProperty("remote_code", "stale_generation");
      }
      expect(adapter.leases).toEqual([]);
      expect(adapter.lease).toBeUndefined();
      for (const [channel, generation] of [["life", 1], ["work", 7]]) {
        const delivery = deliver(adapter, `gone-${channel}`, { attention_channel: channel, binding_generation: generation });
        await delivery.done;
        expect(delivery.statuses).toEqual([409]);
      }
      await expect(adapter.noteUserPresence("later", "life")).rejects.toThrow("no active channel registrations");
      await adapter.renewLease();
      await adapter.close();
      expect(adapter.client.renew).not.toHaveBeenCalled();
      expect(adapter.client.close).not.toHaveBeenCalled();
      expect(adapter.client.open).not.toHaveBeenCalled();
      expect(adapter.client.openChannels).not.toHaveBeenCalled();
    } finally { logging.mockRestore(); }
  });

  it("keeps a registration after a temporary renewal failure", async () => {
    const adapter = isolatedAdapter(vi.fn());
    adapter.lease = { endpoint_id: "ep_reference", lease_token: LEASE_TOKEN, attention_channel: "life", generation: 1 };
    const registration = adapter.registration;
    adapter.client.renew = vi.fn().mockRejectedValueOnce(new WakeBridgeSdkError("unreachable", "host_session_unreachable", 503)).mockResolvedValueOnce({});
    const logging = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      await adapter.renewLease();
      expect(adapter.leaseLost).toBe(false);
      expect(adapter.registration).toBe(registration);
      await adapter.renewLease();
      expect(adapter.client.renew).toHaveBeenCalledTimes(2);
      expect(adapter.registration).toBe(registration);
    } finally { logging.mockRestore(); }
  });

  it("stops renewing and accepting after the endpoint token is fenced, without reopening", async () => {
    const accepted = vi.fn();
    const adapter = isolatedAdapter(accepted);
    adapter.lease = { endpoint_id: "ep_reference", lease_token: LEASE_TOKEN, attention_channel: "life", generation: 1 };
    adapter.leases = [adapter.lease];
    const failure = new WakeBridgeSdkError("request failed", "host_session_request_failed", 409);
    failure.remote_code = "endpoint_fenced";
    adapter.client.renew = vi.fn().mockRejectedValue(failure);
    adapter.client.open = vi.fn();
    const logging = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      await adapter.renewLease();
      expect(adapter.leaseLost).toBe(true);
      expect(adapter.registration).toBeUndefined();
      expect(adapter.leases).toEqual([]);
      await adapter.renewLease();
      expect(adapter.client.renew).toHaveBeenCalledTimes(1);
      expect(adapter.client.open).not.toHaveBeenCalled();
      const rejected = deliver(adapter);
      await rejected.done;
      expect(rejected.statuses).toEqual([409]);
      expect(accepted).not.toHaveBeenCalled();
      await expect(adapter.noteUserPresence("later", "life")).rejects.toThrow("endpoint lease lost");
    } finally { logging.mockRestore(); }
  });

  it("returns retryable 425 while the registration response is still pending", async () => {
    const accepted = vi.fn();
    const adapter = isolatedAdapter(accepted);
    const registration = adapter.registration;
    // The daemon may commit/open and dispatch before its HTTP response reaches start().
    adapter.registration = undefined;
    const early = deliver(adapter);
    await early.done;
    expect(early.statuses).toEqual([425]);
    expect(accepted).not.toHaveBeenCalled();
    adapter.registration = registration;
    const retry = deliver(adapter);
    await retry.done;
    expect(retry.statuses).toEqual([202]);
    expect(accepted).toHaveBeenCalledTimes(1);
  });

  it("does not return 202 before the host acceptance promise resolves", async () => {
    let release;
    let entered;
    const accepting = new Promise((resolve) => { release = resolve; });
    const called = new Promise((resolve) => { entered = resolve; });
    const adapter = isolatedAdapter(async () => { entered(); await accepting; });
    const request = deliver(adapter);
    await called;
    expect(request.statuses).toEqual([]);
    release();
    await request.done;
    expect(request.statuses).toEqual([202]);
  });

  it("returns 503 for rejected acceptance and allows a later attempt to accept", async () => {
    let calls = 0;
    const adapter = isolatedAdapter(async () => {
      if (++calls === 1) throw new Error("host did not accept ownership");
    });
    const rejected = deliver(adapter);
    await rejected.done;
    expect(rejected.statuses).toEqual([503]);
    const retry = deliver(adapter);
    await retry.done;
    expect(retry.statuses).toEqual([202]);
    expect(calls).toBe(2);
  });

  it("dedupes only in one adapter object, not across restart or new attempt ids", async () => {
    const deliveries = [];
    const acceptWake = async (delivery) => { deliveries.push(delivery); };
    const first = isolatedAdapter(acceptWake);
    await deliver(first).done;
    await deliver(first).done;
    expect(deliveries).toHaveLength(1);

    // A fresh process/object has no durable attempt memory, even for the same id.
    const restarted = isolatedAdapter(acceptWake);
    await deliver(restarted).done;
    // Core retries use new ids: even the same object cannot dedupe by batch.
    await deliver(restarted, "attempt-two").done;
    expect(deliveries.map((delivery) => delivery.attempt_id)).toEqual(["attempt-one", "attempt-one", "attempt-two"]);
    expect(new Set(deliveries.map((delivery) => delivery.wake.wake_batch_id)).size).toBe(1);
  });

  it("opens, accepts an attempt once, reports trusted edges, and closes", async () => {
    const lifecycle = [];
    const daemon = createServer(async (request, response) => {
      let raw = "";
      for await (const chunk of request) raw += String(chunk);
      lifecycle.push({ url: request.url, token: request.headers.authorization, body: raw ? JSON.parse(raw) : null });
      const json = (status, body) => {
        response.writeHead(status, { "content-type": "application/json" });
        response.end(JSON.stringify(body));
      };
      if (request.url === "/v1/host-sessions/open") {
        json(201, {
          endpoint: { id: "ep_reference", instance_id: "reference-instance", lease_token: LEASE_TOKEN },
          binding: { attention_channel: "life", generation: 1 },
        });
      } else if (request.url.endsWith("/presence")) {
        json(200, { presence: { endpoint_id: "ep_reference", binding_generation: 1 } });
      } else if (request.url.endsWith("/activity")) {
        json(200, { activity: { duplicate: false } });
      } else if (request.url.endsWith("/close")) {
        json(200, { endpoint: { id: "ep_reference" } });
      } else if (request.url.endsWith("/renew")) {
        json(200, { endpoint: { id: "ep_reference" } });
      } else {
        json(404, { error: "not_found" });
      }
    });
    servers.add(daemon);
    const daemonOrigin = await listen(daemon);
    const deliveries = [];
    const adapter = new ReferenceHostAdapter({
      daemonOrigin,
      hostToken: HOST_TOKEN,
      routeToken: ROUTE_TOKEN,
      sessionRef: "exact-session-one",
      attentionChannel: "life",
      acceptWake: async (delivery) => deliveries.push(delivery),
    });
    adapters.add(adapter);
    const started = await adapter.start();

    const body = {
      protocol_version: 1,
      attempt_id: "attempt-one",
      delivery_nonce: "reference-delivery-nonce-0123456789abcdef",
      wake: {
        schema_version: 1,
        instance_id: "reference-instance",
        wake_batch_id: "wb_reference",
        attention_channel: "life",
        claim_refs: [],
        binding_generation: 1,
      },
    };
    const send = () => fetch(`${started.route_origin}/v1/wakes`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${ROUTE_TOKEN}`,
        "content-type": "application/json",
        "x-wakebridge-delivery-nonce": body.delivery_nonce,
      },
      body: JSON.stringify(body),
    });
    expect((await send()).status).toBe(202);
    expect((await send()).status).toBe(202);
    expect(deliveries).toHaveLength(1);
    expect(deliveries[0]).toMatchObject({ session_ref: "exact-session-one", attempt_id: "attempt-one" });

    await adapter.noteUserPresence("user-message-one");
    await adapter.noteSessionActivity("turn-one");
    await adapter.close();
    adapters.delete(adapter);

    expect(lifecycle.map((entry) => entry.url)).toEqual(expect.arrayContaining([
      "/v1/host-sessions/open",
      "/v1/host-sessions/ep_reference/presence",
      "/v1/host-sessions/ep_reference/activity",
      "/v1/host-sessions/ep_reference/close",
    ]));
    expect(lifecycle.find((entry) => entry.url === "/v1/host-sessions/open").body).toMatchObject({
      adapter_kind: "reference_host",
      session_ref: "exact-session-one",
      attention_channel: "life",
    });
    expect(lifecycle.find((entry) => entry.url.endsWith("/activity")).body)
      .toMatchObject({ kind: "activity_started", observation_id: "turn-one" });
  });

  it("registers two channels once, fences delivery, and targets trusted observations explicitly", async () => {
    const lifecycle = [];
    const daemon = createServer(async (request, response) => {
      let raw = "";
      for await (const chunk of request) raw += String(chunk);
      const body = JSON.parse(raw);
      lifecycle.push({ url: request.url, token: request.headers.authorization, body });
      response.writeHead(request.url === "/v1/host-sessions/open" ? 201 : 200, { "content-type": "application/json" });
      response.end(JSON.stringify(request.url === "/v1/host-sessions/open" ? {
        endpoint: { id: "ep_shared", instance_id: "reference-instance", lease_token: LEASE_TOKEN },
        bindings: [
          { endpoint_id: "ep_shared", attention_channel: "life", generation: 2 },
          { endpoint_id: "ep_shared", attention_channel: "work", generation: 7 },
        ],
      } : { endpoint: { id: "ep_shared" }, presence: {}, activity: {} }));
    });
    servers.add(daemon);
    const deliveries = [];
    const adapter = new ReferenceHostAdapter({
      daemonOrigin: await listen(daemon), hostToken: HOST_TOKEN, routeToken: ROUTE_TOKEN,
      sessionRef: "exact-session-one", attentionChannels: ["life", "work"],
      acceptWake: async (delivery) => deliveries.push(delivery),
    });
    adapters.add(adapter);
    await adapter.start();
    for (const [channel, generation] of [["life", 2], ["work", 7]]) {
      const delivery = deliver(adapter, `attempt-${channel}`, { attention_channel: channel, binding_generation: generation });
      await delivery.done;
      expect(delivery.statuses).toEqual([202]);
    }
    for (const mismatch of [
      { attention_channel: "other", binding_generation: 2 },
      { attention_channel: "life", binding_generation: 7 },
      { attention_channel: "work", binding_generation: 2 },
      { instance_id: "another-instance", binding_generation: 2 },
    ]) {
      // Even a previously accepted attempt must pass current registration fencing.
      const delivery = deliver(adapter, "attempt-life", mismatch);
      await delivery.done;
      expect(delivery.statuses).toEqual([409]);
    }
    expect(deliveries.map((delivery) => [delivery.session_ref, delivery.wake.attention_channel]))
      .toEqual([["exact-session-one", "life"], ["exact-session-one", "work"]]);
    await expect(adapter.noteUserPresence("ambiguous")).rejects.toThrow("attentionChannel is required");
    await expect(adapter.noteSessionActivity("ambiguous")).rejects.toThrow("attentionChannel is required");
    await expect(adapter.noteUserPresence("unknown", "other")).rejects.toThrow("not registered");
    await adapter.noteUserPresence("user-work", "work");
    await adapter.noteSessionActivity("turn-life", "activity_started", "life");
    await adapter.close();
    await adapter.close();
    const opens = lifecycle.filter((entry) => entry.url === "/v1/host-sessions/open");
    expect(opens).toHaveLength(1);
    expect(opens[0].body).toMatchObject({ session_ref: "exact-session-one", attention_channels: ["life", "work"] });
    expect(opens[0].body).not.toHaveProperty("attention_channel");
    expect(lifecycle.find((entry) => entry.url.endsWith("/presence"))).toMatchObject({
      token: `Bearer ${LEASE_TOKEN}`, body: { attention_channel: "work", generation: 7, observation: "user-work" },
    });
    expect(lifecycle.find((entry) => entry.url.endsWith("/activity")).body)
      .toMatchObject({ attention_channel: "life", generation: 2, observation_id: "turn-life", kind: "activity_started" });
    expect(lifecycle.filter((entry) => entry.url.endsWith("/close"))).toHaveLength(1);
  });

  it("renews the shared endpoint only once per interval", async () => {
    const adapter = new ReferenceHostAdapter({
      daemonOrigin: "http://127.0.0.1:4311", hostToken: HOST_TOKEN, routeToken: ROUTE_TOKEN,
      sessionRef: "exact-session-one", attentionChannels: ["life", "work"], acceptWake: async () => {},
    });
    const registration = {
      endpoint: { id: "ep_shared", instance_id: "reference-instance", lease_token: LEASE_TOKEN },
      bindings: [
        { endpoint_id: "ep_shared", attention_channel: "life", generation: 2 },
        { endpoint_id: "ep_shared", attention_channel: "work", generation: 7 },
      ],
    };
    adapter.client.openChannels = vi.fn().mockResolvedValue(registration);
    adapter.client.renew = vi.fn().mockResolvedValue({});
    adapter.client.close = vi.fn().mockResolvedValue({});
    adapters.add(adapter);
    vi.useFakeTimers();
    try {
      await adapter.start();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(adapter.client.renew).toHaveBeenCalledTimes(1);
      expect(adapter.client.renew).toHaveBeenCalledWith(expect.objectContaining({ endpoint_id: "ep_shared", attention_channel: "life" }));
      await adapter.close();
      expect(adapter.client.close).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects conflicting or malformed channel configuration before starting", () => {
    const options = {
      daemonOrigin: "http://127.0.0.1:4311", hostToken: HOST_TOKEN, routeToken: ROUTE_TOKEN,
      sessionRef: "exact-session-one", acceptWake: async () => {},
    };
    expect(() => new ReferenceHostAdapter({ ...options, attentionChannel: "life", attentionChannels: ["work"] }))
      .toThrow("not both");
    for (const attentionChannels of [[], ["life", "life"], [""], "life", [1]]) {
      expect(() => new ReferenceHostAdapter({ ...options, attentionChannels })).toThrow("attentionChannels");
    }
    for (const [value, extraEnv, expected] of [
      ["life,work", {}, "must be a JSON array"],
      ['["life","life"]', {}, "must be a JSON array"],
      ['["life","work"]', { WAKEBRIDGE_ATTENTION_CHANNEL: "life" }, "not both"],
    ]) {
      const env = { ...process.env, WAKEBRIDGE_ATTENTION_CHANNELS: value, ...extraEnv };
      if (!extraEnv.WAKEBRIDGE_ATTENTION_CHANNEL) delete env.WAKEBRIDGE_ATTENTION_CHANNEL;
      const result = spawnSync(process.execPath, ["examples/reference-host-adapter/adapter.mjs"], { env, encoding: "utf8" });
      expect(result.status).toBe(1);
      expect(result.stderr).toContain(expected);
    }
  });
});
