import { createServer } from "node:http";
import { Readable } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { ReferenceHostAdapter } from "../examples/reference-host-adapter/adapter.mjs";

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
  return new ReferenceHostAdapter({
    daemonOrigin: "http://127.0.0.1:4311",
    hostToken: HOST_TOKEN,
    routeToken: ROUTE_TOKEN,
    sessionRef: "exact-session-one",
    attentionChannel: "life",
    acceptWake,
  });
}

function deliver(adapter, attemptId = "attempt-one") {
  const delivery = {
    protocol_version: 1,
    attempt_id: attemptId,
    delivery_nonce: "reference-delivery-nonce-0123456789abcdef",
    wake: {
      schema_version: 1, instance_id: "reference-instance", wake_batch_id: "wb_reference",
      attention_channel: "life", claim_refs: [], binding_generation: 1,
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

describe("reference Host Adapter example", () => {
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
          endpoint: { id: "ep_reference", lease_token: LEASE_TOKEN },
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
  });
});
