import { createServer } from "node:http";
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

describe("reference Host Adapter example", () => {
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
