import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { LocalHttpHostAdapter } from "../src/out-of-process-host.js";
import {
  HostSessionClient,
  type HostAdapterManifest,
  type HostSessionChannelsRegistration,
  type LocalHostDeliveryRequest,
} from "../src/transport-sdk.js";

const HOST_TOKEN = "multichannel-host-bootstrap-0123456789abcdef";
const LIFE_TOKEN = "multichannel-life-bootstrap-0123456789abcdef";
const ROUTE_TOKEN = "multichannel-route-token-0123456789abcdef";
const ROUTE_ORIGIN = "http://127.0.0.1:18789";
const ADAPTER_KIND = "multichannel_fixture";
const LONG_CHANNEL = "x".repeat(201);
const MAX_CHANNEL = "x".repeat(200);

const manifest: HostAdapterManifest = {
  contract_version: 1,
  adapter_kind: ADAPTER_KIND,
  adapter_version: "1.0.0",
  host_kinds: ["fixture_runner"],
  support_tier: "mock",
  tested_host_versions: ["fixture"],
  capabilities: {
    warm_resume: true,
    exact_live_route: true,
    requires_live_binding: true,
    foreground_presence_observable: true,
    session_activity_observable: true,
  },
  receipt_upper_bound: "host_accepted",
};

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

async function fixture() {
  let now = new Date("2026-10-30T00:00:00.000Z");
  const config = {
    instance_id: "multichannel-instance", owner_id: "owner", db_path: ":memory:", timezone: "UTC",
    admin_token: "multichannel-owner-token-0123456789abcdef",
  };
  const bridge = new WakeBridge(config, { autoMockTransport: false, clock: () => now });
  const deliveries: LocalHostDeliveryRequest[] = [];
  const daemon = await startDaemon(config, {
    bridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0,
    host_adapters: [new LocalHttpHostAdapter(manifest, { fetch: async (_url, options) => {
      deliveries.push(JSON.parse(String(options?.body)) as LocalHostDeliveryRequest);
      return new Response(null, { status: 202 });
    } })],
    host_credentials: [
      { id: "all", token: HOST_TOKEN, adapter_kind: ADAPTER_KIND, host_kind: "fixture_runner", attention_channels: ["life", "work", "extra", MAX_CHANNEL, LONG_CHANNEL] },
      { id: "life", token: LIFE_TOKEN, adapter_kind: ADAPTER_KIND, host_kind: "fixture_runner", attention_channels: ["life"] },
    ],
  });
  cleanups.push(async () => { await daemon.close(); bridge.close(); });
  const origin = `http://127.0.0.1:${(daemon.address as AddressInfo).port}`;
  const client = new HostSessionClient({ base_url: origin, host_token: HOST_TOKEN, adapter_kind: ADAPTER_KIND });
  const post = (path: string, body: Record<string, unknown>, token = HOST_TOKEN) => fetch(`${origin}${path}`, {
    method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(body),
  });
  const openBody = (channels: string[] | string, sessionRef = "shared-session") => ({
    adapter_kind: ADAPTER_KIND, session_ref: sessionRef,
    ...(typeof channels === "string" ? { attention_channel: channels } : { attention_channels: channels }),
    route_address: { base_url: `${ROUTE_ORIGIN}/`, token: ROUTE_TOKEN },
  });
  const openChannels = (channels = ["life", "work"], sessionRef = "shared-session") => client.openChannels({
    session_ref: sessionRef, attention_channels: channels, route_origin: ROUTE_ORIGIN, route_token: ROUTE_TOKEN,
  });
  return { bridge, client, post, openBody, openChannels, deliveries, advance: (ms: number) => { now = new Date(now.getTime() + ms); } };
}

function state(bridge: WakeBridge) {
  return Object.fromEntries(["endpoints", "bindings", "presence_leases", "activity_watches", "batches", "delivery_correlations"]
    .map((table) => [table, bridge.db.query(`SELECT * FROM ${table} ORDER BY rowid;`)]));
}

async function activity(f: Awaited<ReturnType<typeof fixture>>, registration: HostSessionChannelsRegistration) {
  for (const lease of f.client.leases(registration)) {
    f.bridge.configureActivityWatch({ attention_channel: lease.attention_channel, enabled: true, idle_after_ms: 60_000 });
    await f.client.renewPresence(lease, { observed_by: "fixture" });
    await f.client.observeActivity(lease, { observation_id: `activity-${lease.attention_channel}`, kind: "activity_started" });
  }
}

async function dispatch(f: Awaited<ReturnType<typeof fixture>>) {
  for (const channel of ["life", "work"]) {
    f.bridge.emit("fixture", { type: "job", dedupe_key: `event-${channel}`, attention_channel_hint: channel, resource: { uri: `job://${channel}` } });
  }
  f.bridge.tick();
  await f.bridge.dispatchDue();
  expect(f.deliveries).toHaveLength(2);
}

describe("multi-channel host sessions", () => {
  it("rejects sequential single-channel opens that would silently rotate another channel's token", async () => {
    const f = await fixture();
    const first = await f.client.open({ session_ref: "shared-session", attention_channel: "life", route_origin: ROUTE_ORIGIN, route_token: ROUTE_TOKEN });
    const response = await f.post("/v1/host-sessions/open", f.openBody("work"));
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({ code: "host_session_channels_required" });
    expect(f.bridge.getBinding("work")).toBeNull();
    expect(f.bridge.getBinding("life")).toEqual(first.binding);
    await expect(f.client.renew(f.client.lease(first))).resolves.toMatchObject({ id: first.endpoint.id });
  });

  it("opens one endpoint with per-channel leases, and renews and closes it endpoint-wide", async () => {
    const f = await fixture();
    const registration = await f.openChannels();
    expect(registration).not.toHaveProperty("binding");
    expect(registration.bindings).toMatchObject([
      { attention_channel: "life", generation: 1, endpoint_id: registration.endpoint.id },
      { attention_channel: "work", generation: 1, endpoint_id: registration.endpoint.id },
    ]);
    expect(f.bridge.listEndpoints()).toHaveLength(1);
    const leases = f.client.leases(registration);
    expect(new Set(leases.map((lease) => lease.lease_token))).toEqual(new Set([registration.endpoint.lease_token]));
    expect(JSON.stringify(registration)).not.toContain(ROUTE_TOKEN);
    await activity(f, registration);
    expect(f.bridge.listPresence()).toHaveLength(2);
    const generations = f.bridge.listBindings();
    f.advance(1_000);
    await expect(f.client.renew(leases[1])).resolves.toMatchObject({ id: registration.endpoint.id });
    expect(f.bridge.getEndpoint(registration.endpoint.id)!.lease_expires_at > registration.endpoint.lease_expires_at).toBe(true);
    expect(f.bridge.listBindings()).toEqual(generations);
    await f.client.close(leases[0]);
    expect(f.bridge.listPresence()).toEqual([]);
    expect(f.bridge.listActivityWatches().map((watch) => watch.activity_lease_expires_at)).toEqual([null, null]);
    await expect(f.client.renewPresence(leases[1], { observed_by: "after-close" })).rejects.toMatchObject({ status: 409 });
  });

  it("rejects incomplete reopens without changing routes, token hashes, generations, or activity", async () => {
    const f = await fixture();
    const registration = await f.openChannels();
    await activity(f, registration);
    const before = state(f.bridge);
    f.advance(1_000);
    for (const channels of [["life"], ["work"], "life", "work"]) {
      const response = await f.post("/v1/host-sessions/open", {
        ...f.openBody(channels), route_address: { base_url: "http://127.0.0.1:18790/", token: `${ROUTE_TOKEN}-changed` },
      });
      expect(response.status).toBe(409);
      expect(await response.json()).toMatchObject({ code: "host_session_channels_required" });
      expect(state(f.bridge)).toEqual(before);
    }
    await expect(f.client.renew(f.client.leases(registration)[0])).resolves.toMatchObject({ id: registration.endpoint.id });
  });

  it.each([
    { attention_channels: [] },
    { attention_channels: ["life", "life"] },
    { attention_channels: ["life", " life "] },
    { attention_channels: ["life", "outside-scope"] },
    { attention_channels: ["life", LONG_CHANNEL] },
    { attention_channels: ["life", ""] },
    { attention_channels: ["life", 7] },
    { attention_channels: "life" },
    { attention_channels: ["life", "work"], attention_channel: "life" },
    { attention_channels: undefined },
    { attention_channels: undefined, attention_channel: LONG_CHANNEL },
    { attention_channels: ["life", "work"], route_address: { base_url: "https://remote.example/", token: ROUTE_TOKEN } },
    { attention_channels: ["life", "work"], route_address: { base_url: `${ROUTE_ORIGIN}/`, token: HOST_TOKEN } },
  ])("validates the entire request before mutating shared session state: %j", async (invalid) => {
    const f = await fixture();
    const registration = await f.openChannels();
    const before = state(f.bridge);
    const response = await f.post("/v1/host-sessions/open", { ...f.openBody(["life", "work"]), ...invalid });
    expect(response.status).toBe(400);
    expect(state(f.bridge)).toEqual(before);
    await expect(f.client.renew(f.client.leases(registration)[0])).resolves.toMatchObject({ id: registration.endpoint.id });
  });

  it("accepts the 200-character channel boundary", async () => {
    const f = await fixture();
    const opened = await f.openChannels(["life", MAX_CHANNEL]);
    expect(opened.bindings.map((binding) => binding.attention_channel)).toEqual(["life", MAX_CHANNEL]);
  });

  it("advances each channel from its own generation when extending an existing session", async () => {
    const f = await fixture();
    const original = await f.openChannels(["life"]);
    await f.openChannels(["life"]);
    const extended = await f.openChannels();
    expect(extended.endpoint.id).toBe(original.endpoint.id);
    expect(extended.bindings.map((binding) => [binding.attention_channel, binding.generation])).toEqual([["life", 3], ["work", 1]]);
    const reopened = await f.openChannels();
    expect(reopened.bindings.map((binding) => [binding.attention_channel, binding.generation])).toEqual([["life", 4], ["work", 2]]);
    expect(f.bridge.listEndpoints()).toHaveLength(1);
  });

  it("keeps endpoint-wide observation IDs deduplicated, while channel-qualified IDs update each watch", async () => {
    const f = await fixture();
    const registration = await f.openChannels();
    const [life, work] = f.client.leases(registration);
    for (const lease of [life, work]) {
      f.bridge.configureActivityWatch({ attention_channel: lease.attention_channel, enabled: true, idle_after_ms: 60_000 });
    }
    await expect(f.client.observeActivity(life, { observation_id: "edge-42", kind: "activity_started" })).resolves.toMatchObject({ duplicate: false });
    await expect(f.client.observeActivity(work, { observation_id: "edge-42", kind: "activity_started" })).resolves.toMatchObject({ duplicate: true });
    expect(f.bridge.getActivityWatch("life")!.last_nonwake_activity_at).not.toBeNull();
    expect(f.bridge.getActivityWatch("work")!.last_nonwake_activity_at).toBeNull();
    expect(f.bridge.getActivityWatch("work")!.activity_lease_expires_at).toBeNull();
    f.advance(1_000);
    for (const lease of [life, work]) {
      await expect(f.client.observeActivity(lease, { observation_id: `${lease.attention_channel}:edge-42`, kind: "activity_started" })).resolves.toMatchObject({ duplicate: false });
    }
    const watches = f.bridge.listActivityWatches();
    expect(watches.map((watch) => watch.last_nonwake_activity_at)).toEqual(["2026-10-30T00:00:01.000Z", "2026-10-30T00:00:01.000Z"]);
    expect(watches.every((watch) => watch.activity_lease_expires_at !== null)).toBe(true);
  });

  it("rotates the shared token once, advances every generation, and fences old leases and nonces", async () => {
    const f = await fixture();
    const first = await f.openChannels();
    const firstLeases = f.client.leases(first);
    await dispatch(f);
    await activity(f, first);
    f.advance(1_000);
    const reopened = await f.openChannels();
    const nextLeases = f.client.leases(reopened);
    expect(reopened.endpoint.id).toBe(first.endpoint.id);
    expect(reopened.endpoint.lease_token).not.toBe(first.endpoint.lease_token);
    expect(reopened.bindings.map((binding) => binding.generation)).toEqual([2, 2]);
    expect(f.bridge.listPresence()).toEqual([]);
    expect(f.bridge.listDeliveryCorrelations().map((correlation) => correlation.state)).toEqual(["discarded", "discarded"]);
    for (const lease of firstLeases) {
      await expect(f.client.renew(lease)).rejects.toMatchObject({ status: 409 });
      await expect(f.client.close(lease)).rejects.toMatchObject({ status: 409 });
      await expect(f.client.renewPresence({ ...lease, lease_token: reopened.endpoint.lease_token }, { observed_by: "stale-generation" })).rejects.toMatchObject({ status: 409 });
    }
    for (const lease of nextLeases) {
      const delivery = f.deliveries.find((item) => item.wake.attention_channel === lease.attention_channel)!;
      await expect(f.client.consumeWakeEcho(lease, { observation_id: `stale-nonce-${lease.attention_channel}`, delivery_nonce: delivery.delivery_nonce })).rejects.toMatchObject({ status: 409 });
    }
    await activity(f, reopened);
    expect(f.bridge.listPresence()).toHaveLength(2);
  });

  it("moves one channel to a new real session without rotating or closing the old endpoint's other channel", async () => {
    const f = await fixture();
    const original = await f.openChannels();
    await activity(f, original);
    const [oldLife, oldWork] = f.client.leases(original);
    const workBinding = f.bridge.getBinding("work");
    const moved = await f.client.open({ session_ref: "different-session", attention_channel: "life", route_origin: ROUTE_ORIGIN, route_token: `${ROUTE_TOKEN}-moved` });
    expect(moved.endpoint.id).not.toBe(original.endpoint.id);
    expect(moved.binding.generation).toBe(2);
    expect(f.bridge.getBinding("work")).toEqual(workBinding);
    expect(f.bridge.listPresence().map((presence) => presence.attention_channel)).toEqual(["work"]);
    await expect(f.client.renewPresence(oldLife, { observed_by: "stale-life" })).rejects.toMatchObject({ status: 409 });
    await expect(f.client.renewPresence(oldWork, { observed_by: "still-work" })).resolves.toMatchObject({ attention_channel: "work" });
    await f.client.close(f.client.lease(moved));
    await expect(f.client.renewPresence(oldWork, { observed_by: "work-after-new-close" })).resolves.toMatchObject({ attention_channel: "work" });
  });

  it.each(["renew", "close"])("requires bootstrap credentials to cover every bound channel for endpoint-wide %s", async (action) => {
    const f = await fixture();
    const registration = await f.openChannels();
    await activity(f, registration);
    const before = state(f.bridge);
    f.advance(1_000);
    const body = { adapter_kind: ADAPTER_KIND, session_ref: "shared-session", attention_channel: "life", expected_generation: 1 };
    const denied = await f.post(`/v1/host-sessions/${action}`, body, LIFE_TOKEN);
    expect(denied.status).toBe(400);
    expect(await denied.json()).toMatchObject({ code: "invalid_host_session" });
    expect(state(f.bridge)).toEqual(before);
    expect((await f.post(`/v1/host-sessions/${action}`, body)).status).toBe(200);
    if (action === "close") expect(f.bridge.listPresence()).toEqual([]);
    else expect(f.bridge.listPresence()).toHaveLength(2);
  });

  it.each([false, true])("rolls back endpoint registration and the first binding if the second binding SQL fails (existing=%s)", async (existing) => {
    const f = await fixture();
    const registration = existing ? await f.openChannels() : null;
    if (registration) {
      await dispatch(f);
      await activity(f, registration);
    }
    f.bridge.db.exec(`CREATE TRIGGER fail_second_binding BEFORE ${existing ? "UPDATE" : "INSERT"} ON bindings
      WHEN NEW.attention_channel='work' BEGIN SELECT RAISE(ABORT, 'fixture second binding failure'); END;`);
    const before = state(f.bridge);
    f.advance(1_000);
    const response = await f.post("/v1/host-sessions/open", {
      ...f.openBody(["life", "work"]), route_address: { base_url: "http://127.0.0.1:18790/", token: `${ROUTE_TOKEN}-changed` },
    });
    expect(response.status).toBe(500);
    expect(state(f.bridge)).toEqual(before);
    if (registration) await expect(f.client.renew(f.client.leases(registration)[0])).resolves.toMatchObject({ id: registration.endpoint.id });
  });

  it.each(["generation", "new-shared-channel"])("checks %s races under the registration transaction's writer lock", async (race) => {
    const f = await fixture();
    const channels = race === "generation" ? ["life", "work"] : ["life"];
    const registration = await f.openChannels(channels);
    const competitor = race === "generation" ? f.bridge.registerEndpoint({ host_kind: "fixture_runner", session_ref: "competitor" }) : registration.endpoint;
    const query = f.bridge.db.query.bind(f.bridge.db);
    let afterRace: ReturnType<typeof state> | undefined;
    let injected = false;
    const spy = vi.spyOn(f.bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      if (!injected && sql.includes("CREATE TEMP TABLE host_session_guard")) {
        injected = true;
        f.bridge.takeover("work", competitor.id, race === "generation" ? 1 : 0);
        afterRace = state(f.bridge);
      }
      return query<T>(sql);
    });
    try {
      const response = await f.post("/v1/host-sessions/open", f.openBody(channels));
      expect(response.status).toBe(409);
      expect(await response.json()).toMatchObject({ code: race === "generation" ? "stale_generation" : "host_session_channels_required" });
      expect(injected).toBe(true);
      expect(state(f.bridge)).toEqual(afterRace);
      expect(f.bridge.getBinding("life")).toEqual(registration.bindings[0]);
      await expect(f.client.renew(f.client.leases(registration)[0])).resolves.toMatchObject({ id: registration.endpoint.id });
    } finally { spy.mockRestore(); }
  });

  it("checks bootstrap scope again if another channel joins just before endpoint-wide renewal", async () => {
    const f = await fixture();
    const registration = await f.openChannels(["life"]);
    const query = f.bridge.db.query.bind(f.bridge.db);
    let afterRace: ReturnType<typeof state> | undefined;
    let injected = false;
    const spy = vi.spyOn(f.bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      if (!injected && sql.includes("CREATE TEMP TABLE bound_endpoint_guard")) {
        injected = true;
        f.bridge.takeover("work", registration.endpoint.id, 0);
        afterRace = state(f.bridge);
      }
      return query<T>(sql);
    });
    try {
      f.advance(1_000);
      const response = await f.post("/v1/host-sessions/renew", {
        adapter_kind: ADAPTER_KIND, session_ref: "shared-session", attention_channel: "life", expected_generation: 1,
      }, LIFE_TOKEN);
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({ code: "invalid_host_session" });
      expect(injected).toBe(true);
      expect(state(f.bridge)).toEqual(afterRace);
    } finally { spy.mockRestore(); }
  });

  it("validates plural SDK input before sending any request", async () => {
    const fetchImpl = vi.fn<typeof fetch>();
    const client = new HostSessionClient({ base_url: ROUTE_ORIGIN, host_token: HOST_TOKEN, adapter_kind: ADAPTER_KIND, fetch: fetchImpl });
    for (const channels of [[], ["life", "life"], ["life", " life "], ["life", LONG_CHANNEL], [""], ["life", 7]]) {
      await expect(client.openChannels({
        session_ref: "shared-session", attention_channels: channels as string[], route_origin: ROUTE_ORIGIN, route_token: ROUTE_TOKEN,
      })).rejects.toMatchObject({ code: "invalid_host_session", status: 400 });
    }
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it.each([
    [{ attention_channel: "life", endpoint_id: "ep_shared", generation: 1 }],
    [{ attention_channel: "life", endpoint_id: "ep_shared", generation: 1 }, { attention_channel: "life", endpoint_id: "ep_shared", generation: 1 }],
    [{ attention_channel: "life", endpoint_id: "ep_shared", generation: 1 }, { attention_channel: "work", endpoint_id: "ep_other", generation: 1 }],
    [{ attention_channel: "life", endpoint_id: "ep_shared", generation: 1 }, { attention_channel: "work", endpoint_id: "ep_shared", generation: 0 }],
  ])("rejects malformed plural SDK registration bindings: %j", async (...bindings) => {
    const client = new HostSessionClient({
      base_url: ROUTE_ORIGIN, host_token: HOST_TOKEN, adapter_kind: ADAPTER_KIND,
      fetch: async () => new Response(JSON.stringify({ endpoint: { id: "ep_shared", lease_token: ROUTE_TOKEN }, bindings })),
    });
    await expect(client.openChannels({
      session_ref: "shared-session", attention_channels: ["life", "work"], route_origin: ROUTE_ORIGIN, route_token: ROUTE_TOKEN,
    })).rejects.toMatchObject({ code: "invalid_host_session_response", status: 502 });
  });
});
