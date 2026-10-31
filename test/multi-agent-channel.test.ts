import { fork, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { LocalHttpHostAdapter } from "../src/out-of-process-host.js";
import {
  HostSessionClient,
  validateLocalHostDeliveryRequest,
  type HostAdapterManifest,
  type HostSessionLease,
  type LocalHostDeliveryRequest,
} from "../src/transport-sdk.js";
import type { BridgeConfig } from "../src/types.js";

const CHANNELS = ["life", "work"] as const;
type Channel = typeof CHANNELS[number];
type AgentAddress = { config: BridgeConfig; origin: string; hostToken: string; sourceToken: string; client: HostSessionClient };
type Host = { origin: string; token: string; deliveries: LocalHostDeliveryRequest[]; timeouts: Set<string> };

function manifest(): HostAdapterManifest {
  return {
    contract_version: 1, adapter_kind: "fixture_host_bridge", adapter_version: "1.0.0",
    host_kinds: ["fixture_runner"], support_tier: "experimental", tested_host_versions: ["fixture-1"],
    capabilities: {
      cold_push: false, warm_resume: true, exact_live_route: true, requires_live_binding: true,
      queue_when_busy: true, foreground_presence_observable: true, session_activity_observable: true,
      receipt_stages: ["transport_accepted"], receipt_upper_bound: "host_accepted", support_tier: "experimental",
    },
    receipt_upper_bound: "host_accepted",
  };
}

function identity(root: string, name: string, extra: Partial<BridgeConfig> = {}) {
  const config: BridgeConfig = {
    instance_id: `fixture-agent-${name}`, owner_id: `fixture-owner-${name}`,
    db_path: join(root, `${name}.sqlite`), timezone: "UTC",
    admin_token: `fixture-${name}-owner-token-0123456789abcdef`,
    endpoint_lease_ms: 24 * 60 * 60_000, retry_delay_ms: 0,
    ...extra,
  };
  const hostToken = `fixture-${name}-host-token-0123456789abcdef`;
  const sourceToken = `fixture-${name}-source-token-0123456789abcdef`;
  return {
    config, hostToken, sourceToken,
    host_credentials: [{ id: "fixture-host", token: hostToken, adapter_kind: manifest().adapter_kind, host_kind: "fixture_runner", attention_channels: [...CHANNELS] }],
    source_credentials: [{ id: "fixture-source", source: "fixture", token: sourceToken }],
  };
}

function addressed(value: ReturnType<typeof identity>, origin: string): AgentAddress {
  return {
    ...value, origin,
    client: new HostSessionClient({ base_url: origin, host_token: value.hostToken, adapter_kind: manifest().adapter_kind }),
  };
}

async function http(agent: AgentAddress, path: string, body?: unknown, token = agent.config.admin_token!) {
  const response = await fetch(`${agent.origin}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(5_000),
  });
  return { status: response.status, body: await response.json() };
}

async function emit(agent: AgentAddress, channel: Channel, key: string) {
  const response = await http(agent, "/v1/ingress/fixture/events", {
    type: "fixture.multi-agent", dedupe_key: key, attention_channel_hint: channel,
    resource: { uri: `fixture://${agent.config.instance_id}/${channel}/${key}` },
  }, agent.sourceToken);
  expect(response.status).toBe(201);
  return response.body;
}

async function dispatch(agent: AgentAddress) {
  expect((await http(agent, "/v1/tick", {})).status).toBe(200);
  const response = await http(agent, "/v1/dispatch", {});
  expect(response.status).toBe(200);
  return response.body.results as { batch_id: string; status: string }[];
}

async function dispatchExpected(agent: AgentAddress, expectedStatuses: string[]) {
  const results: Awaited<ReturnType<typeof dispatch>> = [];
  // A bounded sweep guarantees one candidate can progress, not that all due
  // work fits in a pass. Never silently retry a failed or repeated batch.
  for (let pass = 0; pass < expectedStatuses.length && results.length < expectedStatuses.length; pass += 1) {
    const current = await dispatch(agent);
    expect(current.length, "Expected initial due work to progress in this bounded pass").toBeGreaterThan(0);
    for (const result of current) {
      expect(expectedStatuses).toContain(result.status);
      expect(result.batch_id).toEqual(expect.any(String));
    }
    results.push(...current);
    expect(results.length).toBeLessThanOrEqual(expectedStatuses.length);
    expect(new Set(results.map((result) => result.batch_id)).size).toBe(results.length);
  }
  expect(results).toHaveLength(expectedStatuses.length);
  expect(results.map((result) => result.status).sort()).toEqual([...expectedStatuses].sort());
  return results;
}

async function inspect(agent: AgentAddress) {
  const response = await http(agent, "/v1/inspect");
  expect(response.status).toBe(200);
  return response.body;
}

async function openShared(agent: AgentAddress, host: Host) {
  const registration = await agent.client.openChannels({
    session_ref: "same-session-ref", attention_channels: [...CHANNELS], route_origin: host.origin, route_token: host.token,
  });
  const leases = agent.client.leases(registration);
  expect(leases.map((lease) => lease.attention_channel).sort()).toEqual([...CHANNELS]);
  return { registration, leases: Object.fromEntries(leases.map((lease) => [lease.attention_channel, lease])) as Record<Channel, HostSessionLease> };
}

async function withLab(run: (lab: {
  root: string;
  host: (name: string) => Promise<Host>;
  agent: (name: string, options?: { clock?: () => Date; config?: Partial<BridgeConfig> }) => Promise<AgentAddress & { bridge: WakeBridge }>;
  process: (name: string) => Promise<AgentAddress & { child: ChildProcess; stop: (signal?: NodeJS.Signals) => Promise<void> }>;
}) => Promise<void>) {
  const root = mkdtempSync(join(tmpdir(), "wake-bridge-multi-agent-"));
  const close: (() => Promise<void>)[] = [];
  try {
    await run({
      root,
      host: async (name) => {
        const result: Host = {
          origin: "", token: `fixture-${name}-route-token-0123456789abcdef`, deliveries: [], timeouts: new Set(),
        };
        const server = createServer(async (request, response) => {
          try {
            if (request.method !== "POST" || request.url !== "/v1/wakes") return void response.writeHead(404).end();
            if (request.headers.authorization !== `Bearer ${result.token}`) return void response.writeHead(401).end();
            let raw = "";
            for await (const chunk of request) raw += String(chunk);
            const delivery = validateLocalHostDeliveryRequest(JSON.parse(raw));
            if (request.headers["x-wakebridge-delivery-nonce"] !== delivery.delivery_nonce) return void response.writeHead(400).end();
            result.deliveries.push(delivery);
            // The envelope really arrived over TCP, but the host does not
            // acknowledge it. The transport must not invent acceptance.
            if (result.timeouts.has(delivery.wake.attention_channel)) return;
            response.writeHead(202).end();
          } catch {
            response.writeHead(400).end();
          }
        });
        await new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(0, "127.0.0.1", resolve);
        });
        result.origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
        close.push(async () => {
          server.closeAllConnections();
          await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
        });
        return result;
      },
      agent: async (name, options = {}) => {
        const value = identity(root, name, options.config);
        const bridge = new WakeBridge(value.config, { autoMockTransport: false, clock: options.clock });
        close.push(async () => bridge.close());
        const daemon = await startDaemon(value.config, {
          bridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0,
          host_adapters: [new LocalHttpHostAdapter(manifest(), { timeout_ms: 1_000 })],
          host_credentials: value.host_credentials, source_credentials: value.source_credentials,
        });
        close.push(() => daemon.close());
        return { ...addressed(value, `http://127.0.0.1:${(daemon.address as AddressInfo).port}`), bridge };
      },
      process: async (name) => {
        const value = identity(root, name);
        const child = fork(join(process.cwd(), "dist/test/fixtures/multi-agent-daemon.js"), [], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
        let stderr = "";
        child.stderr!.on("data", (chunk) => { stderr = (stderr + String(chunk)).slice(-4_096); });
        const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
          child.once("exit", (code, signal) => resolve({ code, signal }));
        });
        const stop = async (signal: NodeJS.Signals = "SIGTERM") => {
          if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
          child.kill(signal);
          let timer: ReturnType<typeof setTimeout>;
          let timedOut = false;
          try {
            await Promise.race([
              exited,
              new Promise<void>((resolve) => { timer = setTimeout(() => { timedOut = true; child.kill("SIGKILL"); resolve(); }, 5_000); }),
            ]);
            // Do not remove the temporary DB until this exact child has exited,
            // including the forced-cleanup path after a failed graceful stop.
            await exited;
            if (timedOut) throw new Error("fixture daemon did not exit gracefully");
          } finally {
            clearTimeout(timer!);
          }
        };
        close.push(() => stop());
        const ready = await new Promise<{ origin: string; pid: number }>((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error(`fixture daemon startup timed out: ${stderr}`)), 10_000);
          child.once("message", (message) => { clearTimeout(timer); resolve(message as { origin: string; pid: number }); });
          child.once("error", (error) => { clearTimeout(timer); reject(error); });
          child.once("exit", () => { clearTimeout(timer); reject(new Error(`fixture daemon exited before readiness: ${stderr}`)); });
          child.send({ config: value.config, manifest: manifest(), host_credentials: value.host_credentials, source_credentials: value.source_credentials });
        });
        expect(ready.pid).toBe(child.pid);
        expect(ready.pid).not.toBe(process.pid);
        return { ...addressed(value, ready.origin), child, stop };
      },
    });
  } finally {
    let cleanupError: unknown;
    try {
      for (const cleanup of close.reverse()) {
        try { await cleanup(); } catch (error) { cleanupError ??= error; }
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
    if (cleanupError) throw cleanupError;
  }
}

describe("two independent agent spaces over real loopback HTTP", () => {
  it.each(["shared-session", "split-sessions"] as const)("delivers identical source keys only to their own space (%s), then explicitly consumes", async (topology) => withLab(async (lab) => {
    const agents = await Promise.all([lab.agent("a"), lab.agent("b")]);
    const sessions: { agent: typeof agents[number]; hosts: Host[]; leases: Record<Channel, HostSessionLease> }[] = [];
    for (const agent of agents) {
      const hosts: Host[] = [];
      const leases = {} as Record<Channel, HostSessionLease>;
      if (topology === "shared-session") {
        const host = await lab.host(agent.config.instance_id);
        hosts.push(host);
        const opened = await openShared(agent, host);
        Object.assign(leases, opened.leases);
        expect(leases.life.endpoint_id).toBe(leases.work.endpoint_id);
        expect(leases.life.lease_token).toBe(leases.work.lease_token);
        expect(JSON.stringify(opened.registration)).not.toContain(host.token);
      } else {
        for (const channel of CHANNELS) {
          const host = await lab.host(`${agent.config.instance_id}-${channel}`);
          hosts.push(host);
          leases[channel] = agent.client.lease(await agent.client.open({
            session_ref: `same-${channel}-session-ref`, attention_channel: channel, route_origin: host.origin, route_token: host.token,
          }));
        }
        expect(leases.life.endpoint_id).not.toBe(leases.work.endpoint_id);
        expect(leases.life.lease_token).not.toBe(leases.work.lease_token);
      }
      sessions.push({ agent, hosts, leases });
    }
    // All four routes coexist before either space ingests a colliding key.
    for (const { agent } of sessions) {
      for (const channel of CHANNELS) {
        expect(await emit(agent, channel, `same-dedupe-${channel}`)).toMatchObject({ duplicate: false });
        expect(await emit(agent, channel, `same-dedupe-${channel}`)).toMatchObject({ duplicate: true });
      }
    }
    for (const { agent, hosts, leases } of sessions) {
      // Force this topology across bounded passes without changing the real
      // HTTP transport or the business clock used for leases and receipts.
      let monotonicRead = 0;
      const budgetClock = topology === "split-sessions"
        ? vi.spyOn(performance, "now").mockImplementation(() => monotonicRead++ * 1_000)
        : undefined;
      try {
        expect((await dispatchExpected(agent, ["accepted", "accepted"])).map((result) => result.status)).toEqual(["accepted", "accepted"]);
      } finally {
        budgetClock?.mockRestore();
      }
      const deliveries = hosts.flatMap((host) => host.deliveries);
      expect(deliveries).toHaveLength(2);
      expect(deliveries.map((delivery) => delivery.wake.attention_channel).sort()).toEqual([...CHANNELS]);
      for (const [index, host] of hosts.entries()) {
        if (topology === "split-sessions") expect(host.deliveries.map((delivery) => delivery.wake.attention_channel)).toEqual([CHANNELS[index]]);
        for (const delivery of host.deliveries) {
          const lease = leases[delivery.wake.attention_channel as Channel];
          expect(delivery.wake).toMatchObject({ instance_id: agent.config.instance_id, binding_generation: lease.generation });
          expect(delivery.wake.claim_refs).toMatchObject([{ resource: { uri: `fixture://${agent.config.instance_id}/${lease.attention_channel}/same-dedupe-${lease.attention_channel}` } }]);
          await expect(agent.client.consumeWakeEcho(lease, { observation_id: `echo-${lease.attention_channel}`, delivery_nonce: delivery.delivery_nonce })).resolves.toMatchObject({ duplicate: false });
          const beforeAck = (await http(agent, `/v1/receipts/${delivery.wake.wake_batch_id}`)).body.receipts;
          expect(beforeAck.map((receipt: { stage: string }) => receipt.stage)).toEqual(["transport_accepted", "host_attested"]);
          // The fixture acts as an agent explicitly acknowledging the batch.
          // ackBatch has no public HTTP route; it is the only Core mutation in
          // this suite. This is protocol integration, not a real model run.
          expect(agent.bridge.ackBatch(delivery.wake.wake_batch_id, lease.endpoint_id, lease.generation).state).toBe("seen");
          for (const claim of delivery.wake.claim_refs) {
            expect((await http(agent, `/v1/claims/${claim.claim_id}/consume`, { result: { fixture_agent: agent.config.instance_id } })).body.claim.state).toBe("consumed");
          }
        }
      }
      const state = await inspect(agent);
      expect(state.endpoints).toHaveLength(topology === "shared-session" ? 1 : 2);
      expect(state.bindings).toEqual(expect.arrayContaining(CHANNELS.map((channel) => expect.objectContaining({
        attention_channel: channel, endpoint_id: leases[channel].endpoint_id, generation: leases[channel].generation,
      }))));
      expect(state.events).toHaveLength(2);
      expect(state.claims.map((claim: { state: string }) => claim.state)).toEqual(["consumed", "consumed"]);
      expect(state.delivery_correlations.map((correlation: { state: string }) => correlation.state)).toEqual(["consumed", "consumed"]);
      expect(state.receipts.filter((receipt: { stage: string }) => receipt.stage === "agent_seen")).toHaveLength(2);
      expect(state.receipts.filter((receipt: { stage: string }) => receipt.stage === "agent_consumed")).toHaveLength(2);
      expect(await dispatch(agent)).toEqual([]);
      expect(hosts.flatMap((host) => host.deliveries)).toHaveLength(2);
      if (topology === "shared-session") {
        await expect(agent.client.renew(leases.life)).resolves.toMatchObject({ id: leases.work.endpoint_id });
        await agent.client.close(leases.life);
        await expect(agent.client.renewPresence(leases.work, { observed_by: "fixture" })).rejects.toMatchObject({ status: 409 });
      }
    }
    const [stateA, stateB] = await Promise.all(agents.map(inspect));
    expect(stateA.events.map((event: { id: string }) => event.id)).not.toEqual(stateB.events.map((event: { id: string }) => event.id));
    expect(stateA.instance).toMatchObject({ instance_id: "fixture-agent-a", owner_id: "fixture-owner-a" });
    expect(stateB.instance).toMatchObject({ instance_id: "fixture-agent-b", owner_id: "fixture-owner-b" });
  }));

  it("rejects cross-space credentials, leases, nonces and same-generation wrong-channel echoes without mutation", async () => withLab(async (lab) => {
    const [a, b, hostA, hostB] = await Promise.all([lab.agent("a"), lab.agent("b"), lab.host("a"), lab.host("b")]);
    const [openedA, openedB] = await Promise.all([openShared(a, hostA), openShared(b, hostB)]);
    // Equal generation numbers deliberately exercise the channel fence, not
    // merely the already-existing stale-generation or endpoint checks.
    for (const opened of [openedA, openedB]) expect(opened.leases.life.generation).toBe(opened.leases.work.generation);
    for (const agent of [a, b]) {
      for (const channel of CHANNELS) await emit(agent, channel, `same-dedupe-${channel}`);
      await dispatchExpected(agent, ["accepted", "accepted"]);
    }
    const before = await Promise.all([inspect(a), inspect(b)]);
    for (const [target, other, host, opened, foreign] of [
      [a, b, hostA, openedA, openedB], [b, a, hostB, openedB, openedA],
    ] as const) {
      expect((await http(target, "/v1/claims", undefined, other.config.admin_token!)).status).toBe(401);
      expect((await http(target, `/v1/claims/${before[target === a ? 0 : 1].claims[0].id}/consume`, { result: "forbidden" }, other.config.admin_token!)).status).toBe(401);
      expect((await http(target, "/v1/ingress/fixture/events", { type: "forbidden", dedupe_key: "forbidden", resource: { uri: "fixture://forbidden" } }, other.sourceToken)).status).toBe(401);
      expect((await http(target, "/v1/ingress/other/events", {}, target.sourceToken)).status).toBe(401);
      const wrongHost = new HostSessionClient({ base_url: target.origin, host_token: other.hostToken, adapter_kind: manifest().adapter_kind });
      await expect(wrongHost.openChannels({ session_ref: "same-session-ref", attention_channels: [...CHANNELS], route_origin: host.origin, route_token: host.token })).rejects.toMatchObject({ status: 401 });
      const life = host.deliveries.find((delivery) => delivery.wake.attention_channel === "life")!;
      const foreignNonce = (target === a ? hostB : hostA).deliveries.find((delivery) => delivery.wake.attention_channel === "life")!.delivery_nonce;
      for (const [lease, nonce] of [
        [{ ...opened.leases.life, lease_token: foreign.leases.life.lease_token }, life.delivery_nonce],
        [foreign.leases.life, life.delivery_nonce],
        [opened.leases.life, "unknown-delivery-nonce-0123456789abcdef"],
        [opened.leases.life, foreignNonce],
        [opened.leases.work, life.delivery_nonce],
      ] as const) {
        await expect(target.client.consumeWakeEcho(lease, { observation_id: "forbidden-echo", delivery_nonce: nonce })).rejects.toMatchObject({ status: 409 });
      }
      const response = await fetch(`${host.origin}/v1/wakes`, {
        method: "POST", headers: { authorization: `Bearer ${(target === a ? hostB : hostA).token}`, "content-type": "application/json", "x-wakebridge-delivery-nonce": life.delivery_nonce }, body: JSON.stringify(life),
      });
      expect(response.status).toBe(401);
    }
    expect(await Promise.all([inspect(a), inspect(b)])).toEqual(before);
    expect(hostA.deliveries).toHaveLength(2);
    expect(hostB.deliveries).toHaveLength(2);
  }));

  it("rotates a complete shared session, then takes over and closes only A-life while A-work and B continue", async () => withLab(async (lab) => {
    const [a, b, original, refreshed, lifeHost, hostB] = await Promise.all([
      lab.agent("a"), lab.agent("b"), lab.host("a-original"), lab.host("a-refreshed"), lab.host("a-life"), lab.host("b"),
    ]);
    const old = await openShared(a, original);
    const openedB = await openShared(b, hostB);
    for (const channel of CHANNELS) await emit(a, channel, `before-rotation-${channel}`);
    await dispatchExpected(a, ["accepted", "accepted"]);
    const beforePartial = await inspect(a);
    await expect(a.client.open({ session_ref: "same-session-ref", attention_channel: "life", route_origin: refreshed.origin, route_token: refreshed.token })).rejects.toMatchObject({ status: 409 });
    await expect(a.client.openChannels({ session_ref: "same-session-ref", attention_channels: ["life"], route_origin: refreshed.origin, route_token: refreshed.token })).rejects.toMatchObject({ status: 409 });
    expect(await inspect(a)).toEqual(beforePartial);
    const current = await openShared(a, refreshed);
    const afterRotation = await inspect(a);
    for (const channel of CHANNELS) {
      expect(current.leases[channel]).toMatchObject({ endpoint_id: old.leases[channel].endpoint_id, generation: old.leases[channel].generation + 1 });
      expect(current.leases[channel].lease_token).not.toBe(old.leases[channel].lease_token);
      await expect(a.client.renewPresence(old.leases[channel], { observed_by: "old-host" })).rejects.toMatchObject({ status: 409 });
      await expect(a.client.renewPresence({ ...current.leases[channel], generation: old.leases[channel].generation }, { observed_by: "stale-generation" })).rejects.toMatchObject({ status: 409 });
      const delivery = original.deliveries.find((value) => value.wake.attention_channel === channel)!;
      for (const lease of [old.leases[channel], { ...current.leases[channel], generation: old.leases[channel].generation }, current.leases[channel]]) {
        await expect(a.client.consumeWakeEcho(lease, { observation_id: "stale-echo", delivery_nonce: delivery.delivery_nonce })).rejects.toMatchObject({ status: 409 });
      }
    }
    expect(await inspect(a)).toEqual(afterRotation);
    for (const channel of CHANNELS) await emit(a, channel, `before-takeover-${channel}`);
    await dispatchExpected(a, ["accepted", "accepted"]);
    const replacement = a.client.lease(await a.client.open({
      session_ref: "new-life-session", attention_channel: "life", route_origin: lifeHost.origin, route_token: lifeHost.token,
    }));
    expect(replacement.generation).toBe(current.leases.life.generation + 1);
    const afterTakeover = await Promise.all([inspect(a), inspect(b)]);
    const oldLife = refreshed.deliveries.find((value) => value.wake.attention_channel === "life")!;
    await expect(a.client.consumeWakeEcho(current.leases.life, { observation_id: "taken-over-echo", delivery_nonce: oldLife.delivery_nonce })).rejects.toMatchObject({ status: 409 });
    expect(await Promise.all([inspect(a), inspect(b)])).toEqual(afterTakeover);
    const work = refreshed.deliveries.find((value) => value.wake.attention_channel === "work")!;
    await expect(a.client.consumeWakeEcho(current.leases.work, { observation_id: "unaffected-work", delivery_nonce: work.delivery_nonce })).resolves.toMatchObject({ duplicate: false });
    await emit(a, "life", "after-takeover-life");
    expect((await dispatch(a)).map((result) => result.status)).toEqual(["accepted"]);
    expect(lifeHost.deliveries.map((value) => value.wake.attention_channel)).toEqual(["life"]);
    await a.client.close(replacement);
    for (const channel of CHANNELS) {
      await emit(a, channel, `after-life-close-${channel}`);
      await emit(b, channel, `after-life-close-${channel}`);
    }
    expect((await dispatchExpected(a, ["accepted", "waiting_for_endpoint"])).map((result) => result.status).sort()).toEqual(["accepted", "waiting_for_endpoint"]);
    expect((await dispatchExpected(b, ["accepted", "accepted"])).map((result) => result.status)).toEqual(["accepted", "accepted"]);
    expect(original.deliveries).toHaveLength(2);
    expect(refreshed.deliveries.map((value) => value.wake.attention_channel)).toEqual(["life", "work", "work"]);
    expect(lifeHost.deliveries).toHaveLength(1);
    expect(hostB.deliveries.map((value) => value.wake.attention_channel).sort()).toEqual([...CHANNELS]);
    await expect(b.client.renew(openedB.leases.life)).resolves.toMatchObject({ id: openedB.leases.work.endpoint_id });
    expect((await inspect(a)).bindings).toEqual(expect.arrayContaining([
      expect.objectContaining({ attention_channel: "work", endpoint_id: current.leases.work.endpoint_id, generation: current.leases.work.generation }),
    ]));
  }));

  it.each(["presence", "quiet"] as const)("keeps an A-life %s gate local and releases it to life, never work or B", async (gate) => withLab(async (lab) => {
    let now = Date.parse(gate === "quiet" ? "2026-10-21T22:00:00.000Z" : "2026-10-21T12:00:00.000Z");
    const [a, b, hostA, hostB] = await Promise.all([
      lab.agent("a", { clock: () => new Date(now), config: gate === "quiet" ? {
        quiet_hours: { timezone: "UTC", windows: [{ start: "22:00", end: "08:00" }] },
        policies: [{ id: "work-bypass", version: 1, match: { attention_channel_hint: "work" }, delivery: { mode: "immediate", quiet_hours_policy: "bypass" }, target: { attention_channel: "work" } }],
      } : {} }),
      lab.agent("b", { clock: () => new Date(now) }), lab.host("a"), lab.host("b"),
    ]);
    const openedA = await openShared(a, hostA);
    await openShared(b, hostB);
    if (gate === "presence") await a.client.renewPresence(openedA.leases.life, { observed_by: "fixture-owner-edge", ttl_ms: 60_000 });
    for (const agent of [a, b]) for (const channel of CHANNELS) await emit(agent, channel, `gate-${channel}`);
    expect((await dispatch(a)).map((result) => result.status)).toEqual(["accepted"]);
    expect((await dispatchExpected(b, ["accepted", "accepted"])).map((result) => result.status)).toEqual(["accepted", "accepted"]);
    expect(hostA.deliveries.map((value) => value.wake.attention_channel)).toEqual(["work"]);
    expect(hostB.deliveries.map((value) => value.wake.attention_channel).sort()).toEqual([...CHANNELS]);
    const gated = await inspect(a);
    expect(gated.claims.find((claim: { attention_channel: string }) => claim.attention_channel === "life")).toMatchObject({ state: "deferred" });
    expect(gated.attempts).toHaveLength(1);
    now = gate === "quiet" ? Date.parse("2026-10-22T08:00:00.000Z") : now + 60_000;
    expect((await dispatch(a)).map((result) => result.status)).toEqual(["accepted"]);
    expect(hostA.deliveries.map((value) => value.wake.attention_channel)).toEqual(["work", "life"]);
    expect(hostA.deliveries[1].wake.claim_refs).toMatchObject([{ resource: { uri: `fixture://fixture-agent-a/life/gate-life` } }]);
    expect(hostB.deliveries).toHaveLength(2);
    expect((await inspect(b)).attempts).toHaveLength(2);
  }));

  it("bounds one HTTP timeout to A-life, retries its own attempt, and never redelivers accepted unacknowledged wakes", async () => withLab(async (lab) => {
    let now = Date.parse("2026-10-21T12:00:00.000Z");
    const [a, b, hostA, hostB] = await Promise.all([
      lab.agent("a", { clock: () => new Date(now), config: { ack_timeout_ms: 1_000 } }),
      lab.agent("b", { clock: () => new Date(now), config: { ack_timeout_ms: 1_000 } }), lab.host("a"), lab.host("b"),
    ]);
    await Promise.all([openShared(a, hostA), openShared(b, hostB)]);
    hostA.timeouts.add("life");
    for (const agent of [a, b]) for (const channel of CHANNELS) await emit(agent, channel, `timeout-${channel}`);
    const [resultA, resultB] = await Promise.all([dispatch(a), dispatchExpected(b, ["accepted", "accepted"])]);
    expect(resultA.map((result) => result.status)).toContain("retry_wait");
    expect(resultB.map((result) => result.status)).toEqual(["accepted", "accepted"]);
    expect((await http(b, "/health")).status).toBe(200);
    const timedOut = hostA.deliveries.find((delivery) => delivery.wake.attention_channel === "life")!;
    expect((await http(a, `/v1/receipts/${timedOut.wake.wake_batch_id}`)).body.receipts).toEqual([]);
    hostA.timeouts.clear();
    // A transport timeout may use up the dispatcher's bounded sweep. Resume
    // through its normal next pass instead of asserting an artificial SLA.
    const missingAccepted = 2 - resultA.filter((result) => result.status === "accepted").length;
    const resumed = await dispatchExpected(a, Array.from({ length: missingAccepted }, () => "accepted"));
    const accepted = [...resultA, ...resumed].filter((result) => result.status === "accepted");
    expect(accepted).toHaveLength(2);
    expect(new Set(accepted.map((result) => result.batch_id)).size).toBe(2);
    const lifeDeliveries = hostA.deliveries.filter((delivery) => delivery.wake.attention_channel === "life");
    expect(lifeDeliveries).toHaveLength(2);
    expect(lifeDeliveries[1].wake.wake_batch_id).toBe(timedOut.wake.wake_batch_id);
    expect(lifeDeliveries[1].attempt_id).not.toBe(timedOut.attempt_id);
    expect(lifeDeliveries[1].delivery_nonce).not.toBe(timedOut.delivery_nonce);
    expect((await inspect(a)).attempts.filter((attempt: { batch_id: string }) => attempt.batch_id === timedOut.wake.wake_batch_id).map((attempt: { state: string }) => attempt.state)).toEqual(["failed", "accepted"]);
    now += 2_000;
    expect(await dispatch(a)).toEqual([]);
    expect(await dispatch(b)).toEqual([]);
    expect((await inspect(a)).batches.map((batch: { state: string }) => batch.state)).toEqual(["needs_attention", "needs_attention"]);
    expect((await inspect(b)).batches.map((batch: { state: string }) => batch.state)).toEqual(["needs_attention", "needs_attention"]);
    expect(hostA.deliveries.filter((delivery) => delivery.wake.attention_channel === "work")).toHaveLength(1);
    expect(hostA.deliveries).toHaveLength(3);
    expect(hostB.deliveries.map((delivery) => delivery.wake.attention_channel).sort()).toEqual([...CHANNELS]);
    for (const [agent, host] of [[a, hostA], [b, hostB]] as const) {
      expect(host.deliveries.every((delivery) => delivery.wake.instance_id === agent.config.instance_id)).toBe(true);
    }
  }));

  it("SIGKILLs and restarts only daemon A, preserves its pending work, and leaves daemon B's live HTTP and leases usable", async () => withLab(async (lab) => {
    const [a, b, hostA, hostB] = await Promise.all([lab.process("a"), lab.process("b"), lab.host("a"), lab.host("b")]);
    expect(a.child.pid).not.toBe(b.child.pid);
    const oldA = await openShared(a, hostA);
    const openedB = await openShared(b, hostB);
    for (const channel of CHANNELS) await emit(a, channel, `pending-at-crash-${channel}`);
    expect((await http(a, "/v1/tick", {})).status).toBe(200);
    const pending = await inspect(a);
    expect(pending.batches.map((batch: { state: string }) => batch.state)).toEqual(["pending", "pending"]);
    for (const channel of CHANNELS) await emit(b, channel, `before-a-crash-${channel}`);
    await dispatchExpected(b, ["accepted", "accepted"]);
    const pidB = b.child.pid;
    await a.stop("SIGKILL");
    expect(a.child.signalCode).toBe("SIGKILL");
    expect((await http(b, "/health")).body).toMatchObject({ ok: true, instance_id: b.config.instance_id });
    for (const channel of CHANNELS) await emit(b, channel, `while-a-down-${channel}`);
    expect((await dispatchExpected(b, ["accepted", "accepted"])).map((result) => result.status)).toEqual(["accepted", "accepted"]);
    expect(hostA.deliveries).toHaveLength(0);
    expect(hostB.deliveries).toHaveLength(4);
    const restarted = await lab.process("a");
    expect(restarted.child.pid).not.toBe(a.child.pid);
    expect((await inspect(restarted)).events.map((event: { id: string }) => event.id)).toEqual(pending.events.map((event: { id: string }) => event.id));
    const reopened = await openShared(restarted, hostA);
    for (const channel of CHANNELS) expect(reopened.leases[channel].generation).toBe(oldA.leases[channel].generation + 1);
    expect((await dispatchExpected(restarted, ["accepted", "accepted"])).map((result) => result.status)).toEqual(["accepted", "accepted"]);
    expect(hostA.deliveries).toHaveLength(2);
    expect(hostA.deliveries.flatMap((delivery) => delivery.wake.claim_refs.map((claim) => claim.claim_id)).sort()).toEqual(pending.claims.map((claim: { id: string }) => claim.id).sort());
    expect(hostA.deliveries.every((delivery) => delivery.wake.instance_id === a.config.instance_id)).toBe(true);
    expect(await dispatch(restarted)).toEqual([]);
    expect(b.child.pid).toBe(pidB);
    expect(b.child.exitCode).toBeNull();
    expect((await http(b, "/health")).status).toBe(200);
    const bLife = hostB.deliveries.find((delivery) => delivery.wake.attention_channel === "life")!;
    await expect(b.client.consumeWakeEcho(openedB.leases.life, { observation_id: "b-survived-a-restart", delivery_nonce: bLife.delivery_nonce })).resolves.toMatchObject({ duplicate: false });
    expect(hostB.deliveries).toHaveLength(4);
  }));
});
