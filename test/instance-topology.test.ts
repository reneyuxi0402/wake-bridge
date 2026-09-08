import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AddressInfo } from "node:net";
import { describe, expect, it } from "vitest";
import { startDaemon } from "../src/daemon.js";

function instanceConfig(name: string, token: string) {
  return {
    instance_id: `${name}-instance`,
    owner_id: name,
    db_path: join(mkdtempSync(join(tmpdir(), `wake-bridge-${name}-`)), "bridge.sqlite"),
    timezone: "UTC",
    admin_token: token,
  };
}

function origin(address: AddressInfo | string | null): string {
  if (!address || typeof address === "string") throw new Error("expected TCP daemon address");
  return `http://127.0.0.1:${address.port}`;
}

async function request(base: string, token: string, path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`${base}${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
      ...init.headers,
    },
  });
}

describe("single-space instance topology", () => {
  it("isolates identity, credentials and durable events across two daemon instances", async () => {
    const tokenA = "instance-a-admin-token-0123456789";
    const tokenB = "instance-b-admin-token-0123456789";
    const daemonA = await startDaemon(instanceConfig("agent-a", tokenA), { host: "127.0.0.1", port: 0, scheduler_interval_ms: 0 });
    const daemonB = await startDaemon(instanceConfig("agent-b", tokenB), { host: "127.0.0.1", port: 0, scheduler_interval_ms: 0 });
    const baseA = origin(daemonA.address);
    const baseB = origin(daemonB.address);

    try {
      expect(baseA).not.toBe(baseB);
      const event = {
        source: "manual",
        event: { type: "job.completed", dedupe_key: "same-key", resource: { uri: "job://same" } },
      };
      expect((await request(baseA, tokenA, "/v1/events", { method: "POST", body: JSON.stringify(event) })).status).toBe(201);

      const beforeB = await request(baseB, tokenB, "/v1/events");
      expect(beforeB.status).toBe(200);
      expect((await beforeB.json()).events).toEqual([]);
      expect((await request(baseB, tokenA, "/v1/events")).status).toBe(401);

      expect((await request(baseB, tokenB, "/v1/events", { method: "POST", body: JSON.stringify(event) })).status).toBe(201);
      const eventsA = await (await request(baseA, tokenA, "/v1/events")).json();
      const eventsB = await (await request(baseB, tokenB, "/v1/events")).json();
      expect(eventsA.events).toHaveLength(1);
      expect(eventsB.events).toHaveLength(1);
      expect(eventsA.events[0]).toMatchObject({ instance_id: "agent-a-instance", owner_id: "agent-a" });
      expect(eventsB.events[0]).toMatchObject({ instance_id: "agent-b-instance", owner_id: "agent-b" });
    } finally {
      await daemonA.close();
      await daemonB.close();
    }
  });
});
