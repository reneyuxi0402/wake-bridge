import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { sqlJson, sqlValue } from "../src/db.js";
import { WakeBridgeMcpServer } from "../src/mcp.js";

/**
 * Regression cover for a 2026-09-30 outage: `claims` grew past 1000 rows and the
 * scheduler stopped seeing new work, because every listing is `ORDER BY created_at
 * ASC LIMIT <page>` and the default page was 1000.  A backlog of retired rows is
 * therefore enough to hide live ones, with no error anywhere — reads keep looking
 * healthy while reporting only the oldest page.
 */

function dbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-backlog-")), "bridge.sqlite");
}

function bridge(): WakeBridge {
  return new WakeBridge({
    instance_id: "i",
    owner_id: "o",
    db_path: dbPath(),
    timezone: "UTC",
    endpoint_lease_ms: 60_000,
  } as any);
}

async function callTool(target: WakeBridge, name: string, args: Record<string, unknown> = {}): Promise<any> {
  const server = new WakeBridgeMcpServer(target);
  const response = await server.handleRequest({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } });
  return JSON.parse((response?.result as any).content?.[0]?.text);
}

/**
 * Inserts `count` already-retired claims dated well before now, so anything emitted
 * afterwards sorts past the first listing page.  These never reach the scheduler, so
 * they are written directly rather than driven through emit/dismiss — one transaction
 * instead of 2×count sqlite3 invocations.
 */
function buryRetiredClaims(target: WakeBridge, count: number): void {
  const statements: string[] = [];
  for (let index = 0; index < count; index += 1) {
    const stamp = new Date(Date.UTC(2020, 0, 1) + index * 1000).toISOString();
    statements.push(
      `INSERT INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, note, created_at, updated_at, snooze_until, consumed_result_json, dismissed_reason)
       VALUES(${sqlValue(`ac_buried_${index}`)}, ${sqlValue("i")}, ${sqlValue("policy")}, ${sqlJson([])}, ${sqlJson({ uri: `job://buried/${index}` })}, ${sqlValue("default")}, 1, ${sqlValue("default")}, ${sqlValue(stamp)}, NULL, 0, ${sqlValue("dismissed")}, ${sqlValue("reconsider")}, NULL, ${sqlValue(stamp)}, ${sqlValue(stamp)}, NULL, NULL, ${sqlValue("buried by test")});`,
    );
  }
  target.db.transaction(statements);
}

describe("claim backlog beyond one listing page", () => {
  it("still promotes and dispatches a claim created after 1000 retired ones", async () => {
    const target = bridge();
    buryRetiredClaims(target, 1000);
    const fresh = target.emit("manual", {
      type: "job.completed",
      dedupe_key: "fresh:1",
      resource: { uri: "job://fresh/1" },
    });

    const endpoint = target.registerEndpoint({ host_kind: "mock", session_ref: "session" });
    target.takeover("default", endpoint.id, 0);
    expect(target.tick().eligible_claims).toContain(fresh.claim!.id);

    const dispatched = await target.dispatchDue();
    expect(dispatched[0]?.status).toBe("accepted");
    expect(target.getClaim(fresh.claim!.id)?.state).toBe("batched");
    target.close();
  });

  it("counts every claim state from SQL rather than the first listing page", async () => {
    const target = bridge();
    buryRetiredClaims(target, 1000);
    target.emit("manual", { type: "job.completed", dedupe_key: "fresh:2", resource: { uri: "job://fresh/2" } });

    expect(target.countClaimsByState()).toMatchObject({ dismissed: 1000, pending: 1 });
    const status = await callTool(target, "attention_status");
    expect(status.claims).toMatchObject({ dismissed: 1000, pending: 1 });
    target.close();
  });

  it("keeps tick() scanning only live claims, whatever the backlog size", () => {
    const target = bridge();
    buryRetiredClaims(target, 1200);
    target.emit("manual", { type: "job.completed", dedupe_key: "fresh:3", resource: { uri: "job://fresh/3" } });

    expect(target.listClaims({ states: ["pending", "deferred", "eligible"], limit: 100_000 })).toHaveLength(1);
    expect(target.listClaims({ limit: 100_000 })).toHaveLength(1201);
    target.close();
  });

  it("lists the newest events, not the first page ever written", async () => {
    const target = bridge();
    const statements: string[] = [];
    for (let index = 0; index < 1000; index += 1) {
      const stamp = new Date(Date.UTC(2020, 0, 1) + index * 1000).toISOString();
      statements.push(
        `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview, matched_policy_id, matched_policy_version)
         VALUES(${sqlValue(`evt_old_${index}`)}, ${sqlValue("i")}, ${sqlValue("o")}, ${sqlValue("manual")}, ${sqlValue("job.completed")}, 1, ${sqlValue(stamp)}, ${sqlValue(stamp)}, ${sqlValue(`old:${index}`)}, NULL, ${sqlValue("normal")}, ${sqlValue("default")}, NULL, ${sqlJson({ uri: `job://old/${index}` })}, ${sqlJson({})}, NULL, NULL, NULL);`,
        `INSERT INTO event_status(event_id, state, updated_at) VALUES(${sqlValue(`evt_old_${index}`)}, ${sqlValue("consumed")}, ${sqlValue(stamp)});`,
      );
    }
    target.db.transaction(statements);
    const fresh = target.emit("manual", { type: "job.completed", dedupe_key: "fresh:events", resource: { uri: "job://fresh/events" } });

    // The default page is 100 rows; oldest-first it would be nothing but evt_old_*.
    const listed = await callTool(target, "attention_event_list");
    expect(listed.events.map((event: any) => event.event_id ?? event.id)).toContain(fresh.event.id);
    expect(target.listEvents({ limit: 100 })[0]?.id).toBe("evt_old_0");
    target.close();
  });

  it("still surfaces a fresh needs_attention batch behind 1000 settled ones", async () => {
    const target = bridge();
    const statements: string[] = [];
    for (let index = 0; index < 1000; index += 1) {
      const stamp = new Date(Date.UTC(2020, 0, 1) + index * 1000).toISOString();
      statements.push(
        `INSERT INTO batches(id, instance_id, policy_id, policy_version, attention_channel, coalesce_key, claim_ids_json, event_ids_json, state, deadline, not_before, attempt, created_at, updated_at)
         VALUES(${sqlValue(`wb_settled_${index}`)}, ${sqlValue("i")}, ${sqlValue("default")}, 1, ${sqlValue("default")}, NULL, ${sqlJson([])}, ${sqlJson([])}, ${sqlValue("seen")}, NULL, ${sqlValue(stamp)}, 0, ${sqlValue(stamp)}, ${sqlValue(stamp)});`,
      );
    }
    statements.push(
      `INSERT INTO batches(id, instance_id, policy_id, policy_version, attention_channel, coalesce_key, claim_ids_json, event_ids_json, state, deadline, not_before, attempt, created_at, updated_at)
       VALUES(${sqlValue("wb_fresh")}, ${sqlValue("i")}, ${sqlValue("default")}, 1, ${sqlValue("default")}, NULL, ${sqlJson([])}, ${sqlJson([])}, ${sqlValue("needs_attention")}, NULL, ${sqlValue("2026-10-01T00:00:00.000Z")}, 1, ${sqlValue("2026-10-01T00:00:00.000Z")}, ${sqlValue("2026-10-01T00:00:00.000Z")});`,
    );
    target.db.transaction(statements);

    const health = await callTool(target, "attention_wake_health");
    expect(health.deliveries.map((delivery: any) => delivery.batch_id ?? delivery.id)).toContain("wb_fresh");
    expect(target.countBatchesByState()).toMatchObject({ seen: 1000, needs_attention: 1 });
    target.close();
  });
});
