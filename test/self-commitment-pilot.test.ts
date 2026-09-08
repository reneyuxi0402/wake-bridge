import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";

describe("self-commitment pilot", () => {
  it("survives restart, wakes the bound session once, and closes with seen + consumed receipts", async () => {
    const path = join(mkdtempSync(join(tmpdir(), "wake-bridge-self-pilot-")), "bridge.sqlite");
    const config = {
      instance_id: "pilot-instance",
      owner_id: "pilot-owner",
      db_path: path,
      timezone: "Asia/Shanghai",
      endpoint_lease_ms: 60_000,
    };
    const first = new WakeBridge(config);
    const endpoint = first.registerEndpoint({ host_kind: "mock", session_ref: "life-window" });
    const binding = first.takeover("life", endpoint.id, 0);
    const scheduled = first.scheduleClaim({
      resource: { uri: "memory://thought/self-pilot" },
      eligible_after: new Date(Date.now() - 1_000).toISOString(),
      attention_channel: "life",
      reason_code: "reconsider",
      note: "做完梦后重新判断",
      idempotency_key: "self-pilot-1",
    });
    expect(scheduled.claim.origin).toBe("self_commitment");
    first.close();

    const second = new WakeBridge(config);
    second.tick();
    await expect(second.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);
    expect(second.mockTransport.calls).toHaveLength(1);
    expect(second.mockTransport.calls[0]?.payload.claim_refs).toMatchObject([{
      claim_id: scheduled.claim.id,
      source: "self_commitment",
      origin: "self_commitment",
      resource: { uri: "memory://thought/self-pilot" },
    }]);
    const batch = second.listBatches()[0];
    second.ackBatch(batch.id, endpoint.id, binding.generation);
    second.consumeClaim(scheduled.claim.id, { reviewed: true });
    expect(second.listReceipts(batch.id).map((receipt) => receipt.stage)).toEqual([
      "transport_accepted",
      "agent_seen",
      "agent_consumed",
    ]);
    expect(await second.dispatchDue()).toEqual([]);
    second.close();
  });
});
