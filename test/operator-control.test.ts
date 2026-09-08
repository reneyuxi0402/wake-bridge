import { mkdtempSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { operatorStatus, retryDeadLetter } from "../src/operator-control.js";
import type { BridgeConfig, TransportContext, TransportResult, WakeTransport } from "../src/types.js";

class PermanentFailureTransport implements WakeTransport {
  readonly kind = "permanent_failure";
  dispatch(_context: TransportContext): TransportResult {
    return {
      accepted: false,
      retryable: false,
      error_class: "provider_rejected",
      error_message: "provider token super-secret-value was rejected",
    };
  }
}

class AcceptTransport implements WakeTransport {
  readonly kind = "permanent_failure";
  dispatch(): TransportResult {
    return { accepted: true, transport_kind: this.kind };
  }
}

function config(): BridgeConfig {
  return {
    instance_id: "operator-instance",
    owner_id: "operator-owner",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-operator-")), "bridge.sqlite"),
    timezone: "UTC",
    admin_token: "owner-operator-token-0123456789abcdef",
  };
}

function deadLetter(configValue: BridgeConfig): { bridge: WakeBridge; batchId: string; claimId: string } {
  const bridge = new WakeBridge(configValue, {
    autoMockTransport: false,
    transports: [new PermanentFailureTransport()],
    recoverDispatchLeases: false,
  });
  const endpoint = bridge.registerEndpoint({
    host_kind: "fixture",
    session_ref: "secret-session-name",
    capabilities: { cold_push: true },
    routes: [{ kind: "permanent_failure", address: { token: "route-super-secret-value" } }],
  });
  bridge.takeover("default", endpoint.id, 0);
  const emitted = bridge.emit("fixture", {
    type: "operator.test",
    dedupe_key: "operator:one",
    resource: { uri: "fixture://operator/one" },
  });
  bridge.tick();
  const batch = bridge.listBatches()[0];
  if (!batch || !emitted.claim) throw new Error("fixture did not create a batch and claim");
  return { bridge, batchId: batch.id, claimId: emitted.claim.id };
}

describe("operator status and dead-letter recovery", () => {
  it("reports secret-free queue/host/source health without mutating an expired or failed queue", async () => {
    const fixture = deadLetter(config());
    try {
      await expect(fixture.bridge.dispatchDue()).resolves.toMatchObject([{ status: "dead_letter", error_class: "provider_rejected" }]);
      fixture.bridge.db.exec(`
        UPDATE outbox_attempts SET error_class='provider secret leaked' WHERE batch_id='${fixture.batchId}';
        UPDATE batches SET last_error='provider secret leaked' WHERE id='${fixture.batchId}';
      `);
      const before = fixture.bridge.getBatch(fixture.batchId);
      const status = operatorStatus(fixture.bridge, { sources: [{
        source: "fixture-source",
        enabled: true,
        state: "needs_attention",
        last_started_at: "2026-08-30T00:00:00.000Z",
        last_success_at: null,
        last_error_at: "2026-08-30T00:00:01.000Z",
        last_error_class: "source connector token=source-secret-value",
        consecutive_failures: 1,
        next_poll_at: null,
        checkpoint_revision: 3,
        checkpoint_subject_ref: "fixture:owner",
        checkpoint_binding_fingerprint: `sha256:${"1".repeat(64)}`,
        last_run: null,
      }] });
      expect(status).toMatchObject({
        ok: false,
        health: "needs_attention",
        attention_required: true,
        queue: {
          counts: { dead_letter: 1 },
          failed_attempts_by_class: { invalid_error_class: 1 },
          dead_letters: [{ batch_id: fixture.batchId, attempt: 1, error_class: "invalid_error_class" }],
        },
        hosts: { endpoints: { total: 1, live: 1 }, bindings: { total: 1, live: 1 } },
        sources: {
          runtime_observed: true,
          counts: { needs_attention: 1 },
          items: [{ source: "fixture-source", last_error_class: "invalid_error_class" }],
        },
      });
      const encoded = JSON.stringify(status);
      expect(encoded).not.toContain("route-super-secret-value");
      expect(encoded).not.toContain("secret-session-name");
      expect(encoded).not.toContain("provider token super-secret-value");
      expect(encoded).not.toContain("provider secret leaked");
      expect(encoded).not.toContain("source-secret-value");
      expect(fixture.bridge.getBatch(fixture.batchId)).toEqual(before);
    } finally {
      fixture.bridge.close();
    }
  });

  it("reopens exactly one dead-letter generation, survives restart, and does not dispatch in the retry call", async () => {
    const configValue = config();
    const fixture = deadLetter(configValue);
    await fixture.bridge.dispatchDue();
    fixture.bridge.close();

    const left = new WakeBridge(configValue, { autoMockTransport: false, transports: [new AcceptTransport()], recoverDispatchLeases: false });
    const right = new WakeBridge(configValue, { autoMockTransport: false, transports: [new AcceptTransport()], recoverDispatchLeases: false });
    try {
      expect(operatorStatus(left).queue.counts.dead_letter).toBe(1);
      const first = retryDeadLetter(left, { batch_id: fixture.batchId, expected_attempt: 1, reason: "operator confirmed route repair" });
      const replay = retryDeadLetter(right, { batch_id: fixture.batchId, expected_attempt: 1, reason: "response was lost" });
      expect(first).toMatchObject({ retried: true, batch: { state: "retry_wait", attempt: 1, last_error: "operator_retry" } });
      expect(replay).toMatchObject({ retried: false, batch: { state: "retry_wait", attempt: 1 } });
      expect(left.listAttempts(fixture.batchId)).toHaveLength(1);
      expect(left.listReceipts(fixture.batchId)).toHaveLength(0);
      expect(left.db.query("SELECT from_state, to_state, reason FROM batch_transitions WHERE batch_id='" + fixture.batchId + "' ORDER BY seq DESC LIMIT 1"))
        .toEqual([{ from_state: "dead_letter", to_state: "retry_wait", reason: "operator_retry:operator confirmed route repair" }]);

      await expect(left.dispatchDue()).resolves.toMatchObject([{ status: "accepted", batch_id: fixture.batchId }]);
      expect(left.getBatch(fixture.batchId)).toMatchObject({ state: "dispatched", attempt: 2 });
      expect(() => retryDeadLetter(right, {
        batch_id: fixture.batchId,
        expected_attempt: 1,
        reason: "stale command",
      })).toThrow(/only a dead-letter/u);
    } finally {
      right.close();
      left.close();
    }
  });

  it("rejects retry after the underlying claim was finalized", async () => {
    const fixture = deadLetter(config());
    try {
      await fixture.bridge.dispatchDue();
      fixture.bridge.dismissClaim(fixture.claimId, "handled elsewhere");
      expect(() => retryDeadLetter(fixture.bridge, {
        batch_id: fixture.batchId,
        expected_attempt: 1,
        reason: "do not revive finalized work",
      })).toThrow(/no longer retryable/u);
    } finally {
      fixture.bridge.close();
    }
  });

  it("keeps live status/retry owner-authenticated and source credentials powerless", async () => {
    const configValue = config();
    const fixture = deadLetter(configValue);
    await fixture.bridge.dispatchDue();
    const sourceToken = "source-operator-token-0123456789abcdef";
    const daemon = await startDaemon(configValue, {
      bridge: fixture.bridge,
      host: "127.0.0.1",
      port: 0,
      scheduler_interval_ms: 0,
      source_credentials: [{ id: "fixture-source", source: "fixture", token: sourceToken }],
    });
    const address = daemon.address as AddressInfo;
    const base = `http://127.0.0.1:${address.port}`;
    const request = (path: string, token: string, body?: unknown) => fetch(`${base}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: { authorization: `Bearer ${token}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    try {
      expect((await request("/v1/status", sourceToken)).status).toBe(401);
      const live = await request("/v1/status", configValue.admin_token!);
      expect(live.status).toBe(200);
      await expect(live.json()).resolves.toMatchObject({
        health: "needs_attention",
        queue: { counts: { dead_letter: 1 } },
        sources: { runtime_observed: true },
      });
      expect((await request(`/v1/batches/${fixture.batchId}/retry`, sourceToken, {
        expected_attempt: 1, reason: "source must not retry",
      })).status).toBe(401);
      const retried = await request(`/v1/batches/${fixture.batchId}/retry`, configValue.admin_token!, {
        expected_attempt: 1, reason: "owner confirmed repair",
      });
      expect(retried.status).toBe(200);
      await expect(retried.json()).resolves.toMatchObject({ retried: true, batch: { state: "retry_wait", attempt: 1 } });
    } finally {
      await daemon.close();
      fixture.bridge.close();
    }
  });
});
