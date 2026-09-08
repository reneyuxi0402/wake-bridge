import { describe, expect, it, vi } from "vitest";
import {
  EVENT_CONTRACT_VERSION,
  SourcePushClient,
  validateWakeEventInput,
} from "../src/event-sdk.js";
import {
  SOURCE_ADAPTER_CONTRACT_VERSION,
  validateSourceManifest,
} from "../src/source-sdk.js";
import {
  HostSessionClient,
  LOCAL_HOST_PROTOCOL_VERSION,
  validateLocalHostDeliveryRequest,
} from "../src/transport-sdk.js";

describe("public event and source SDK contracts", () => {
  it("negotiates exact v1 contracts and fails closed on unsupported versions", () => {
    expect(validateWakeEventInput({
      schema_version: EVENT_CONTRACT_VERSION,
      type: "job.completed",
      dedupe_key: "job:1",
      resource: { uri: "job://one" },
    }, new Date("2026-08-30T00:00:00Z"))).toMatchObject({
      schema_version: 1,
      occurred_at: "2026-08-30T00:00:00.000Z",
    });
    expect(() => validateWakeEventInput({
      schema_version: 2,
      type: "job.completed",
      dedupe_key: "job:1",
      resource: { uri: "job://one" },
    })).toThrow(/unsupported/u);
    expect(() => validateSourceManifest({
      contract_version: 2 as 1,
      id: "fixture",
      version: "1.0.0",
      subject_ref: "fixture:owner",
      binding_fingerprint: `sha256:${"1".repeat(64)}`,
      read_side_effects: "none",
      credential_custody: "none",
      upstream_credential_breadth: "none",
      upstream_credential_scopes: [],
      connector_capabilities: ["events.read"],
    })).toThrow(/unsupported/u);
    expect(SOURCE_ADAPTER_CONTRACT_VERSION).toBe(1);
  });

  it("pushes only to its configured source path and rejects a mismatched receipt", async () => {
    const fetcher = vi.fn(async (url: string | URL, init?: RequestInit) => new Response(JSON.stringify({
      event: {
        id: "evt_one", source: "ci", type: "job.completed", dedupe_key: "job:1",
        schema_version: 1, occurred_at: "2026-08-30T00:00:00.000Z", received_at: "2026-08-30T00:00:01.000Z",
      },
      duplicate: false,
      suppressed: false,
    }), { status: 201, headers: { "content-type": "application/json" } }));
    const client = new SourcePushClient({
      base_url: "http://127.0.0.1:4311",
      source: "ci",
      token: "source-ci-token-0123456789abcdef",
      fetch: fetcher as typeof fetch,
    });
    await expect(client.emit({ type: "job.completed", dedupe_key: "job:1", resource: { uri: "job://one" } }))
      .resolves.toMatchObject({ event: { source: "ci" } });
    expect(String(fetcher.mock.calls[0][0])).toBe("http://127.0.0.1:4311/v1/ingress/ci/events");
    const request = JSON.parse(String(fetcher.mock.calls[0][1]?.body));
    expect(request).toMatchObject({ schema_version: 1, type: "job.completed" });
    expect(request).not.toHaveProperty("source");

    expect(() => new SourcePushClient({
      base_url: "https://wake.example.com",
      source: "ci",
      token: "source-ci-token-0123456789abcdef",
    })).toThrow(/loopback/u);
    expect(() => new SourcePushClient({
      base_url: "http://127.0.0.1:4311",
      source: "ci",
      token: "source-ci-token-0123456789abcdef",
      timeout_ms: 0,
    })).toThrow(/timeout_ms/u);
  });
});

describe("public out-of-process host contract", () => {
  it("validates exact local protocol v1 and rejects non-loopback daemon origins", () => {
    expect(validateLocalHostDeliveryRequest({
      protocol_version: LOCAL_HOST_PROTOCOL_VERSION,
      attempt_id: "attempt",
      delivery_nonce: "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn",
      wake: {
        schema_version: 1,
        instance_id: "fixture",
        wake_batch_id: "batch",
        attention_channel: "default",
        claim_refs: [],
        binding_generation: 1,
      },
    })).toMatchObject({ protocol_version: 1, wake: { binding_generation: 1 } });
    expect(() => validateLocalHostDeliveryRequest({ protocol_version: 2 })).toThrow(/invalid/u);
    expect(() => new HostSessionClient({
      base_url: "https://wake.example.com",
      host_token: "host-token-0123456789abcdef0123456789",
      adapter_kind: "fixture_host",
    })).toThrow(/loopback/u);
  });
});
