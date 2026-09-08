import { createServer, type ServerResponse } from "node:http";
import { mkdtempSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { describe, expect, it } from "vitest";
import { startGroupChatConnector } from "../src/connectors/group-chat-http.js";
import { WakeBridge } from "../src/core.js";
import { SourceSupervisor, bootstrapSourceConnector } from "../src/source-connector.js";

describe("Group Chat source connector", () => {
  it("binds a scoped principal and maps a real feed page without copying message bodies", async () => {
    const upstreamToken = "group-chat-service-token";
    let unsupportedSchema = false;
    const upstream = createServer((request, response) => {
      if (request.headers.authorization !== `Bearer ${upstreamToken}`) {
        json(response, 403, { error: "forbidden" });
        return;
      }
      if (request.url === "/api/consumer") {
        json(response, 200, {
          provider: "group-chat",
          provider_version: "0.1.0",
          principal_member_id: "heng",
          scopes: ["events:read", "messages:read"],
          room_allowlist: ["room_home"],
        });
        return;
      }
      if (request.url?.startsWith("/api/events?")) {
        json(response, 200, {
          events: [{
            schema_version: unsupportedSchema ? 2 : 1,
            event_id: unsupportedSchema ? "gev_2" : "gev_1",
            event_cursor: unsupportedSchema ? 10 : 9,
            type: "room.mention.created",
            occurred_at: "2026-09-01T12:00:00.000Z",
            room_id: "room_home",
            message_id: "msg_1",
            message_cursor: 4,
            actor_id: "yuxi",
            direct: true,
            metadata: { mentioned_member_id: "heng" },
          }],
          head_event_cursor: unsupportedSchema ? 10 : 9,
          next_cursor: unsupportedSchema ? 10 : 9,
          has_more: false,
          secret_body: "must not be stored",
        });
        return;
      }
      json(response, 404, { error: "not_found" });
    });
    upstream.listen(0, "127.0.0.1");
    await once(upstream, "listening");
    const upstreamAddress = upstream.address() as AddressInfo;
    const connectorToken = "connector-local-token";
    const connector = await startGroupChatConnector({
      upstream_url: `http://127.0.0.1:${upstreamAddress.port}`,
      upstream_token: upstreamToken,
      connector_token: connectorToken,
      port: 0,
    });
    const connectorAddress = connector.address as AddressInfo;
    const bridge = new WakeBridge({
      instance_id: "group-chat-connector-test",
      owner_id: "heng",
      db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-group-chat-")), "bridge.sqlite"),
      timezone: "UTC",
    });
    const supervisor = new SourceSupervisor(bridge, [{
      id: "group_chat",
      base_url: `http://127.0.0.1:${connectorAddress.port}`,
      token_env: "GROUP_CHAT_CONNECTOR_TOKEN",
      enabled: false,
      limit: 100,
    }], { GROUP_CHAT_CONNECTOR_TOKEN: connectorToken });
    try {
      expect(connector.manifest).toMatchObject({
        subject_ref: "group-chat:principal:heng",
        read_side_effects: "none",
        credential_custody: "connector",
      });
      await expect(supervisor.runOnce("group_chat")).resolves.toMatchObject({
        state: "healthy",
        last_run: { events_seen: 1, events_inserted: 1 },
      });
      const event = bridge.listEvents({ source: "group_chat" })[0];
      expect(event).toMatchObject({
        type: "mention",
        dedupe_key: "event:gev_1",
        resource: { uri: "group-chat://room_home/messages/msg_1", cursor: 4 },
        metadata: { room_id: "room_home", direct: true },
      });
      const durable = JSON.stringify({
        event,
        checkpoint: bridge.db.query("SELECT * FROM source_checkpoints"),
      });
      expect(durable).not.toContain("must not be stored");
      expect(durable).not.toContain(upstreamToken);
      expect(durable).not.toContain(connectorToken);
      unsupportedSchema = true;
      await expect(supervisor.runOnce("group_chat")).resolves.toMatchObject({
        state: "needs_attention",
        last_error_class: "source_connector_contract_error",
      });
      expect(bridge.listEvents({ source: "group_chat" })).toHaveLength(1);
    } finally {
      await supervisor.stop();
      bridge.close();
      await connector.close();
      await new Promise<void>((resolve, reject) =>
        upstream.close((error) => (error ? reject(error) : resolve())),
      );
    }
  });

  it("supports explicit from-now bootstrap at the provider head", async () => {
    const upstream = createServer((request, response) => {
      if (request.url === "/api/consumer") {
        json(response, 200, {
          provider: "group-chat",
          provider_version: "0.1.0",
          principal_member_id: "fixture-member",
          scopes: ["events:read", "messages:read"],
          room_allowlist: ["room_home"],
        });
        return;
      }
      json(response, 200, { events: [], head_event_cursor: 42, next_cursor: 42, has_more: false });
    });
    upstream.listen(0, "127.0.0.1");
    await once(upstream, "listening");
    const upstreamAddress = upstream.address() as AddressInfo;
    const connector = await startGroupChatConnector({
      upstream_url: `http://127.0.0.1:${upstreamAddress.port}`,
      upstream_token: "service-token",
      connector_token: "connector-token",
      port: 0,
    });
    const address = connector.address as AddressInfo;
    const bridge = new WakeBridge({
      instance_id: "group-chat-bootstrap-test",
      owner_id: "fixture-owner",
      db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-group-bootstrap-")), "bridge.sqlite"),
      timezone: "UTC",
    });
    try {
      await expect(bootstrapSourceConnector(
        bridge,
        {
          id: "group_chat",
          base_url: `http://127.0.0.1:${address.port}`,
          token_env: "CONNECTOR_TOKEN",
          enabled: false,
        },
        "from-now",
        { CONNECTOR_TOKEN: "connector-token" },
        undefined,
        {
          subject_ref: connector.manifest.subject_ref,
          binding_fingerprint: connector.manifest.binding_fingerprint,
        },
      )).resolves.toMatchObject({ cursor: { v: 1, event_cursor: 42 } });
      expect(bridge.listEvents({ source: "group_chat" })).toHaveLength(0);
    } finally {
      bridge.close();
      await connector.close();
      await new Promise<void>((resolve, reject) =>
        upstream.close((error) => (error ? reject(error) : resolve())),
      );
    }
  });
});

function json(response: ServerResponse, status: number, value: unknown): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json");
  response.end(JSON.stringify(value));
}
