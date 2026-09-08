import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { bootstrapSourceConnector, SourceSupervisor } from "../src/source-connector.js";
import { startBotlingKnowsConnector } from "../src/connectors/botlingknows-mcp.js";
import { StreamableHttpMcpClient } from "../src/connectors/mcp-http-client.js";

async function jsonBody(request: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk as Uint8Array));
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : null;
}

function json(response: ServerResponse, status: number, value: unknown): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json");
  response.end(JSON.stringify(value));
}

function sse(response: ServerResponse, value: unknown, sessionId?: string): void {
  response.statusCode = 200;
  response.setHeader("content-type", "text/event-stream");
  if (sessionId) response.setHeader("mcp-session-id", sessionId);
  response.end(`event: message\ndata: ${JSON.stringify(value)}\n\n`);
}

async function fakeBotlingMcp(upstreamToken: string): Promise<{
  url: string;
  calls: any[];
  close: () => Promise<void>;
}> {
  const calls: any[] = [];
  const server = createServer(async (request, response) => {
    if (request.url !== `/mcp/${upstreamToken}`) {
      json(response, 404, { error: "not_found" });
      return;
    }
    const body = await jsonBody(request);
    calls.push({ body, session: request.headers["mcp-session-id"], accept: request.headers.accept });
    if (body.method === "initialize") {
      sse(response, {
        jsonrpc: "2.0",
        id: body.id,
        result: { protocolVersion: "2025-06-18", capabilities: {}, serverInfo: { name: "fixture", version: "1" } },
      }, "fixture-session");
      return;
    }
    if (body.method === "notifications/initialized") {
      response.statusCode = 202;
      response.end();
      return;
    }
    if (body.method === "tools/call") {
      const envelope = {
        ok: true,
        data: {
          notifications: [{
            id: 9,
            notif_type: "mention",
            category: "direct",
            created_at: "2026-08-29T06:00:00.000Z",
            question_id: 44,
            content_id: 99,
            actor_id: 7,
            summary: "must not cross the connector boundary",
          }],
        },
        meta: { next_cursor: null, unread_notifications: 1 },
      };
      sse(response, {
        jsonrpc: "2.0",
        id: body.id,
        result: { content: [{ type: "text", text: JSON.stringify(envelope) }], isError: false },
      });
      return;
    }
    json(response, 400, { error: "unexpected_method" });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}/mcp/${upstreamToken}`,
    calls,
    close: async () => new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
  };
}

describe("botlingknows MCP source connector", () => {
  it("rejects cleartext remote upstreams and non-loopback connector binds", async () => {
    expect(() => new StreamableHttpMcpClient("http://example.com/mcp/credential"))
      .toThrowError(/invalid/);
    await expect(startBotlingKnowsConnector({
      upstream_url: "https://example.com/mcp/credential",
      connector_token: "local-token",
      subject_ref: "botlingknows:test",
      host: "0.0.0.0",
      port: 0,
    })).rejects.toMatchObject({ code: "unsafe_connector_bind" });
  });

  it("keeps the upstream MCP identity in the connector and emits only normalized notification facts", async () => {
    const upstreamToken = "fake-upstream-token";
    const connectorToken = "fake-connector-token";
    const upstream = await fakeBotlingMcp(upstreamToken);
    const connector = await startBotlingKnowsConnector({
      upstream_url: upstream.url,
      connector_token: connectorToken,
      subject_ref: "botlingknows:test",
      host: "127.0.0.1",
      port: 0,
    });
    const connectorAddress = connector.address as AddressInfo;
    const bridge = new WakeBridge({
      instance_id: "botling-connector-test",
      owner_id: "owner",
      db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-botling-")), "bridge.sqlite"),
      timezone: "UTC",
    });
    const supervisor = new SourceSupervisor(bridge, [{
      id: "botlingknows",
      base_url: `http://127.0.0.1:${connectorAddress.port}`,
      token_env: "BOTLING_CONNECTOR_TOKEN",
      enabled: false,
      limit: 20,
    }], { BOTLING_CONNECTOR_TOKEN: connectorToken });
    try {
      const unauthorized = await fetch(`http://127.0.0.1:${connectorAddress.port}/v1/manifest`);
      expect(unauthorized.status).toBe(401);
      const status = await supervisor.runOnce("botlingknows");
      expect(status).toMatchObject({
        state: "healthy",
        checkpoint_revision: 1,
        last_run: { events_seen: 1, events_inserted: 1 },
      });
      expect(upstream.calls.map((call) => call.body.method)).toEqual([
        "initialize", "notifications/initialized", "tools/call",
      ]);
      expect(upstream.calls[1].session).toBe("fixture-session");
      expect(upstream.calls[2].body.params).toMatchObject({
        name: "botling_knows",
        arguments: {
          action: "notifications",
          payload: { view: "full", mark_read: false, limit: 20 },
        },
      });
      expect(upstream.calls[0].accept).toBe("application/json, text/event-stream");
      const event = bridge.listEvents({ source: "botlingknows" })[0];
      expect(event).toMatchObject({
        type: "mention",
        dedupe_key: "notification:9",
        resource: { uri: "botlingknows://content/99", cursor: 9 },
        metadata: { notification_id: 9, question_id: 44, content_id: 99 },
      });
      const durable = JSON.stringify({
        event,
        checkpoint: bridge.db.query("SELECT * FROM source_checkpoints"),
      });
      expect(durable).not.toContain("must not cross");
      expect(durable).not.toContain(upstreamToken);
      expect(durable).not.toContain(connectorToken);
      expect(durable).not.toContain(upstream.url);
    } finally {
      await supervisor.stop();
      bridge.close();
      await connector.close();
      await upstream.close();
    }
  });

  it("bootstraps from-now without emitting historical notifications and refuses to overwrite the checkpoint", async () => {
    const upstream = await fakeBotlingMcp("bootstrap-upstream-token");
    const connectorToken = "bootstrap-connector-token";
    const connector = await startBotlingKnowsConnector({
      upstream_url: upstream.url,
      connector_token: connectorToken,
      subject_ref: "botlingknows:test",
      host: "127.0.0.1",
      port: 0,
    });
    const address = connector.address as AddressInfo;
    const bridge = new WakeBridge({
      instance_id: "botling-bootstrap-test",
      owner_id: "owner",
      db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-bootstrap-")), "bridge.sqlite"),
      timezone: "UTC",
    });
    const config = {
      id: "botlingknows",
      base_url: `http://127.0.0.1:${address.port}`,
      token_env: "BOOTSTRAP_CONNECTOR_TOKEN",
      enabled: false,
      limit: 20,
    };
    const environment = { BOOTSTRAP_CONNECTOR_TOKEN: connectorToken };
    try {
      const expected = { subject_ref: "botlingknows:test", binding_fingerprint: `sha256:${createHash("sha256").update(upstream.url).digest("hex")}` };
      await expect(bootstrapSourceConnector(bridge, config, "from-now", environment, undefined, expected)).resolves.toMatchObject({
        source: "botlingknows",
        adapter_version: "0.1.0",
        revision: 1,
        cursor: { v: 1, high_watermark: 9, page_cursor: null, pending_max_id: 9 },
      });
      expect(upstream.calls.at(-1)?.body.params).toMatchObject({
        arguments: {
          action: "notifications",
          payload: { view: "full", mark_read: false, limit: 50 },
        },
      });
      expect(bridge.listEvents({ source: "botlingknows" })).toHaveLength(0);
      await expect(bootstrapSourceConnector(bridge, config, "from-now", environment, undefined, expected))
        .rejects.toMatchObject({ code: "source_already_bootstrapped" });

      const supervisor = new SourceSupervisor(bridge, [config], environment);
      await expect(supervisor.runOnce("botlingknows")).resolves.toMatchObject({
        state: "healthy",
        checkpoint_revision: 2,
        last_run: { events_seen: 0, events_inserted: 0 },
      });
      await supervisor.stop();
      expect(bridge.listEvents({ source: "botlingknows" })).toHaveLength(0);
    } finally {
      bridge.close();
      await connector.close();
      await upstream.close();
    }
  });

  it("projects revoked upstream identity as a structural connector failure without leaking its URL", async () => {
    const upstreamToken = "revoked-upstream-token";
    const upstream = createServer((_request, response) => {
      json(response, 401, { error: "unauthorized", detail: upstreamToken });
    });
    upstream.listen(0, "127.0.0.1");
    await once(upstream, "listening");
    const upstreamAddress = upstream.address() as AddressInfo;
    const connector = await startBotlingKnowsConnector({
      upstream_url: `http://127.0.0.1:${upstreamAddress.port}/mcp/${upstreamToken}`,
      connector_token: "local-connector-token",
      subject_ref: "botlingknows:test",
      host: "127.0.0.1",
      port: 0,
    });
    try {
      const address = connector.address as AddressInfo;
      const response = await fetch(`http://127.0.0.1:${address.port}/v1/poll`, {
        method: "POST",
        headers: {
          authorization: "Bearer local-connector-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({ cursor: null, limit: 20 }),
      });
      expect(response.status).toBe(424);
      const text = await response.text();
      expect(text).toContain("upstream_mcp_unauthorized");
      expect(text).not.toContain(upstreamToken);
      expect(text).not.toContain(String(upstreamAddress.port));
    } finally {
      await connector.close();
      await new Promise<void>((resolve, reject) => upstream.close((error) => error ? reject(error) : resolve()));
    }
  });
});
