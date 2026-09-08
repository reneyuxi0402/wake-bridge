import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { createHash, timingSafeEqual } from "node:crypto";
import { BridgeError } from "../core.js";
import {
  BotlingKnowsSourceAdapter,
  BOTLING_KNOWS_MAX_PAGE_SIZE,
  botlingKnowsCursorFromNow,
  type BotlingKnowsNotificationClient,
  type BotlingKnowsNotificationPage,
} from "../adapters/botlingknows.js";
import type { SourcePollContext } from "../types.js";
import { StreamableHttpMcpClient } from "./mcp-http-client.js";

export interface BotlingKnowsConnectorOptions {
  upstream_url: string;
  connector_token: string;
  subject_ref: string;
  host?: string;
  port?: number;
  fetcher?: typeof fetch;
}

function jsonResponse(response: ServerResponse, status: number, value: unknown): void {
  const text = JSON.stringify(value);
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("content-length", Buffer.byteLength(text));
  response.end(text);
}

async function readJson(request: IncomingMessage): Promise<Record<string, any>> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.from(chunk as Uint8Array);
    bytes += buffer.length;
    if (bytes > 1_048_576) throw new BridgeError("connector request is too large", "oversized_request", 413);
    chunks.push(buffer);
  }
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("object required");
    return value as Record<string, any>;
  } catch {
    throw new BridgeError("connector request must be a JSON object", "invalid_json", 400);
  }
}

function secretEqual(actual: string, expected: string): boolean {
  const left = Buffer.from(actual, "utf8");
  const right = Buffer.from(expected, "utf8");
  return left.length === right.length && left.length > 0 && timingSafeEqual(left, right);
}

class BotlingKnowsMcpNotificationClient implements BotlingKnowsNotificationClient {
  constructor(private readonly mcp: StreamableHttpMcpClient) {}

  async notifications(input: {
    view: "full";
    mark_read: false;
    limit: number;
    cursor?: string;
  }): Promise<BotlingKnowsNotificationPage> {
    const envelope = await this.mcp.callTool("botling_knows", {
      action: "notifications",
      payload: input,
      idempotency_key: "",
    });
    if (envelope.ok !== true || !envelope.data || !Array.isArray(envelope.data.notifications)) {
      throw new BridgeError("botlingknows MCP returned an error envelope", "botlingknows_mcp_error", 424);
    }
    return {
      notifications: envelope.data.notifications,
      next_cursor: envelope.meta?.next_cursor ?? null,
    };
  }
}

export async function startBotlingKnowsConnector(options: BotlingKnowsConnectorOptions): Promise<{
  server: ReturnType<typeof createServer>;
  address: AddressInfo | string | null;
  close: () => Promise<void>;
}> {
  if (!options.connector_token) throw new BridgeError("connector token is required", "invalid_connector_config", 400);
  const host = options.host ?? "127.0.0.1";
  if (!["127.0.0.1", "::1"].includes(host)) {
    throw new BridgeError("connector must bind a numeric loopback address", "unsafe_connector_bind", 409);
  }
  if (!options.subject_ref) throw new BridgeError("connector subject_ref is required", "invalid_connector_config", 400);
  const client = new BotlingKnowsMcpNotificationClient(new StreamableHttpMcpClient(options.upstream_url, options.fetcher));
  const adapter = new BotlingKnowsSourceAdapter(client, {
    subject_ref: options.subject_ref,
    binding_fingerprint: `sha256:${createHash("sha256").update(options.upstream_url).digest("hex")}`,
  });
  const server = createServer(async (request, response) => {
    try {
      const path = (request.url || "/").split("?")[0];
      if (request.method === "GET" && path === "/health") {
        jsonResponse(response, 200, { ok: true, source: adapter.manifest.id });
        return;
      }
      const bearer = typeof request.headers.authorization === "string" && request.headers.authorization.startsWith("Bearer ")
        ? request.headers.authorization.slice(7)
        : "";
      if (!secretEqual(bearer, options.connector_token)) {
        jsonResponse(response, 401, { error: "unauthorized" });
        return;
      }
      if (request.method === "GET" && path === "/v1/manifest") {
        jsonResponse(response, 200, adapter.manifest);
        return;
      }
      if (request.method === "POST" && path === "/v1/poll") {
        const body = await readJson(request);
        const limit = Number(body.limit);
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > BOTLING_KNOWS_MAX_PAGE_SIZE || !("cursor" in body)) {
          throw new BridgeError("poll context is invalid", "invalid_source_poll", 400);
        }
        jsonResponse(response, 200, await adapter.poll({ cursor: body.cursor, limit } as SourcePollContext));
        return;
      }
      if (request.method === "POST" && path === "/v1/bootstrap") {
        const body = await readJson(request);
        if (body.mode !== "from-now") {
          throw new BridgeError("unsupported bootstrap mode", "invalid_source_bootstrap", 400);
        }
        jsonResponse(response, 200, { cursor: await botlingKnowsCursorFromNow(client) });
        return;
      }
      jsonResponse(response, 404, { error: "not_found" });
    } catch (error) {
      const bridgeError = error instanceof BridgeError ? error : new BridgeError("connector failed", "connector_error", 500);
      const structural = bridgeError.code.startsWith("invalid_source_")
        || bridgeError.code === "botlingknows_mcp_error"
        || (bridgeError.code.startsWith("upstream_mcp_") && bridgeError.status !== 503);
      jsonResponse(response, structural ? 424 : bridgeError.status, { error: bridgeError.message, code: bridgeError.code });
    }
  });
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => { server.off("listening", onListening); reject(error); };
    const onListening = () => { server.off("error", onError); resolve(); };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(options.port ?? 4391, host);
  });
  return {
    server,
    address: server.address(),
    close: async () => new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
  };
}
