import { createHash, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import {
  GROUP_CHAT_MAX_PAGE_SIZE,
  GroupChatSourceAdapter,
  type GroupChatEventFeedClient,
  type GroupChatEventPage,
  type GroupChatEventType,
} from "../adapters/group-chat.js";
import { BridgeError } from "../core.js";
import { WakeBridgeSdkError } from "../sdk-error.js";
import type { SourceCursor, SourcePollContext } from "../types.js";

interface GroupChatConsumerIdentity {
  provider: "group-chat";
  provider_version: string;
  principal_member_id: string;
  scopes: string[];
  room_allowlist: string[];
}

export interface GroupChatConnectorOptions {
  upstream_url: string;
  upstream_token: string;
  connector_token: string;
  host?: string;
  port?: number;
  fetcher?: typeof fetch;
}

class GroupChatHttpClient implements GroupChatEventFeedClient {
  readonly baseUrl: string;

  constructor(
    upstreamUrl: string,
    private readonly token: string,
    private readonly fetcher: typeof fetch = fetch,
  ) {
    let url: URL;
    try {
      url = new URL(upstreamUrl);
    } catch {
      throw new BridgeError("group chat upstream URL is invalid", "invalid_connector_config", 400);
    }
    if (
      url.protocol !== "http:" ||
      !["127.0.0.1", "::1", "[::1]"].includes(url.hostname) ||
      url.username ||
      url.password ||
      (url.pathname !== "/" && url.pathname !== "") ||
      url.search ||
      url.hash
    ) {
      throw new BridgeError(
        "group chat upstream must be a loopback HTTP origin",
        "unsafe_connector_upstream",
        409,
      );
    }
    if (!token) throw new BridgeError("group chat upstream token is required", "invalid_connector_config", 400);
    this.baseUrl = url.origin;
  }

  async identity(): Promise<GroupChatConsumerIdentity> {
    const value = await this.getJson("/api/consumer") as Partial<GroupChatConsumerIdentity>;
    if (
      value.provider !== "group-chat" ||
      typeof value.provider_version !== "string" ||
      !value.provider_version ||
      typeof value.principal_member_id !== "string" ||
      !value.principal_member_id ||
      !Array.isArray(value.scopes) ||
      !value.scopes.includes("events:read") ||
      !value.scopes.includes("messages:read") ||
      !Array.isArray(value.room_allowlist) ||
      value.room_allowlist.length === 0 ||
      value.room_allowlist.some((room) => typeof room !== "string" || !room)
    ) {
      throw new BridgeError("group chat consumer identity is invalid", "invalid_source_page", 502);
    }
    return value as GroupChatConsumerIdentity;
  }

  async readEvents(input: {
    after_event_cursor: string | number | null;
    limit: number;
    wait_ms: 0;
    types: GroupChatEventType[];
  }): Promise<GroupChatEventPage> {
    const after = input.after_event_cursor ?? 0;
    if ((typeof after !== "string" && typeof after !== "number") || input.wait_ms !== 0) {
      throw new BridgeError("group chat feed request is invalid", "invalid_source_poll", 400);
    }
    const query = new URLSearchParams({
      after_event_cursor: String(after),
      limit: String(input.limit),
      wait_ms: "0",
      types: input.types.join(","),
    });
    return await this.getJson(`/api/events?${query.toString()}`) as GroupChatEventPage;
  }

  async cursorFromNow(): Promise<SourceCursor> {
    const value = await this.getJson("/api/events?after_event_cursor=0&limit=1&wait_ms=0") as {
      head_event_cursor?: unknown;
    };
    if (!Number.isSafeInteger(value.head_event_cursor) || Number(value.head_event_cursor) < 0) {
      throw new BridgeError("group chat event head is invalid", "invalid_source_page", 502);
    }
    return { v: 1, event_cursor: Number(value.head_event_cursor) };
  }

  private async getJson(path: string): Promise<unknown> {
    let response: Response;
    try {
      response = await this.fetcher(`${this.baseUrl}${path}`, {
        headers: { authorization: `Bearer ${this.token}`, accept: "application/json" },
        signal: AbortSignal.timeout(15_000),
      });
    } catch (error) {
      throw new BridgeError(
        `group chat upstream is unavailable: ${error instanceof Error ? error.message : String(error)}`,
        "source_connector_unavailable",
        503,
      );
    }
    if (!response.ok) {
      const code = response.status === 401 || response.status === 403
        ? "source_connector_unauthorized"
        : response.status >= 500
          ? "source_connector_http_error"
          : "source_connector_contract_error";
      throw new BridgeError(`group chat upstream returned HTTP ${response.status}`, code, 502);
    }
    try {
      return await response.json();
    } catch {
      throw new BridgeError("group chat upstream returned invalid JSON", "source_connector_invalid_json", 502);
    }
  }
}

export async function startGroupChatConnector(options: GroupChatConnectorOptions): Promise<{
  server: ReturnType<typeof createServer>;
  address: AddressInfo | string | null;
  manifest: GroupChatSourceAdapter["manifest"];
  close: () => Promise<void>;
}> {
  if (!options.connector_token) {
    throw new BridgeError("connector token is required", "invalid_connector_config", 400);
  }
  const host = options.host ?? "127.0.0.1";
  if (!["127.0.0.1", "::1"].includes(host)) {
    throw new BridgeError("connector must bind a numeric loopback address", "unsafe_connector_bind", 409);
  }
  const client = new GroupChatHttpClient(options.upstream_url, options.upstream_token, options.fetcher);
  const identity = await client.identity();
  const bindingMaterial = JSON.stringify({
    origin: client.baseUrl,
    principal_member_id: identity.principal_member_id,
    room_allowlist: [...identity.room_allowlist].sort(),
  });
  const adapter = new GroupChatSourceAdapter(client, {
    subject_ref: `group-chat:principal:${identity.principal_member_id}`,
    binding_fingerprint: `sha256:${createHash("sha256").update(bindingMaterial).digest("hex")}`,
  });
  const server = createServer(async (request, response) => {
    try {
      const path = (request.url || "/").split("?")[0];
      if (request.method === "GET" && path === "/health") {
        jsonResponse(response, 200, {
          ok: true,
          source: adapter.manifest.id,
          provider_version: identity.provider_version,
        });
        return;
      }
      const bearer =
        typeof request.headers.authorization === "string" &&
        request.headers.authorization.startsWith("Bearer ")
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
        if (
          !Number.isSafeInteger(limit) ||
          limit < 1 ||
          limit > GROUP_CHAT_MAX_PAGE_SIZE ||
          !("cursor" in body)
        ) {
          throw new BridgeError("poll context is invalid", "invalid_source_poll", 400);
        }
        jsonResponse(
          response,
          200,
          await adapter.poll({ cursor: body.cursor, limit } as SourcePollContext),
        );
        return;
      }
      if (request.method === "POST" && path === "/v1/bootstrap") {
        const body = await readJson(request);
        if (body.mode !== "from-now") {
          throw new BridgeError("unsupported bootstrap mode", "invalid_source_bootstrap", 400);
        }
        jsonResponse(response, 200, { cursor: await client.cursorFromNow() });
        return;
      }
      jsonResponse(response, 404, { error: "not_found" });
    } catch (error) {
      const bridgeError = error instanceof BridgeError
        ? error
        : error instanceof WakeBridgeSdkError
          ? new BridgeError(error.message, error.code, 424)
          : new BridgeError("connector failed", "connector_error", 500);
      jsonResponse(response, bridgeError.status, {
        error: bridgeError.message,
        code: bridgeError.code,
      });
    }
  });
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(options.port ?? 4392, host);
  });
  return {
    server,
    address: server.address(),
    manifest: adapter.manifest,
    close: async () =>
      new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      ),
  };
}

function jsonResponse(response: ServerResponse, status: number, value: unknown): void {
  const text = JSON.stringify(value);
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("content-length", Buffer.byteLength(text));
  response.end(text);
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.from(chunk as Uint8Array);
    bytes += buffer.length;
    if (bytes > 1_048_576) {
      throw new BridgeError("connector request is too large", "oversized_request", 413);
    }
    chunks.push(buffer);
  }
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error();
    return value as Record<string, unknown>;
  } catch {
    throw new BridgeError("connector request must be a JSON object", "invalid_json", 400);
  }
}

function secretEqual(actual: string, expected: string): boolean {
  const left = Buffer.from(actual, "utf8");
  const right = Buffer.from(expected, "utf8");
  return left.length === right.length && left.length > 0 && timingSafeEqual(left, right);
}
