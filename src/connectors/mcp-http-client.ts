import { BridgeError } from "../core.js";

type Fetch = typeof fetch;
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "[::1]", "::1"]);

function decodeJsonRpc(text: string): Record<string, any> {
  const documents = text.split(/\r?\n/u)
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trim())
    .filter(Boolean);
  const raw = documents.at(-1) || text.trim();
  if (!raw) return {};
  try {
    const value = JSON.parse(raw) as unknown;
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("object required");
    return value as Record<string, any>;
  } catch {
    throw new BridgeError("upstream MCP returned invalid JSON-RPC", "upstream_mcp_invalid_response", 424);
  }
}

export class StreamableHttpMcpClient {
  private requestId = 0;

  constructor(
    private readonly endpoint: string,
    private readonly fetcher: Fetch = fetch,
  ) {
    let url: URL;
    try { url = new URL(endpoint); } catch {
      throw new BridgeError("upstream MCP URL is invalid", "upstream_mcp_invalid_config", 400);
    }
    const transportAllowed = url.protocol === "https:"
      || url.protocol === "http:" && LOOPBACK_HOSTS.has(url.hostname);
    if (!transportAllowed || url.username || url.password || url.hash) {
      throw new BridgeError("upstream MCP URL is invalid", "upstream_mcp_invalid_config", 400);
    }
  }

  async callTool(name: string, args: Record<string, unknown>): Promise<Record<string, any>> {
    const initialized = await this.post({
      jsonrpc: "2.0",
      id: this.nextId(),
      method: "initialize",
      params: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "wakebridge-source-connector", version: "0.1.0" },
      },
    });
    if (initialized.body.error) throw new BridgeError("upstream MCP initialize failed", "upstream_mcp_initialize_failed", 424);
    await this.post({ jsonrpc: "2.0", method: "notifications/initialized" }, initialized.sessionId, true);
    const called = await this.post({
      jsonrpc: "2.0",
      id: this.nextId(),
      method: "tools/call",
      params: { name, arguments: args },
    }, initialized.sessionId);
    if (called.body.error || called.body.result?.isError === true) {
      throw new BridgeError("upstream MCP tool call failed", "upstream_mcp_tool_failed", 424);
    }
    const content = called.body.result?.content;
    const text = Array.isArray(content)
      ? content.find((item: unknown) => item && typeof item === "object" && (item as any).type === "text")?.text
      : undefined;
    if (typeof text !== "string") {
      throw new BridgeError("upstream MCP tool result has no text content", "upstream_mcp_invalid_response", 424);
    }
    return decodeJsonRpc(text);
  }

  private nextId(): number {
    this.requestId += 1;
    return this.requestId;
  }

  private async post(payload: Record<string, unknown>, sessionId?: string, allowEmpty = false): Promise<{ body: Record<string, any>; sessionId?: string }> {
    let response: Response;
    try {
      response = await this.fetcher(this.endpoint, {
        method: "POST",
        headers: {
          accept: "application/json, text/event-stream",
          "content-type": "application/json",
          ...(sessionId ? { "mcp-session-id": sessionId } : {}),
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(15_000),
      });
    } catch {
      throw new BridgeError("upstream MCP is unavailable", "upstream_mcp_unavailable", 503);
    }
    if (!response.ok) {
      const retryable = response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500;
      const errorClass = response.status === 401 || response.status === 403
        ? "upstream_mcp_unauthorized"
        : retryable ? "upstream_mcp_http_error" : "upstream_mcp_contract_error";
      throw new BridgeError(
        `upstream MCP returned HTTP ${response.status}`,
        errorClass,
        retryable ? 503 : 424,
      );
    }
    const text = await response.text();
    return {
      body: allowEmpty && !text.trim() ? {} : decodeJsonRpc(text),
      sessionId: response.headers.get("mcp-session-id") || sessionId,
    };
  }
}
