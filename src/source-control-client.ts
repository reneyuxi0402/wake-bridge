import { BridgeError } from "./core.js";

export interface AgentSourceControl {
  status(): Promise<Record<string, unknown>>;
  verify(source: string): Promise<Record<string, unknown>>;
  bootstrap(source: string, input: { expected_subject_ref: string; expected_binding_fingerprint: string }): Promise<Record<string, unknown>>;
  rebind(source: string, input: { expected_checkpoint_revision: number; expected_subject_ref: string; expected_binding_fingerprint: string; reason: string }): Promise<Record<string, unknown>>;
  enable(source: string): Promise<Record<string, unknown>>;
  disable(source: string): Promise<Record<string, unknown>>;
}

export class DaemonSourceControlClient implements AgentSourceControl {
  private readonly origin: string;

  constructor(baseUrl: string, private readonly adminToken: string, private readonly fetcher: typeof fetch = fetch) {
    let url: URL;
    try { url = new URL(baseUrl); } catch { throw new BridgeError("Wake Bridge daemon URL is invalid", "source_control_unavailable", 503); }
    if (url.protocol !== "http:" || !["127.0.0.1", "[::1]", "::1"].includes(url.hostname)
      || url.username || url.password || url.pathname !== "/" || url.search || url.hash || !adminToken) {
      throw new BridgeError("Wake Bridge source control requires an authenticated loopback daemon", "source_control_unavailable", 503);
    }
    this.origin = url.origin;
  }

  status(): Promise<Record<string, unknown>> { return this.request("GET", "/v1/sources"); }
  verify(source: string): Promise<Record<string, unknown>> { return this.request("POST", `/v1/sources/${this.source(source)}/verify`, {}); }
  bootstrap(source: string, input: { expected_subject_ref: string; expected_binding_fingerprint: string }): Promise<Record<string, unknown>> {
    return this.request("POST", `/v1/sources/${this.source(source)}/bootstrap`, { mode: "from-now", ...input });
  }
  rebind(source: string, input: { expected_checkpoint_revision: number; expected_subject_ref: string; expected_binding_fingerprint: string; reason: string }): Promise<Record<string, unknown>> {
    return this.request("POST", `/v1/sources/${this.source(source)}/rebind`, input);
  }
  enable(source: string): Promise<Record<string, unknown>> { return this.request("POST", `/v1/sources/${this.source(source)}/enable`, {}); }
  disable(source: string): Promise<Record<string, unknown>> { return this.request("POST", `/v1/sources/${this.source(source)}/disable`, {}); }

  private source(value: string): string {
    if (!/^[A-Za-z0-9_.:-]{1,128}$/u.test(value)) throw new BridgeError("source is invalid", "invalid_arguments", 400);
    return encodeURIComponent(value);
  }

  private async request(method: "GET" | "POST", path: string, body?: unknown): Promise<Record<string, unknown>> {
    let response: Response;
    try {
      response = await this.fetcher(`${this.origin}${path}`, {
        method,
        headers: { authorization: `Bearer ${this.adminToken}`, accept: "application/json", ...(body === undefined ? {} : { "content-type": "application/json" }) },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(20_000),
      });
    } catch {
      throw new BridgeError("Wake Bridge source control daemon is unavailable", "source_control_unavailable", 503);
    }
    let value: Record<string, unknown> = {};
    try { value = await response.json() as Record<string, unknown>; } catch {}
    if (!response.ok) {
      throw new BridgeError(
        typeof value.error === "string" ? value.error : "Wake Bridge source control failed",
        typeof value.code === "string" ? value.code : "source_control_error",
        response.status,
      );
    }
    return value;
  }
}
