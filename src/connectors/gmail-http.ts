import { createHash, timingSafeEqual } from "node:crypto";
import { lstatSync, readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { MIN_SECRET_LENGTH } from "../validation.js";
import {
  GMAIL_MAX_PAGE_SIZE,
  GmailSourceAdapter,
  gmailCursorFromNow,
  type GmailHistoryPage,
  type GmailMessageMetadata,
  type GmailNotificationClient,
  type GmailProfile,
} from "../adapters/gmail.js";
import { BridgeError } from "../core.js";
import type { SourceAdapterManifest, SourcePollContext, SourceUpstreamCredentialBreadth } from "../types.js";

const GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me";
const GOOGLE_TOKEN_ORIGIN = "https://oauth2.googleapis.com";
const GOOGLE_TOKEN_PATH = "/token";
const MAX_JSON_BYTES = 1_048_576;

interface OAuthCredentialFile {
  access_token?: string;
  refresh_token?: string;
  scope?: string | string[];
  expiry_date?: number;
}

interface OAuthClientDefinition {
  client_id?: string;
  client_secret?: string;
  token_uri?: string;
}

interface OAuthClientFile {
  installed?: OAuthClientDefinition;
  web?: OAuthClientDefinition;
}

export interface GmailConnectorOptions {
  credentials_dir: string;
  connector_token: string;
  attention_channel?: string;
  host?: string;
  port?: number;
  fetcher?: typeof fetch;
}

function privateJson<T>(path: string): T {
  let stat;
  try {
    stat = lstatSync(path);
  } catch (error) {
    throw new BridgeError(`gmail credential file is unavailable: ${error instanceof Error ? error.message : String(error)}`, "gmail_credentials_unavailable", 424);
  }
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0) {
    throw new BridgeError("gmail credential files must be regular mode-600 files", "gmail_credentials_insecure", 424);
  }
  try {
    const value = JSON.parse(readFileSync(path, "utf8")) as T;
    if (!value || typeof value !== "object") throw new Error("JSON object required");
    return value;
  } catch (error) {
    throw new BridgeError(`gmail credential file is invalid: ${error instanceof Error ? error.message : String(error)}`, "gmail_credentials_invalid", 424);
  }
}

function credentialScopes(value: OAuthCredentialFile["scope"]): string[] {
  const raw = Array.isArray(value) ? value : typeof value === "string" ? value.split(/\s+/u) : [];
  return [...new Set(raw.map((scope) => String(scope).trim()).filter(Boolean))].sort();
}

function credentialBreadth(scopes: string[]): SourceUpstreamCredentialBreadth {
  if (scopes.length === 0) return "unknown";
  const readOnly = new Set([
    "https://www.googleapis.com/auth/gmail.metadata",
    "https://www.googleapis.com/auth/gmail.readonly",
  ]);
  return scopes.every((scope) => readOnly.has(scope)) ? "read_only" : "broad";
}

function secretEqual(actual: string, expected: string): boolean {
  const left = Buffer.from(actual, "utf8");
  const right = Buffer.from(expected, "utf8");
  return left.length === right.length && left.length > 0 && timingSafeEqual(left, right);
}

function safeSubjectRef(emailAddress: string): string {
  const email = emailAddress.trim().toLowerCase();
  if (!/^[^\s:@]+@[^\s:@]+$/u.test(email) || email.length > 170) {
    throw new BridgeError("gmail profile email address is invalid", "gmail_identity_invalid", 424);
  }
  return `gmail:account:${email}`;
}

function googleTokenEndpoint(value: string | undefined): string {
  let url: URL;
  try {
    url = new URL(value || "");
  } catch {
    throw new BridgeError("gmail OAuth token endpoint is invalid", "gmail_credentials_invalid", 424);
  }
  if (url.origin !== GOOGLE_TOKEN_ORIGIN || url.pathname !== GOOGLE_TOKEN_PATH || url.search || url.hash || url.username || url.password) {
    throw new BridgeError("gmail OAuth token endpoint is not the Google token endpoint", "gmail_credentials_invalid", 424);
  }
  return url.href;
}

export class GmailRestClient implements GmailNotificationClient {
  readonly scopes: string[];
  readonly clientId: string;
  private readonly clientSecret: string;
  private readonly refreshToken: string;
  private readonly tokenEndpoint: string;
  private accessToken: string | null;
  private expiresAt: number;

  constructor(credentialsDir: string, private readonly fetcher: typeof fetch = fetch) {
    if (!credentialsDir) throw new BridgeError("gmail credentials directory is required", "invalid_connector_config", 400);
    const credentials = privateJson<OAuthCredentialFile>(join(credentialsDir, "credentials.json"));
    const clients = privateJson<OAuthClientFile>(join(credentialsDir, "gcp-oauth.keys.json"));
    const client = clients.installed ?? clients.web;
    if (!client?.client_id || !credentials.refresh_token) {
      throw new BridgeError("gmail OAuth credentials are incomplete", "gmail_credentials_invalid", 424);
    }
    this.clientId = client.client_id;
    this.clientSecret = client.client_secret ?? "";
    this.refreshToken = credentials.refresh_token;
    this.tokenEndpoint = googleTokenEndpoint(client.token_uri);
    this.scopes = credentialScopes(credentials.scope);
    const expiry = Number(credentials.expiry_date);
    this.accessToken = typeof credentials.access_token === "string" && credentials.access_token ? credentials.access_token : null;
    this.expiresAt = Number.isFinite(expiry) ? expiry : 0;
  }

  private async token(): Promise<string> {
    if (this.accessToken && Date.now() < this.expiresAt - 60_000) return this.accessToken;
    let response: Response;
    try {
      response = await this.fetcher(this.tokenEndpoint, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: this.clientId,
          client_secret: this.clientSecret,
          refresh_token: this.refreshToken,
          grant_type: "refresh_token",
        }),
        signal: AbortSignal.timeout(15_000),
      });
    } catch (error) {
      throw new BridgeError(`gmail token refresh is unavailable: ${error instanceof Error ? error.message : String(error)}`, "gmail_unavailable", 503);
    }
    const body = await response.json().catch(() => ({})) as Record<string, unknown>;
    if (!response.ok || typeof body.access_token !== "string" || !body.access_token) {
      if (response.status === 400 || response.status === 401 || response.status === 403) {
        throw new BridgeError("gmail OAuth credential was rejected", "gmail_auth_failed", 424);
      }
      if (response.status === 429) throw new BridgeError("gmail token refresh was rate limited", "gmail_rate_limited", 429);
      throw new BridgeError("gmail token refresh failed", "gmail_unavailable", 503);
    }
    this.accessToken = body.access_token;
    this.expiresAt = Date.now() + Math.max(60, Number(body.expires_in) || 3600) * 1000;
    return this.accessToken;
  }

  private async get(path: string, notFoundCode: "gmail_history_expired" | "gmail_message_not_found" | null = null): Promise<Record<string, any>> {
    let response: Response;
    try {
      response = await this.fetcher(`${GMAIL_API}/${path}`, {
        headers: { authorization: `Bearer ${await this.token()}`, accept: "application/json" },
        signal: AbortSignal.timeout(20_000),
      });
    } catch (error) {
      if (error instanceof BridgeError) throw error;
      throw new BridgeError(`gmail API is unavailable: ${error instanceof Error ? error.message : String(error)}`, "gmail_unavailable", 503);
    }
    if (response.status === 404 && notFoundCode) {
      throw new BridgeError(
        notFoundCode === "gmail_history_expired" ? "gmail history cursor expired" : "gmail message no longer exists",
        notFoundCode,
        notFoundCode === "gmail_history_expired" ? 424 : 404,
      );
    }
    if (response.status === 401 || response.status === 403) {
      throw new BridgeError("gmail API rejected the OAuth credential", "gmail_auth_failed", 424);
    }
    if (response.status === 429) throw new BridgeError("gmail API rate limit exceeded", "gmail_rate_limited", 429);
    if (!response.ok) throw new BridgeError(`gmail API returned HTTP ${response.status}`, "gmail_unavailable", 503);
    try {
      return await response.json() as Record<string, any>;
    } catch {
      throw new BridgeError("gmail API returned invalid JSON", "invalid_source_page", 502);
    }
  }

  async profile(): Promise<GmailProfile> {
    const value = await this.get("profile");
    return { emailAddress: String(value.emailAddress || ""), historyId: String(value.historyId || "") };
  }

  async history(input: { start_history_id: string; page_token: string | null; max_results: number }): Promise<GmailHistoryPage> {
    const query = new URLSearchParams({
      startHistoryId: input.start_history_id,
      maxResults: String(Math.min(GMAIL_MAX_PAGE_SIZE, input.max_results)),
      labelId: "INBOX",
      historyTypes: "messageAdded",
      fields: "history(id,messagesAdded(message(id,threadId))),nextPageToken,historyId",
    });
    if (input.page_token) query.set("pageToken", input.page_token);
    return await this.get(`history?${query.toString()}`, "gmail_history_expired") as GmailHistoryPage;
  }

  async messageMetadata(messageId: string): Promise<GmailMessageMetadata> {
    const query = new URLSearchParams({
      format: "metadata",
      metadataHeaders: "X-Wake-Bridge-Omitted",
      fields: "id,threadId,labelIds,internalDate",
    });
    return await this.get(`messages/${encodeURIComponent(messageId)}?${query.toString()}`, "gmail_message_not_found") as GmailMessageMetadata;
  }
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
    if (bytes > MAX_JSON_BYTES) throw new BridgeError("connector request is too large", "oversized_request", 413);
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

function connectorStatus(error: BridgeError): number {
  if (["gmail_auth_failed", "gmail_history_expired", "gmail_credentials_unavailable", "gmail_credentials_invalid", "gmail_credentials_insecure", "gmail_identity_invalid"].includes(error.code)
    || error.code.startsWith("invalid_source_")) return 424;
  return error.status;
}

export async function startGmailConnector(options: GmailConnectorOptions): Promise<{
  server: ReturnType<typeof createServer>;
  address: AddressInfo | string | null;
  manifest: SourceAdapterManifest;
  close: () => Promise<void>;
}> {
  if (!options.connector_token || options.connector_token.length < MIN_SECRET_LENGTH) {
    throw new BridgeError(`connector token must be at least ${MIN_SECRET_LENGTH} characters`, "invalid_connector_config", 400);
  }
  const host = options.host ?? "127.0.0.1";
  if (!["127.0.0.1", "::1"].includes(host)) {
    throw new BridgeError("connector must bind a numeric loopback address", "unsafe_connector_bind", 409);
  }
  const client = new GmailRestClient(options.credentials_dir, options.fetcher);
  const profile = await client.profile();
  const subjectRef = safeSubjectRef(profile.emailAddress);
  const bindingMaterial = JSON.stringify({ provider: "gmail", email: profile.emailAddress.trim().toLowerCase(), client_id: client.clientId });
  const adapter = new GmailSourceAdapter(client, {
    subject_ref: subjectRef,
    binding_fingerprint: `sha256:${createHash("sha256").update(bindingMaterial).digest("hex")}`,
    upstream_credential_scopes: client.scopes,
    upstream_credential_breadth: credentialBreadth(client.scopes),
    attention_channel: options.attention_channel,
  });
  const server = createServer(async (request, response) => {
    try {
      const path = (request.url || "/").split("?")[0];
      if (request.method === "GET" && path === "/health") {
        jsonResponse(response, 200, { ok: true, source: "gmail" });
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
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > GMAIL_MAX_PAGE_SIZE || !("cursor" in body)) {
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
        jsonResponse(response, 200, { cursor: await gmailCursorFromNow(client) });
        return;
      }
      jsonResponse(response, 404, { error: "not_found" });
    } catch (error) {
      const bridgeError = error instanceof BridgeError ? error : new BridgeError("connector failed", "connector_error", 500);
      jsonResponse(response, connectorStatus(bridgeError), { error: bridgeError.message, code: bridgeError.code });
    }
  });
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => { server.off("listening", onListening); reject(error); };
    const onListening = () => { server.off("error", onError); resolve(); };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(options.port ?? 4393, host);
  });
  return {
    server,
    address: server.address(),
    manifest: adapter.manifest,
    close: async () => new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
  };
}
