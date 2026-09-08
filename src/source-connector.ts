import { readFileSync } from "node:fs";
import { BridgeError, WakeBridge } from "./core.js";
import { SourceCheckpointStore, SourceControlStore, SourceRunner } from "./source-adapter.js";
import { validateSourceManifest } from "./source-sdk.js";
import { WakeBridgeSdkError } from "./sdk-error.js";
import type {
  PullSourceAdapter,
  SourceAdapterManifest,
  SourceConnectorConfig,
  SourceConnectorFile,
  SourcePollContext,
  SourcePollResult,
  SourceSupervisorStatus,
  SourceVerification,
} from "./types.js";

const SOURCE_ID = /^[A-Za-z0-9_.:-]{1,128}$/u;
const ENV_NAME = /^[A-Z_][A-Z0-9_]{0,127}$/u;
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "[::1]", "::1"]);

function integer(value: unknown, fallback: number, min: number, max: number, field: string): number {
  const candidate = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(candidate) || candidate < min || candidate > max) {
    throw new BridgeError(`${field} is invalid`, "invalid_source_connector", 400);
  }
  return candidate;
}

export function validateSourceConnectorConfig(value: SourceConnectorConfig): Required<SourceConnectorConfig> {
  if (!value || !SOURCE_ID.test(value.id || "")) {
    throw new BridgeError("source connector id is invalid", "invalid_source_connector", 400);
  }
  if (!ENV_NAME.test(value.token_env || "")) {
    throw new BridgeError(`source connector ${value.id} token_env is invalid`, "invalid_source_connector", 400);
  }
  let url: URL;
  try { url = new URL(value.base_url); } catch {
    throw new BridgeError(`source connector ${value.id} base_url is invalid`, "invalid_source_connector", 400);
  }
  if (url.protocol !== "http:" || !LOOPBACK_HOSTS.has(url.hostname) || url.username || url.password
    || (url.pathname !== "/" && url.pathname !== "") || url.search || url.hash) {
    throw new BridgeError(`source connector ${value.id} must use a loopback HTTP origin`, "unsafe_source_connector", 409);
  }
  return {
    id: value.id,
    base_url: url.origin,
    token_env: value.token_env,
    enabled: value.enabled !== false,
    poll_interval_ms: integer(value.poll_interval_ms, 60_000, 1_000, 86_400_000, "poll_interval_ms"),
    max_backoff_ms: integer(value.max_backoff_ms, 900_000, 1_000, 86_400_000, "max_backoff_ms"),
    limit: integer(value.limit, 100, 1, 1000, "limit"),
    max_pages_per_cycle: integer(value.max_pages_per_cycle, 4, 1, 100, "max_pages_per_cycle"),
  };
}

export function loadSourceConnectorFile(path: string): Required<SourceConnectorConfig>[] {
  let document: unknown;
  try { document = JSON.parse(readFileSync(path, "utf8")); } catch (error) {
    throw new BridgeError(`cannot read source connector file: ${error instanceof Error ? error.message : String(error)}`, "invalid_source_connector_file", 400);
  }
  const file = document as SourceConnectorFile;
  if (!file || file.version !== 1 || !Array.isArray(file.sources)) {
    throw new BridgeError("source connector file must have version 1 and a sources array", "invalid_source_connector_file", 400);
  }
  const sources = file.sources.map(validateSourceConnectorConfig);
  const ids = new Set<string>();
  for (const source of sources) {
    if (ids.has(source.id)) throw new BridgeError(`duplicate source connector id: ${source.id}`, "invalid_source_connector_file", 400);
    ids.add(source.id);
  }
  return sources;
}

type Fetch = typeof fetch;

async function connectorJson(fetcher: Fetch, config: Required<SourceConnectorConfig>, environment: NodeJS.ProcessEnv, path: string, body?: unknown): Promise<unknown> {
  const token = environment[config.token_env];
  if (!token) throw new BridgeError(`source connector ${config.id} token is unavailable`, "source_connector_auth_unavailable", 503);
  let response: Response;
  try {
    response = await fetcher(`${config.base_url}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: { authorization: `Bearer ${token}`, accept: "application/json", ...(body === undefined ? {} : { "content-type": "application/json" }) },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(15_000),
    });
  } catch (error) {
    throw new BridgeError(`source connector ${config.id} is unavailable: ${error instanceof Error ? error.message : String(error)}`, "source_connector_unavailable", 503);
  }
  if (!response.ok) {
    const errorClass = response.status === 401 || response.status === 403
      ? "source_connector_unauthorized"
      : response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500
        ? "source_connector_http_error"
        : "source_connector_contract_error";
    throw new BridgeError(`source connector ${config.id} returned HTTP ${response.status}`, errorClass, 502);
  }
  try { return await response.json(); } catch {
    throw new BridgeError(`source connector ${config.id} returned invalid JSON`, "source_connector_invalid_json", 502);
  }
}

/** One connected poll view. Provider credentials never cross this boundary. */
export class HttpPullSourceAdapter implements PullSourceAdapter {
  private constructor(
    readonly manifest: SourceAdapterManifest,
    private readonly config: Required<SourceConnectorConfig>,
    private readonly environment: NodeJS.ProcessEnv,
    private readonly fetcher: Fetch,
  ) {}

  static async connect(config: Required<SourceConnectorConfig>, environment: NodeJS.ProcessEnv = process.env, fetcher: Fetch = fetch): Promise<HttpPullSourceAdapter> {
    const manifest = await connectorJson(fetcher, config, environment, "/v1/manifest") as SourceAdapterManifest;
    if (!manifest || manifest.id !== config.id) {
      throw new BridgeError(`source connector ${config.id} manifest id does not match`, "source_connector_identity_mismatch", 409);
    }
    if (!["connector", "none"].includes(manifest.credential_custody)) {
      throw new BridgeError(`source connector ${config.id} reports invalid credential custody`, "source_connector_identity_mismatch", 409);
    }
    return new HttpPullSourceAdapter(manifest, config, environment, fetcher);
  }

  async poll(context: SourcePollContext): Promise<SourcePollResult> {
    return await connectorJson(this.fetcher, this.config, this.environment, "/v1/poll", context) as SourcePollResult;
  }
}

export async function bootstrapSourceConnector(
  bridge: WakeBridge,
  rawConfig: SourceConnectorConfig,
  mode: "from-now",
  environment: NodeJS.ProcessEnv = process.env,
  fetcher: Fetch = fetch,
  expected?: { subject_ref: string; binding_fingerprint: string },
): Promise<ReturnType<SourceCheckpointStore["commit"]>> {
  const config = validateSourceConnectorConfig(rawConfig);
  const adapter = await HttpPullSourceAdapter.connect(config, environment, fetcher);
  const manifest = validateSourceManifest(adapter.manifest);
  if (manifest.read_side_effects !== "none") {
    throw new BridgeError(`source ${manifest.id} has ${manifest.read_side_effects} read side effects`, "unsafe_source_read", 409);
  }
  if (!expected || manifest.subject_ref !== expected.subject_ref || manifest.binding_fingerprint !== expected.binding_fingerprint) {
    throw new BridgeError(`source ${manifest.id} identity was not explicitly confirmed`, "source_identity_unconfirmed", 409);
  }
  const checkpoints = new SourceCheckpointStore(bridge);
  if (checkpoints.get(manifest.id)) {
    throw new BridgeError(`source ${manifest.id} already has a checkpoint`, "source_already_bootstrapped", 409);
  }
  const result = await connectorJson(fetcher, config, environment, "/v1/bootstrap", { mode });
  if (!result || typeof result !== "object" || !("cursor" in result)) {
    throw new BridgeError(`source connector ${config.id} returned an invalid bootstrap`, "source_connector_invalid_bootstrap", 502);
  }
  return checkpoints.commit(manifest, (result as { cursor: any }).cursor, 0);
}

interface SourceSlot {
  config: Required<SourceConnectorConfig>;
  status: SourceSupervisorStatus;
  timer?: NodeJS.Timeout;
  promise?: Promise<void>;
}

export class SourceSupervisor {
  private readonly runner: SourceRunner;
  private readonly controls: SourceControlStore;
  private readonly slots = new Map<string, SourceSlot>();
  private stopped = false;

  constructor(
    bridge: WakeBridge,
    configs: SourceConnectorConfig[],
    private readonly environment: NodeJS.ProcessEnv = process.env,
    private readonly fetcher: Fetch = fetch,
  ) {
    this.runner = new SourceRunner(bridge);
    this.controls = new SourceControlStore(bridge);
    for (const raw of configs) {
      const config = validateSourceConnectorConfig(raw);
      if (this.slots.has(config.id)) throw new BridgeError(`duplicate source connector id: ${config.id}`, "invalid_source_connector", 400);
      config.enabled = this.controls.get(config.id) ?? config.enabled;
      const checkpoint = this.runner.checkpoints.get(config.id);
      this.slots.set(config.id, {
        config,
        status: {
          source: config.id, enabled: config.enabled, state: config.enabled ? "idle" : "stopped",
          last_started_at: null, last_success_at: null, last_error_at: null, last_error_class: null,
          consecutive_failures: 0, next_poll_at: null,
          checkpoint_revision: checkpoint?.revision ?? 0,
          checkpoint_subject_ref: checkpoint?.subject_ref ?? null,
          checkpoint_binding_fingerprint: checkpoint?.binding_fingerprint ?? null,
          last_run: null,
        },
      });
    }
  }

  list(): SourceSupervisorStatus[] {
    return [...this.slots.values()].map((slot) => ({ ...slot.status, last_run: slot.status.last_run ? { ...slot.status.last_run } : null }));
  }

  async verify(source: string): Promise<SourceVerification> {
    const slot = this.slot(source);
    const adapter = await HttpPullSourceAdapter.connect(slot.config, this.environment, this.fetcher);
    const manifest = validateSourceManifest(adapter.manifest);
    const storedCheckpoint = this.runner.checkpoints.get(source);
    const checkpoint = storedCheckpoint ? {
      source: storedCheckpoint.source,
      adapter_version: storedCheckpoint.adapter_version,
      subject_ref: storedCheckpoint.subject_ref,
      binding_fingerprint: storedCheckpoint.binding_fingerprint,
      revision: storedCheckpoint.revision,
      updated_at: storedCheckpoint.updated_at,
    } : null;
    return {
      source,
      manifest,
      checkpoint,
      binding_matches: Boolean(storedCheckpoint && storedCheckpoint.subject_ref === manifest.subject_ref
        && storedCheckpoint.binding_fingerprint === manifest.binding_fingerprint),
      enabled: slot.config.enabled,
    };
  }

  async bootstrap(source: string, expected: { subject_ref: string; binding_fingerprint: string }): Promise<SourceVerification> {
    const slot = this.slot(source);
    if (slot.config.enabled) throw new BridgeError("disable the source before bootstrap", "source_must_be_disabled", 409);
    await bootstrapSourceConnector(this.runner.bridge, slot.config, "from-now", this.environment, this.fetcher, expected);
    this.refreshCheckpoint(slot);
    return await this.verify(source);
  }

  async rebind(source: string, expectedRevision: number, expected: { subject_ref: string; binding_fingerprint: string }, reason: string): Promise<SourceVerification> {
    const slot = this.slot(source);
    if (slot.config.enabled) throw new BridgeError("disable the source before rebind", "source_must_be_disabled", 409);
    if (!reason || reason.length > 300) throw new BridgeError("source rebind reason is invalid", "invalid_arguments", 400);
    const adapter = await HttpPullSourceAdapter.connect(slot.config, this.environment, this.fetcher);
    const manifest = validateSourceManifest(adapter.manifest);
    if (manifest.subject_ref !== expected.subject_ref || manifest.binding_fingerprint !== expected.binding_fingerprint) {
      throw new BridgeError(`source ${source} identity was not explicitly confirmed`, "source_identity_unconfirmed", 409);
    }
    const result = await connectorJson(this.fetcher, slot.config, this.environment, "/v1/bootstrap", { mode: "from-now" });
    if (!result || typeof result !== "object" || !("cursor" in result)) {
      throw new BridgeError(`source connector ${source} returned an invalid bootstrap`, "source_connector_invalid_bootstrap", 502);
    }
    this.runner.checkpoints.replace(manifest, (result as { cursor: any }).cursor, expectedRevision, reason);
    this.refreshCheckpoint(slot);
    return await this.verify(source);
  }

  async enable(source: string): Promise<SourceSupervisorStatus> {
    const slot = this.slot(source);
    const verified = await this.verify(source);
    if (!verified.checkpoint || !verified.binding_matches) {
      throw new BridgeError("source identity is not bound to its checkpoint", "source_identity_mismatch", 409);
    }
    this.controls.set(source, true);
    slot.config.enabled = true;
    slot.status.enabled = true;
    if (!this.stopped && !slot.promise) this.schedule(slot, 0);
    return this.list().find((item) => item.source === source)!;
  }

  async disable(source: string): Promise<SourceSupervisorStatus> {
    const slot = this.slot(source);
    this.controls.set(source, false);
    slot.config.enabled = false;
    slot.status.enabled = false;
    if (slot.timer) clearTimeout(slot.timer);
    slot.timer = undefined;
    slot.status.next_poll_at = null;
    await slot.promise;
    slot.status.state = "stopped";
    return this.list().find((item) => item.source === source)!;
  }

  start(): void {
    this.stopped = false;
    for (const slot of this.slots.values()) if (slot.config.enabled) this.schedule(slot, 0);
  }

  async runOnce(source: string): Promise<SourceSupervisorStatus> {
    const slot = this.slot(source);
    if (slot.promise) throw new BridgeError(`source connector ${source} is already polling`, "source_connector_busy", 409);
    if (slot.timer) clearTimeout(slot.timer);
    slot.timer = undefined;
    slot.promise = this.launch(slot);
    await slot.promise;
    return this.list().find((status) => status.source === source)!;
  }

  private slot(source: string): SourceSlot {
    const slot = this.slots.get(source);
    if (!slot) throw new BridgeError(`source connector ${source} is not configured`, "source_connector_not_found", 404);
    return slot;
  }

  private refreshCheckpoint(slot: SourceSlot): void {
    const checkpoint = this.runner.checkpoints.get(slot.config.id);
    slot.status.checkpoint_revision = checkpoint?.revision ?? 0;
    slot.status.checkpoint_subject_ref = checkpoint?.subject_ref ?? null;
    slot.status.checkpoint_binding_fingerprint = checkpoint?.binding_fingerprint ?? null;
  }

  async stop(): Promise<void> {
    this.stopped = true;
    for (const slot of this.slots.values()) {
      if (slot.timer) clearTimeout(slot.timer);
      slot.timer = undefined;
      slot.status.next_poll_at = null;
    }
    await Promise.all([...this.slots.values()].map((slot) => slot.promise).filter(Boolean) as Promise<void>[]);
    for (const slot of this.slots.values()) slot.status.state = "stopped";
  }

  private schedule(slot: SourceSlot, delay: number): void {
    if (this.stopped || !slot.config.enabled) return;
    if (slot.timer) clearTimeout(slot.timer);
    slot.status.next_poll_at = new Date(Date.now() + delay).toISOString();
    slot.timer = setTimeout(() => {
      slot.timer = undefined;
      if (slot.promise) return;
      slot.promise = this.launch(slot);
    }, delay);
    slot.timer.unref();
  }

  private launch(slot: SourceSlot): Promise<void> {
    let nextDelay: number | null = null;
    return this.run(slot)
      .then((delay) => { nextDelay = delay; })
      .finally(() => {
        slot.promise = undefined;
        if (nextDelay !== null) this.schedule(slot, nextDelay);
      });
  }

  private async run(slot: SourceSlot): Promise<number | null> {
    slot.status.state = "polling";
    slot.status.last_started_at = new Date().toISOString();
    slot.status.next_poll_at = null;
    try {
      const adapter = await HttpPullSourceAdapter.connect(slot.config, this.environment, this.fetcher);
      let result = await this.runner.runOnce(adapter, { limit: slot.config.limit });
      const aggregate = { ...result };
      for (let page = 1; result.has_more && page < slot.config.max_pages_per_cycle; page += 1) {
        result = await this.runner.runOnce(adapter, { limit: slot.config.limit });
        aggregate.checkpoint_revision = result.checkpoint_revision;
        aggregate.events_seen += result.events_seen;
        aggregate.events_inserted += result.events_inserted;
        aggregate.events_duplicate += result.events_duplicate;
        aggregate.has_more = result.has_more;
      }
      slot.status.state = "healthy";
      slot.status.last_success_at = new Date().toISOString();
      slot.status.last_error_at = null;
      slot.status.last_error_class = null;
      slot.status.consecutive_failures = 0;
      slot.status.checkpoint_revision = aggregate.checkpoint_revision;
      this.refreshCheckpoint(slot);
      slot.status.last_run = aggregate;
      return aggregate.has_more ? 0 : slot.config.poll_interval_ms;
    } catch (error) {
      const errorClass = error instanceof WakeBridgeSdkError ? error.code : "source_connector_error";
      const retryable = errorClass === "source_connector_unavailable" || errorClass === "source_connector_http_error";
      slot.status.state = retryable ? "backoff" : "needs_attention";
      slot.status.last_error_at = new Date().toISOString();
      slot.status.last_error_class = errorClass;
      slot.status.consecutive_failures += 1;
      if (retryable) {
        return Math.min(slot.config.max_backoff_ms, slot.config.poll_interval_ms * (2 ** Math.min(10, slot.status.consecutive_failures - 1)));
      }
      return null;
    }
  }
}
