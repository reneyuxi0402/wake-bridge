import { BridgeError, WakeBridge } from "./core.js";
import { sqlValue } from "./db.js";
import { validateSourceManifest } from "./source-sdk.js";
import type {
  PullSourceAdapter,
  SourceAdapterManifest,
  SourceCheckpoint,
  SourceCursor,
  SourceRunResult,
} from "./types.js";

const MAX_CURSOR_BYTES = 4096;

function cursorJson(cursor: SourceCursor): string {
  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(cursor);
  } catch {
    throw new BridgeError("source cursor must be JSON", "invalid_source_cursor", 400);
  }
  if (typeof encoded !== "string") {
    throw new BridgeError("source cursor must be JSON", "invalid_source_cursor", 400);
  }
  if (Buffer.byteLength(encoded, "utf8") > MAX_CURSOR_BYTES) {
    throw new BridgeError("source cursor is too large", "invalid_source_cursor", 413);
  }
  return encoded;
}

export class SourceCheckpointStore {
  constructor(readonly bridge: WakeBridge) {}

  get(source: string): SourceCheckpoint | null {
    const row = this.bridge.db.query<Record<string, unknown>>(
      `SELECT * FROM source_checkpoints WHERE instance_id=${sqlValue(this.bridge.config.instance_id)} AND source=${sqlValue(source)} LIMIT 1;`,
    )[0];
    if (!row) return null;
    let cursor: SourceCursor;
    try {
      cursor = JSON.parse(String(row.cursor_json)) as SourceCursor;
      cursorJson(cursor);
    } catch (error) {
      if (error instanceof BridgeError) throw error;
      throw new BridgeError("stored source cursor is corrupt", "source_cursor_corrupt", 500);
    }
    return {
      instance_id: String(row.instance_id),
      source: String(row.source),
      adapter_version: String(row.adapter_version),
      subject_ref: String(row.subject_ref),
      binding_fingerprint: String(row.binding_fingerprint),
      cursor,
      revision: Number(row.revision),
      updated_at: String(row.updated_at),
    };
  }

  /** Compare-and-swap prevents two pollers from silently moving one cursor. */
  commit(manifest: SourceAdapterManifest, cursor: SourceCursor, expectedRevision: number): SourceCheckpoint {
    const encoded = cursorJson(cursor);
    const now = this.bridge.nowIso();
    const statements = expectedRevision === 0
      ? [`INSERT OR IGNORE INTO source_checkpoints(instance_id, source, adapter_version, subject_ref, binding_fingerprint, cursor_json, revision, updated_at)
          VALUES(${sqlValue(this.bridge.config.instance_id)}, ${sqlValue(manifest.id)}, ${sqlValue(manifest.version)}, ${sqlValue(manifest.subject_ref)}, ${sqlValue(manifest.binding_fingerprint)}, ${sqlValue(encoded)}, 1, ${sqlValue(now)});`]
      : [`UPDATE source_checkpoints SET adapter_version=${sqlValue(manifest.version)}, subject_ref=${sqlValue(manifest.subject_ref)}, binding_fingerprint=${sqlValue(manifest.binding_fingerprint)}, cursor_json=${sqlValue(encoded)}, revision=revision+1, updated_at=${sqlValue(now)}
          WHERE instance_id=${sqlValue(this.bridge.config.instance_id)} AND source=${sqlValue(manifest.id)} AND revision=${expectedRevision}
            AND subject_ref=${sqlValue(manifest.subject_ref)} AND binding_fingerprint=${sqlValue(manifest.binding_fingerprint)};`];
    this.bridge.db.transaction(statements);
    const checkpoint = this.get(manifest.id);
    if (!checkpoint || checkpoint.revision !== expectedRevision + 1 || checkpoint.adapter_version !== manifest.version
      || checkpoint.subject_ref !== manifest.subject_ref || checkpoint.binding_fingerprint !== manifest.binding_fingerprint
      || cursorJson(checkpoint.cursor) !== encoded) {
      throw new BridgeError("source cursor changed concurrently", "source_cursor_conflict", 409);
    }
    return checkpoint;
  }

  replace(manifest: SourceAdapterManifest, cursor: SourceCursor, expectedRevision: number, reason: string): SourceCheckpoint {
    const before = this.get(manifest.id);
    if (!before || before.revision !== expectedRevision) {
      throw new BridgeError("source checkpoint changed concurrently", "source_cursor_conflict", 409);
    }
    const encoded = cursorJson(cursor);
    const now = this.bridge.nowIso();
    this.bridge.db.transaction([
      `INSERT INTO source_checkpoint_history(instance_id, source, adapter_version, subject_ref, binding_fingerprint, cursor_json, revision, updated_at, archived_at, reason)
       SELECT instance_id, source, adapter_version, subject_ref, binding_fingerprint, cursor_json, revision, updated_at, ${sqlValue(now)}, ${sqlValue(reason)}
       FROM source_checkpoints
       WHERE instance_id=${sqlValue(this.bridge.config.instance_id)} AND source=${sqlValue(manifest.id)} AND revision=${expectedRevision};`,
      `UPDATE source_checkpoints SET adapter_version=${sqlValue(manifest.version)}, subject_ref=${sqlValue(manifest.subject_ref)}, binding_fingerprint=${sqlValue(manifest.binding_fingerprint)}, cursor_json=${sqlValue(encoded)}, revision=revision+1, updated_at=${sqlValue(now)}
       WHERE instance_id=${sqlValue(this.bridge.config.instance_id)} AND source=${sqlValue(manifest.id)} AND revision=${expectedRevision};`,
    ]);
    const checkpoint = this.get(manifest.id);
    if (!checkpoint || checkpoint.revision !== expectedRevision + 1 || checkpoint.subject_ref !== manifest.subject_ref
      || checkpoint.binding_fingerprint !== manifest.binding_fingerprint || cursorJson(checkpoint.cursor) !== encoded) {
      throw new BridgeError("source checkpoint replacement failed", "source_cursor_conflict", 409);
    }
    return checkpoint;
  }
}

export class SourceControlStore {
  constructor(readonly bridge: WakeBridge) {}

  get(source: string): boolean | null {
    const row = this.bridge.db.query<Record<string, unknown>>(
      `SELECT enabled FROM source_controls WHERE instance_id=${sqlValue(this.bridge.config.instance_id)} AND source=${sqlValue(source)} LIMIT 1;`,
    )[0];
    return row ? Boolean(Number(row.enabled)) : null;
  }

  set(source: string, enabled: boolean): void {
    const now = this.bridge.nowIso();
    this.bridge.db.exec(`INSERT INTO source_controls(instance_id, source, enabled, updated_at)
      VALUES(${sqlValue(this.bridge.config.instance_id)}, ${sqlValue(source)}, ${sqlValue(enabled)}, ${sqlValue(now)})
      ON CONFLICT(instance_id, source) DO UPDATE SET enabled=excluded.enabled, updated_at=excluded.updated_at;`);
  }
}

export interface SourceRunnerOptions {
  limit?: number;
  /** Unsafe sources remain test-only unless an embedding opts in explicitly. */
  allow_read_side_effects?: boolean;
  /**
   * Broad/unknown provider credentials are allowed behind an external connector,
   * but remain fail-closed when injected into the Wake Bridge process itself.
   */
  allow_in_process_broad_credentials?: boolean;
}

export class SourceRunner {
  readonly checkpoints: SourceCheckpointStore;

  constructor(readonly bridge: WakeBridge) {
    this.checkpoints = new SourceCheckpointStore(bridge);
  }

  async runOnce(adapter: PullSourceAdapter, options: SourceRunnerOptions = {}): Promise<SourceRunResult> {
    const manifest = validateSourceManifest(adapter.manifest);
    if (manifest.read_side_effects !== "none" && !options.allow_read_side_effects) {
      throw new BridgeError(`source ${manifest.id} has ${manifest.read_side_effects} read side effects`, "unsafe_source_read", 409);
    }
    // This gate checks the deployment's declared custody; it is not a sandbox
    // and cannot inspect where an arbitrary adapter object really keeps a token.
    // Production launchers must derive/attest custody instead of trusting an
    // untrusted provider payload. Provider credentials never arrive via events.
    if (manifest.credential_custody === "wake_bridge_process"
      && !["none", "read_only"].includes(manifest.upstream_credential_breadth)
      && !options.allow_in_process_broad_credentials) {
      throw new BridgeError(`source ${manifest.id} injects a broad or unknown credential into Wake Bridge`, "unsafe_source_credential", 409);
    }
    const limit = Math.min(1000, Math.max(1, Math.floor(options.limit ?? 100)));
    const before = this.checkpoints.get(manifest.id);
    if (before && (before.subject_ref !== manifest.subject_ref || before.binding_fingerprint !== manifest.binding_fingerprint)) {
      throw new BridgeError(`source ${manifest.id} identity differs from its checkpoint`, "source_identity_mismatch", 409);
    }
    const result = await adapter.poll({ cursor: before?.cursor ?? null, limit });
    if (!result || !Array.isArray(result.events) || result.events.length > limit) {
      throw new BridgeError("source poll returned an invalid or oversized event page", "invalid_source_page", 502);
    }
    cursorJson(result.next_cursor);
    let inserted = 0;
    let duplicate = 0;
    for (const event of result.events) {
      const emitted = this.bridge.emitEvent(manifest.id, event);
      if (emitted.duplicate) duplicate += 1;
      else inserted += 1;
    }
    // Cursor advancement happens only after every event in this page is
    // durably accepted. A crash in the gap replays the page and is absorbed by
    // the event dedupe key rather than losing source facts.
    const checkpoint = this.checkpoints.commit(manifest, result.next_cursor, before?.revision ?? 0);
    return {
      source: manifest.id,
      adapter_version: manifest.version,
      checkpoint_revision: checkpoint.revision,
      events_seen: result.events.length,
      events_inserted: inserted,
      events_duplicate: duplicate,
      has_more: result.has_more === true,
    };
  }
}
