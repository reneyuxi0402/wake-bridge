import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, rmSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

/**
 * A deliberately small SQLite bridge.  Wake Bridge is local-first and the
 * runtime ships with the sqlite3 CLI on the supported hosts.  Keeping all
 * SQL here means the domain layer can still use real SQLite transactions,
 * WAL, foreign keys and unique constraints without tying the core to a
 * native Node sqlite ABI.
 */
export class SqliteDatabase {
  readonly path: string;
  private readonly ephemeral: boolean;

  constructor(dbPath: string, options: { allowSchemaUpgrade?: boolean } = {}) {
    this.ephemeral = dbPath === ":memory:";
    this.path = this.ephemeral
      ? join(tmpdir(), `wake-bridge-${randomUUID()}.sqlite`)
      : dbPath;
    mkdirSync(dirname(this.path), { recursive: true });
    const existed = existsSync(this.path);
    const existingBytes = existed ? statSync(this.path).size : 0;
    const existingVersion = this.userVersion();
    if (existingVersion > CURRENT_SCHEMA_VERSION) {
      throw new Error(`database schema ${existingVersion} is newer than supported schema ${CURRENT_SCHEMA_VERSION}`);
    }
    if (existingVersion === 0 && existingBytes > 0) {
      throw new Error("database is non-empty but has no Wake Bridge schema version");
    }
    if (existingVersion > 0 && existingVersion < MIN_SUPPORTED_SCHEMA_VERSION) {
      throw new Error(`database schema ${existingVersion} is older than the supported upgrade floor ${MIN_SUPPORTED_SCHEMA_VERSION}`);
    }
    if (existingVersion > 0 && existingVersion < CURRENT_SCHEMA_VERSION && options.allowSchemaUpgrade !== true) {
      throw new Error(`database schema ${existingVersion} requires explicit upgrade to ${CURRENT_SCHEMA_VERSION}; run wakebridge upgrade first`);
    }
    if (existingVersion < CURRENT_SCHEMA_VERSION) this.configure();
  }

  private userVersion(): number {
    const result = spawnSync("sqlite3", ["-batch", "-json", this.path], {
      input: ".timeout 5000\nPRAGMA user_version;\n",
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
    });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`sqlite3 failed (${result.status}): ${(result.stderr || result.stdout || "").trim()}`);
    const output = (result.stdout || "").trim();
    if (!output) return 0;
    const rows = JSON.parse(output) as Array<{ user_version?: number }>;
    return Number(rows[0]?.user_version ?? 0);
  }

  private configure(): void {
    this.exec([
      "PRAGMA journal_mode=WAL;",
      "PRAGMA synchronous=NORMAL;",
      "PRAGMA foreign_keys=ON;",
      "PRAGMA busy_timeout=5000;",
      "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);",
      SCHEMA_SQL,
      "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));",
      "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(2, strftime('%Y-%m-%dT%H:%M:%fZ','now'));",
      "PRAGMA user_version=2;",
    ].join("\n"));
    this.migrateSourceIdentity();
    this.migrateActivityWatches();
    this.migrateActivityWatchRescue();
    this.migrateDeliveryCorrelations();
    this.migrateEndpointRoutes();
    this.migrateActivityWatchCondition();
  }

  private migrateSourceIdentity(): void {
    const columns = new Set(this.query<{ name: string }>("PRAGMA table_info(source_checkpoints);").map((row) => String(row.name)));
    const statements: string[] = [];
    if (!columns.has("subject_ref")) statements.push("ALTER TABLE source_checkpoints ADD COLUMN subject_ref TEXT NOT NULL DEFAULT ''; ");
    if (!columns.has("binding_fingerprint")) statements.push("ALTER TABLE source_checkpoints ADD COLUMN binding_fingerprint TEXT NOT NULL DEFAULT ''; ");
    statements.push(`
CREATE TABLE IF NOT EXISTS source_checkpoint_history (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_id TEXT NOT NULL,
  source TEXT NOT NULL,
  adapter_version TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  binding_fingerprint TEXT NOT NULL,
  cursor_json TEXT NOT NULL,
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  archived_at TEXT NOT NULL,
  reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_controls (
  instance_id TEXT NOT NULL,
  source TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(instance_id, source)
);
CREATE INDEX IF NOT EXISTS idx_source_checkpoint_history ON source_checkpoint_history(instance_id, source, seq);
INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(3, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
PRAGMA user_version=3;`);
    this.exec(statements.join("\n"));
  }

  private migrateActivityWatches(): void {
    this.exec(`
CREATE TABLE IF NOT EXISTS activity_watches (
  instance_id TEXT NOT NULL,
  attention_channel TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  idle_after_ms INTEGER NOT NULL,
  endpoint_id TEXT,
  binding_generation INTEGER,
  epoch INTEGER NOT NULL,
  armed_at TEXT,
  last_nonwake_activity_at TEXT,
  activity_lease_expires_at TEXT,
  fired_epoch INTEGER,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(instance_id, attention_channel)
);
CREATE TABLE IF NOT EXISTS activity_observations (
  instance_id TEXT NOT NULL,
  endpoint_id TEXT NOT NULL,
  binding_generation INTEGER NOT NULL,
  observation_id TEXT NOT NULL,
  attention_channel TEXT NOT NULL,
  kind TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  received_at TEXT NOT NULL,
  PRIMARY KEY(instance_id, endpoint_id, binding_generation, observation_id)
);
CREATE INDEX IF NOT EXISTS idx_activity_watches_due ON activity_watches(instance_id, enabled, activity_lease_expires_at, last_nonwake_activity_at);
CREATE INDEX IF NOT EXISTS idx_activity_observations_received ON activity_observations(instance_id, received_at);
INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(4, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
PRAGMA user_version=4;`);
  }

  private migrateActivityWatchRescue(): void {
    const columns = new Set(this.query<{ name: string }>("PRAGMA table_info(activity_watches);").map((row) => String(row.name)));
    const statements: string[] = [];
    if (!columns.has("rescue_mode")) statements.push("ALTER TABLE activity_watches ADD COLUMN rescue_mode TEXT NOT NULL DEFAULT 'once';");
    if (!columns.has("repeat_after_ms")) statements.push("ALTER TABLE activity_watches ADD COLUMN repeat_after_ms INTEGER NOT NULL DEFAULT 3600000;");
    if (!columns.has("rescue_sequence")) statements.push("ALTER TABLE activity_watches ADD COLUMN rescue_sequence INTEGER NOT NULL DEFAULT 0;");
    if (!columns.has("last_rescue_at")) statements.push("ALTER TABLE activity_watches ADD COLUMN last_rescue_at TEXT;");
    if (!columns.has("next_rescue_at")) statements.push("ALTER TABLE activity_watches ADD COLUMN next_rescue_at TEXT;");
    if (!columns.has("covered_epoch")) statements.push("ALTER TABLE activity_watches ADD COLUMN covered_epoch INTEGER;");
    if (!columns.has("coverage_claim_id")) statements.push("ALTER TABLE activity_watches ADD COLUMN coverage_claim_id TEXT;");
    if (!columns.has("coverage_eligible_after")) statements.push("ALTER TABLE activity_watches ADD COLUMN coverage_eligible_after TEXT;");
    if (!columns.has("repeat_after_ms")) statements.push("UPDATE activity_watches SET repeat_after_ms=idle_after_ms;");
    statements.push(`
INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(5, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
PRAGMA user_version=5;`);
    this.exec(statements.join("\n"));
  }

  private migrateDeliveryCorrelations(): void {
    this.exec(`
CREATE TABLE IF NOT EXISTS delivery_correlations (
  attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES outbox_attempts(id) ON DELETE CASCADE,
  batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
  endpoint_id TEXT NOT NULL,
  binding_generation INTEGER NOT NULL,
  transport_kind TEXT NOT NULL,
  nonce_hash TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  consumed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_delivery_correlations_lookup ON delivery_correlations(endpoint_id, binding_generation, nonce_hash, state);
CREATE INDEX IF NOT EXISTS idx_delivery_correlations_expiry ON delivery_correlations(state, expires_at);
INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(6, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
PRAGMA user_version=6;`);
  }

  private migrateEndpointRoutes(): void {
    const columns = new Set(this.query<{ name: string }>("PRAGMA table_info(endpoints);").map((row) => String(row.name)));
    const hasLegacyColumn = columns.has("cold_routes_json");
    const hasRoutesColumn = columns.has("routes_json");
    if (hasLegacyColumn === hasRoutesColumn) {
      throw new Error(hasLegacyColumn
        ? "endpoints schema contains both cold_routes_json and routes_json; refusing an ambiguous route migration"
        : "endpoints schema contains neither cold_routes_json nor routes_json");
    }
    const rename = hasLegacyColumn ? "ALTER TABLE endpoints RENAME COLUMN cold_routes_json TO routes_json;" : "";
    this.exec(`
BEGIN IMMEDIATE;
${rename}
INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(7, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
PRAGMA user_version=7;
COMMIT;`);
  }

  private migrateActivityWatchCondition(): void {
    const columns = new Set(this.query<{ name: string }>("PRAGMA table_info(activity_watches);").map((row) => String(row.name)));
    if (columns.has("mode")) {
      this.exec("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(8, strftime('%Y-%m-%dT%H:%M:%fZ','now')); PRAGMA user_version=8;");
      return;
    }
    this.exec(`
BEGIN IMMEDIATE;
ALTER TABLE activity_watches RENAME TO activity_watches_v7;
CREATE TABLE activity_watches (
  instance_id TEXT NOT NULL,
  attention_channel TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  idle_after_ms INTEGER NOT NULL,
  endpoint_id TEXT,
  binding_generation INTEGER,
  epoch INTEGER NOT NULL,
  armed_at TEXT,
  last_nonwake_activity_at TEXT,
  activity_lease_expires_at TEXT,
  fired_epoch INTEGER,
  mode TEXT NOT NULL DEFAULT 'once',
  repeat_after_ms INTEGER NOT NULL DEFAULT 3600000,
  sequence INTEGER NOT NULL DEFAULT 0,
  last_triggered_at TEXT,
  next_due_at TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(instance_id, attention_channel)
);
INSERT INTO activity_watches(instance_id, attention_channel, enabled, idle_after_ms, endpoint_id, binding_generation, epoch,
  armed_at, last_nonwake_activity_at, activity_lease_expires_at, fired_epoch, mode, repeat_after_ms, sequence,
  last_triggered_at, next_due_at, updated_at)
SELECT instance_id, attention_channel, enabled, idle_after_ms, endpoint_id, binding_generation, epoch, armed_at,
  last_nonwake_activity_at, activity_lease_expires_at, fired_epoch, 'once', repeat_after_ms, rescue_sequence,
  last_rescue_at, NULL, updated_at FROM activity_watches_v7;
DROP TABLE activity_watches_v7;
CREATE INDEX idx_activity_watches_due ON activity_watches(instance_id, enabled, activity_lease_expires_at, last_nonwake_activity_at);
INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(8, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
PRAGMA user_version=8;
COMMIT;`);
  }

  exec(sql: string): void {
    const result = spawnSync("sqlite3", ["-batch", this.path], {
      input: this.withConnectionPragmas(sql),
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      throw new Error(`sqlite3 failed (${result.status}): ${(result.stderr || result.stdout || "").trim()}`);
    }
  }

  query<T extends Record<string, unknown> = Record<string, unknown>>(sql: string): T[] {
    const result = spawnSync("sqlite3", ["-batch", "-json", this.path], {
      input: this.withConnectionPragmas(sql),
      encoding: "utf8",
      maxBuffer: 32 * 1024 * 1024,
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      throw new Error(`sqlite3 failed (${result.status}): ${(result.stderr || result.stdout || "").trim()}`);
    }
    const output = (result.stdout || "").trim();
    if (!output) return [];
    try {
      const parsed = JSON.parse(output) as unknown;
      return Array.isArray(parsed) ? (parsed as T[]) : [];
    } catch (error) {
      throw new Error(`sqlite3 returned invalid JSON: ${String(error)}; output=${output.slice(0, 500)}`);
    }
  }

  transaction(statements: string[]): void {
    this.exec(["BEGIN IMMEDIATE;", ...statements, "COMMIT;"].join("\n"));
  }

  private withConnectionPragmas(sql: string): string {
    // Every operation is a fresh sqlite3 process/connection.  These pragmas
    // therefore belong on every invocation, not only on initial migration.
    // `.timeout` is a sqlite3 CLI command and does not add a result row to
    // `-json` output (unlike PRAGMA busy_timeout=...).
    return ".timeout 5000\nPRAGMA foreign_keys=ON;\n" + sql;
  }

  close(): void {
    if (this.ephemeral) {
      try {
        rmSync(this.path, { force: true });
        rmSync(`${this.path}-wal`, { force: true });
        rmSync(`${this.path}-shm`, { force: true });
      } catch {
        // Best-effort cleanup for tests. SQLite has already closed each CLI
        // connection, so inability to unlink is harmless.
      }
    }
  }
}

export const CURRENT_SCHEMA_VERSION = 8;
export const MIN_SUPPORTED_SCHEMA_VERSION = 6;

export function sqlValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "boolean") return value ? "1" : "0";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "bigint") return value.toString();
  const text = typeof value === "string" ? value : JSON.stringify(value);
  return `'${String(text ?? "").replaceAll("'", "''")}'`;
}

export function sqlJson(value: unknown): string {
  return sqlValue(JSON.stringify(value ?? null));
}

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY NOT NULL,
  instance_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  source TEXT NOT NULL,
  type TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  occurred_at TEXT NOT NULL,
  received_at TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  coalesce_key TEXT,
  priority_hint TEXT,
  attention_channel_hint TEXT,
  actor_ref TEXT,
  resource_json TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  payload_preview TEXT,
  matched_policy_id TEXT,
  matched_policy_version INTEGER,
  UNIQUE(instance_id, source, dedupe_key)
);

CREATE TABLE IF NOT EXISTS event_status (
  event_id TEXT PRIMARY KEY NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  state TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_transitions (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT,
  at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
  id TEXT NOT NULL,
  version INTEGER NOT NULL,
  enabled INTEGER NOT NULL,
  order_no INTEGER NOT NULL,
  rule_json TEXT NOT NULL,
  installed_at TEXT NOT NULL,
  PRIMARY KEY(id, version)
);

CREATE TABLE IF NOT EXISTS claims (
  id TEXT PRIMARY KEY NOT NULL,
  instance_id TEXT NOT NULL,
  origin TEXT NOT NULL,
  event_ids_json TEXT NOT NULL,
  resource_json TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  attention_channel TEXT NOT NULL,
  eligible_after TEXT NOT NULL,
  expires_at TEXT,
  defer_while_presence INTEGER NOT NULL,
  state TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  snooze_until TEXT,
  consumed_result_json TEXT,
  dismissed_reason TEXT
);

CREATE TABLE IF NOT EXISTS claim_transitions (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT,
  at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoints (
  id TEXT PRIMARY KEY NOT NULL,
  instance_id TEXT NOT NULL,
  host_kind TEXT NOT NULL,
  session_ref TEXT NOT NULL,
  lease_token_hash TEXT NOT NULL,
  lease_expires_at TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  routes_json TEXT NOT NULL,
  registered_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bindings (
  instance_id TEXT NOT NULL,
  attention_channel TEXT NOT NULL,
  endpoint_id TEXT NOT NULL REFERENCES endpoints(id) ON DELETE RESTRICT,
  generation INTEGER NOT NULL,
  bound_at TEXT NOT NULL,
  PRIMARY KEY(instance_id, attention_channel)
);

CREATE TABLE IF NOT EXISTS presence_leases (
  instance_id TEXT NOT NULL,
  attention_channel TEXT NOT NULL,
  endpoint_id TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
  binding_generation INTEGER NOT NULL,
  renewed_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  observed_by TEXT NOT NULL,
  observation TEXT NOT NULL,
  PRIMARY KEY(instance_id, attention_channel)
);

CREATE TABLE IF NOT EXISTS batches (
  id TEXT PRIMARY KEY NOT NULL,
  instance_id TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  attention_channel TEXT NOT NULL,
  claim_ids_json TEXT NOT NULL,
  event_ids_json TEXT NOT NULL,
  coalesce_key TEXT,
  state TEXT NOT NULL,
  not_before TEXT NOT NULL,
  deadline TEXT,
  attempt INTEGER NOT NULL DEFAULT 0,
  binding_generation INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  lease_expires_at TEXT,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS batch_transitions (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT,
  at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_attempts (
  id TEXT PRIMARY KEY NOT NULL,
  batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
  attempt_no INTEGER NOT NULL,
  state TEXT NOT NULL,
  transport_kind TEXT,
  endpoint_id TEXT,
  binding_generation INTEGER,
  leased_until TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  error_class TEXT,
  error_message TEXT,
  UNIQUE(batch_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS receipts (
  id TEXT PRIMARY KEY NOT NULL,
  batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
  claim_id TEXT,
  stage TEXT NOT NULL,
  at TEXT NOT NULL,
  endpoint_id TEXT,
  binding_generation INTEGER,
  transport_kind TEXT,
  details_json TEXT
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  scope TEXT NOT NULL,
  key TEXT NOT NULL,
  result_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(scope, key)
);

CREATE TABLE IF NOT EXISTS source_checkpoints (
  instance_id TEXT NOT NULL,
  source TEXT NOT NULL,
  adapter_version TEXT NOT NULL,
  cursor_json TEXT NOT NULL,
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(instance_id, source)
);

CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
CREATE INDEX IF NOT EXISTS idx_event_status_state ON event_status(state);
CREATE INDEX IF NOT EXISTS idx_claims_due ON claims(state, eligible_after);
CREATE INDEX IF NOT EXISTS idx_batches_due ON batches(state, not_before, deadline);
CREATE INDEX IF NOT EXISTS idx_attempts_lease ON outbox_attempts(state, leased_until);
CREATE INDEX IF NOT EXISTS idx_receipts_batch ON receipts(batch_id, at);
`;
