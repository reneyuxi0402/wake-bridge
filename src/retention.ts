import { BridgeError, type WakeBridge } from "./core.js";
import { sqlValue } from "./db.js";
import { FINALIZED_CLAIM_STATES } from "./types.js";

const DAY_MS = 24 * 60 * 60_000;
const MAX_RETENTION_DAYS = 3650;

export interface PruneHistoryInput {
  older_than_days: number;
  /** Without this the call only reports what would be deleted. */
  apply?: boolean;
}

export interface PruneHistoryResult {
  cutoff: string;
  applied: boolean;
  deleted: {
    batches: number;
    batch_transitions: number;
    outbox_attempts: number;
    receipts: number;
    delivery_correlations: number;
    claims: number;
    claim_transitions: number;
  };
}

/**
 * Operator-invoked retention for finished delivery history.  Nothing calls this
 * on a schedule; history is kept until an operator runs it.
 *
 * Deleted, when last updated before the cutoff:
 * - `seen` / `cancelled` batches that no longer carry a live claim, with their
 *   transitions, attempts, receipts and delivery correlations (FK cascade);
 * - consumed / dismissed / expired claims that no surviving batch references,
 *   with their transitions (FK cascade).
 *
 * Never deleted: events, event status and transitions, and idempotency keys.
 * They are the dedupe record; removing them would let a re-delivered source
 * event or a replayed schedule wake the agent again.  Batches waiting for
 * delivery, dispatched, needs_attention or dead_letter, and any claim that can
 * still be delivered, are also kept whatever their age.
 */
export function pruneHistory(bridge: WakeBridge, input: PruneHistoryInput): PruneHistoryResult {
  const days = input?.older_than_days;
  if (!Number.isSafeInteger(days) || days < 1 || days > MAX_RETENTION_DAYS) {
    throw new BridgeError(`older_than_days must be an integer between 1 and ${MAX_RETENTION_DAYS}`, "invalid_arguments", 400);
  }
  const apply = input.apply === true;
  const cutoff = new Date(bridge.now().getTime() - days * DAY_MS).toISOString();
  const instance = sqlValue(bridge.config.instance_id);
  const finalized = FINALIZED_CLAIM_STATES.map(sqlValue).join(",");
  // Selection, counts and deletes share one connection and one write
  // transaction, so the counts describe exactly the rows that are deleted.
  const rows = bridge.db.query<Record<string, number>>([
    "BEGIN IMMEDIATE;",
    `CREATE TEMP TABLE prune_batches AS SELECT b.id FROM batches b
      WHERE b.instance_id=${instance} AND b.state IN ('seen','cancelled') AND b.updated_at<${sqlValue(cutoff)}
        AND NOT EXISTS (SELECT 1 FROM json_each(b.claim_ids_json) j JOIN claims c ON c.id=j.value WHERE c.state NOT IN (${finalized}));`,
    `CREATE TEMP TABLE kept_claim_refs AS SELECT DISTINCT j.value AS claim_id FROM batches b, json_each(b.claim_ids_json) j
      WHERE b.instance_id=${instance} AND b.id NOT IN (SELECT id FROM prune_batches);`,
    `CREATE TEMP TABLE prune_claims AS SELECT c.id FROM claims c
      WHERE c.instance_id=${instance} AND c.state IN (${finalized}) AND c.updated_at<${sqlValue(cutoff)}
        AND c.id NOT IN (SELECT claim_id FROM kept_claim_refs);`,
    `SELECT
      (SELECT COUNT(*) FROM prune_batches) AS batches,
      (SELECT COUNT(*) FROM batch_transitions WHERE batch_id IN (SELECT id FROM prune_batches)) AS batch_transitions,
      (SELECT COUNT(*) FROM outbox_attempts WHERE batch_id IN (SELECT id FROM prune_batches)) AS outbox_attempts,
      (SELECT COUNT(*) FROM receipts WHERE batch_id IN (SELECT id FROM prune_batches)) AS receipts,
      (SELECT COUNT(*) FROM delivery_correlations WHERE batch_id IN (SELECT id FROM prune_batches)) AS delivery_correlations,
      (SELECT COUNT(*) FROM prune_claims) AS claims,
      (SELECT COUNT(*) FROM claim_transitions WHERE claim_id IN (SELECT id FROM prune_claims)) AS claim_transitions;`,
    ...(apply
      ? [
        "DELETE FROM batches WHERE id IN (SELECT id FROM prune_batches);",
        "DELETE FROM claims WHERE id IN (SELECT id FROM prune_claims);",
        "COMMIT;",
      ]
      : ["ROLLBACK;"]),
  ].join("\n"));
  const counts = rows[0] ?? {};
  return {
    cutoff,
    applied: apply,
    deleted: {
      batches: Number(counts.batches ?? 0),
      batch_transitions: Number(counts.batch_transitions ?? 0),
      outbox_attempts: Number(counts.outbox_attempts ?? 0),
      receipts: Number(counts.receipts ?? 0),
      delivery_correlations: Number(counts.delivery_correlations ?? 0),
      claims: Number(counts.claims ?? 0),
      claim_transitions: Number(counts.claim_transitions ?? 0),
    },
  };
}
