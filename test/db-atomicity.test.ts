import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { SqliteDatabase } from "../src/db.js";

/**
 * Every operation runs through the sqlite3 CLI.  By default the CLI keeps
 * executing after a failing statement and still reaches COMMIT, so a
 * "transaction" that reported an error could leave part of its writes behind.
 */

function dbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-atomic-")), "bridge.sqlite");
}

describe("transaction atomicity", () => {
  it("rolls back every statement when one of them fails", () => {
    const db = new SqliteDatabase(dbPath());
    db.exec("CREATE TABLE probe(value TEXT NOT NULL UNIQUE);");

    expect(() => db.transaction([
      "INSERT INTO probe(value) VALUES('first');",
      "INSERT INTO probe(value) VALUES('first');",
      "INSERT INTO probe(value) VALUES('after-failure');",
    ])).toThrow(/UNIQUE constraint failed/);

    expect(db.query("SELECT value FROM probe;")).toEqual([]);
    db.close();
  });

  it("leaves no partial claim behind when emit loses a dedupe race", () => {
    const target = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC" } as any);
    const input = { type: "job.completed", dedupe_key: "race:1", resource: { uri: "job://race/1" } };
    const first = target.emit("manual", input);

    // Simulate a second worker that passed the duplicate pre-read before the
    // first worker committed: the event insert then fails on the unique key.
    vi.spyOn(target as any, "duplicateEmitResult").mockReturnValueOnce(null);
    const second = target.emit("manual", input);

    expect(second.duplicate).toBe(true);
    expect(second.event.id).toBe(first.event.id);
    expect(target.listClaims({ limit: 100_000 })).toHaveLength(1);
    expect(target.getClaim(first.claim!.id)?.event_ids).toEqual([first.event.id]);
    expect(target.db.query("SELECT COUNT(*) AS total FROM claim_transitions;")[0]).toEqual({ total: 1 });
    target.close();
  });
});
