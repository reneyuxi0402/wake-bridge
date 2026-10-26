import type { SpawnSyncReturns } from "node:child_process";

const DATABASE_ERROR_MESSAGES = {
  database_unavailable: "SQLite executable is unavailable",
  database_output_limit: "SQLite output exceeded the buffer limit",
  database_execution_failed: "SQLite execution failed",
  database_invalid_json: "SQLite returned invalid JSON",
} as const;

type DatabaseErrorClass = keyof typeof DATABASE_ERROR_MESSAGES;

/** Internal diagnostic only. Never retain the SQL, process output, or cause. */
export class DatabaseError extends Error {
  constructor(readonly error_class: DatabaseErrorClass, uniqueConflict = false) {
    // Core's dedupe-race recovery recognizes this fixed signal. Do not include
    // SQLite's table/column suffix or any other part of its diagnostic output.
    super(error_class === "database_execution_failed" && uniqueConflict
      ? "SQLite execution failed: UNIQUE constraint failed"
      : DATABASE_ERROR_MESSAGES[error_class]);
    this.name = "DatabaseError";
  }
}

export function assertSqliteSuccess(result: SpawnSyncReturns<string>): void {
  if (result.error) {
    const code = (result.error as NodeJS.ErrnoException).code;
    throw new DatabaseError(code === "ENOBUFS"
      ? "database_output_limit"
      : code === "ENOENT" || code === "EACCES" || code === "EPERM"
        ? "database_unavailable"
        : "database_execution_failed");
  }
  if (result.status !== 0) {
    throw new DatabaseError("database_execution_failed", /UNIQUE constraint failed/i.test(result.stderr || result.stdout || ""));
  }
}

export function operationalErrorClass(error: unknown): DatabaseErrorClass | "internal_error" {
  return error instanceof DatabaseError && Object.hasOwn(DATABASE_ERROR_MESSAGES, error.error_class)
    ? error.error_class
    : "internal_error";
}
