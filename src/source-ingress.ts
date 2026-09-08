import { readFileSync } from "node:fs";
import { BridgeError } from "./core.js";

export interface SourceIngestCredential {
  id: string;
  source: string;
  token: string;
}

export interface SourceIngestCredentialReference {
  id: string;
  source: string;
  token_env: string;
}

export interface SourceIngestCredentialFile {
  version: 1;
  credentials: SourceIngestCredentialReference[];
}

const CREDENTIAL_ID = /^[a-z][a-z0-9._-]{0,63}$/u;
const SOURCE_ID = /^[A-Za-z0-9_.:-]{1,128}$/u;
const TOKEN_ENV = /^[A-Z_][A-Z0-9_]{0,127}$/u;

export function validateSourceIngestCredential(credential: SourceIngestCredential): SourceIngestCredential {
  if (!credential || !CREDENTIAL_ID.test(credential.id)) {
    throw new BridgeError("source ingest credential id is invalid", "invalid_source_credential", 400);
  }
  if (!SOURCE_ID.test(credential.source)) {
    throw new BridgeError("source ingest credential source is invalid", "invalid_source_credential", 400);
  }
  if (typeof credential.token !== "string" || credential.token.length < 32) {
    throw new BridgeError("source ingest credential token must contain at least 32 characters", "invalid_source_credential", 400);
  }
  return credential;
}

export function loadSourceIngestCredentialFile(
  path: string,
  environment: NodeJS.ProcessEnv = process.env,
): SourceIngestCredential[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new BridgeError(`source credential file is unreadable: ${String(error)}`, "invalid_source_credential_file", 400);
  }
  const file = parsed as Partial<SourceIngestCredentialFile>;
  if (file?.version !== 1 || !Array.isArray(file.credentials)) {
    throw new BridgeError("source credential file must use version 1 and contain credentials", "invalid_source_credential_file", 400);
  }
  const ids = new Set<string>();
  const credentials = file.credentials.map((reference) => {
    if (!reference || !CREDENTIAL_ID.test(reference.id) || !SOURCE_ID.test(reference.source) || !TOKEN_ENV.test(reference.token_env)) {
      throw new BridgeError("source credential reference is invalid", "invalid_source_credential_file", 400);
    }
    if (ids.has(reference.id)) {
      throw new BridgeError(`duplicate source credential id: ${reference.id}`, "invalid_source_credential_file", 400);
    }
    ids.add(reference.id);
    const token = environment[reference.token_env];
    if (!token) {
      throw new BridgeError(`source credential environment variable is missing: ${reference.token_env}`, "source_credential_unavailable", 400);
    }
    return validateSourceIngestCredential({ id: reference.id, source: reference.source, token });
  });
  return credentials;
}
