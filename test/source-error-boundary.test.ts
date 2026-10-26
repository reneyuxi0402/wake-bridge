import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { startDaemon } from "../src/daemon.js";
import { loadSourceConnectorFile } from "../src/source-connector.js";

const PRIVATE = "SOURCE_PRIVATE_FIXTURE_MARKER";
const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function temporaryRoot(): string {
  const root = mkdtempSync(join(tmpdir(), "source-error-boundary-"));
  roots.push(root);
  return root;
}

describe("source operational error boundaries", () => {
  it.each(["malformed", "missing"])("does not echo a %s connector file's content or path", (kind) => {
    const file = join(temporaryRoot(), `${PRIVATE}.json`);
    if (kind === "malformed") writeFileSync(file, `{"token":"${PRIVATE}", invalid}`);
    let caught: unknown;
    try { loadSourceConnectorFile(file); } catch (error) { caught = error; }
    expect(caught).toMatchObject({ code: "invalid_source_connector_file", status: 400 });
    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toBe("cannot read source connector file; check the file path and JSON syntax");
    expect(JSON.stringify(Object.getOwnPropertyDescriptors(caught as Error))).not.toContain(PRIVATE);
  });

  it("does not return a connector fetch exception over HTTP verify", async () => {
    const root = temporaryRoot();
    const daemon = await startDaemon({ instance_id: "source-error", owner_id: "owner", db_path: join(root, "bridge.sqlite") }, {
      host: "127.0.0.1", port: 0, unsafe_no_auth: true, scheduler_interval_ms: 0,
      source_connectors: [{ id: "fixture", base_url: "http://127.0.0.1:4391", token_env: "SOURCE_TOKEN", enabled: false }],
      source_environment: { SOURCE_TOKEN: PRIVATE },
      source_fetch: async () => { throw new Error(`token=${PRIVATE}; path=/private/${PRIVATE}`); },
    });
    try {
      const response = await fetch(`http://127.0.0.1:${(daemon.address as AddressInfo).port}/v1/sources/fixture/verify`, { method: "POST", body: "{}" });
      expect(response.status).toBe(503);
      const body = await response.json();
      expect(body).toEqual({ error: "source connector fixture is unavailable", code: "source_connector_unavailable" });
      expect(JSON.stringify(body)).not.toContain(PRIVATE);
    } finally { await daemon.close(); daemon.bridge.close(); }
  });
});
