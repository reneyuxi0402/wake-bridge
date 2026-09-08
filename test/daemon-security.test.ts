import { mkdtempSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { loadSourceIngestCredentialFile } from "../src/source-ingress.js";

function dbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-daemon-security-")), "bridge.sqlite");
}

function origin(address: AddressInfo | string | null): string {
  if (!address || typeof address === "string") throw new Error("expected TCP daemon address");
  return `http://127.0.0.1:${address.port}`;
}

function request(base: string, path: string, token?: string, body?: Record<string, unknown>): Promise<Response> {
  return fetch(`${base}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      ...(token ? { authorization: `Bearer ${token}` } : {}),
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

describe("daemon authority boundaries", () => {
  it("fails closed without an owner credential and confines the explicit escape hatch to loopback", async () => {
    const config = { instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC" };
    await expect(startDaemon(config, { host: "127.0.0.1", port: 0 })).rejects.toThrow(/admin_token is required/);
    await expect(startDaemon(config, { host: "0.0.0.0", port: 0, unsafe_no_auth: true })).rejects.toThrow(/loopback/);

    const daemon = await startDaemon(config, { host: "127.0.0.1", port: 0, scheduler_interval_ms: 0, unsafe_no_auth: true });
    try {
      expect((await request(origin(daemon.address), "/health")).status).toBe(200);
    } finally {
      await daemon.close();
      daemon.bridge.close();
    }
  });

  it("rejects weak owner credentials", async () => {
    await expect(startDaemon({
      instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC", admin_token: "short",
    }, { host: "127.0.0.1", port: 0 })).rejects.toThrow(/at least 32/);
  });

  it("injects source identity from a narrow credential without granting owner authority", async () => {
    const ownerToken = "owner-admin-token-0123456789abcdef";
    const jobsToken = "source-jobs-token-0123456789abcdef";
    const deployToken = "source-deploy-token-0123456789abcdef";
    const config = { instance_id: "secure", owner_id: "owner", db_path: dbPath(), timezone: "UTC", admin_token: ownerToken };
    const daemon = await startDaemon(config, {
      host: "127.0.0.1",
      port: 0,
      scheduler_interval_ms: 0,
      source_credentials: [
        { id: "jobs-primary", source: "jobs", token: jobsToken },
        { id: "deploy-primary", source: "deploy", token: deployToken },
      ],
    });
    const base = origin(daemon.address);
    try {
      expect((await request(base, "/v1/inspect", jobsToken)).status).toBe(401);
      expect((await request(base, "/v1/events", jobsToken, {
        source: "jobs",
        event: { type: "job.completed", dedupe_key: "owner-route", resource: { uri: "job://owner-route" } },
      })).status).toBe(401);
      expect((await request(base, "/v1/ingress/deploy/events", jobsToken, {
        type: "deployment.completed", dedupe_key: "wrong-scope", resource: { uri: "deployment://wrong-scope" },
      })).status).toBe(401);
      expect((await request(base, "/v1/ingress/jobs/events", ownerToken, {
        type: "job.completed", dedupe_key: "owner-on-source-route", resource: { uri: "job://owner-on-source-route" },
      })).status).toBe(401);

      const override = await request(base, "/v1/ingress/jobs/events", jobsToken, {
        source: "deploy",
        type: "job.completed",
        dedupe_key: "override",
        resource: { uri: "job://override" },
      });
      expect(override.status).toBe(400);
      await expect(override.json()).resolves.toMatchObject({ code: "source_identity_override" });

      const accepted = await request(base, "/v1/ingress/jobs/events", jobsToken, {
        type: "job.completed",
        dedupe_key: "job-1",
        resource: { uri: "job://1" },
      });
      expect(accepted.status).toBe(201);
      await expect(accepted.json()).resolves.toMatchObject({ event: { source: "jobs", dedupe_key: "job-1" } });

      const inspected = await request(base, "/v1/events", ownerToken);
      expect(inspected.status).toBe(200);
      await expect(inspected.json()).resolves.toMatchObject({ events: [{ source: "jobs", dedupe_key: "job-1" }] });
    } finally {
      await daemon.close();
      daemon.bridge.close();
    }
  });

  it("rejects token reuse across owner, host, and source authority", async () => {
    const shared = "shared-authority-token-0123456789abcdef";
    const config = { instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC", admin_token: shared };
    const bridge = new WakeBridge(config);
    try {
      await expect(startDaemon(config, {
        bridge,
        host: "127.0.0.1",
        port: 0,
        source_credentials: [{ id: "jobs-primary", source: "jobs", token: shared }],
      })).rejects.toThrow(/reused across credential scopes/);
    } finally {
      bridge.close();
    }
  });
});

describe("source ingest credential files", () => {
  it("keeps token values in the environment and rejects missing or duplicate references", () => {
    const path = join(mkdtempSync(join(tmpdir(), "wake-bridge-source-credentials-")), "credentials.json");
    writeFileSync(path, JSON.stringify({
      version: 1,
      credentials: [{ id: "jobs-primary", source: "jobs", token_env: "WAKEBRIDGE_SOURCE_JOBS_TOKEN" }],
    }));
    expect(loadSourceIngestCredentialFile(path, {
      WAKEBRIDGE_SOURCE_JOBS_TOKEN: "source-jobs-token-0123456789abcdef",
    })).toEqual([{ id: "jobs-primary", source: "jobs", token: "source-jobs-token-0123456789abcdef" }]);
    expect(() => loadSourceIngestCredentialFile(path, {})).toThrow(/environment variable is missing/);

    writeFileSync(path, JSON.stringify({
      version: 1,
      credentials: [
        { id: "jobs-primary", source: "jobs", token_env: "ONE" },
        { id: "jobs-primary", source: "jobs", token_env: "TWO" },
      ],
    }));
    expect(() => loadSourceIngestCredentialFile(path, {
      ONE: "source-token-one-0123456789abcdef",
      TWO: "source-token-two-0123456789abcdef",
    })).toThrow(/duplicate/);
  });
});
