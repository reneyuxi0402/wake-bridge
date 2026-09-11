import { createServer } from "node:http";
import { pathToFileURL } from "node:url";
import {
  HostSessionClient,
  WakeBridgeSdkError,
  validateLocalHostDeliveryRequest,
} from "wake-bridge/transport";

const DEFAULT_RENEW_INTERVAL_MS = 60_000;
const MAX_ACCEPTED_ATTEMPTS = 1_000;

function requireSecret(value, name) {
  if (typeof value !== "string" || value.length < 32) {
    throw new Error(`${name} must contain at least 32 characters`);
  }
  return value;
}

async function readJson(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 256 * 1024) throw new WakeBridgeSdkError("request body is too large", "invalid_host_delivery", 413);
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new WakeBridgeSdkError("request body is not valid JSON", "invalid_host_delivery", 400);
  }
}

/**
 * Minimal out-of-process Host Adapter lifecycle.
 *
 * `acceptWake` is the only host-specific seam. It must enqueue the fixed Wake
 * envelope into the exact session named by `sessionRef`, and resolve only once
 * that host has accepted ownership of the delivery.
 */
export class ReferenceHostAdapter {
  constructor(options) {
    this.daemonOrigin = options.daemonOrigin;
    this.hostToken = requireSecret(options.hostToken, "hostToken");
    this.routeToken = requireSecret(options.routeToken, "routeToken");
    if (this.hostToken === this.routeToken) throw new Error("hostToken and routeToken must be distinct");
    this.sessionRef = options.sessionRef;
    this.attentionChannel = options.attentionChannel;
    this.acceptWake = options.acceptWake;
    this.port = options.port ?? 0;
    this.renewIntervalMs = options.renewIntervalMs ?? DEFAULT_RENEW_INTERVAL_MS;
    this.acceptedAttempts = new Set();
    this.client = new HostSessionClient({
      base_url: this.daemonOrigin,
      host_token: this.hostToken,
      adapter_kind: "reference_host",
    });
  }

  async start() {
    this.server = createServer((request, response) => {
      void this.handleRequest(request, response);
    });
    await new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.port, "127.0.0.1", resolve);
    });
    const address = this.server.address();
    if (!address || typeof address === "string") throw new Error("reference adapter did not obtain a TCP address");
    this.routeOrigin = `http://127.0.0.1:${address.port}`;
    try {
      this.registration = await this.client.open({
        session_ref: this.sessionRef,
        attention_channel: this.attentionChannel,
        route_origin: this.routeOrigin,
        route_token: this.routeToken,
      });
      this.lease = this.client.lease(this.registration);
      this.renewTimer = setInterval(() => {
        void this.client.renew(this.lease).catch((error) => {
          process.stderr.write(`[reference-host-adapter] lease renew failed: ${String(error)}\n`);
        });
      }, this.renewIntervalMs);
      this.renewTimer.unref();
      return { route_origin: this.routeOrigin, registration: this.registration };
    } catch (error) {
      await new Promise((resolve) => this.server.close(resolve));
      throw error;
    }
  }

  async noteUserPresence(observationId) {
    if (!this.lease) throw new Error("adapter is not started");
    return this.client.renewPresence(this.lease, {
      observed_by: "reference_host",
      observation: observationId,
    });
  }

  async noteSessionActivity(observationId, kind = "user_input") {
    if (!this.lease) throw new Error("adapter is not started");
    return this.client.observeActivity(this.lease, { observation_id: observationId, kind });
  }

  async close() {
    if (this.renewTimer) clearInterval(this.renewTimer);
    if (this.lease) {
      try { await this.client.close(this.lease); } catch { /* lease expiry is a safe close fallback */ }
    }
    if (this.server?.listening) await new Promise((resolve) => this.server.close(resolve));
  }

  async handleRequest(request, response) {
    try {
      if (request.method !== "POST" || request.url !== "/v1/wakes") {
        response.writeHead(404).end();
        return;
      }
      if (request.headers.authorization !== `Bearer ${this.routeToken}`) {
        response.writeHead(401).end();
        return;
      }
      const delivery = validateLocalHostDeliveryRequest(await readJson(request));
      if (request.headers["x-wakebridge-delivery-nonce"] !== delivery.delivery_nonce) {
        response.writeHead(400).end();
        return;
      }
      if (!this.acceptedAttempts.has(delivery.attempt_id)) {
        await this.acceptWake({
          session_ref: this.sessionRef,
          wake: delivery.wake,
          attempt_id: delivery.attempt_id,
          delivery_nonce: delivery.delivery_nonce,
        });
        this.acceptedAttempts.add(delivery.attempt_id);
        if (this.acceptedAttempts.size > MAX_ACCEPTED_ATTEMPTS) {
          this.acceptedAttempts.delete(this.acceptedAttempts.values().next().value);
        }
      }
      response.writeHead(202).end();
    } catch (error) {
      const status = error instanceof WakeBridgeSdkError ? error.status : 503;
      response.writeHead(status).end();
    }
  }
}

async function main() {
  const adapter = new ReferenceHostAdapter({
    daemonOrigin: process.env.WAKEBRIDGE_DAEMON_ORIGIN ?? "http://127.0.0.1:4311",
    hostToken: process.env.WAKEBRIDGE_HOST_TOKEN,
    routeToken: process.env.WAKEBRIDGE_ROUTE_TOKEN,
    sessionRef: process.env.WAKEBRIDGE_SESSION_REF ?? "reference-session",
    attentionChannel: process.env.WAKEBRIDGE_ATTENTION_CHANNEL ?? "default",
    port: Number(process.env.WAKEBRIDGE_REFERENCE_HOST_PORT ?? "4393"),
    acceptWake: async (delivery) => {
      // Replace this callback with the host's exact-session queue/injection API.
      const { delivery_nonce: _deliveryNonce, ...safeDelivery } = delivery;
      process.stdout.write(`${JSON.stringify({ accepted_by_demo_sink: true, ...safeDelivery })}\n`);
    },
  });
  const started = await adapter.start();
  process.stderr.write(`[reference-host-adapter] listening at ${started.route_origin}\n`);
  const shutdown = () => { void adapter.close().finally(() => process.exit(0)); };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  });
}
