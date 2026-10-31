import { createServer } from "node:http";
import { pathToFileURL } from "node:url";
import {
  HostSessionClient,
  WakeBridgeSdkError,
  validateLocalHostDeliveryRequest,
} from "wake-bridge/transport";

const DEFAULT_RENEW_INTERVAL_MS = 60_000;
const MAX_ACCEPTED_ATTEMPTS = 1_000;

function validateChannels(channels) {
  if (!Array.isArray(channels) || channels.length === 0
    || channels.some((channel) => typeof channel !== "string" || !channel.trim())
    || new Set(channels).size !== channels.length) {
    throw new Error("attentionChannels must be a non-empty array of distinct, non-empty channel strings");
  }
  return [...channels];
}

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
    if (options.attentionChannels !== undefined && options.attentionChannel !== undefined) {
      throw new Error("use either attentionChannel or attentionChannels, not both");
    }
    this.attentionChannel = options.attentionChannel;
    this.attentionChannels = options.attentionChannels === undefined ? undefined : validateChannels(options.attentionChannels);
    this.acceptWake = options.acceptWake;
    this.port = options.port ?? 0;
    this.renewIntervalMs = options.renewIntervalMs ?? DEFAULT_RENEW_INTERVAL_MS;
    this.acceptedAttempts = new Set();
    this.leaseLost = false;
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
      const input = {
        session_ref: this.sessionRef,
        route_origin: this.routeOrigin,
        route_token: this.routeToken,
      };
      this.registration = this.attentionChannels === undefined
        ? await this.client.open({ ...input, attention_channel: this.attentionChannel })
        : await this.client.openChannels({ ...input, attention_channels: this.attentionChannels });
      this.leases = this.attentionChannels === undefined
        ? [this.client.lease(this.registration)]
        : this.client.leases(this.registration);
      // One lease represents the shared endpoint for renew/close, not one call per channel.
      this.lease = this.leases[0];
      this.leaseLost = false;
      this.renewTimer = setInterval(() => {
        void this.renewLease();
      }, this.renewIntervalMs);
      this.renewTimer.unref();
      return { route_origin: this.routeOrigin, registration: this.registration };
    } catch (error) {
      await new Promise((resolve) => this.server.close(resolve));
      throw error;
    }
  }

  async renewLease() {
    const lease = this.lease;
    if (!lease) return;
    try {
      await this.client.renew(lease);
    } catch (error) {
      // A late renewal failure must not invalidate a replacement registration.
      if (this.lease !== lease) return;
      if (error instanceof WakeBridgeSdkError && error.remote_code === "endpoint_fenced") {
        this.leaseLost = true;
        clearInterval(this.renewTimer);
        this.lease = undefined;
        this.leases = [];
        this.registration = undefined;
        process.stderr.write("[reference-host-adapter] endpoint lease lost; delivery stopped. Close this adapter and explicitly register the intended session again.\n");
      } else {
        // Temporary unreachability can recover with the same token. Never
        // reopen automatically: that could take routing back from another host.
        process.stderr.write("[reference-host-adapter] lease renew failed; will retry with the existing token.\n");
      }
    }
  }

  targetLease(attentionChannel) {
    if (this.leaseLost) throw new Error("endpoint lease lost; explicit registration is required");
    if (!this.lease) throw new Error(this.registration ? "adapter has no active channel registrations; explicit registration is required" : "adapter is not started");
    if (attentionChannel === undefined && this.leases.length > 1) {
      throw new Error("attentionChannel is required for multi-channel presence/activity");
    }
    const lease = attentionChannel === undefined ? this.lease
      : this.leases.find((candidate) => candidate.attention_channel === attentionChannel);
    if (!lease) throw new Error("attentionChannel is not registered by this adapter");
    return lease;
  }

  retireChannel(lease, error) {
    if (!(error instanceof WakeBridgeSdkError)
      || !["stale_generation", "channel_deleted"].includes(error.remote_code)
      // A late observation failure must not invalidate a replacement handle.
      || !this.leases.includes(lease)) return;
    this.leases = this.leases.filter((candidate) => candidate !== lease);
    const { binding, bindings = [binding], ...registration } = this.registration;
    this.registration = {
      ...registration,
      bindings: bindings.filter((candidate) => candidate.attention_channel !== lease.attention_channel
        || candidate.generation !== lease.generation),
    };
    if (this.lease === lease) this.lease = this.leases[0];
    // Losing every channel does not prove ownership of the shared endpoint.
    // Keep empty bindings to reject delivery, but do not close or reopen it.
    if (!this.lease) clearInterval(this.renewTimer);
    process.stderr.write("[reference-host-adapter] channel registration lost; presence, activity and delivery stopped for that channel. Explicit registration is required to restore it.\n");
  }

  async noteUserPresence(observationId, attentionChannel) {
    const lease = this.targetLease(attentionChannel);
    try {
      return await this.client.renewPresence(lease, {
        observed_by: "reference_host",
        observation: observationId,
      });
    } catch (error) {
      this.retireChannel(lease, error);
      throw error;
    }
  }

  async noteSessionActivity(observationId, kind = "activity_started", attentionChannel) {
    const lease = this.targetLease(attentionChannel);
    try {
      return await this.client.observeActivity(lease, { observation_id: observationId, kind });
    } catch (error) {
      this.retireChannel(lease, error);
      throw error;
    }
  }

  async close() {
    if (this.renewTimer) clearInterval(this.renewTimer);
    const lease = this.lease;
    this.lease = undefined;
    this.leases = [];
    this.registration = undefined;
    if (lease) {
      try { await this.client.close(lease); } catch { /* lease expiry is a safe close fallback */ }
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
      // The daemon can dispatch after committing open, before its response reaches us.
      if (!this.registration) {
        response.writeHead(this.leaseLost ? 409 : 425).end();
        return;
      }
      const bindings = this.registration.bindings ?? [this.registration.binding];
      if (delivery.wake.instance_id !== this.registration.endpoint.instance_id
        || !bindings.some((binding) => binding.attention_channel === delivery.wake.attention_channel
          && binding.generation === delivery.wake.binding_generation)) {
        response.writeHead(409).end();
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
  let attentionOptions;
  if (process.env.WAKEBRIDGE_ATTENTION_CHANNELS !== undefined) {
    if (process.env.WAKEBRIDGE_ATTENTION_CHANNEL !== undefined) {
      throw new Error("set either WAKEBRIDGE_ATTENTION_CHANNEL or WAKEBRIDGE_ATTENTION_CHANNELS, not both");
    }
    try {
      attentionOptions = { attentionChannels: validateChannels(JSON.parse(process.env.WAKEBRIDGE_ATTENTION_CHANNELS)) };
    } catch {
      throw new Error('WAKEBRIDGE_ATTENTION_CHANNELS must be a JSON array of distinct, non-empty channel strings, e.g. ["life","work"]');
    }
  } else {
    attentionOptions = { attentionChannel: process.env.WAKEBRIDGE_ATTENTION_CHANNEL ?? "default" };
  }
  const adapter = new ReferenceHostAdapter({
    daemonOrigin: process.env.WAKEBRIDGE_DAEMON_ORIGIN ?? "http://127.0.0.1:4311",
    hostToken: process.env.WAKEBRIDGE_HOST_TOKEN,
    routeToken: process.env.WAKEBRIDGE_ROUTE_TOKEN,
    sessionRef: process.env.WAKEBRIDGE_SESSION_REF ?? "reference-session",
    ...attentionOptions,
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
