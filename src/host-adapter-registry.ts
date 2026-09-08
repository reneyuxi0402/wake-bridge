import { BridgeError, type WakeBridge } from "./core.js";
import {
  validateHostAdapterManifest,
  type HostAdapter,
  type HostAdapterLifecycleContext,
  type HostAdapterManifest,
} from "./transport-sdk.js";

export interface HostBootstrapCredential {
  id: string;
  token: string;
  adapter_kind: string;
  host_kind: string;
  attention_channels: string[];
}

/** Core-owned orchestration. This class is intentionally not a package export. */
export class HostAdapterRegistry {
  private readonly adapters = new Map<string, HostAdapter>();
  private started: HostAdapter[] = [];
  private bridge: WakeBridge | null = null;

  constructor(adapters: HostAdapter[] = []) {
    for (const adapter of adapters) this.register(adapter);
  }

  register(adapter: HostAdapter): void {
    validateHostAdapterManifest(adapter.manifest);
    if (adapter.kind !== adapter.manifest.adapter_kind) {
      throw new BridgeError("adapter kind does not match manifest", "invalid_host_adapter", 400);
    }
    if (this.adapters.has(adapter.kind)) {
      throw new BridgeError(`duplicate host adapter: ${adapter.kind}`, "duplicate_host_adapter", 409);
    }
    this.adapters.set(adapter.kind, adapter);
  }

  list(): HostAdapterManifest[] {
    return [...this.adapters.values()].map((adapter) => adapter.manifest);
  }

  get(kind: string): HostAdapter | null {
    return this.adapters.get(kind) ?? null;
  }

  async start(bridge: WakeBridge, context: HostAdapterLifecycleContext): Promise<void> {
    this.bridge = bridge;
    for (const adapter of this.adapters.values()) {
      const existing = bridge.transports.get(adapter.kind);
      if (existing && existing !== adapter) {
        throw new BridgeError(`transport kind already registered: ${adapter.kind}`, "duplicate_host_adapter", 409);
      }
      bridge.registerTransport(adapter);
      this.started.push(adapter);
      await adapter.start?.(context);
    }
  }

  async stop(): Promise<void> {
    let firstError: unknown;
    for (const adapter of [...this.started].reverse()) {
      try {
        await adapter.stop?.();
      } catch (error) {
        firstError ??= error;
      } finally {
        this.bridge?.unregisterTransport(adapter.kind, adapter);
      }
    }
    this.started = [];
    this.bridge = null;
    if (firstError) throw firstError;
  }
}
