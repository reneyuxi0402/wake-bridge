import { WakeBridge } from "../../src/core.js";
import { startDaemon, type DaemonOptions } from "../../src/daemon.js";
import { LocalHttpHostAdapter } from "../../src/out-of-process-host.js";
import type { HostAdapterManifest } from "../../src/transport-sdk.js";
import type { BridgeConfig } from "../../src/types.js";

// A real, separately supervised daemon process. No fixture-only HTTP routes,
// external services, models, or provider credentials are installed here.
process.once("message", async (input: {
  config: BridgeConfig;
  manifest: HostAdapterManifest;
  host_credentials: DaemonOptions["host_credentials"];
  source_credentials: DaemonOptions["source_credentials"];
}) => {
  const bridge = new WakeBridge(input.config, { autoMockTransport: false });
  const daemon = await startDaemon(input.config, {
    bridge,
    host: "127.0.0.1",
    port: 0,
    scheduler_interval_ms: 0,
    host_adapters: [new LocalHttpHostAdapter(input.manifest)],
    host_credentials: input.host_credentials,
    source_credentials: input.source_credentials,
  });
  const address = daemon.address;
  if (!address || typeof address === "string") throw new Error("expected loopback TCP address");
  process.once("SIGTERM", () => {
    void daemon.close().then(() => {
      bridge.close();
      process.disconnect();
    }).catch(() => process.exit(1));
  });
  process.send!({ origin: `http://127.0.0.1:${address.port}`, pid: process.pid });
});
