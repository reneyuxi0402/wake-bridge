import { readFileSync } from "node:fs";

const manifest = JSON.parse(readFileSync(new URL("../../../package.json", import.meta.url), "utf8"));

export const WAKE_BRIDGE_VERSION = manifest.version;
