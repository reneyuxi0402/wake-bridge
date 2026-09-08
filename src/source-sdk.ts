import { WakeBridgeSdkError } from "./sdk-error.js";
import type { SourceAdapterManifest } from "./types.js";

export const SOURCE_ADAPTER_CONTRACT_VERSION = 1 as const;
export const SUPPORTED_SOURCE_ADAPTER_CONTRACT_VERSIONS = [SOURCE_ADAPTER_CONTRACT_VERSION] as const;

const SOURCE_ID = /^[A-Za-z0-9_.:-]{1,128}$/u;
const VERSION = /^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$/u;
const SUBJECT_REF = /^[A-Za-z0-9][A-Za-z0-9._:@/+~-]{0,199}$/u;
const BINDING_FINGERPRINT = /^sha256:[0-9a-f]{64}$/u;
const MAX_DECLARATION_COUNT = 32;

function boundedStrings(value: unknown, field: string, { required = false } = {}): string[] {
  if (!Array.isArray(value) || value.length > MAX_DECLARATION_COUNT
    || value.some((item) => typeof item !== "string" || !item || item.length > 200)
    || (required && value.length === 0)) {
    throw new WakeBridgeSdkError(`source adapter ${field} are invalid`, "invalid_source_manifest", 400);
  }
  return [...value];
}

export function validateSourceManifest(manifest: SourceAdapterManifest): SourceAdapterManifest {
  if (manifest?.contract_version !== SOURCE_ADAPTER_CONTRACT_VERSION) {
    throw new WakeBridgeSdkError("unsupported source adapter contract version", "invalid_source_manifest", 400);
  }
  if (!SOURCE_ID.test(manifest.id || "")) {
    throw new WakeBridgeSdkError("source adapter id is invalid", "invalid_source_manifest", 400);
  }
  if (!VERSION.test(manifest.version || "")) {
    throw new WakeBridgeSdkError("source adapter version is invalid", "invalid_source_manifest", 400);
  }
  if (!SUBJECT_REF.test(manifest.subject_ref || "")) {
    throw new WakeBridgeSdkError("source adapter subject_ref is invalid", "invalid_source_manifest", 400);
  }
  if (!BINDING_FINGERPRINT.test(manifest.binding_fingerprint || "")) {
    throw new WakeBridgeSdkError("source adapter binding_fingerprint is invalid", "invalid_source_manifest", 400);
  }
  if (!["none", "marks_read", "consumes", "unknown"].includes(manifest.read_side_effects)) {
    throw new WakeBridgeSdkError("source adapter must declare read_side_effects", "invalid_source_manifest", 400);
  }
  if (!["none", "connector", "wake_bridge_process"].includes(manifest.credential_custody)) {
    throw new WakeBridgeSdkError("source adapter must declare credential_custody", "invalid_source_manifest", 400);
  }
  if (!["none", "read_only", "broad", "unknown"].includes(manifest.upstream_credential_breadth)) {
    throw new WakeBridgeSdkError("source adapter must declare upstream_credential_breadth", "invalid_source_manifest", 400);
  }
  const scopes = boundedStrings(manifest.upstream_credential_scopes, "upstream credential scopes");
  const capabilities = boundedStrings(manifest.connector_capabilities, "connector capabilities", { required: true });
  if (manifest.credential_custody === "none" && (manifest.upstream_credential_breadth !== "none" || scopes.length > 0)) {
    throw new WakeBridgeSdkError("credential-free source has an upstream credential", "invalid_source_manifest", 400);
  }
  if (manifest.upstream_credential_breadth === "none" && scopes.length > 0) {
    throw new WakeBridgeSdkError("credential-free upstream has credential scopes", "invalid_source_manifest", 400);
  }
  return { ...manifest, upstream_credential_scopes: scopes, connector_capabilities: capabilities };
}

export type {
  PullSourceAdapter,
  SourceAdapterManifest,
  SourceCredentialCustody,
  SourceCursor,
  SourcePollContext,
  SourcePollResult,
  SourceReadSideEffects,
  SourceUpstreamCredentialBreadth,
} from "./types.js";
export { WakeBridgeSdkError } from "./sdk-error.js";
