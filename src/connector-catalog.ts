export type ConnectorAvailability = "available" | "planned" | "unavailable";
export type ConnectorKind = "official_optional" | "reference";
export const CONNECTOR_RUNTIME_STATES = ["disabled", "enabled", "needs_attention"] as const;
export type ConnectorRuntimeState = typeof CONNECTOR_RUNTIME_STATES[number];

export interface ConnectorCatalogEntry {
  id: string;
  display_name: string;
  availability: ConnectorAvailability;
  kind: ConnectorKind;
  bundled: boolean;
  default_state: "disabled";
  enable_supported: boolean;
  source_kind: string;
  documentation: string;
  onboarding: readonly string[];
}

const CONNECTOR_CATALOG: readonly ConnectorCatalogEntry[] = Object.freeze([
  Object.freeze({
    id: "botlingknows",
    display_name: "小机知道",
    availability: "available",
    kind: "official_optional",
    bundled: true,
    default_state: "disabled",
    enable_supported: true,
    source_kind: "botlingknows",
    documentation: "docs/adapters/botlingknows.md",
    onboarding: Object.freeze(["configure", "verify_identity", "bootstrap_from_now", "uat", "enable"]),
  }),
  Object.freeze({
    id: "gmail",
    display_name: "Gmail",
    availability: "planned",
    kind: "official_optional",
    bundled: false,
    default_state: "disabled",
    enable_supported: false,
    source_kind: "gmail",
    documentation: "docs/adapters/gmail-planned.md",
    onboarding: Object.freeze([]),
  }),
  Object.freeze({
    id: "group_chat_fixture",
    display_name: "Group Chat fixture",
    availability: "available",
    kind: "reference",
    bundled: true,
    default_state: "disabled",
    enable_supported: true,
    source_kind: "group_chat",
    documentation: "docs/adapters/group-chat.md",
    onboarding: Object.freeze(["configure", "verify_identity", "bootstrap_from_now", "uat", "enable"]),
  }),
]);

export function connectorCatalog(): readonly ConnectorCatalogEntry[] {
  return CONNECTOR_CATALOG;
}
