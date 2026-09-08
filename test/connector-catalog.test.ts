import { describe, expect, it } from "vitest";
import { CONNECTOR_RUNTIME_STATES, connectorCatalog } from "../src/connector-catalog.js";

describe("public Source Connector catalog", () => {
  it("is explicit, disabled by default, and does not advertise unapproved sources", () => {
    const catalog = connectorCatalog();
    expect(catalog.map((entry) => entry.id)).toEqual([
      "botlingknows",
      "gmail",
      "group_chat_fixture",
    ]);
    expect(catalog.every((entry) => entry.default_state === "disabled")).toBe(true);
    expect(CONNECTOR_RUNTIME_STATES).toEqual(["disabled", "enabled", "needs_attention"]);
    expect(catalog.some((entry) => /yingfeng|迎风/u.test(`${entry.id} ${entry.display_name}`))).toBe(false);
  });

  it("keeps Gmail non-enableable until its proposal and UAT are complete", () => {
    const gmail = connectorCatalog().find((entry) => entry.id === "gmail");
    expect(gmail).toMatchObject({
      availability: "planned",
      bundled: false,
      enable_supported: false,
    });
  });
});
