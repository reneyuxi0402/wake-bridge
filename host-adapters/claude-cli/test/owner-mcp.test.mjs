import assert from "node:assert/strict";
import test from "node:test";
import { ToolsListTransform, rewriteJsonlLine, rewriteToolsListMessage } from "../src/owner-mcp.mjs";

test("only canonicalizes attention_ack tools/list schema", () => {
  const response = { jsonrpc: "2.0", id: 7, result: { tools: [
    { name: "attention_ack", description: "ack", inputSchema: { type: "object", anyOf: [{ required: ["wake_batch_id"] }, { required: ["batch_id"] }], properties: { batch_id: { type: "string" } } } },
    { name: "attention_schedule", inputSchema: { type: "object", properties: { x: { type: "string" } } } },
  ] } };
  const rewritten = rewriteToolsListMessage(response);
  assert.deepEqual(rewritten.result.tools[0].inputSchema, {
    type: "object", properties: { wake_batch_id: { type: "string", minLength: 1, maxLength: 200 } },
    required: ["wake_batch_id"], additionalProperties: false,
  });
  assert.equal(rewritten.result.tools[0].description, "ack");
  assert.deepEqual(rewritten.result.tools[1], response.result.tools[1]);
  assert.deepEqual(rewriteToolsListMessage({ jsonrpc: "2.0", id: 8, result: { tools: [] } }), { jsonrpc: "2.0", id: 8, result: { tools: [] } });
});

test("JSONL mapping preserves non-target and malformed lines", () => {
  const other = '{"jsonrpc":"2.0","id":1,"result":{"ok":true}}\n';
  assert.equal(rewriteJsonlLine(other), other);
  const malformed = "not-json\n";
  assert.equal(rewriteJsonlLine(malformed), malformed);
  const line = JSON.stringify({ jsonrpc: "2.0", id: 2, result: { tools: [{ name: "attention_ack", inputSchema: { anyOf: [] } }] } }) + "\n";
  const parsed = JSON.parse(rewriteJsonlLine(line));
  assert.deepEqual(parsed.result.tools[0].inputSchema.required, ["wake_batch_id"]);
});

test("JSONL transform preserves UTF-8 across byte-sized chunks", async () => {
  const input = Buffer.from(`${JSON.stringify({ jsonrpc: "2.0", id: 3, result: { message: "中文 🌊", tools: [{ name: "other", inputSchema: { type: "object" } }] } })}\n`);
  const transform = new ToolsListTransform();
  const chunks = [];
  transform.on("data", (chunk) => chunks.push(chunk));
  for (const byte of input) transform.write(Buffer.from([byte]));
  transform.end();
  await new Promise((resolve, reject) => { transform.once("end", resolve); transform.once("error", reject); });
  assert.deepEqual(Buffer.concat(chunks), input);
});
