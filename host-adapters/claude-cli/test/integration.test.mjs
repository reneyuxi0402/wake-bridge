import assert from 'node:assert/strict';
import { test } from 'node:test';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:net';
import { randomBytes } from 'node:crypto';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const bridgeCli = join(dirname(fileURLToPath(import.meta.resolve('wake-bridge/package.json'))), 'dist/src/cli.js');
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('WAKEBRIDGE_') && !key.startsWith('WB_CLAUDE_')));
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(read, description) {
  const deadline = Date.now() + 12000;
  while (Date.now() < deadline) {
    const value = await read();
    if (value) return value;
    await pause(30);
  }
  throw new Error(`timeout: ${description}`);
}
async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
function child(args, env) {
  const process = spawn(globalThis.process.execPath, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
  const messages = [];
  let output = '', errors = '', buffer = '', sequence = 0;
  process.stdout.on('data', data => {
    output += data; buffer += data;
    for (;;) {
      const end = buffer.indexOf('\n');
      if (end < 0) break;
      const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
      try { messages.push(JSON.parse(line)); } catch { /* daemon's pretty JSON is captured as output */ }
    }
  });
  process.stderr.on('data', data => { errors += data; });
  return {
    process, messages, output: () => output, errors: () => errors,
    send(message) { process.stdin.write(JSON.stringify(message) + '\n'); },
    async rpc(method, params = {}) {
      const id = ++sequence;
      this.send({ jsonrpc: '2.0', id, method, params });
      const response = await until(() => {
        if (process.exitCode !== null) throw new Error(`MCP process exited before ${method}: ${errors}`);
        return messages.find(value => value.id === id);
      }, method);
      assert.equal(response.error, undefined);
      return response.result;
    },
    async initialize() {
      const result = await this.rpc('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'isolated-conformance', version: '1' } });
      this.send({ jsonrpc: '2.0', method: 'notifications/initialized' });
      return result;
    },
    async tool(name, args) {
      const result = await this.rpc('tools/call', { name, arguments: args });
      assert.notEqual(result.isError, true, result.content?.[0]?.text);
      return JSON.parse(result.content[0].text);
    },
    async close() {
      if (process.exitCode !== null || process.signalCode !== null) return;
      process.stdin.end();
      const timer = setTimeout(() => process.kill('SIGTERM'), 1500);
      const hard = setTimeout(() => process.kill('SIGKILL'), 4000);
      await new Promise(resolve => process.once('exit', resolve));
      clearTimeout(timer); clearTimeout(hard);
    },
  };
}
async function hook(env, payload, expected = 0) {
  const result = await new Promise((resolve, reject) => {
    const p = spawn(process.execPath, [join(root, 'src/hook.mjs')], { env, stdio: ['pipe', 'pipe', 'pipe'] });
    let error = '';
    p.stderr.on('data', data => { error += data; });
    p.once('error', reject);
    p.once('exit', code => resolve({ code, error }));
    p.stdin.end(JSON.stringify(payload));
  });
  assert.equal(result.code, expected, result.error);
}

test('published Core: online channel delivery, explicit MCP ack, takeover fence, and no cold fallback', { timeout: 60000 }, async () => {
  const temp = mkdtempSync(join(tmpdir(), 'wakebridge-claude-conformance-'));
  const children = [];
  try {
    const config = join(temp, 'space/wakebridge.config.json');
    const initialized = spawnSync(process.execPath, [bridgeCli, 'init', '--data-dir', join(temp, 'space'), '--instance-id', 'claude-conformance', '--owner-id', 'conformance-owner'], { env: cleanEnv(), encoding: 'utf8' });
    assert.equal(initialized.status, 0, initialized.stderr);
    const credential = JSON.parse(readFileSync(config, 'utf8')).admin_token;
    const hostToken = randomBytes(32).toString('hex');
    const daemon = child([bridgeCli, 'daemon', '--config', config, '--host-adapters', join(root, 'examples/host-adapters.json'), '--port', '0', '--scheduler-interval-ms', '50'], { ...cleanEnv(), WAKEBRIDGE_CLAUDE_HOST_TOKEN: hostToken });
    children.push(daemon);
    const origin = await until(() => {
      const port = /"port":\s*(\d+)/.exec(daemon.output())?.[1];
      if (daemon.process.exitCode !== null) throw new Error(daemon.errors());
      return port && `http://127.0.0.1:${port}`;
    }, 'daemon startup');
    const get = async path => {
      const response = await fetch(`${origin}${path}`, { headers: { authorization: `Bearer ${credential}` }, signal: AbortSignal.timeout(5000) });
      assert.ok(response.ok);
      return response.json();
    };
    const owner = child([join(root, 'src/owner-mcp.mjs'), '--bridge-config', config, '--daemon-origin', origin], { ...cleanEnv(), WAKEBRIDGE_ADMIN_TOKEN: 'wrong-inherited-token', WAKEBRIDGE_DB: '/never/use/this.sqlite' });
    children.push(owner); await owner.initialize();
    const tools = await owner.rpc('tools/list');
    const ackSchema = tools.tools.find(tool => tool.name === 'attention_ack').inputSchema;
    assert.deepEqual(ackSchema.required, ['wake_batch_id']);
    assert.deepEqual(Object.keys(ackSchema.properties), ['wake_batch_id']);
    assert.equal(ackSchema.anyOf, undefined);

    async function openChannel(label) {
      const stateDir = join(temp, label); mkdirSync(stateDir, { mode: 0o700 });
      const env = { ...cleanEnv(), WAKEBRIDGE_CLAUDE_HOST_TOKEN: hostToken, WAKEBRIDGE_CLAUDE_ROUTE_TOKEN: randomBytes(32).toString('hex'), WB_CLAUDE_CHANNEL_PORT: String(await freePort()), WAKEBRIDGE_DAEMON_ORIGIN: origin, WAKEBRIDGE_ATTENTION_CHANNEL: 'default', WB_CLAUDE_SESSION_STATE_DIR: stateDir };
      const channel = child([join(root, 'src/channel.mjs')], env); children.push(channel);
      const initialized = await channel.initialize();
      assert.deepEqual(initialized.capabilities.experimental['claude/channel'], {});
      assert.match(initialized.instructions, /attention_ack/);
      await hook(env, { hook_event_name: 'SessionStart', session_id: label, source: 'startup' });
      return { channel, env, session: label };
    }
    const first = await openChannel('exact-session-one');
    const schedule = id => owner.tool('attention_schedule', { resource: `probe://claude/${id}`, eligible_after: new Date().toISOString(), attention_channel: 'default', defer_while_presence: false, idempotency_key: id });
    const scheduled = await schedule('explicit-ack');
    const notification = await until(() => first.channel.messages.find(value => value.method === 'notifications/claude/channel'), 'live notification');
    assert.ok(Object.values(notification.params.meta).every(value => typeof value === 'string'), 'Claude channel metadata values must be strings');
    assert.ok(!notification.params.content.includes('<channel'), 'Claude supplies the channel wrapper');
    const batchId = notification.params.meta.wake_batch_id;
    const dispatched = await until(async () => (await get('/v1/batches')).batches.find(batch => batch.id === batchId && batch.state === 'dispatched'), 'durable transport acceptance');
    assert.equal(dispatched.attempt, 1);
    assert.deepEqual((await get(`/v1/receipts/${batchId}`)).receipts.map(receipt => receipt.stage), ['transport_accepted']);
    await owner.tool('attention_ack', { wake_batch_id: batchId });
    assert.equal((await owner.tool('attention_status', { batch_id: batchId })).batch.state, 'seen');
    await owner.tool('attention_consume', { claim_id: scheduled.claim.id, result: { fixture_handled: true } });
    assert.ok((await get(`/v1/receipts/${batchId}`)).receipts.some(receipt => receipt.stage === 'agent_seen'));

    await schedule('old-unacknowledged');
    const oldMessage = await until(() => first.channel.messages.filter(value => value.method === 'notifications/claude/channel')[1], 'old session unacknowledged notification');
    await until(async () => (await get('/v1/batches')).batches.find(batch => batch.id === oldMessage.params.meta.wake_batch_id && batch.state === 'dispatched'), 'old unacknowledged batch');

    const bindingOne = (await get('/v1/bindings')).bindings[0];
    const second = await openChannel('exact-session-two');
    const bindingTwo = (await get('/v1/bindings')).bindings[0];
    assert.ok(bindingTwo.generation > bindingOne.generation);
    const staleAck = await owner.rpc('tools/call', { name: 'attention_ack', arguments: { wake_batch_id: oldMessage.params.meta.wake_batch_id } });
    assert.equal(staleAck.isError, true);
    assert.match(staleAck.content[0].text, /stale_generation/);
    await hook(first.env, { hook_event_name: 'SessionEnd', session_id: first.session });
    assert.equal((await get('/v1/bindings')).bindings[0].generation, bindingTwo.generation);
    const countBefore = first.channel.messages.filter(value => value.method === 'notifications/claude/channel').length;
    await schedule('after-takeover');
    const secondMessage = await until(() => second.channel.messages.find(value => value.method === 'notifications/claude/channel'), 'new exact session');
    assert.equal(first.channel.messages.filter(value => value.method === 'notifications/claude/channel').length, countBefore);
    await until(async () => (await get('/v1/batches')).batches.find(batch => batch.id === secondMessage.params.meta.wake_batch_id && batch.state === 'dispatched'), 'second accepted batch');
    await owner.tool('attention_ack', { wake_batch_id: secondMessage.params.meta.wake_batch_id });
    await hook(second.env, { hook_event_name: 'SessionEnd', session_id: second.session });
    const closedEndpoint = (await get('/v1/endpoints')).endpoints.find(endpoint => endpoint.id === bindingTwo.endpoint_id);
    assert.ok(new Date(closedEndpoint.lease_expires_at).getTime() <= Date.now(), 'live SessionEnd must close the lease before the pipe exits');
    await second.channel.close();
    await hook(second.env, { hook_event_name: 'SessionEnd', session_id: second.session });
    const third = await openChannel('channel-stopped-before-session-end');
    const bindingThree = (await get('/v1/bindings')).bindings[0];
    await new Promise(resolve => { third.channel.process.once('exit', resolve); third.channel.process.kill('SIGKILL'); });
    await hook(third.env, { hook_event_name: 'SessionEnd', session_id: third.session });
    const fallbackClosed = (await get('/v1/endpoints')).endpoints.find(endpoint => endpoint.id === bindingThree.endpoint_id);
    assert.ok(new Date(fallbackClosed.lease_expires_at).getTime() <= Date.now(), 'SessionEnd must close the saved exact lease after channel exit');
    await schedule('closed-session');
    await until(async () => (await get('/v1/batches')).batches.find(batch => batch.state === 'waiting_for_endpoint'), 'no cold fallback');
    for (const item of [first.channel, second.channel]) {
      assert.ok(!item.output().includes(hostToken));
      assert.ok(!item.errors().includes(hostToken));
      assert.ok(!item.output().includes(credential));
    }
  } finally {
    for (const process of children.reverse()) await process.close();
    rmSync(temp, { recursive: true, force: true });
  }
});
