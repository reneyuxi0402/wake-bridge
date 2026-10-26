// Explicit node --test entrypoint; intentionally not *.test.* (Vitest's glob).
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { installReporter, parseProcesses, runDiagnostic, selectNode, stackOnly } from './ci-test-diagnostic.mjs';
import Reporter from './ci-test-reporter.mjs';

function fixture(t) {
  const cwd = mkdtempSync(join(tmpdir(), 'wakebridge-ci-stub-'));
  t.after(() => rmSync(cwd, { recursive: true, force: true }));
  return cwd;
}

function stub(cwd, source, options = {}) {
  return runDiagnostic({ cwd, executable: process.execPath, args: ['-e', source],
    metadata: false, logOutput: false, idleMs: 1_000, hardMs: 3_000,
    pollMs: 25, graceMs: 100, sampleBudgetMs: 100, ...options });
}

function events(cwd) {
  return readFileSync(join(cwd, 'ci-test-evidence', 'diagnostic.jsonl'), 'utf8')
    .trim().split('\n').map((line) => JSON.parse(line));
}

test('successful stub preserves exit status and removes only its temporary config', async (t) => {
  const cwd = fixture(t);
  const result = await stub(cwd, 'console.log("stub completed")');
  assert.equal(result.exit_code, 0);
  assert.equal(result.owned_processes_remaining, 0);
  assert.equal(result.observation_failed, false);
  assert.equal(existsSync(join(cwd, 'vitest.config.mjs')), false);
  assert.equal(events(cwd).some((event) => event.event === 'capture-start'), false);
});

test('nonzero command status is not converted to success', async (t) => {
  const cwd = fixture(t);
  const result = await stub(cwd, 'process.exit(7)');
  assert.equal(result.code, 7);
  assert.equal(result.exit_code, 7);
  assert.equal(existsSync(join(cwd, 'vitest.config.mjs')), false);
});

test('no output samples once; independent deadline stops its own group', async (t) => {
  const cwd = fixture(t);
  let samples = 0;
  const started = Date.now();
  const result = await stub(cwd, "process.on('SIGTERM', () => {}); setInterval(() => {}, 1000)", {
    idleMs: 150, hardMs: 700,
    sampleProcess: async () => {
      samples += 1;
      return 'Process: private-command --private-argument\nUser ID: private-user\nCall graph:\n  uv_spawn\nBinary Images:\nprivate-image';
    },
  });
  assert.equal(result.timed_out, true);
  assert.equal(result.exit_code, 1);
  assert.equal(result.owned_processes_remaining, 0);
  assert.ok(Date.now() - started < 5_000);
  assert.equal(samples, 1);
  assert.ok(events(cwd).some((event) => event.event === 'group-signal' && event.signal === 'SIGKILL'));
  assert.equal(events(cwd).filter((event) => event.event === 'capture-start').length, 1);
  const stack = readFileSync(join(cwd, 'ci-test-evidence', 'sample-node.txt'), 'utf8');
  assert.match(stack, /uv_spawn/u);
  assert.doesNotMatch(stack, /private-/u);
});

test('continued output cannot postpone the hard deadline or its preceding capture', async (t) => {
  const cwd = fixture(t);
  const result = await stub(cwd, 'setInterval(() => console.log("alive"), 30)', {
    idleMs: 200, hardMs: 700, sampleProcess: async () => 'Call graph:\n uv_spawn\n',
  });
  assert.equal(result.timed_out, true);
  assert.equal(result.owned_processes_remaining, 0);
  assert.equal(events(cwd).find((event) => event.event === 'capture-start').reason, 'deadline-approaching');
});

test('sampling failure cannot be reported as a clean observation even if the test succeeds', async (t) => {
  const cwd = fixture(t);
  const result = await stub(cwd, 'setTimeout(() => process.exit(0), 400)', {
    idleMs: 100,
    sampleProcess: async () => { throw new Error('sample not permitted'); },
  });
  assert.equal(result.code, 0);
  assert.equal(result.observation_failed, true);
  assert.equal(result.exit_code, 1);
  assert.ok(events(cwd).some((event) => event.event === 'sample-error' && event.error === 'sample not permitted'));
});

test('macOS sample smoke retains a real owned stub call graph only', { skip: process.platform !== 'darwin' }, async (t) => {
  const cwd = fixture(t);
  const result = await stub(cwd, 'setTimeout(() => process.exit(0), 1800)', {
    idleMs: 150, hardMs: 6_000,
  });
  assert.equal(result.exit_code, 0);
  assert.ok(events(cwd).some((event) => event.event === 'sample-complete' && event.kind === 'node'));
  const stack = readFileSync(join(cwd, 'ci-test-evidence', 'sample-node.txt'), 'utf8');
  assert.match(stack, /^Call graph:/u);
  assert.doesNotMatch(stack, /^(?:Process|Path|User ID|Command|Binary Images):/mu);
  assert.doesNotMatch(stack, /\/(?:Users|home)\//u);
});

test('parent exit does not hide a reparented child in the original group', async (t) => {
  const cwd = fixture(t);
  const result = await stub(cwd, `
    const { spawn } = require('node:child_process');
    spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' }).unref();
    setTimeout(() => process.exit(0), 200);
  `);
  assert.equal(result.code, 0);
  assert.equal(result.leaked_after_exit, true);
  assert.equal(result.exit_code, 1);
  assert.equal(result.owned_processes_remaining, 0);
  const log = events(cwd);
  assert.ok(log.some((event) => event.event === 'owned-processes-after-exit' && event.processes.length > 0));
  assert.ok(log.some((event) => event.event === 'owned-processes-after-exit'
    && event.processes.some((row) => row.executable === 'node' && row.ppid !== row.pgid)));
  assert.ok(log.some((event) => event.event === 'group-signal' && event.signal === 'SIGTERM'));
});

test('existing configuration is never overwritten; changed temporary config is not deleted', (t) => {
  const cwd = fixture(t);
  const config = join(cwd, 'vitest.config.mjs');
  writeFileSync(config, '// existing');
  assert.throws(() => installReporter(cwd), /refusing to override/u);
  assert.equal(readFileSync(config, 'utf8'), '// existing');
  const fresh = fixture(t);
  const cleanup = installReporter(fresh);
  writeFileSync(join(fresh, 'vitest.config.mjs'), '// changed');
  assert.throws(cleanup, /refusing to remove/u);
  assert.equal(readFileSync(join(fresh, 'vitest.config.mjs'), 'utf8'), '// changed');
});

test('old evidence is rejected rather than combined with a new run', async (t) => {
  const cwd = fixture(t);
  await stub(cwd, 'process.exit(0)');
  await assert.rejects(stub(cwd, 'process.exit(0)'), /refusing to mix runs/u);
  assert.equal(existsSync(join(cwd, 'vitest.config.mjs')), false);
});

test('ps schema retains identity, not executable paths; stack drops headers and images', () => {
  const rows = parseProcesses('42 41 40 Sun Oct 25 12:00:00 2026 S 0:00.01 /example/node');
  assert.equal(rows[0].executable, 'node');
  assert.equal(rows[0].started, 'Sun Oct 25 12:00:00 2026');
  assert.throws(() => parseProcesses('unrecognized'), /ownership is unknown/u);
  assert.equal(stackOnly('Command: secret\nCall graph:\n /home/private/node\nBinary Images:\nsecret', '/example'),
    'Call graph:\n <home>/node\n');
  assert.throws(() => stackOnly('no stack here', '/example'), /no raw output retained/u);
  const tree = [{ pid: 1, ppid: 0, executable: 'node' }, { pid: 2, ppid: 1, executable: 'node' },
    { pid: 3, ppid: 2, executable: 'sqlite3' }, { pid: 4, ppid: 1, executable: 'node' }];
  assert.equal(selectNode(tree).row.pid, 2);
  assert.match(selectNode(tree).basis, /ancestor/u);
  assert.equal(selectNode(tree.slice(0, 2)).row.pid, 2);
  assert.match(selectNode(tree.slice(0, 2)).basis, /not established/u);
});

test('reporter emits Vitest 3.2 ready/result markers without result payloads', () => {
  const original = console.log;
  const lines = [];
  console.log = (line) => lines.push(line);
  try {
    const reporter = new Reporter();
    const module = { moduleId: join(process.cwd(), 'test', 'stub.test.ts'), state: () => 'passed' };
    const testCase = { module, fullName: 'suite > case', result: () => ({ state: 'passed', errors: ['not serialized'] }) };
    reporter.onTestModuleStart(module);
    reporter.onTestCaseReady(testCase);
    reporter.onTestCaseResult(testCase);
    reporter.onTestModuleEnd(module);
  } finally { console.log = original; }
  assert.equal(lines.length, 4);
  assert.match(lines[1], /test-start/u);
  assert.match(lines[2], /test-end/u);
  assert.doesNotMatch(lines.join('\n'), /not serialized/u);
});
