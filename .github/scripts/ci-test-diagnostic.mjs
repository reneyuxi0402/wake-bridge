import { execFile, spawn } from 'node:child_process';
import { appendFileSync, closeSync, existsSync, mkdirSync, openSync, readFileSync, readdirSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { arch, homedir, platform, release } from 'node:os';
import { basename, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const reporter = fileURLToPath(new URL('./ci-test-reporter.mjs', import.meta.url));
const delay = (ms) => new Promise((done) => setTimeout(done, ms));

// Never include execFile's Error.message: it can contain command arguments/output.
function command(file, args, cwd, timeout = 3_000) {
  return new Promise((done, reject) => {
    execFile(file, args, { cwd, encoding: 'utf8', timeout, maxBuffer: 4 * 1024 * 1024 }, (error, stdout) => {
      if (error) reject(new Error(`${basename(file)} failed: code=${error.code ?? 'unknown'} signal=${error.signal ?? 'none'}`));
      else done(stdout.trim());
    });
  });
}

export function sanitize(text, cwd) {
  return String(text).split(cwd).join('<checkout>').split(homedir()).join('<home>')
    .replace(/\/(?:Users|home)\/[^\s/]+/gu, '<home>')
    .replace(/\/(?:private\/)?var\/folders\/[^\s)]+/gu, '<temp>');
}

// ps comm is not argv. Keep only its basename; lstart supplies PID-reuse identity.
export function parseProcesses(text) {
  return text.split('\n').filter((line) => line.trim()).map((line) => {
    const fields = line.trim().split(/\s+/u);
    if (fields.length < 11 || !fields.slice(0, 3).every((field) => /^\d+$/u.test(field))) {
      throw new Error('unrecognized ps row; ownership is unknown');
    }
    return { pid: Number(fields[0]), ppid: Number(fields[1]), pgid: Number(fields[2]),
      started: fields.slice(3, 8).join(' '), state: fields[8], cpu: fields[9],
      executable: basename(fields.slice(10).join(' ')) };
  });
}

export function stackOnly(text, cwd) {
  // sample headers may include argv, user IDs and paths. Retain only call stacks.
  const start = text.indexOf('Call graph:');
  if (start < 0) throw new Error('sample returned no Call graph; no raw output retained');
  return sanitize(text.slice(start).split(/^Binary Images:/mu)[0], cwd);
}

export function selectNode(rows) {
  const isNode = (row) => /^node(?:\d+)?$/u.test(row.executable);
  const byPid = new Map(rows.map((row) => [row.pid, row]));
  let parent = byPid.get(rows.find((row) => row.executable === 'sqlite3')?.ppid);
  for (let steps = 0; parent && steps < rows.length; steps += 1) {
    if (isNode(parent)) return { row: parent, basis: 'nearest owned Node ancestor of sqlite3' };
    parent = byPid.get(parent.ppid);
  }
  const depth = (row) => {
    let count = 0;
    while (byPid.has(row.ppid) && count < rows.length) { count += 1; row = byPid.get(row.ppid); }
    return count;
  };
  return { row: rows.filter(isNode).sort((a, b) => depth(b) - depth(a))[0],
    basis: 'deepest owned Node; worker identity not established' };
}

export function installReporter(cwd) {
  if (readdirSync(cwd).some((name) => /^(?:vitest|vite)\.(?:config|workspace)\./u.test(name))) {
    throw new Error('existing Vitest/Vite config; refusing to override it');
  }
  const path = join(cwd, 'vitest.config.mjs');
  const content = `// Temporary CI observation only; test CLI settings remain authoritative.\nexport default { test: { reporters: ['default', ${JSON.stringify(reporter)}] } };\n`;
  const fd = openSync(path, 'wx');
  try { writeFileSync(fd, content); } finally { closeSync(fd); }
  const identity = statSync(path);
  return () => {
    if (!existsSync(path)) throw new Error('temporary config disappeared before cleanup');
    if (statSync(path).ino !== identity.ino || readFileSync(path, 'utf8') !== content) {
      throw new Error('temporary config changed; refusing to remove it');
    }
    unlinkSync(path);
  };
}

// Options exist only for local node:test stubs. The CLI accepts no timing/command
// overrides: CI always executes the unchanged npm test with these fixed limits.
export async function runDiagnostic({ cwd = process.cwd(), evidenceDir = join(cwd, 'ci-test-evidence'),
  executable = 'npm', args = ['test'], idleMs = 120_000, hardMs = 12 * 60_000,
  pollMs = 1_000, graceMs = 2_000, sampleBudgetMs = 10_000,
  sampleProcess = async (row) => command('/usr/bin/sample', [String(row.pid), '1', '10', '-file', '/dev/stdout'], cwd),
  metadata = true, logOutput = true,
} = {}) {
  if (existsSync(evidenceDir) && readdirSync(evidenceDir).length) {
    throw new Error('evidence directory is not empty; refusing to mix runs');
  }
  mkdirSync(evidenceDir, { recursive: true });
  const record = (event, fields = {}) => {
    const line = JSON.stringify({ time: new Date().toISOString(), event, ...fields });
    appendFileSync(join(evidenceDir, 'diagnostic.jsonl'), `${line}\n`);
    console.log(`[ci-diagnostic] ${line}`);
  };
  let removeConfig;
  let child;
  let interval;
  let idleTimer;
  let preDeadline;
  let hardDeadline;
  let snapshotPromise;
  let capturePromise;
  let captured = false;
  let expired = false;
  let stopping;
  let observationFailed = false;
  let groupGone = false;
  const seen = new Map();
  const timers = () => [interval, idleTimer, preDeadline, hardDeadline].forEach(clearTimeout);
  const snapshot = () => {
    // Serialize only ps reads, not the independent deadline or sampling commands.
    if (snapshotPromise) return snapshotPromise;
    snapshotPromise = command('/bin/ps', ['-axo', 'pid=,ppid=,pgid=,lstart=,stat=,time=,comm='], cwd)
      .then((output) => {
        const rows = parseProcesses(output);
        const owned = new Map(rows.filter((row) => !groupGone && row.pgid === child.pid).map((row) => [row.pid, row]));
        for (const row of rows) {
          if (seen.get(row.pid) === row.started) owned.set(row.pid, row);
        }
        let previous;
        do {
          previous = owned.size;
          for (const row of rows) if (owned.has(row.ppid)) owned.set(row.pid, row);
        } while (owned.size !== previous);
        for (const row of owned.values()) seen.set(row.pid, row.started);
        if (!rows.some((row) => row.pgid === child.pid)) groupGone = true;
        return [...owned.values()];
      }).finally(() => { snapshotPromise = undefined; });
    return snapshotPromise;
  };
  const freshSnapshot = async () => {
    // Do not classify an exited child using a ps read begun before its exit.
    await snapshotPromise?.catch(() => {});
    return snapshot();
  };
  const capture = (reason) => {
    if (captured) return capturePromise;
    captured = true;
    record('capture-start', { reason });
    capturePromise = (async () => {
      try {
        const rows = await snapshot();
        record('process-snapshot', { processes: rows });
        const node = selectNode(rows);
        const targets = [
          ['node', node.row, node.basis],
          ['sqlite3', rows.find((row) => row.executable === 'sqlite3'), 'owned sqlite3'],
        ];
        await Promise.all(targets.map(async ([kind, row, basis]) => {
          if (!row) { record('sample-unavailable', { kind, reason: 'no owned matching process' }); return; }
          try {
            const current = (await snapshot()).find((candidate) => candidate.pid === row.pid && candidate.started === row.started);
            if (!current) throw new Error('target exited or changed identity before sample');
            record('sample-target', { kind, pid: row.pid, basis });
            const stack = stackOnly(await sampleProcess(current), cwd);
            writeFileSync(join(evidenceDir, `sample-${kind}.txt`), stack);
            record('sample-complete', { kind, pid: row.pid });
          } catch (error) {
            observationFailed = true;
            record('sample-error', { kind, error: sanitize(error.message, cwd) });
          }
        }));
      } catch (error) {
        observationFailed = true;
        record('capture-error', { error: sanitize(error.message, cwd) });
      }
    })();
    return capturePromise;
  };
  const stop = () => {
    if (stopping) return stopping;
    stopping = (async () => {
      for (const signal of ['SIGTERM', 'SIGKILL']) {
        // Only this detached group is signalled, never a name/all-user sweep.
        if (!groupGone) {
          try { process.kill(-child.pid, signal); record('group-signal', { pgid: child.pid, signal }); }
          catch (error) { if (error.code !== 'ESRCH') record('signal-error', { code: error.code }); }
        }
        await delay(graceMs);
        try {
          const rows = await snapshot();
          record('cleanup-snapshot', { processes: rows });
          if (!rows.length) return;
        } catch (error) {
          observationFailed = true;
          record('cleanup-error', { error: sanitize(error.message, cwd) });
        }
      }
    })();
    return stopping;
  };
  const signalHandler = () => { expired = true; void stop(); };
  try {
    removeConfig = installReporter(cwd);
    record('temporary-reporter-config', { observation_can_perturb_timing: true });
    if (metadata) {
      const details = { sha: await command('git', ['rev-parse', 'HEAD'], cwd),
        os: `${platform()} ${release()}`, arch: arch(), node: process.version,
        npm: await command('npm', ['--version'], cwd), sqlite3: await command('sqlite3', ['--version'], cwd) };
      writeFileSync(join(evidenceDir, 'metadata.json'), `${JSON.stringify(details, null, 2)}\n`);
    }
    child = spawn(executable, args, { cwd, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    const exited = new Promise((done, reject) => {
      child.once('error', () => reject(new Error('test command failed to spawn')));
      child.once('exit', (code, signal) => done({ code, signal }));
    });
    const resetIdle = () => {
      clearTimeout(idleTimer);
      if (!captured) idleTimer = setTimeout(() => { void capture('no-output'); }, idleMs);
    };
    for (const [stream, output] of [[child.stdout, process.stdout], [child.stderr, process.stderr]]) {
      stream.on('data', (chunk) => { resetIdle(); if (logOutput) output.write(chunk); });
    }
    resetIdle();
    process.once('SIGTERM', signalHandler);
    process.once('SIGINT', signalHandler);
    interval = setInterval(() => {
      void snapshot().catch((error) => {
        observationFailed = true;
        record('tracking-error', { error: sanitize(error.message, cwd) });
      });
    }, pollMs);
    // Attempt a bounded sample before the independent hard deadline even when
    // output continues. The deadline does not await ps or sample completion.
    preDeadline = setTimeout(() => { void capture('deadline-approaching'); }, Math.max(0, hardMs - sampleBudgetMs));
    hardDeadline = setTimeout(() => {
      expired = true;
      record('hard-deadline');
      void stop();
    }, hardMs);
    const outcome = await exited;
    timers();
    let remaining;
    try { remaining = await freshSnapshot(); }
    catch (error) { observationFailed = true; record('final-snapshot-error', { error: sanitize(error.message, cwd) }); }
    const leaked = remaining?.length > 0;
    if (leaked) record('owned-processes-after-exit', { processes: remaining });
    if (leaked || expired || observationFailed) await stop();
    if (capturePromise) await capturePromise;
    try { remaining = await freshSnapshot(); }
    catch { remaining = undefined; observationFailed = true; }
    const result = { ...outcome, timed_out: expired, leaked_after_exit: leaked,
      observation_failed: observationFailed, owned_processes_remaining: remaining?.length ?? null,
      exit_code: expired || leaked || observationFailed || remaining?.length ? 1 : (outcome.code ?? 1) };
    writeFileSync(join(evidenceDir, 'result.json'), `${JSON.stringify(result, null, 2)}\n`);
    record('result', result);
    return result;
  } finally {
    timers();
    process.removeListener('SIGTERM', signalHandler);
    process.removeListener('SIGINT', signalHandler);
    if (child && child.exitCode === null && child.signalCode === null) await stop();
    child?.stdout.destroy();
    child?.stderr.destroy();
    removeConfig?.();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (process.argv.length !== 2) throw new Error('CI diagnostic accepts no arguments');
  runDiagnostic().then((result) => { process.exitCode = result.exit_code; }, (error) => {
    console.error(`[ci-diagnostic] failed: ${sanitize(error.message, process.cwd())}`);
    process.exitCode = 1;
  });
}
