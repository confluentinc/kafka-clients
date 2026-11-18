#!/usr/bin/env node
// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

// Hours of steady state: runs soakclient.js for a given time (6 to 24 hours
// for a real run, by hand or on a schedule) and judges its resources, as the
// steady state of a long-lived producer must hold them (root CLAUDE.md,
// JavaScript Binding Conventions, Tests, Q68: soak where JavaScript has a hot
// path). Every --sample-seconds the soak client writes a metrics window
// (its RSS, JavaScript heap and external memory, the memory of Buffers), and
// this program reads the client process's open file descriptors and threads
// (/proc on Linux, lsof and ps on macOS). Once the client has exited it
// passes when, over the samples after the warm-up:
//   - the least-squares slope of the RSS, the heap and the external memory
//     stays under --max-slope-mib-per-hour each;
//   - the open file descriptors and the threads stay flat: their spread is at
//     most --fd-spread and --thread-spread;
//   - every callback ran once: the client's SUMMARY line reports no missing
//     and no duplicate delivery report, every record produced was delivered
//     or failed, and its verdict is PASS.
// It prints a JSON report, also written to <dir>/report.json beside the
// client's metrics (<dir>/metrics.jsonl) and the descriptor and thread samples
// (<dir>/handles.jsonl), <dir> being --out or a new temporary directory, and exits 0 on a pass, 1 on a failure and 2 when it
// cannot run. `make -C javascript soak` runs it, by hand or on a schedule
// (.semaphore/soak-javascript.yml). Usage (the options after `--` go to
// soakclient.js):
//
//   node verifiable_clients/soak/soak_check.js --hours 6 --out soak-run -- -i soak1 -b localhost:9092 -t soak-topic -r 2000

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');

/**
 * The options of this program.
 *
 * @typedef {{ hours: number, warmupMinutes: number, sampleSeconds: number, maxSlopeMibPerHour: number, fdSpread: number, threadSpread: number, out: string | null, client: string[] }} Options
 */

/**
 * Reads the command line.
 *
 * @param {string[]} argv the arguments
 * @returns {Options} the options
 */
function parseArgs(argv) {
  /** @type {Options} */
  const options = {
    hours: 6,
    warmupMinutes: 30,
    sampleSeconds: 60,
    maxSlopeMibPerHour: 2,
    fdSpread: 8,
    threadSpread: 0,
    out: null,
    client: [],
  };
  const names = {
    '--hours': 'hours',
    '--warmup-minutes': 'warmupMinutes',
    '--sample-seconds': 'sampleSeconds',
    '--max-slope-mib-per-hour': 'maxSlopeMibPerHour',
    '--fd-spread': 'fdSpread',
    '--thread-spread': 'threadSpread',
  };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--') {
      options.client = argv.slice(i + 1);
      break;
    }
    if (argv[i] === '--out') {
      if (argv[i + 1] === undefined || argv[i + 1] === '') throw new Error('soak_check: --out needs a directory');
      options.out = argv[i + 1];
      i++;
      continue;
    }
    const key = /** @type {Record<string, keyof Options>} */ (names)[argv[i]];
    const value = Number(argv[i + 1]);
    if (key === undefined || argv[i + 1] === undefined || !Number.isFinite(value) || value < 0) {
      throw new Error(`soak_check: bad argument ${argv[i]} ${argv[i + 1] ?? ''}`);
    }
    /** @type {Record<string, unknown>} */ (options)[key] = value;
    i++;
  }
  if (options.hours <= 0 || options.sampleSeconds <= 0)
    throw new Error('soak_check: --hours and --sample-seconds must be greater than zero');
  if (options.warmupMinutes * 60 >= options.hours * 3600) throw new Error('soak_check: the warm-up must end before the run does');
  return options;
}

/**
 * The least-squares slope of `ys` over `xs`.
 *
 * @param {number[]} xs the abscissae
 * @param {number[]} ys the ordinates
 * @returns {number} the slope, 0 for fewer than two points
 */
function slope(xs, ys) {
  const n = xs.length;
  if (n < 2) return 0;
  const mx = xs.reduce((a, b) => a + b, 0) / n;
  const my = ys.reduce((a, b) => a + b, 0) / n;
  let num = 0;
  let den = 0;
  for (let i = 0; i < n; i++) {
    num += (xs[i] - mx) * (ys[i] - my);
    den += (xs[i] - mx) ** 2;
  }
  return den === 0 ? 0 : num / den;
}

/**
 * A sample: when, in milliseconds since the epoch, and the values read.
 *
 * @typedef {{ at: number, values: Record<string, number> }} Sample
 */

/**
 * The memory samples of a soak client's metrics file: one per window, the
 * window's average of each memory gauge, in MiB.
 *
 * @param {string} text the JSONL file's text
 * @returns {Sample[]} the samples
 */
function memorySamples(text) {
  /** @type {Sample[]} */
  const samples = [];
  for (const line of text.split('\n')) {
    if (line.trim() === '') continue;
    const record = JSON.parse(line);
    /** @type {Record<string, number>} */
    const values = {};
    for (const gauge of ['memory.rss', 'memory.heap_used', 'memory.external']) {
      const bucket = record.gauges?.[gauge];
      if (bucket !== undefined && Number(bucket.count) > 0) values[gauge] = Number(bucket.average);
    }
    if (Object.keys(values).length > 0) samples.push({ at: Number(record.window_end_ms), values });
  }
  return samples;
}

/**
 * The SUMMARY line soakclient.js prints, as numbers and its verdict.
 *
 * @param {string} stdout the client's output
 * @returns {Record<string, string> | null} its fields, or null when there is none
 */
function summaryOf(stdout) {
  const line = stdout.split('\n').find((l) => l.startsWith('SUMMARY '));
  if (line === undefined) return null;
  /** @type {Record<string, string>} */
  const fields = {};
  for (const [, key, value] of line.matchAll(/(\w+)=(\S+)/g)) fields[key] = value;
  // The verdict may hold spaces (`FAIL (...)`).
  fields.verdict = line.slice(line.indexOf('verdict=') + 'verdict='.length);
  return fields;
}

/**
 * The verdict of a run.
 *
 * @param {{ memory: Sample[], handles: Sample[], summary: Record<string, string> | null, exitCode: number | null, start: number }} run what was measured
 * @param {Options} options the bounds
 * @returns {{ pass: boolean, failures: string[], slopes: Record<string, number>, spreads: Record<string, number> }} the verdict
 */
function judge(run, options) {
  /** @type {string[]} */
  const failures = [];
  const after = run.start + options.warmupMinutes * 60_000;
  /** @type {Record<string, number>} */
  const slopes = {};
  const memory = run.memory.filter((s) => s.at >= after);
  if (memory.length < 3) failures.push(`only ${memory.length} memory samples after the warm-up`);
  for (const gauge of ['memory.rss', 'memory.heap_used', 'memory.external']) {
    const points = memory.filter((s) => gauge in s.values);
    const perHour = slope(
      points.map((s) => s.at / 3_600_000),
      points.map((s) => s.values[gauge]),
    );
    slopes[gauge] = perHour;
    if (perHour > options.maxSlopeMibPerHour) {
      failures.push(`${gauge} grows ${perHour.toFixed(3)} MiB/h, more than ${options.maxSlopeMibPerHour}`);
    }
  }
  /** @type {Record<string, number>} */
  const spreads = {};
  const handles = run.handles.filter((s) => s.at >= after);
  for (const [name, bound] of /** @type {[string, number][]} */ ([
    ['fds', options.fdSpread],
    ['threads', options.threadSpread],
  ])) {
    const values = handles.map((s) => s.values[name]).filter((v) => Number.isFinite(v));
    if (values.length === 0) {
      failures.push(`no ${name} samples after the warm-up`);
      continue;
    }
    spreads[name] = Math.max(...values) - Math.min(...values);
    if (spreads[name] > bound) failures.push(`${name} went from ${Math.min(...values)} to ${Math.max(...values)}, a spread over ${bound}`);
  }
  const s = run.summary;
  if (s === null) failures.push('the client printed no SUMMARY line');
  else {
    if (s.missing_reports !== '0') failures.push(`${s.missing_reports} records had no delivery report`);
    if (s.duplicate_reports !== '0') failures.push(`${s.duplicate_reports} duplicate delivery reports`);
    if (Number(s.produced) !== Number(s.delivered) + Number(s.failed)) {
      failures.push(`produced ${s.produced}, but delivered ${s.delivered} and failed ${s.failed}`);
    }
    if (s.verdict !== 'PASS') failures.push(`the client's verdict: ${s.verdict}`);
  }
  if (run.exitCode !== 0) failures.push(`the client exited with ${run.exitCode}`);
  return { pass: failures.length === 0, failures, slopes, spreads };
}

/**
 * The open file descriptors in `lsof -p` output: its lines whose FD column is a
 * descriptor's number (`12u`), not the working directory, the program text or
 * a mapped library (`cwd`, `txt`).
 *
 * @param {string} text the output, its header line first
 * @returns {number} the descriptors
 */
function lsofDescriptors(text) {
  return text
    .split('\n')
    .slice(1)
    .filter((line) => /^\d+/.test(line.trim().split(/\s+/)[3] ?? '')).length;
}

/**
 * The open file descriptors and threads of a process, or NaN where they cannot
 * be read: a process that has exited has none to read, and a count of zero
 * (`ps` naming no thread of it) is no reading of a running one.
 *
 * @param {number} pid the process
 * @returns {Record<string, number>} its `fds` and `threads`
 */
function handlesOf(pid) {
  const read = (/** @type {number} */ n) => (n > 0 ? n : NaN);
  if (process.platform === 'linux') {
    const count = (/** @type {string} */ dir) => {
      try {
        return fs.readdirSync(dir).length;
      } catch {
        return NaN;
      }
    };
    return { fds: read(count(`/proc/${pid}/fd`)), threads: read(count(`/proc/${pid}/task`)) };
  }
  const lsof = spawnSync('lsof', ['-n', '-P', '-p', String(pid)], { encoding: 'utf8' });
  const ps = spawnSync('ps', ['-M', '-p', String(pid)], { encoding: 'utf8' });
  const lines = (/** @type {string} */ text) => text.split('\n').filter((l) => l.trim() !== '').length - 1;
  return {
    fds: lsof.status === 0 ? read(lsofDescriptors(lsof.stdout)) : NaN,
    threads: ps.status === 0 ? read(lines(ps.stdout)) : NaN,
  };
}

/**
 * Runs the soak client and judges it.
 *
 * @param {Options} options the options
 * @returns {Promise<number>} the exit code
 */
async function main(options) {
  const dir = options.out ?? fs.mkdtempSync(path.join(os.tmpdir(), 'soak-check-'));
  fs.mkdirSync(dir, { recursive: true });
  const metricsFile = path.join(dir, 'metrics.jsonl');
  fs.rmSync(metricsFile, { force: true });
  const start = Date.now();
  const child = spawn(
    process.execPath,
    [
      path.join(__dirname, 'soakclient.js'),
      ...options.client,
      '--metrics-file',
      metricsFile,
      '--metrics-interval',
      String(options.sampleSeconds),
      '--runtime-seconds',
      String(options.hours * 3600),
    ],
    { stdio: ['ignore', 'pipe', 'inherit'] },
  );
  let stdout = '';
  child.stdout.on('data', (d) => {
    stdout += d;
    process.stdout.write(d);
  });
  /** @type {Sample[]} */
  const handles = [];
  const handlesFile = path.join(dir, 'handles.jsonl');
  fs.rmSync(handlesFile, { force: true });
  const sampler = setInterval(() => {
    // Not once the client has exited: its last moments are no steady state.
    if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
    const sample = { at: Date.now(), values: handlesOf(child.pid) };
    handles.push(sample);
    fs.appendFileSync(handlesFile, `${JSON.stringify(sample)}\n`);
  }, options.sampleSeconds * 1000);
  /** @type {number | null} */
  const exitCode = await new Promise((resolve) => child.on('close', (code) => resolve(code)));
  clearInterval(sampler);
  const memory = fs.existsSync(metricsFile) ? memorySamples(fs.readFileSync(metricsFile, 'utf8')) : [];
  const verdict = judge({ memory, handles, summary: summaryOf(stdout), exitCode, start }, options);
  const report = { ...verdict, units: 'slopes in MiB/h', samples: { memory: memory.length, handles: handles.length }, metricsFile };
  const text = JSON.stringify(report, null, 2);
  fs.writeFileSync(path.join(dir, 'report.json'), `${text}\n`);
  console.log(text);
  return verdict.pass ? 0 : 1;
}

module.exports = { handlesOf, judge, lsofDescriptors, memorySamples, parseArgs, slope, summaryOf };

if (require.main === module) {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exit(2);
  }
  main(options).then(
    (code) => {
      process.exitCode = code;
    },
    (error) => {
      console.error(error);
      process.exitCode = 2;
    },
  );
}
