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

// The steady-state check of a long soak (soak/soak_check.js): how it reads
// the soak client's metrics and SUMMARY line, and the verdict it gives, over
// made-up runs; a short run against a broker is in its usage.

const assert = require('node:assert/strict');

const { handlesOf, judge, lsofDescriptors, memorySamples, parseArgs, slope, summaryOf } = require('../soak/soak_check');

const HOUR = 3_600_000;
const START = 1_700_000_000_000;
const OPTIONS = parseArgs(['--hours', '6', '--warmup-minutes', '30', '--sample-seconds', '60']);
const PASSING_SUMMARY =
  'SUMMARY variant=producer testid=t topic=x produced=10 delivered=9 failed=1 missing_reports=0 duplicate_reports=0 offset_regressions=0 verdict=PASS';

/**
 * A metrics window as soakclient.js writes it.
 *
 * @param {number} at its end, in milliseconds
 * @param {number} rss the RSS, MiB
 * @param {number} heap the heap, MiB
 * @param {number} external the external memory, MiB
 * @returns {string} the JSONL line
 */
function window(at, rss, heap, external) {
  /** @param {number} v */
  const bucket = (v) => ({ average: String(v), max: String(v), total: String(v), count: '1' });
  return JSON.stringify({
    window_end_ms: String(at),
    gauges: { 'memory.rss': bucket(rss), 'memory.heap_used': bucket(heap), 'memory.external': bucket(external) },
  });
}

/**
 * A run of `hours`, one sample a minute.
 *
 * @param {(h: number) => number} rss the RSS at hour h
 * @param {(h: number) => number} fds the open fds at hour h
 * @returns {Parameters<typeof judge>[0]} the run
 */
function run(rss, fds = () => 40) {
  const lines = [];
  const handles = [];
  for (let m = 1; m <= 6 * 60; m++) {
    const h = m / 60;
    lines.push(window(START + h * HOUR, rss(h), 20, 5));
    handles.push({ at: START + h * HOUR, values: { fds: fds(h), threads: 12 } });
  }
  return { memory: memorySamples(lines.join('\n')), handles, summary: summaryOf(PASSING_SUMMARY), exitCode: 0, start: START };
}

test('the slope is the least-squares one', () => {
  assert.equal(slope([0, 1, 2, 3], [1, 3, 5, 7]), 2);
  assert.equal(slope([0], [5]), 0);
  assert.equal(slope([1, 1], [2, 3]), 0);
});

test('a flat run, with noise and a warm-up that grows, passes', () => {
  const verdict = judge(
    run((h) => (h < 0.5 ? 100 + 200 * h : 200 + Math.sin(h * 50))),
    OPTIONS,
  );
  assert.deepEqual(verdict.failures, []);
  assert.equal(verdict.pass, true);
  assert.ok(Math.abs(verdict.slopes['memory.rss']) < 0.5, String(verdict.slopes['memory.rss']));
  assert.deepEqual(verdict.spreads, { fds: 0, threads: 0 });
});

test('memory that keeps growing after the warm-up fails, naming the gauge and its slope', () => {
  const verdict = judge(
    run((h) => 100 + 3 * h),
    OPTIONS,
  );
  assert.equal(verdict.pass, false);
  assert.deepEqual(verdict.failures, ['memory.rss grows 3.000 MiB/h, more than 2']);
});

test('file descriptors that leak fail, and so does a thread more', () => {
  const r = run(
    () => 100,
    (h) => 40 + Math.floor(h * 4),
  );
  r.handles[r.handles.length - 1].values.threads = 13;
  const verdict = judge(r, OPTIONS);
  assert.deepEqual(verdict.failures, ['fds went from 42 to 64, a spread over 8', 'threads went from 12 to 13, a spread over 0']);
});

test('a callback that did not run once fails, read from the SUMMARY line', () => {
  const r = run(() => 100);
  r.summary = summaryOf(
    'SUMMARY variant=producer testid=t topic=x produced=10 delivered=8 failed=0 missing_reports=1 duplicate_reports=1 offset_regressions=0 verdict=FAIL (1 duplicate delivery report(s))',
  );
  r.exitCode = 1;
  assert.deepEqual(judge(r, OPTIONS).failures, [
    '1 records had no delivery report',
    '1 duplicate delivery reports',
    'produced 10, but delivered 8 and failed 0',
    "the client's verdict: FAIL (1 duplicate delivery report(s))",
    'the client exited with 1',
  ]);
});

test('a run with no SUMMARY line, or too few samples after the warm-up, fails', () => {
  const verdict = judge({ memory: [], handles: [], summary: summaryOf('no summary\n'), exitCode: 0, start: START }, OPTIONS);
  assert.deepEqual(verdict.failures, [
    'only 0 memory samples after the warm-up',
    'no fds samples after the warm-up',
    'no threads samples after the warm-up',
    'the client printed no SUMMARY line',
  ]);
});

test('the options: the client arguments follow --, --out names the directory, and a warm-up longer than the run is refused', () => {
  assert.deepEqual(parseArgs(['--hours', '1', '--', '-i', 'x', '-t', 'y']).client, ['-i', 'x', '-t', 'y']);
  assert.equal(parseArgs(['--hours', '1', '--out', 'soak-run', '--', '-i', 'x']).out, 'soak-run');
  assert.equal(parseArgs(['--hours', '1']).out, null);
  assert.throws(() => parseArgs(['--out']), /--out needs a directory/);
  assert.throws(() => parseArgs(['--hours', '0.5', '--warmup-minutes', '30']), /the warm-up must end before the run does/);
  assert.throws(() => parseArgs(['--hours', 'x']), /bad argument --hours x/);
});

test("lsof's output counts only numbered descriptors, not the working directory, the program or its libraries", () => {
  const out = [
    'COMMAND   PID     USER   FD   TYPE             DEVICE SIZE/OFF                NODE NAME',
    'node    59450 pratyush  cwd    DIR               1,15     1152            42023242 /work',
    'node    59450 pratyush  txt    REG               1,15  1361216 1152921500312571448 /usr/local/bin/node',
    'node    59450 pratyush  txt    REG               1,15  2374000 1152921500312573276 /usr/lib/dyld',
    'node    59450 pratyush    0r   CHR                3,2      0t0                 338 /dev/null',
    'node    59450 pratyush    1w   REG               1,15        0            62582216 /tmp/out',
    'node    59450 pratyush   12u  IPv4 0x1234567890abcdef      0t0                 TCP 127.0.0.1:50000->127.0.0.1:9092 (ESTABLISHED)',
    '',
  ].join('\n');
  assert.equal(lsofDescriptors(out), 3);
});

test("this process's own file descriptors and threads can be read", () => {
  const { fds, threads } = handlesOf(process.pid);
  assert.ok(fds > 0, String(fds));
  assert.ok(threads > 0, String(threads));
});

test('a process that has exited reads as no sample, not as zero threads and descriptors', () => {
  const { spawnSync } = require('node:child_process');
  const exited = spawnSync(process.execPath, ['-e', '0']);
  assert.equal(exited.status, 0);
  assert.deepEqual(handlesOf(exited.pid), { fds: NaN, threads: NaN });
});
