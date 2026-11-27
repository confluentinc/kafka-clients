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

// The memory tests' probe children: each probe runs in a fresh Node process,
// with --expose-gc and the allocator's poisoning (freed memory filled, guard
// pages around large blocks), so a use after free reads garbage and a
// double free aborts, and it reads the addon's test-only counts
// (native/producer.c producerStats(), native/error.c errorStats(),
// native/headers.c headerStats(), native/metrics.c metricsStats()).
//
// The package a probe loads is this checkout's, or the one
// CONFLUENT_KAFKA_JS_PROBE_ROOT names: a copy with a mutated addon, which the
// mutation tests (test/memory_mutation/) build to prove each probe fails when
// the code it guards is broken.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

/** The package root whose dist/ and native/ the probes load. */
const ROOT = process.env.CONFLUENT_KAFKA_JS_PROBE_ROOT || path.join(__dirname, '..', '..');

/**
 * The allocator's poisoning: macOS's libmalloc fills freed blocks and guards
 * large ones; glibc fills allocated and freed memory with a pattern and checks
 * the heap on every call. Another platform runs without.
 *
 * @type {Record<string, string>}
 */
const POISON =
  process.platform === 'darwin'
    ? { MallocScribble: '1', MallocGuardEdges: '1' }
    : process.platform === 'linux'
      ? { MALLOC_PERTURB_: '165', MALLOC_CHECK_: '3' }
      : {};

/**
 * What a probe script finds defined: `D`, the compiled package's directory;
 * `native`, the addon (dist/internal/native); `settle()`, collections with
 * timer turns between them so every finalizer runs; and `leaks()`, each
 * native count's made minus freed, which a probe expects to be all zero once
 * everything is closed and collected.
 */
const PRELUDE = `
'use strict';
const D = ${JSON.stringify(path.join(ROOT, 'dist'))};
const native = require(D + '/internal/native');
async function settle(rounds = 10) {
  for (let i = 0; i < rounds; i++) {
    globalThis.gc();
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
function leaks() {
  const s = native.producerStats();
  const e = native.errorStats();
  const h = native.headerStats();
  const m = native.metricsStats();
  const c = native.consumerStats();
  return {
    producers: s.producersMade - s.producersFreed,
    ownerUnits: s.ownerUnitsTaken - s.ownerUnitsReleased,
    contexts: s.contextsMade - s.contextsFreed,
    slots: s.slotsTaken - s.slotsReturned,
    futures: s.futuresHeld - s.futuresDestroyed,
    waits: s.waitsStarted - s.waitsCompleted,
    deferralsUnreported: s.deferralsMade - s.deferralsReported,
    deferralsUnreleased: s.deferralsMade - s.deferralsReleased,
    bufferRefs: s.bufferRefsTaken - s.bufferRefsReleased,
    mockRecords: s.mockRecordsMade - s.mockRecordsFreed,
    injectedErrors: s.injectedMade - s.injectedFreed,
    errors: e.owned + e.made - e.freed - e.given,
    recordHeaders: h.made - h.freed,
    metricsViews: m.viewsMade - m.viewsFreed,
    metrics: m.metricsMade - m.metricsFreed,
    consumers: c.consumersMade - c.consumersFreed,
    batches: c.batchesMade - c.batchesDestroyed,
    consumerBuffers: c.buffersMade - c.buffersFreed,
    headerSources: c.sourcesMade - c.sourcesFreed,
    listeners: c.listenersMade - c.listenersFreed,
    commitCallbacks: c.commitCallbacksMade - c.commitCallbacksFreed,
    consumerDeferrals: c.deferralsMade - c.deferralsFreed,
  };
}
`;

/** Each count of `leaks()` at zero: everything made was freed, once. */
const NO_LEAKS = {
  producers: 0,
  ownerUnits: 0,
  contexts: 0,
  slots: 0,
  futures: 0,
  waits: 0,
  deferralsUnreported: 0,
  deferralsUnreleased: 0,
  bufferRefs: 0,
  mockRecords: 0,
  injectedErrors: 0,
  errors: 0,
  recordHeaders: 0,
  metricsViews: 0,
  metrics: 0,
  consumers: 0,
  batches: 0,
  consumerBuffers: 0,
  headerSources: 0,
  listeners: 0,
  commitCallbacks: 0,
  consumerDeferrals: 0,
};

/**
 * What a crashed native layer prints: a signal, an allocator's abort, Node's
 * fatal errors.
 */
const CRASH_TEXT =
  /Segmentation fault|Bus error|SIGSEGV|SIGBUS|SIGABRT|double free|pointer being freed was not allocated|malloc\(\)|free\(\)|corrupted|FATAL ERROR|Fatal error|panicked/;

/**
 * Runs `script`, after the prelude, in a fresh Node process with --expose-gc
 * and the allocator's poisoning, from the package root.
 *
 * @param {string} script the probe's body
 * @param {{ flags?: string[], env?: Record<string, string>, timeout?: number, poison?: boolean, prelude?: boolean }} [options]
 *   Node flags besides --expose-gc, extra environment, the time limit in ms
 *   (the child is killed after it), whether to poison the allocator (default
 *   true) and whether to prepend the prelude (default true)
 * @returns {{ status: number | null, signal: string | null, stdout: string, stderr: string, ms: number }} the outcome
 */
function runProbe(script, options = {}) {
  const { flags = [], env = {}, timeout = 120000, poison = true, prelude = true } = options;
  const started = process.hrtime.bigint();
  const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', ...flags, '-e', (prelude ? PRELUDE : '') + script], {
    cwd: ROOT,
    encoding: 'utf8',
    timeout,
    maxBuffer: 64 * 1024 * 1024,
    env: { ...process.env, ...(poison ? POISON : {}), ...env },
  });
  return {
    status: r.status,
    signal: r.signal,
    stdout: r.stdout ?? '',
    stderr: r.stderr ?? '',
    ms: Number((process.hrtime.bigint() - started) / 1000000n),
  };
}

/**
 * The last line a probe printed, parsed as JSON.
 *
 * @param {{ stdout: string, stderr: string }} r a probe's outcome
 * @returns {unknown} the parsed line
 */
function lastJson(r) {
  const lines = r.stdout.trim().split('\n');
  const last = lines[lines.length - 1];
  try {
    return JSON.parse(last);
  } catch {
    throw new Error(`the probe printed no JSON last:\n${r.stdout}\n${r.stderr}`);
  }
}

/**
 * Runs a probe, checks it exited cleanly (no signal, no crash text, status
 * 0) and returns the JSON line it printed last.
 *
 * @param {string} script the probe's body
 * @param {{ flags?: string[], env?: Record<string, string>, timeout?: number }} [options] as runProbe takes them
 * @returns {Record<string, any>} the probe's result
 */
function probeResult(script, options = {}) {
  const r = runProbe(script, options);
  assert.equal(r.signal, null, `the probe was killed by ${r.signal}:\n${r.stdout}\n${r.stderr}`);
  assert.doesNotMatch(r.stderr, CRASH_TEXT);
  assert.equal(r.status, 0, r.stderr);
  return /** @type {Record<string, any>} */ (lastJson(r));
}

/**
 * The probe of dropped futures: three records sent, each `send()` awaited,
 * their futures dropped and collections run before any delivery, then the
 * records delivered and the producer closed. It prints the callbacks' calls,
 * what the FinalizationRegistry saw collected before the delivery and in the
 * end (each record's future, `future <i>`, and value Buffer, `value <i>`),
 * and the leaks.
 *
 * @param {string} make the expression that builds the producer
 * @param {string} deliver the statements that deliver the records
 * @param {string} topic the records' topic
 * @returns {string} the probe's body
 */
function droppedFuturesProbe(make, deliver, topic) {
  return `
    const { KafkaProducer, MockProducer, ProducerRecord } = require(D + '/producer');
    const collected = [];
    const registry = new FinalizationRegistry((name) => collected.push(name));
    const calls = [];
    let beforeDelivery = null;
    (async () => {
      const hold = setInterval(() => {}, 1000);
      await (async () => {
        const p = ${make};
        // The record is made in a function of its own: a suspended async
        // function keeps the values its frame last held, which would keep
        // the last record's Buffer.
        const send = (i) => {
          const value = Buffer.alloc(16, 97 + i);
          registry.register(value, 'value ' + i);
          return p.send({
            record: new ProducerRecord({ topic: ${JSON.stringify(topic)}, value }),
            callback: ({ metadata, exception }) => calls.push([i, metadata.topic, exception === null ? null : exception.name]),
          });
        };
        for (let i = 0; i < 3; i++) registry.register(await send(i), 'future ' + i);
        // Collections before any delivery, the futures dropped: the callbacks
        // are the records' contexts' own.
        await settle();
        beforeDelivery = collected.slice().sort();
        calls.push('before delivery');
        ${deliver}
        await p.close();
      })();
      await settle();
      console.log(JSON.stringify({ calls, beforeDelivery, collected: collected.sort(), leaks: leaks() }));
      clearInterval(hold);
    })();
  `;
}

/**
 * What the probe of dropped futures prints as its calls when each callback
 * ran once, after the collections, with its delivery.
 *
 * @param {string} topic the records' topic
 * @returns {unknown[]} the calls
 */
const droppedFutureCalls = (topic) => ['before delivery', [0, topic, null], [1, topic, null], [2, topic, null]];

module.exports = {
  ROOT,
  POISON,
  PRELUDE,
  NO_LEAKS,
  CRASH_TEXT,
  runProbe,
  lastJson,
  probeResult,
  droppedFuturesProbe,
  droppedFutureCalls,
};
