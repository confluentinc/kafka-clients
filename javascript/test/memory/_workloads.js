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

// The workloads the memory tests run in probe children
// (test/memory/_probe.js), each with the checks of its outcome: the headers
// model (headers_model.test.js), and the same with the send and future
// workloads under the garbage collector's stress flags (gc_stress.test.js).

const assert = require('node:assert/strict');

const { NO_LEAKS, CRASH_TEXT, lastJson } = require('./_probe');

// The headers model: a record's Headers driven through random sequences of
// add, remove, lastHeader, headers(key), toArray, iteration, a copy, a send
// (which seals them) and collections, each read checked against a plain-JS
// model of Java's org.apache.kafka.common.header.internals.RecordHeaders: an
// ordered list of (key, value) pairs, read-only once the record is sent, a
// change then refused with RecordHeaders#canWrite's IllegalStateException
// ("RecordHeaders has been closed."). While the headers can change, a Header
// read from them is a copy; once read-only, a view into them
// (javascript/CLAUDE.md, the headers' option 1), and a change is refused
// without freeing the views held (boundary rule 1). Every Header read is
// kept for a while and checked again, after the headers changed and after
// collections, some across sequences. In a probe child with the allocator's
// poisoning, a view or a value Buffer left over freed memory reads garbage or
// crashes. Each sequence comes from a seeded generator, the seeds from
// HEADERS_MODEL_SEED (the first) and HEADERS_MODEL_SEEDS (how many), so a
// failure is reproduced from the seed it prints.

const HEADERS_MODEL_PROBE = `
const { Header, Headers } = require(D + '/common/header');
const { KafkaProducer, ProducerRecord } = require(D + '/producer');

const FIRST_SEED = Number(process.env.HEADERS_MODEL_SEED);
const SEEDS = Number(process.env.HEADERS_MODEL_SEEDS);
const STEPS = 80;
const KEYS = ['a', 'b', 'trace', '', 'é'];
const CLOSED = 'RecordHeaders has been closed.';

// mulberry32: a small seeded generator, so a sequence is its seed's.
function generator(seed) {
  let s = seed >>> 0;
  const next = () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const int = (n) => Math.floor(next() * n);
  return { next, int, pick: (list) => list[int(list.length)] };
}

const hex = (b) => (b === null ? null : Buffer.from(b.buffer, b.byteOffset, b.byteLength).toString('hex'));

// A value: null, empty, a few bytes, a large block (guarded by the allocator)
// or a view into the middle of a larger Buffer.
function randomValue(g) {
  const r = g.next();
  if (r < 0.15) return null;
  if (r < 0.3) return Buffer.alloc(0);
  if (r < 0.85) return Buffer.from(Array.from({ length: 1 + g.int(40) }, () => g.int(256)));
  if (r < 0.95) return Buffer.alloc(4096 + g.int(2000), g.int(256));
  const backing = Buffer.from(Array.from({ length: 64 }, () => g.int(256)));
  const start = g.int(32);
  return backing.subarray(start, start + 1 + g.int(31));
}

const pairs = (headers) => headers.map((h) => [h.key(), hex(h.value())]);

function check(what, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(what + ': read ' + a + ', the model has ' + e);
}

// How many of each operation ran, before and after the seal, so the test
// shows it reached both.
const ops = {};
const count = (name) => (ops[name] = (ops[name] ?? 0) + 1);

function refused(what, change) {
  let thrown = null;
  try {
    change();
  } catch (e) {
    thrown = e;
  }
  if (thrown === null) throw new Error(what + ' changed read-only headers');
  if (thrown.name !== 'IllegalStateError' || thrown.message !== CLOSED) {
    throw new Error(what + ' on read-only headers threw ' + thrown.name + ': ' + thrown.message);
  }
}

async function sequence(seed, producer, carried) {
  const g = generator(seed);
  const trace = [];
  const initial = Array.from({ length: g.int(4) }, () => [g.pick(KEYS), randomValue(g)]);
  const record = new ProducerRecord({
    topic: 'headers-model',
    partition: null,
    key: null,
    value: Buffer.from('v'),
    headers: initial.map(([key, value]) => new Header({ key, value })),
  });
  const h = record.headers;
  let model = initial.map(([key, value]) => [key, hex(value)]);
  let sealed = false;
  // Every Header read, with the key and value it had then, and some of their
  // value Buffers on their own, which keep their Header alive.
  let held = [];
  const hold = (headers) => {
    for (const header of headers) {
      const value = header.value();
      held.push({ header, key: header.key(), value: hex(value) });
      if (value !== null && g.next() < 0.3) held.push({ buffer: value, value: hex(value) });
    }
  };
  const readHeld = (x) => (x.header === undefined ? hex(x.buffer) : [x.header.key(), hex(x.header.value())]);
  const expectedHeld = (x) => (x.header === undefined ? x.value : [x.key, x.value]);
  const checkHeld = (what) => {
    for (const x of held) check(what + ': a Header or value read before', readHeld(x), expectedHeld(x));
  };
  try {
    for (let step = 0; step < STEPS; step++) {
      const op = g.pick(['add', 'add', 'add', 'addHeader', 'addHeader', 'remove', 'remove', 'last', 'last', 'last',
        'byKey', 'byKey', 'toArray', 'toArray', 'iterate', 'iterate', 'copy', 'send', 'gc', 'gc', 'held', 'drop']);
      const key = g.pick(KEYS);
      trace.push(op + (['add', 'remove', 'last', 'byKey'].includes(op) ? '(' + JSON.stringify(key) + ')' : ''));
      count(op + (sealed ? ' sealed' : ''));
      switch (op) {
        case 'add': {
          const value = randomValue(g);
          if (sealed) refused('add', () => h.add({ key, value }));
          else {
            h.add({ key, value });
            model.push([key, hex(value)]);
          }
          break;
        }
        case 'addHeader': {
          // A new Header, or one read before (a copy, or a view of other headers).
          const headers = held.filter((x) => x.header !== undefined);
          const header = headers.length > 0 && g.next() < 0.5 ? g.pick(headers).header : new Header({ key, value: randomValue(g) });
          const pair = [header.key(), hex(header.value())];
          if (sealed) refused('add(header)', () => h.add(header));
          else {
            h.add(header);
            model.push(pair);
          }
          break;
        }
        case 'remove':
          if (sealed) refused('remove', () => h.remove(key));
          else {
            h.remove(key);
            model = model.filter(([k]) => k !== key);
          }
          break;
        case 'last': {
          const got = h.lastHeader(key);
          const expected = model.filter(([k]) => k === key).pop() ?? null;
          check('lastHeader', got === null ? null : [got.key(), hex(got.value())], expected);
          if (got !== null) hold([got]);
          break;
        }
        case 'byKey': {
          const got = [...h.headers(key)];
          check('headers(key)', pairs(got), model.filter(([k]) => k === key));
          hold(got.slice(0, 2));
          break;
        }
        case 'toArray': {
          const got = h.toArray();
          check('toArray', pairs(got), model);
          hold(got.filter(() => g.next() < 0.3));
          break;
        }
        case 'iterate': {
          const got = [...h];
          check('iteration', pairs(got), model);
          hold(got.filter(() => g.next() < 0.3));
          break;
        }
        case 'copy': {
          // Java's new RecordHeaders(headers): writable, apart from these.
          const copy = new Headers(h);
          check('a copy', pairs(copy.toArray()), model);
          copy.add({ key: 'copy-only', value: null });
          check('after a change to a copy', pairs(h.toArray()), model);
          break;
        }
        case 'send':
          // send() seals the record's headers; the record's delivery, which
          // fails against the unreachable broker, is not what is checked.
          await producer.send(record);
          sealed = true;
          break;
        case 'gc':
          globalThis.gc();
          break;
        case 'held':
          checkHeld('checking the Headers held');
          break;
        case 'drop':
          held = held.filter(() => g.next() < 0.5);
          break;
      }
      if (held.length > 0 && g.next() < 0.2) checkHeld('after ' + op);
    }
    check('in the end', pairs(h.toArray()), model);
    checkHeld('in the end');
    for (const x of held.slice(0, 3)) carried.push({ seed, ...x });
    return null;
  } catch (e) {
    return { seed, step: trace.length - 1, trace: trace.slice(-12), error: String(e && e.stack ? e.stack.split('\\n').slice(0, 3).join(' | ') : e) };
  }
}

(async () => {
  const hold = setInterval(() => {}, 1000);
  const out = { sequences: 0, failures: [] };
  // The shared empty RecordHeaders, made on first use, live for the process.
  new Headers().internalSetReadOnly();
  new Headers().lastHeader('warm');
  await settle();
  const base = native.headerStats();
  await (async () => {
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 1 } });
    // Headers read in a sequence, checked again once later sequences ran and
    // their records were collected.
    const carried = [];
    for (let seed = FIRST_SEED; seed < FIRST_SEED + SEEDS; seed++) {
      // A crash ends the child: the last seed it printed is the one.
      console.log('seed ' + seed);
      const failure = await sequence(seed, producer, carried);
      out.sequences++;
      if (failure !== null) out.failures.push(failure);
    }
    await settle();
    for (const x of carried) {
      const read = x.header === undefined ? hex(x.buffer) : [x.header.key(), hex(x.header.value())];
      const expected = x.header === undefined ? x.value : [x.key, x.value];
      if (JSON.stringify(read) !== JSON.stringify(expected)) {
        out.failures.push({ seed: x.seed, error: 'a Header or value kept past its record read ' + JSON.stringify(read) + ', not ' + JSON.stringify(expected) });
      }
    }
    out.carried = carried.length;
    carried.length = 0;
    await producer.close();
  })();
  await settle();
  const s = native.headerStats();
  out.live = {
    headers: s.headersMade - base.headersMade - (s.headersFreed - base.headersFreed),
    header: s.headerMade - base.headerMade - (s.headerFreed - base.headerFreed),
  };
  out.reads = { copies: s.headerCopies - base.headerCopies, views: s.headerViews - base.headerViews };
  out.ops = ops;
  out.leaks = leaks();
  console.log(JSON.stringify(out));
  clearInterval(hold);
})();
`;

/**
 * Checks the headers model's outcome: no crash, every read as the model has
 * it, every sequence run, both kinds of Header read, every handle freed.
 *
 * @param {{ status: number | null, signal: string | null, stdout: string, stderr: string }} r the probe's outcome
 * @param {number} seeds how many sequences it ran
 */
function checkHeadersModel(r, seeds) {
  const started = r.stdout.match(/^seed \d+$/gm) ?? [];
  const last = started.length > 0 ? started[started.length - 1] : 'no seed';
  const reproduce = (/** @type {number | string} */ seed) => `HEADERS_MODEL_SEED=${seed} HEADERS_MODEL_SEEDS=1`;
  assert.equal(r.signal, null, `killed by ${r.signal} in ${last} (${reproduce(last.slice(5))}):\n${r.stderr}`);
  assert.doesNotMatch(r.stderr, CRASH_TEXT, `crashed in ${last} (${reproduce(last.slice(5))})`);
  assert.equal(r.status, 0, `${last} (${reproduce(last.slice(5))}):\n${r.stderr}`);
  const out =
    /** @type {{ sequences: number, failures: { seed: number }[], carried: number, live: unknown, reads: { copies: number, views: number }, ops: Record<string, number>, leaks: unknown }} */ (
      lastJson(r)
    );
  assert.deepEqual(out.failures, [], out.failures.map((f) => `seed ${f.seed} (${reproduce(f.seed)}): ${JSON.stringify(f)}`).join('\n'));
  assert.equal(out.sequences, seeds);
  assert.ok(out.carried > 0, 'no Header was kept across sequences');
  // Every change and every read ran on headers that could still change and
  // on read-only ones.
  for (const op of ['add', 'addHeader', 'remove', 'last', 'byKey', 'toArray', 'iterate', 'copy']) {
    for (const name of [op, `${op} sealed`]) assert.ok((out.ops[name] ?? 0) > 0, `${name} never ran: ${JSON.stringify(out.ops)}`);
  }
  // Both kinds of Header were read, and every native handle was destroyed,
  // once, when its last holder was collected (native/headers.c headerStats()).
  assert.ok(out.reads.copies > 0 && out.reads.views > 0, JSON.stringify(out.reads));
  assert.deepEqual(out.live, { headers: 0, header: 0 });
  assert.deepEqual(out.leaks, NO_LEAKS);
}

// The send workload: 1,000 records through a KafkaProducer against an
// unreachable broker, 25 sends at once, with garbage made between sends so
// collections land in their work. The key, value and header bytes take every
// form a caller hands over: a small Uint8Array, whose bytes V8 keeps inside
// the heap, where a compaction moves them; a Buffer of its own; a slice of
// Node's Buffer pool; a view into the middle of a larger ArrayBuffer; an
// empty one; null. Each record the sends built is read back
// (native/headers.c recordSpy()) and compared, byte for byte, with what was
// given, and its key and value must be the caller's own memory. Then 200
// strings through stringSerializer(), the bytes the client makes itself.
// Every record fails with the metadata timeout, which each future, read
// after the collections, holds.
const SEND_PROBE = `
const { Header } = require(D + '/common/header');
const { stringSerializer } = require(D + '/common/serialization');
const { KafkaProducer, ProducerRecord } = require(D + '/producer');
const hex = (b) => (b === null ? null : Buffer.from(b.buffer, b.byteOffset, b.byteLength).toString('hex'));
function bytes(i) {
  switch (i % 6) {
    case 0:
      return Uint8Array.from({ length: 1 + (i % 60) }, (_, j) => (i + j) & 255);
    case 1:
      return Buffer.alloc(100 + (i % 200), i & 255);
    case 2:
      return Buffer.from('pooled-' + i);
    case 3: {
      const backing = new ArrayBuffer(256);
      new Uint8Array(backing).fill(i & 255);
      return new Uint8Array(backing, 7 + (i % 50), 20);
    }
    case 4:
      return new Uint8Array(0);
    default:
      return null;
  }
}
function churn(i) {
  const junk = [];
  for (let j = 0; j < 50; j++) junk.push({ j, s: 'x' + i + j });
  return junk.length;
}
(async () => {
  const hold = setInterval(() => {}, 1000);
  const out = { checked: 0, mismatches: [], outcomes: {}, encoded: 0 };
  await (async () => {
    const configs = { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 1 };
    const p = new KafkaProducer({ configs });
    for (let round = 0; round < 40; round++) {
      native.recordSpy(true);
      const expected = [];
      const sends = [];
      for (let j = 0; j < 25; j++) {
        const i = round * 25 + j;
        const key = bytes(i);
        const value = bytes(i + 3);
        const headers = i % 3 === 0 ? [new Header({ key: 'h' + (i % 5), value: bytes(i + 1) }), new Header({ key: 'n', value: null })] : [];
        expected.push({
          key: hex(key),
          value: hex(value),
          headers: headers.map((h) => [h.key(), hex(h.value())]),
          keyInPlace: key !== null && key.length > 0,
          valueInPlace: value !== null && value.length > 0,
        });
        sends.push(
          p.send({
            record: new ProducerRecord({ topic: 'gc-stress', partition: null, key, value, headers }),
            callback: i % 4 === 0 ? () => Promise.resolve() : () => {},
          }),
        );
        churn(i);
      }
      const futures = await Promise.all(sends);
      const spied = native.recordSpy(false);
      if (spied.length !== expected.length) out.mismatches.push({ round, spied: spied.length });
      spied.forEach((x, j) => {
        out.checked++;
        const got = {
          key: hex(x.key),
          value: hex(x.value),
          headers: x.headers.map(([k, v]) => [k, hex(v)]),
          keyInPlace: x.keyInPlace,
          valueInPlace: x.valueInPlace,
        };
        if (JSON.stringify(got) !== JSON.stringify(expected[j])) out.mismatches.push({ record: round * 25 + j, got, expected: expected[j] });
      });
      for (const f of futures) {
        const outcome = await f.get().then(
          () => 'resolved',
          (e) => e.name + ': ' + (e.cause ? e.cause.name : 'no cause'),
        );
        out.outcomes[outcome] = (out.outcomes[outcome] ?? 0) + 1;
      }
    }
    await p.close();
    const t = new KafkaProducer({ configs, keySerializer: stringSerializer(), valueSerializer: stringSerializer() });
    const strings = [];
    for (let i = 0; i < 200; i++) strings.push(['k-' + i + '-é', 'v-' + i + '-' + 'ü'.repeat(i % 7)]);
    native.recordSpy(true);
    const sends = [];
    for (const [key, value] of strings) {
      sends.push(t.send(new ProducerRecord({ topic: 'gc-stress', key, value })));
      churn(sends.length);
    }
    await Promise.all(sends);
    native.recordSpy(false).forEach((x, i) => {
      if (hex(x.key) === Buffer.from(strings[i][0]).toString('hex') && hex(x.value) === Buffer.from(strings[i][1]).toString('hex')) out.encoded++;
    });
    await t.close();
  })();
  await settle();
  out.leaks = leaks();
  console.log(JSON.stringify(out));
  clearInterval(hold);
})();
`;

/**
 * Checks the send workload's outcome: no crash, every record as given, byte
 * for byte, every future failed with the metadata timeout, every handle
 * freed.
 *
 * @param {{ status: number | null, signal: string | null, stdout: string, stderr: string }} r the probe's outcome
 */
function checkSend(r) {
  checkExit(r);
  const out = /** @type {{ checked: number, mismatches: unknown[], outcomes: unknown, encoded: number, leaks: unknown }} */ (lastJson(r));
  assert.deepEqual(out.mismatches, []);
  assert.equal(out.checked, 1000);
  // Java's org.apache.kafka.common.errors.TimeoutException, from the
  // metadata wait, wrapped by the future's get().
  assert.deepEqual(out.outcomes, { 'ExecutionError: TimeoutError': 1000 });
  assert.equal(out.encoded, 200);
  assert.deepEqual(out.leaks, NO_LEAKS);
}

// The future workload: 600 records through a MockProducer that completes
// none by itself, 20 at a time; for each, waits started before the delivery
// (an untimed get, a timed one, thenApply, whenComplete, allOf), a timed get
// that times out, callbacks returning nothing or a Promise, then
// completeNext() for most and errorNext() with an error of the probe's for
// some, with garbage made between them. Every future must answer with its
// record's metadata (the mock's offsets count its sends, as Java's
// MockProducer#send assigns them) or the injected error itself, and the
// mock's history() must hold the records' values, byte for byte.
const FUTURE_PROBE = `
const { KafkaFuture } = require(D + '/common');
const { MockProducer, ProducerRecord } = require(D + '/producer');
const hex = (b) => (b === null ? null : Buffer.from(b.buffer, b.byteOffset, b.byteLength).toString('hex'));
function churn(i) {
  const junk = [];
  for (let j = 0; j < 50; j++) junk.push({ j, s: 'x' + i + j });
  return junk.length;
}
(async () => {
  const hold = setInterval(() => {}, 1000);
  const out = { mismatches: [], timedOut: {}, history: false, completions: 0 };
  await (async () => {
    const m = new MockProducer({ autoComplete: false, partitioner: null, keySerializer: null, valueSerializer: null });
    const sent = [];
    for (let round = 0; round < 30; round++) {
      const futures = [];
      for (let j = 0; j < 20; j++) {
        const i = round * 20 + j;
        const value = Buffer.alloc(8 + (i % 50), i & 255);
        sent.push(hex(value));
        futures.push(
          await m.send({
            record: new ProducerRecord({ topic: 'gc-stress', value }),
            callback: j % 3 === 0 ? () => new Promise((resolve) => setImmediate(resolve)) : () => {},
          }),
        );
        churn(i);
      }
      const answer = (p) =>
        p.then(
          (v) => ['ok', v === undefined ? null : String(typeof v === 'object' && v !== null ? v.offset : v)],
          (e) => ['failed', e.name, e.cause === undefined ? null : e.cause.message],
        );
      const gets = futures.map((f, j) => answer(j % 2 === 0 ? f.get() : f.get({ timeout: 60000 })));
      const applied = futures.map((f) => answer(f.thenApply((md) => md.offset).get()));
      futures.forEach((f) => f.whenComplete(() => out.completions++));
      const all = answer(KafkaFuture.allOf(futures).get());
      const timedOut = await futures[1].get({ timeout: 0 }).then(
        () => 'resolved',
        (e) => e.name + ': ' + e.message,
      );
      out.timedOut[timedOut] = (out.timedOut[timedOut] ?? 0) + 1;
      const expected = [];
      for (let j = 0; j < 20; j++) {
        const i = round * 20 + j;
        if (j % 7 === 6) {
          m.errorNext(new Error('injected ' + i));
          expected.push(['failed', 'ExecutionError', 'injected ' + i]);
        } else {
          m.completeNext();
          expected.push(['ok', String(i)]);
        }
        churn(j);
      }
      const got = { gets: await Promise.all(gets), applied: await Promise.all(applied), all: await all };
      const want = { gets: expected, applied: expected, all: ['failed', 'ExecutionError', 'injected ' + (round * 20 + 6)] };
      if (JSON.stringify(got) !== JSON.stringify(want)) out.mismatches.push({ round, got, want });
    }
    out.history = JSON.stringify(m.history().map((r) => hex(r.value))) === JSON.stringify(sent);
    m.clear();
    await m.close();
  })();
  await settle();
  out.leaks = leaks();
  console.log(JSON.stringify(out));
  clearInterval(hold);
})();
`;

/**
 * Checks the future workload's outcome: no crash, every future answered with
 * its record's metadata or the injected error, the history byte for byte,
 * every handle freed.
 *
 * @param {{ status: number | null, signal: string | null, stdout: string, stderr: string }} r the probe's outcome
 */
function checkFutures(r) {
  checkExit(r);
  const out = /** @type {{ mismatches: unknown[], timedOut: unknown, history: boolean, completions: number, leaks: unknown }} */ (
    lastJson(r)
  );
  assert.deepEqual(out.mismatches, []);
  // The core's text for a record's future (javascript/CLAUDE.md, the
  // record's future).
  assert.deepEqual(out.timedOut, { 'TimeoutError: Timed out waiting for KafkaFuture after 0 ms': 30 });
  assert.equal(out.history, true);
  assert.equal(out.completions, 600);
  assert.deepEqual(out.leaks, NO_LEAKS);
}

/**
 * Checks that a probe exited cleanly: no signal, no crash text, status 0.
 *
 * @param {{ status: number | null, signal: string | null, stdout: string, stderr: string }} r the probe's outcome
 */
function checkExit(r) {
  assert.equal(r.signal, null, `the probe was killed by ${r.signal}:\n${r.stdout}\n${r.stderr}`);
  assert.doesNotMatch(r.stderr, CRASH_TEXT);
  assert.equal(r.status, 0, r.stderr);
}

module.exports = { HEADERS_MODEL_PROBE, checkHeadersModel, SEND_PROBE, checkSend, FUTURE_PROBE, checkFutures };
