// Copyright 2025 Confluent Inc.
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

// Broker-backed producer test: runs only when KAFKA_BOOTSTRAP_SERVERS is set
// (`make -C javascript test-integration` starts a scratch broker for it).

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { ExecutionError, TimeoutError: RootTimeoutError } = require('@confluentinc/kafka-javascript');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { TimeoutError } = require('@confluentinc/kafka-javascript/common/errors');
const { Header, Headers } = require('@confluentinc/kafka-javascript/common/header');
const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { fetchRecords } = require('./_fetch_records');
const { createFuture } = require('../_internal');
// Every probe child runs with the allocator's poisoning (freed memory filled,
// guard pages around large blocks), so a use after free or a double free in
// the native layer, at a collection or a destroy, fails the probe.
const { POISON } = require('../memory/_probe');

/** @typedef {import('@confluentinc/kafka-javascript/producer').Callback} Callback */
/** @template T @typedef {import('@confluentinc/kafka-javascript/common/serialization').Serializer<T>} Serializer */

// A string in every test that runs: the tests are skipped when it is unset.
const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to produce to';
if (SKIP) console.log(`skipping the integration tests: ${SKIP}`);

// A topic no earlier test produced to: its metadata is fetched on the first
// send, so the records queue for it in the core.
let topicCount = 0;
/** @param {string} what */
function freshTopic(what) {
  return `js-it-${what}-${process.pid}-${Date.now()}-${topicCount++}`;
}

const N = 300;

(SKIP ? test.skip : test)(
  'produces one record with a key, a value and headers',
  async () => {
    const topic = freshTopic('one');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
    try {
      const before = Date.now();
      let callbacks = 0;
      /** @type {RecordMetadata | undefined} */
      let callbackMetadata;
      const future = await producer.send({
        record: new ProducerRecord({
          topic,
          partition: null,
          key: Buffer.from('k1'),
          value: Buffer.from('hello-from-node'),
          headers: [new Header({ key: 'h1', value: Buffer.from('v1') }), new Header({ key: 'h2', value: null })],
        }),
        callback: ({ metadata: m, exception: error }) => {
          callbacks++;
          callbackMetadata = m;
          assert.equal(error, null);
        },
      });
      assert.ok(future instanceof KafkaFuture);
      const metadata = await future.get();
      assert.equal(callbacks, 1);
      assert.equal(future.isDone(), true);
      assert.equal(future.isCompletedExceptionally(), false);
      assert.ok(metadata instanceof RecordMetadata);
      assert.ok(callbackMetadata);
      assert.equal(callbackMetadata.toString(), metadata.toString());
      assert.equal(metadata.topic, topic);
      assert.equal(metadata.partition >= 0, true);
      assert.equal(metadata.hasOffset, true);
      assert.ok(metadata.offset >= 0, `offset ${metadata.offset}`);
      assert.equal(metadata.hasTimestamp, true);
      assert.ok(metadata.timestamp >= before - 60000, `timestamp ${metadata.timestamp}`);
      assert.equal(metadata.serializedKeySize, 2);
      assert.equal(metadata.serializedValueSize, 15);
      // A data object: a spread and a JSON copy keep every field, Java's longs
      // as bigints, and as decimal strings in JSON.
      const fields = {
        topic,
        partition: metadata.partition,
        offset: metadata.offset,
        hasOffset: true,
        timestamp: metadata.timestamp,
        hasTimestamp: true,
        serializedKeySize: 2,
        serializedValueSize: 15,
      };
      assert.equal(typeof metadata.offset, 'bigint');
      assert.equal(typeof metadata.timestamp, 'bigint');
      assert.deepEqual(JSON.parse(JSON.stringify(metadata)), {
        ...fields,
        offset: String(metadata.offset),
        timestamp: String(metadata.timestamp),
      });
      assert.deepEqual({ ...metadata }, fields);
      // get() again returns the same outcome, timed or not, and so do the
      // future's other readers and dependants.
      assert.equal((await future.get()).toString(), metadata.toString());
      assert.equal(await future.get(1000), metadata);
      assert.equal(future.getNow(null), metadata);
      assert.equal(await future.toCompletionStage(), metadata);
      assert.equal(future.thenApply((m) => m.offset).getNow(-1n), metadata.offset);
      assert.equal(await KafkaFuture.allOf([future]).get(), null);
      // A record's future is never cancelled.
      assert.equal(future.cancel(true), false);
      assert.equal(future.isCancelled(), false);

      // A second record lands after the first on the same partition.
      const next = await (
        await producer.send({
          record: new ProducerRecord({ topic, partition: metadata.partition, key: null, value: Buffer.from('second') }),
        })
      ).get();
      assert.equal(next.partition, metadata.partition);
      assert.equal(next.offset, metadata.offset + 1n);
      assert.equal(next.serializedKeySize, -1);
      await producer.flush();

      const { records } = await fetchRecords(BOOTSTRAP, topic, metadata.partition);
      assert.deepEqual(
        records.map((r) => {
          assert.ok(r.value);
          return [r.offset, r.key && r.key.toString(), r.value.toString(), r.headers.map((h) => [h.key, h.value && h.value.toString()])];
        }),
        [
          [
            Number(metadata.offset),
            'k1',
            'hello-from-node',
            [
              ['h1', 'v1'],
              ['h2', null],
            ],
          ],
          [Number(next.offset), null, 'second', []],
        ],
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a record's dependants complete inside the delivery, whatever their functions throw or cancel",
  async () => {
    const topic = freshTopic('dependants');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
    try {
      const future = await producer.send(new ProducerRecord({ topic, value: Buffer.from('v') }));
      // A thrown value whose class cannot be read fails its dependant, and
      // stops neither the record's other dependants nor its get().
      const revoked = Proxy.revocable({}, {});
      revoked.revoke();
      const failing = future.thenApply(() => {
        throw revoked.proxy;
      });
      const failed = failing.get(10000);
      const sibling = future.thenApply((m) => m.topic);
      // A cancel() inside a dependant's action finishes the cancelled future's
      // own dependants, and an allOf over it, before it returns.
      // The library's own factory (test/_internal.js).
      const other = createFuture(false);
      const otherDep = other.whenComplete(() => {});
      const otherAll = KafkaFuture.allOf([other]);
      /** @type {boolean[]} */
      const seen = [];
      future.whenComplete(() => {
        seen.push(other.cancel(true), otherDep.isDone(), otherDep.isCancelled(), otherAll.isDone(), otherAll.isCancelled());
      });
      const metadata = await future.get(10000);
      assert.equal(metadata.topic, topic);
      assert.equal(sibling.getNow(null), topic);
      await assert.rejects(failed, (/** @type {ExecutionError} */ e) => e.constructor === ExecutionError && e.cause === revoked.proxy);
      assert.equal(failing.isCompletedExceptionally(), true);
      assert.deepEqual(seen, [true, true, true, true, true]);
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a record's future waits through the FFI's get_cb and get_with_timeout_cb, completing after its callback",
  async () => {
    const topic = freshTopic('native-future');
    // A long linger keeps the record undelivered while its future is read.
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 3000 } });
    try {
      // The topic's metadata first, so the next record is taken at once.
      await (await producer.send(new ProducerRecord({ topic, value: Buffer.from('warm-up') }))).get();
      /** @type {string[]} */
      const order = [];
      let future = /** @type {KafkaFuture<RecordMetadata> | null} */ (null);
      future = await producer.send({
        record: new ProducerRecord({ topic, value: Buffer.from('v') }),
        callback: () => {
          assert.ok(future);
          order.push(`callback done=${future.isDone()}`);
        },
      });
      future.whenComplete(() => order.push('dependant'));
      assert.equal(future.isDone(), false);
      // @ts-expect-error a string for a RecordMetadata future's valueIfAbsent, which Java's getNow(T) refuses: the run time returns what it is given
      assert.equal(future.getNow('absent'), 'absent');
      // The FFI keeps the time of a timed wait: no JavaScript timer runs.
      const setTimeoutBefore = globalThis.setTimeout;
      let timers = 0;
      globalThis.setTimeout = /** @type {typeof setTimeout} */ (
        /** @param {Parameters<typeof setTimeout>} args */
        (...args) => {
          timers += 1;
          return setTimeoutBefore(...args);
        }
      );
      try {
        // The root TimeoutError (Java's java.util.concurrent.TimeoutException),
        // with the core's message; a negative timeout does not wait.
        await assert.rejects(
          future.get({ timeout: 100 }),
          (/** @type {RootTimeoutError} */ e) =>
            e.constructor === RootTimeoutError && e.message === 'Timed out waiting for KafkaFuture after 100 ms',
        );
        await assert.rejects(
          future.get(-1),
          (/** @type {RootTimeoutError} */ e) =>
            e.constructor === RootTimeoutError && e.message === 'Timed out waiting for KafkaFuture after 0 ms',
        );
      } finally {
        globalThis.setTimeout = setTimeoutBefore;
      }
      assert.equal(timers, 0);
      assert.equal(future.isDone(), false);
      const metadata = await future.get();
      assert.equal(metadata.topic, topic);
      // The callback ran before the future completed, then the dependant.
      assert.deepEqual(order, ['callback done=false', 'dependant']);
      assert.equal(future.isDone(), true);
      assert.equal(future.getNow(null), metadata);
      // A done future answers a timed get at once, with the same metadata.
      assert.equal(await future.get({ timeout: 0 }), metadata);
    } finally {
      await producer.close();
    }
  },
  120000,
);

// A record's native future across close, collection and destroy
// (javascript/CLAUDE.md, rule 5): read each way (never; get() once and twice;
// a timed get that times out, then get(); a dependant; toCompletionStage()),
// each producer closed and collected. What it shows: every producer and JS
// future is collected (no wait keeps its JS future, nor a future its
// producer) and the producers' threads are gone (each closed producer was
// destroyed), and a future kept past its producer's destroy still answers
// from its delivery. Then 200 records sent one after another on one producer
// use at most 2 slots of its table of record futures: each slot is given back
// once JS recorded the delivery, which is when the context destroys the
// native future (a context that never gives its slot back would take a new
// one per record).
//
// Not covered: a native future never destroyed, a wait that never releases
// its unit of the producer struct, or its reference on the send's context.
// None has an effect JavaScript can see (a future and a context hold no
// thread, and a unit only keeps a small struct), and the RSS they add, a few
// hundred bytes per record, is within the noise of a bound that would not
// flake.
(SKIP || !['linux', 'darwin'].includes(process.platform) ? test.skip : test)(
  "a record's native future is released across close, collection and destroy, whatever read it",
  () => {
    const topic = freshTopic('destroy-futures');
    const script = `
    const { execSync } = require('node:child_process');
    const fs = require('node:fs');
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
    const threads = () =>
      process.platform === 'linux'
        ? fs.readdirSync('/proc/self/task').length
        : execSync('ps -M -p ' + process.pid).toString().trim().split('\\n').length - 1;
    const collected = [];
    const registry = new FinalizationRegistry((name) => collected.push(name));
    const count = (name) => collected.filter((c) => c === name).length;
    // The slots the hand-overs give the records' futures.
    const futureClass = Object.getPrototypeOf(KafkaFuture.completedFuture(null));
    const setNative = futureClass.internalSetNative;
    const slots = new Set();
    futureClass.internalSetNative = function (producer, slot) {
      slots.add(slot);
      return setNative.call(this, producer, slot);
    };
    const settle = async () => {
      for (let i = 0; i < 10; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 100));
      }
    };
    const kept = [];
    // In a function of its own, so no frame keeps the producer.
    async function one(round) {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 500 } });
      registry.register(p, 'producer');
      await (await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: Buffer.from('w') }))).get();
      const futures = [];
      for (let i = 0; i < 6; i++) futures.push(await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: Buffer.from('v' + i) })));
      for (const f of futures) registry.register(f, 'future');
      const waits = [futures[1].get(), futures[2].get(), futures[2].get()];
      const timedOut = await futures[3].get({ timeout: 20 }).then(() => 'resolved', (e) => e.constructor.name + ': ' + e.message);
      waits.push(futures[3].get(), futures[4].whenComplete(() => {}).get(), futures[5].toCompletionStage());
      const results = await Promise.all(waits);
      await p.close();
      if (round === 0) kept.push(futures[0]);
      return timedOut + ' ' + results.length;
    }
    (async () => {
      // A closed producer no longer holds the loop: the test does.
      const hold = setInterval(() => {}, 1000);
      console.log(await one(0));
      await settle();
      const before = threads();
      for (let r = 1; r <= 5; r++) await one(r);
      await settle();
      console.log('producers', count('producer'), 'futures', count('future'), 'threads', threads() - before < 10);
      // In a function of its own, so no frame keeps the future after.
      await (async () => {
        const f = kept.pop();
        console.log('kept', f.isDone(), f.getNow(null).topic === process.env.TOPIC, (await f.get()).topic === process.env.TOPIC, (await f.get({ timeout: 0 })).topic === process.env.TOPIC);
      })();
      await settle();
      console.log('futures', count('future'));
      // Sequential records, each awaited, on one producer: a slot given back
      // at each delivery is taken again by a later record.
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP } });
      await (await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: Buffer.from('w') }))).get();
      slots.clear();
      for (let i = 0; i < 200; i++) await (await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: Buffer.from('s' + i) }))).get();
      console.log('slots', slots.size <= 2 ? 'at most 2' : slots.size);
      await p.close();
      clearInterval(hold);
    })();
  `;
    const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 170000,
      env: { ...process.env, ...POISON, BOOTSTRAP, TOPIC: topic },
    });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(
      r.stdout,
      'TimeoutError: Timed out waiting for KafkaFuture after 20 ms 6\n' +
        // Six producers, 35 futures: all but the one kept.
        'producers 6 futures 35 threads true\n' +
        'kept true true true true\n' +
        'futures 36\n' +
        'slots at most 2\n',
    );
  },
  180000,
);

(SKIP ? test.skip : test)(
  "a record's future waited on natively across close(0) fails as Java's get() throws, and the process exits",
  () => {
    // Lingering records (linger.ms 60000), each future's get() waiting through
    // the FFI (get_cb, get_with_timeout_cb) when close(0) aborts them: Java's
    // get() throws ExecutionException, caused by the KafkaException
    // "Producer is closed forcefully.". This checks Java's answer, and that the
    // process exits right after the last answer, so each wait's count ended
    // (javascript/CLAUDE.md, "Threads and liveness"). It does not exercise the
    // hold of a wait: both waits are answered before the close completes,
    // while the open producer still holds Node. The producers stay reachable,
    // so no garbage collection releases what a wait left held.
    const topic = freshTopic('closed-wait');
    const script = `
    const { performance } = require('node:perf_hooks');
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const describe = (e) => e.constructor.name + ' caused by ' + e.cause.constructor.name + ': ' + e.cause.message;
    const producers = [];
    let mark;
    process.on('exit', () => console.log('exited after', performance.now() - mark < 1000 ? 'less than 1s' : 'more'));
    (async () => {
      const outcomes = new Set();
      for (let round = 0; round < 5; round++) {
        const p = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 60000 } });
        producers.push(p);
        const untimed = await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: Buffer.from('u') }));
        const timed = await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: Buffer.from('t') }));
        const waits = [untimed.get(), timed.get({ timeout: 60000 })].map((w) => w.then(() => 'resolved', describe));
        outcomes.add('done before the close: ' + untimed.isDone() + ' ' + timed.isDone());
        await p.close(0);
        for (const outcome of await Promise.all(waits)) outcomes.add(outcome);
      }
      mark = performance.now();
      console.log([...outcomes].join('\\n'));
    })();
  `;
    const r = spawnSync(process.execPath, ['--no-warnings', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 90000,
      env: { ...process.env, BOOTSTRAP, TOPIC: topic },
    });
    assert.equal(r.signal, null, `the process did not exit on its own: ${r.stdout}${r.stderr}`);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(
      r.stdout,
      'done before the close: false false\nExecutionError caused by KafkaError: Producer is closed forcefully.\nexited after less than 1s\n',
    );
  },
  180000,
);

(SKIP ? test.skip : test)(
  'partitionsFor() on a closed producer answers from its cached metadata, or rejects for a topic it has none for, and the process exits',
  () => {
    // Java 4.4.0's partitionsFor() after close(): a topic the producer's
    // metadata holds is answered from it (waitOnMetadata needs no update),
    // and any other topic asks for one, which ProducerMetadata#awaitUpdate
    // refuses with KafkaException("Requested metadata update after close").
    // Both answers come after the close's completion, so only the hold of a
    // waiting call keeps Node running until them (javascript/CLAUDE.md,
    // "Threads and liveness"): the child has nothing else holding Node,
    // prints the answers, and exits right after the last.
    const topic = freshTopic('closed-pf');
    const script = `
      const { performance } = require('node:perf_hooks');
      const { KafkaError } = require('@confluentinc/kafka-javascript/common');
      const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
      const names = (partitions) => partitions.map((info) => info.topic() + '-' + info.partition()).join(' ');
      const describe = (e) => (e.constructor === KafkaError ? 'KafkaError' : 'another class, ' + e.constructor.name) + ': ' + e.message;
      let mark;
      process.on('exit', () => console.log('exited after', performance.now() - mark < 1000 ? 'less than 1s' : 'more'));
      (async () => {
        const p = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP } });
        // The first send creates the topic (auto-creation), with one partition.
        await (await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: Buffer.from('v') }))).get();
        const open = names(await p.partitionsFor(process.env.TOPIC));
        await p.close();
        const known = await p.partitionsFor(process.env.TOPIC).then(names, describe);
        const unknown = await p.partitionsFor(process.env.TOPIC + '-none').then(names, describe);
        mark = performance.now();
        console.log(['open: ' + open, 'known after close: ' + known, 'unknown after close: ' + unknown].join('\\n'));
      })();
    `;
    const r = spawnSync(process.execPath, ['--no-warnings', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 90000,
      env: { ...process.env, BOOTSTRAP, TOPIC: topic },
    });
    assert.equal(r.signal, null, `the process did not exit on its own: ${r.stdout}${r.stderr}`);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(
      r.stdout,
      `open: ${topic}-0\nknown after close: ${topic}-0\nunknown after close: KafkaError: Requested metadata update after close\nexited after less than 1s\n`,
    );
  },
  180000,
);

(SKIP ? test.skip : test)(
  'NUL bytes in a key, a value and a header value cross byte-exact',
  async () => {
    // Only the strings the C ABI reads as NUL-terminated (topic, header key,
    // config) refuse a NUL; bytes cross with their length.
    const topic = freshTopic('nul');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
    try {
      const sent = [
        { key: Buffer.from('k\0ey'), value: Buffer.from('v\0al\0'), header: Buffer.from('h\0v') },
        { key: Buffer.from([0, 1, 0]), value: new Uint8Array([0, 0, 7, 0]), header: Buffer.from([0]) },
      ];
      let partition;
      for (const { key, value, header } of sent) {
        const metadata = await (
          await producer.send({
            record: new ProducerRecord({ topic, partition: 0, key, value, headers: [new Header({ key: 'h', value: header })] }),
          })
        ).get();
        partition = metadata.partition;
      }
      assert.ok(partition !== undefined);
      const { records } = await fetchRecords(BOOTSTRAP, topic, partition);
      /** @param {string | Uint8Array} d */
      const bytes = (d) => (typeof d === 'string' ? Buffer.from(d, 'utf8') : Buffer.from(d));
      assert.deepEqual(
        records.map((r) => [r.key, r.value, r.headers]),
        sent.map(({ key, value, header }) => [bytes(key), bytes(value), [{ key: 'h', value: bytes(header), keyBytes: Buffer.from('h') }]]),
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'the serializers bytes, a header one adds and the sizes reach the broker',
  async () => {
    const topic = freshTopic('serde');
    /** @type {Serializer<number>} */
    const keySerializer = { serialize: ({ data }) => (data === null ? null : Buffer.from(`key-${data}`)) };
    /** @type {Serializer<{ a: number }>} */
    const valueSerializer = {
      serialize: async ({ headers, data }) => {
        headers.add({ key: 'type', value: Buffer.from('json') });
        return Buffer.from(JSON.stringify(data));
      },
    };
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' }, keySerializer, valueSerializer });
    try {
      const metadata = await (await producer.send(new ProducerRecord({ topic, partition: 0, key: 7, value: { a: 1 } }))).get();
      assert.equal(metadata.serializedKeySize, 'key-7'.length);
      assert.equal(metadata.serializedValueSize, '{"a":1}'.length);
      const { records } = await fetchRecords(BOOTSTRAP, topic, metadata.partition);
      assert.deepEqual(
        records.map((r) => [r.key, r.value, r.headers]),
        [[Buffer.from('key-7'), Buffer.from('{"a":1}'), [{ key: 'type', value: Buffer.from('json'), keyBytes: Buffer.from('type') }]]],
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'CreateTime: the timestamp given, or the send time, reaches the broker',
  async () => {
    const topic = freshTopic('create-time');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
    try {
      // The two C record paths: without headers, and through the options
      // builder with them.
      const explicit = 1700000000123;
      const withoutHeaders = await (
        await producer.send({
          record: new ProducerRecord({ topic, partition: 0, timestamp: explicit, key: null, value: Buffer.from('a') }),
        })
      ).get();
      const withHeaders = await (
        await producer.send({
          record: new ProducerRecord({
            topic,
            partition: 0,
            // A long given as a bigint.
            timestamp: BigInt(explicit) + 1n,
            key: null,
            value: Buffer.from('b'),
            headers: [new Header({ key: 'h', value: Buffer.from('v') })],
          }),
        })
      ).get();
      assert.equal(withoutHeaders.timestamp, BigInt(explicit));
      assert.equal(withHeaders.timestamp, BigInt(explicit) + 1n);
      // No timestamp: the producer stamps its current time.
      const before = Date.now();
      const future = await producer.send({
        record: new ProducerRecord({ topic, partition: 0, timestamp: null, key: null, value: Buffer.from('c') }),
      });
      const after = Date.now();
      const stamped = await future.get();
      assert.ok(stamped.timestamp >= before && stamped.timestamp <= after, `${before} <= ${stamped.timestamp} <= ${after}`);

      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        records.map((r) => {
          assert.ok(r.value);
          return [r.value.toString(), r.timestamp];
        }),
        [
          ['a', explicit],
          ['b', explicit + 1],
          ['c', Number(stamped.timestamp)],
        ],
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

// Sends N records to partition 0 of `topic`, awaiting each send() (the
// hand-over) before the next, and returns their futures in send order.
/**
 * @param {KafkaProducer} producer
 * @param {string} topic
 * @param {number} n
 * @param {(i: number) => string} value
 */
async function sendSequentially(producer, topic, n, value) {
  const futures = [];
  for (let i = 0; i < n; i++) {
    futures.push(await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(value(i)) }) }));
  }
  return futures;
}

/** @param {KafkaFuture<RecordMetadata>[]} futures */
async function assertSendOrder(futures) {
  /** @type {bigint[]} */
  const offsets = [];
  for (const future of futures) offsets.push((await future.get()).offset);
  const outOfOrder = offsets.filter((offset, i) => i > 0 && offset <= offsets[i - 1]).length;
  assert.equal(outOfOrder, 0, `offsets in send order: ${offsets.slice(0, 20).join(',')}...`);
  return offsets;
}

(SKIP ? test.skip : test)(
  'awaited sends to a fresh topic land in send order',
  async () => {
    const topic = freshTopic('order');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      // The first records queue in the core on the missing metadata.
      const futures = await sendSequentially(producer, topic, N, (i) => String(i));
      const offsets = await assertSendOrder(futures);
      assert.deepEqual(
        offsets,
        Array.from({ length: N }, (_, i) => BigInt(i)),
      );
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        records.map((r) => {
          assert.ok(r.value);
          return r.value.toString();
        }),
        Array.from({ length: N }, (_, i) => String(i)),
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'awaited sends that wait for buffer memory land in send order',
  async () => {
    const topic = freshTopic('memory');
    // Batches of one record each, with memory for only a few: a send waits for
    // memory (the would-block path) on cached metadata.
    const producer = new KafkaProducer({
      configs: {
        'bootstrap.servers': BOOTSTRAP,
        'buffer.memory': 16384,
        'batch.size': 4096,
        'linger.ms': 0,
      },
    });
    try {
      // Metadata cached first.
      await (await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('warm-up') }) })).get();
      const payload = 'x'.repeat(3000);
      const futures = await sendSequentially(producer, topic, N, (i) => `${i}:${payload}`);
      const offsets = await assertSendOrder(futures);
      assert.deepEqual(
        offsets,
        Array.from({ length: N }, (_, i) => BigInt(i + 1)),
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'flush after awaited sends completes every future',
  async () => {
    const topic = freshTopic('flush');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 1000 } });
    try {
      const futures = await sendSequentially(producer, topic, 50, (i) => String(i));
      await producer.flush();
      assert.deepEqual(
        futures.map((f) => f.isDone()),
        futures.map(() => true),
      );
      await assertSendOrder(futures);
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'close after awaited sends delivers every record',
  async () => {
    const topic = freshTopic('close');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 1000 } });
    let callbacks = 0;
    const futures = [];
    for (let i = 0; i < 50; i++) {
      futures.push(
        await producer.send({
          record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(String(i)) }),
          callback: ({ metadata: m, exception: error }) => {
            assert.equal(error, null);
            callbacks++;
          },
        }),
      );
    }
    await producer.close();
    assert.equal(callbacks, 50);
    assert.deepEqual(
      futures.map((f) => f.isDone() && !f.isCompletedExceptionally()),
      futures.map(() => true),
    );
    await assertSendOrder(futures);
    const { highWatermark, records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.equal(highWatermark, 50);
    assert.equal(records.length, 50);
  },
  120000,
);

(SKIP ? test.skip : test)(
  'a Buffer reused after each awaited send keeps every record value',
  async () => {
    const topic = freshTopic('reuse');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 100 } });
    try {
      const buffer = Buffer.alloc(8);
      const futures = [];
      for (let i = 0; i < 20; i++) {
        // Rewritten once send() resolved: the client no longer reads it.
        buffer.write(`value-${String(i).padStart(2, '0')}`);
        futures.push(await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: buffer }) }));
      }
      buffer.fill(0x7a);
      await producer.flush();
      await assertSendOrder(futures);
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        records.map((r) => {
          assert.ok(r.value);
          return r.value.toString();
        }),
        Array.from({ length: 20 }, (_, i) => `value-${String(i).padStart(2, '0')}`),
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'a Buffer changed once send() settled: the broker gets the bytes given',
  async () => {
    // A fresh topic: the first send waits for its metadata, so its record is
    // still in the core while send() is pending, reading the caller's own
    // memory. The key and value are left alone until send() settled; the
    // header value is copied during the call, so it may change at once.
    const topic = freshTopic('borrowed');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
    try {
      const key = Buffer.from('key-0');
      const value = new Uint8Array(Buffer.from('value-0'));
      const header = Buffer.from('header-0');
      const record = new ProducerRecord({ topic, partition: 0, key, value, headers: [new Header({ key: 'h', value: header })] });
      const sent = producer.send(record);
      header.fill(0x7a);
      const future = await sent;
      key.fill(0x7a);
      value.fill(0x7a);
      await future.get();
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        records.map((r) => {
          assert.ok(r.value);
          return [
            r.key && r.key.toString(),
            r.value.toString(),
            r.headers.map((h) => {
              assert.ok(h.value);
              return h.value.toString();
            }),
          ];
        }),
        [['key-0', 'value-0', ['header-0']]],
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

// An accepted record's Buffers are released at its hand-over, when send()
// resolves, and not held until delivery (javascript/CLAUDE.md, rule 8). A
// record the producer takes needs a broker, hence an integration test; the
// unit tests cover a record refused at its hand-over. linger.ms holds the
// batch in the producer while the Buffers are collected; a child process
// runs with --expose-gc.
(SKIP ? test.skip : test)(
  "an accepted record's key and value Buffers are released when send() resolves, before delivery",
  () => {
    const topic = freshTopic('released');
    const script = `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 5000 } });
    const settle = async () => {
      for (let i = 0; i < 5; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 50));
      }
    };
    (async () => {
      // The topic's metadata first, so the next record is taken at once.
      await (await producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: null, value: Buffer.from('warm-up') }))).get();
      const refs = [];
      const future = await (async () => {
        const key = Buffer.from('released-key');
        const value = Buffer.alloc(65536, 0x61);
        refs.push(new WeakRef(key), new WeakRef(value));
        return producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key, value }));
      })();
      await settle();
      console.log('done before delivery', future.isDone());
      console.log('alive', refs.filter((r) => r.deref() !== undefined).length);
      const metadata = await future.get();
      console.log('delivered', metadata.serializedValueSize);
      await producer.close();
    })();
  `;
    const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 90000,
      env: { ...process.env, ...POISON, BOOTSTRAP, TOPIC: topic },
    });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, 'done before delivery false\nalive 0\ndelivered 65536\n');
  },
  120000,
);

(SKIP ? test.skip : test)(
  "strings are encoded as Java's StringSerializer encodes them: a lone surrogate as ?",
  async () => {
    const topic = freshTopic('surrogates');
    // stringSerializer() as a constructor argument, and in a serde config key without ().
    const producer = new KafkaProducer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'client.id': 'js-\uD800', 'value.serializer': stringSerializer },
      keySerializer: stringSerializer(),
      valueSerializer: null,
    });
    // Java's StringSerializer bytes for each string.
    const cases = [
      ['a\uD800b', '613f62'],
      ['a\uDC00b', '613f62'],
      ['\uDC00\uD800', '3f3f'],
      ['\uD83D\uDE00', 'f09f9880'],
    ];
    try {
      for (const [s] of cases) {
        await (
          await producer.send(new ProducerRecord({ topic, partition: 0, key: s, value: s, headers: [new Header({ key: s, value: null })] }))
        ).get();
      }
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        records.map((r) => {
          assert.ok(r.key && r.value);
          return [r.key.toString('hex'), r.value.toString('hex'), r.headers[0].keyBytes.toString('hex')];
        }),
        cases.map(([, hex]) => [hex, hex, hex]),
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'a serde config function that reads the whole configuration leaves a working producer',
  async () => {
    // It spreads the configuration, reading bootstrap.servers too: the producer
    // keys reach the core, and only schema.registry.url, which the core doesn't
    // know, is kept from it.
    const topic = freshTopic('spread');
    let seen = /** @type {Record<string, unknown> | null} */ (null);
    const producer = new KafkaProducer({
      'bootstrap.servers': BOOTSTRAP,
      'schema.registry.url': 'http://sr',
      'value.serializer': ({ configs }) => {
        seen = { ...configs };
        return /** @type {Serializer<string>} */ ({ serialize: ({ data }) => Buffer.from(JSON.stringify(data)) });
      },
    });
    try {
      assert.ok(seen);
      assert.equal(seen['bootstrap.servers'], BOOTSTRAP);
      const metadata = await (await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: 'spread' }))).get();
      assert.equal(metadata.topic, topic);
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        records.map((r) => {
          assert.ok(r.value);
          return r.value.toString();
        }),
        ['"spread"'],
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a record's headers are read-only once send() has taken it",
  async () => {
    const topic = freshTopic('sealed');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      const headers = new Headers([new Header({ key: 'a', value: null })]);
      const record = new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('v'), headers });
      record.headers.add({ key: 'b', value: Buffer.from('1') });
      const sent = producer.send(record);
      // Handled at creation: the later await still sees a rejection, and a
      // failing assertion before it leaves no unhandled rejection.
      sent.catch(() => {});
      assert.throws(() => record.headers.add({ key: 'c', value: null }), {
        name: 'IllegalStateError',
        message: 'RecordHeaders has been closed.',
      });
      // The Headers given was copied into the record: it stays writable.
      headers.add({ key: 'c', value: null });
      await (await sent).get();
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        records[0].headers.map((h) => h.key),
        ['a', 'b'],
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

// Watches the event loop during a send loop: `turn()` counts the sends
// between two of its turns (a setImmediate that reschedules itself), which is
// bounded whatever the load, unlike a timer's gap. One turn is due at least
// after FAST_PATH_YIELD_EVERY + 1 sends taken at once, so two consecutive
// turns are at most about 2 x 1001 sends apart.
/** @typedef {{ turns: number, sends: number, maxSendsPerTurn: number, running: boolean, stop: () => void }} EventLoopWatch */

/** @returns {EventLoopWatch} */
function eventLoopWatch() {
  // Its stop() is set below.
  const watch = /** @type {EventLoopWatch} */ ({ turns: 0, sends: 0, maxSendsPerTurn: 0, running: true });
  const turn = () => {
    if (!watch.running) return;
    watch.turns++;
    watch.maxSendsPerTurn = Math.max(watch.maxSendsPerTurn, watch.sends);
    watch.sends = 0;
    setImmediate(turn);
  };
  setImmediate(turn);
  watch.stop = () => {
    watch.running = false;
    watch.maxSendsPerTurn = Math.max(watch.maxSendsPerTurn, watch.sends);
  };
  return watch;
}
const MAX_SENDS_PER_TURN = 2 * 1001 + 64;

/** @param {string} what */
async function warmedProducer(what) {
  const topic = freshTopic(what);
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 5 } });
  // Warmed: the topic's metadata is cached, so the producer takes every
  // record at once and no send() suspends on its own.
  await (await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('warm') }) })).get();
  return { topic, producer };
}

(SKIP ? test.skip : test)(
  'a loop awaiting sends taken at once lets the event loop and the callbacks run',
  async () => {
    const { topic, producer } = await warmedProducer('yield');
    try {
      const SENDS = 200000;
      const value = Buffer.alloc(100, 0x78);
      const watch = eventLoopWatch();
      let callbacks = 0;
      let nested = /** @type {Promise<KafkaFuture<RecordMetadata>> | null} */ (null);
      const rssBefore = process.memoryUsage().rss;
      let rssPeak = rssBefore;
      for (let i = 0; i < SENDS; i++) {
        watch.sends++;
        await producer.send({
          record: new ProducerRecord({ topic, partition: 0, key: null, value }),
          callback: ({ metadata: m, exception: error }) => {
            assert.equal(error, null);
            callbacks++;
            // A send from inside a delivery callback (inside a drain, maybe the
            // one send() runs on entry): its own pump does not nest.
            if (nested === null) {
              const sent = producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('nested') }) });
              // Handled at creation, awaited below.
              sent.catch(() => {});
              nested = sent;
            }
          },
        });
        if (i % 10000 === 0) rssPeak = Math.max(rssPeak, process.memoryUsage().rss);
      }
      const callbacksDuringLoop = callbacks;
      watch.stop();
      // The event loop kept turning and the delivery callbacks ran while the
      // loop sent: their backlog stays bounded instead of growing with the run.
      assert.ok(watch.turns > 0, 'the event loop never turned during the loop');
      assert.ok(watch.maxSendsPerTurn <= MAX_SENDS_PER_TURN, `${watch.maxSendsPerTurn} sends ran between two event-loop turns`);
      assert.ok(callbacksDuringLoop > SENDS / 2, `only ${callbacksDuringLoop} of ${SENDS} callbacks ran during the loop`);
      assert.ok(rssPeak - rssBefore < 300e6, `RSS grew by ${Math.round((rssPeak - rssBefore) / 1e6)} MB`);
      assert.ok(nested !== null);
      const nestedMetadata = await (await nested).get();
      assert.equal(nestedMetadata.topic, topic);
      await producer.flush();
      assert.equal(callbacks, SENDS);
    } finally {
      await producer.close();
    }
  },
  300000,
);

(SKIP ? test.skip : test)(
  'a loop awaiting one send in seven lets the event loop run',
  async () => {
    // 7 divides 1001 (the yield period when only one send per period waits for
    // the turn), so a turn given to a fire-and-forget send would never reach
    // the awaited one.
    const { topic, producer } = await warmedProducer('yield-seven');
    try {
      const ITERATIONS = 30000;
      const L = 7;
      const value = Buffer.alloc(100, 0x78);
      let ticks = 0;
      const timer = setInterval(() => ticks++, 1);
      const watch = eventLoopWatch();
      let callbacks = 0;
      /** @param {Parameters<Callback>[0]} options */
      const callback = ({ metadata: m, exception: error }) => {
        assert.equal(error, null);
        callbacks++;
      };
      const fired = [];
      for (let i = 0; i < ITERATIONS; i++) {
        for (let j = 0; j < L; j++) {
          watch.sends++;
          const sent = producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value }), callback });
          if (j === 0) await sent;
          else {
            // Handled at creation, awaited after the loop.
            sent.catch(() => {});
            fired.push(sent);
          }
        }
      }
      const ticksDuringLoop = ticks;
      watch.stop();
      clearInterval(timer);
      assert.ok(ticksDuringLoop > 0, 'the timer never fired during the loop');
      assert.ok(watch.turns > 0, 'the event loop never turned during the loop');
      assert.ok(watch.maxSendsPerTurn <= MAX_SENDS_PER_TURN, `${watch.maxSendsPerTurn} sends ran between two event-loop turns`);
      await Promise.all(fired);
      await producer.flush();
      assert.equal(callbacks, ITERATIONS * L);
    } finally {
      await producer.close();
    }
  },
  300000,
);

(SKIP ? test.skip : test)(
  'queued hand-overs of a second caller do not keep the first from yielding',
  async () => {
    // Caller B's hand-overs are queued and complete inside caller A's pumps,
    // with no event-loop turn between them: they must not restart the count A's
    // sends taken at once reach the deferral with.
    const topic = freshTopic('yield-two-callers');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 5, 'max.block.ms': 1 } });
    try {
      // With max.block.ms 1 the first sends may time out before the metadata
      // arrives: retry until partition 0 is known.
      for (let i = 0; ; i++) {
        try {
          await (await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('warm') }) })).get();
          break;
        } catch (e) {
          if (i > 200) throw e;
          await new Promise((r) => setTimeout(r, 50));
        }
      }
      const SENDS = 300000;
      const value = Buffer.alloc(100, 0x78);
      let aDone = false;
      let bSends = 0;
      let bFirst = null;
      const watch = eventLoopWatch();
      // Partition 1000 does not exist: each hand-over is queued and resolves
      // about 1 ms later with a future failed with Kafka's TimeoutError.
      const b = (async () => {
        while (!aDone) {
          const future = await producer.send({
            record: new ProducerRecord({ topic, partition: 1000, key: null, value: Buffer.from('b') }),
          });
          // A failed future's get() rejects with ExecutionError, the delivery error its cause.
          const outcome = future.get().then(
            () => 'ok',
            (e) => (e instanceof ExecutionError && e.cause instanceof TimeoutError ? 'TimeoutError' : String(e)),
          );
          if (bFirst === null) bFirst = await outcome;
          bSends++;
        }
      })();
      for (let i = 0; i < SENDS; i++) {
        watch.sends++;
        await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value }) });
      }
      aDone = true;
      watch.stop();
      await b;
      assert.ok(bSends > 0, 'caller B never handed a record over');
      assert.equal(bFirst, 'TimeoutError');
      assert.ok(watch.turns > 0, 'the event loop never turned during the loop');
      assert.ok(watch.maxSendsPerTurn <= MAX_SENDS_PER_TURN, `${watch.maxSendsPerTurn} sends ran between two event-loop turns`);
      await producer.flush();
    } finally {
      await producer.close();
    }
  },
  300000,
);

(SKIP ? test.skip : test)(
  'send() runs the delivery callbacks already due before handing its record over',
  async () => {
    const { topic, producer } = await warmedProducer('pump');
    try {
      const SENDS = 50000;
      let inSend = false;
      let callbacks = 0;
      let callbacksInsideSend = 0;
      /** @param {Parameters<Callback>[0]} options */
      const callback = ({ metadata: m, exception: error }) => {
        assert.equal(error, null);
        callbacks++;
        if (inSend) callbacksInsideSend++;
      };
      for (let i = 0; i < SENDS; i++) {
        inSend = true;
        let sent;
        try {
          sent = producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(`v${i}`) }), callback });
        } finally {
          inSend = false;
        }
        await sent;
      }
      // Without the pump, callbacks run only from the threadsafe function's
      // call_js, outside every send() call.
      assert.ok(callbacksInsideSend > 0, 'no delivery callback ran inside a send() call');
      await producer.flush();
      assert.equal(callbacks, SENDS);
    } finally {
      await producer.close();
    }
  },
  300000,
);

(SKIP ? test.skip : test)(
  "partitionsFor resolves to the topic's partitions, PartitionInfo with Node values",
  async () => {
    const { Node, PartitionInfo } = require('@confluentinc/kafka-javascript/common');
    const topic = freshTopic('pf');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      // The first send creates the topic (auto-creation), with one partition.
      await (await producer.send(new ProducerRecord({ topic, value: Buffer.from('v') }))).get();
      const partitions = await producer.partitionsFor(topic);
      assert.ok(Array.isArray(partitions));
      assert.equal(partitions.length, 1);
      const [info] = partitions;
      assert.ok(info instanceof PartitionInfo);
      assert.equal(info.topic(), topic);
      assert.equal(info.partition(), 0);
      assert.ok(info.leader() instanceof Node);
      const leader = info.leader();
      const replicas = info.replicas();
      const inSyncReplicas = info.inSyncReplicas();
      assert.ok(leader && replicas && inSyncReplicas);
      assert.equal(leader.isEmpty, false);
      assert.deepEqual(
        replicas.map((n) => n.id),
        [leader.id],
      );
      assert.ok(replicas[0].equals(leader));
      assert.deepEqual(
        inSyncReplicas.map((n) => n.id),
        [leader.id],
      );
      assert.deepEqual(info.offlineReplicas(), []);
      assert.equal(
        info.toString(),
        `Partition(topic = ${topic}, partition = 0, leader = ${leader.id}, replicas = [${leader.id}], isr = [${leader.id}], offlineReplicas = [])`,
      );
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'metrics() counts the records sent, read again on each call',
  async () => {
    const { MetricName } = require('@confluentinc/kafka-javascript/common');
    const topic = freshTopic('metrics');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'client.id': 'js-it-metrics' } });
    try {
      const key = new MetricName({
        name: 'record-send-total',
        group: 'producer-metrics',
        description: '',
        tags: new Map([['client-id', 'js-it-metrics']]),
      });
      const before = producer.metrics().get(key);
      assert.ok(before);
      assert.equal(before.metricValue(), 0);
      for (let i = 0; i < 3; i++) await (await producer.send(new ProducerRecord({ topic, value: Buffer.from(`v${i}`) }))).get();
      const after = producer.metrics().get(key);
      assert.ok(after);
      assert.equal(after.metricValue(), 3);
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  "an error the core reports carries its payload: an invalid topic's InvalidTopicError lists it",
  async () => {
    const { InvalidTopicError } = require('@confluentinc/kafka-javascript/common/errors');
    // Java's Metadata.maybeThrowExceptionForTopic throws
    // InvalidTopicException(Collections.singleton(topic)) for the broker's
    // INVALID_TOPIC_EXCEPTION: the payload crosses the FFI through its view
    // (kafka_common_Error_invalid_topic, then its invalid_topics accessor).
    const topic = 'invalid topic name!';
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'max.block.ms': 30000 } });
    try {
      let error;
      try {
        await producer.partitionsFor(topic);
      } catch (e) {
        error = e;
      }
      assert.ok(error instanceof InvalidTopicError, String(error));
      assert.equal(error.message, `Invalid topics: [${topic}]`);
      assert.deepEqual(error.invalidTopics(), new Set([topic]));
      // Once the cluster lists the topic as invalid, the send path throws
      // Java's KafkaProducer#waitOnMetadata InvalidTopicException(topic): the
      // (String message) form, whose message is the topic and whose
      // invalidTopics() is empty.
      let sendError;
      try {
        await (await producer.send(new ProducerRecord({ topic, value: Buffer.from('v') }))).get();
      } catch (e) {
        sendError = e instanceof ExecutionError ? e.cause : e;
      }
      assert.ok(sendError instanceof InvalidTopicError, String(sendError));
      assert.equal(sendError.message, topic);
      assert.deepEqual(sendError.invalidTopics(), new Set());
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  "close() from inside a send callback closes at once as close({ timeout: 0 }), as Java's does",
  async () => {
    const { KafkaError } = require('@confluentinc/kafka-javascript/common');
    // Java's KafkaProducer#close: called from a Callback, it logs a warning and
    // closes as close(0) instead of joining its own I/O thread; the records
    // still in the accumulator fail with "Producer is closed forcefully.". Both
    // forms, untimed and timed.
    for (const timed of [false, true]) {
      const topic = freshTopic(timed ? 'close-in-cb-timed' : 'close-in-cb');
      // linger.ms holds the nested record in the accumulator well past the
      // close. The first record fetches the topic's metadata itself, so the
      // nested send needs no warm-up.
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 10000 } });
      /** @type {Promise<KafkaFuture<RecordMetadata>> | undefined} */
      let nested;
      /** @type {Promise<number> | undefined} */
      let closing;
      /** @type {Error | null | undefined} */
      let nestedException;
      // Each callback fires exactly once.
      let firstCalls = 0;
      let nestedCalls = 0;
      const first = await producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('first') }),
        callback: () => {
          firstCalls += 1;
          const sent = producer.send({
            record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('nested') }),
            callback: ({ exception }) => {
              nestedCalls += 1;
              nestedException = exception;
            },
          });
          // Handled at creation: a refusal by the closing producer fails this
          // test at its await below, not the whole file as an unhandled
          // rejection.
          sent.catch(() => {});
          nested = sent;
          const start = Date.now();
          const closed = (timed ? producer.close({ timeout: 60000 }) : producer.close()).then(() => Date.now() - start);
          // Handled at creation, awaited below.
          closed.catch(() => {});
          closing = closed;
        },
      });
      await first.get();
      assert.ok(closing && nested);
      // It does not wait for the lingering record, nor for its own callback.
      const elapsed = await closing;
      assert.ok(elapsed < 5000, `close() inside the callback took ${elapsed} ms`);
      const future = await nested;
      await assert.rejects(future.get(), (e) => {
        assert.ok(e instanceof ExecutionError);
        assert.ok(e.cause instanceof KafkaError);
        assert.equal(e.cause.message, 'Producer is closed forcefully.');
        return true;
      });
      assert.ok(nestedException instanceof KafkaError);
      assert.equal(nestedException.message, 'Producer is closed forcefully.');
      assert.equal(firstCalls, 1);
      assert.equal(nestedCalls, 1);
    }
  },
  120000,
);

// BaseProducerSendTest's and PlaintextProducerSendTest's lingering-record
// tests (Apache Kafka 4.4.0), with Java's createProducer(lingerMs =
// Int.MaxValue, deliveryTimeoutMs = Int.MaxValue): nothing leaves the
// accumulator until flush() or close() sends or aborts it. Java's topics have
// RF 2; the scratch broker auto-creates them with one partition, RF 1.
const INT_MAX = 2147483647;
const LINGERING = { 'linger.ms': INT_MAX, 'delivery.timeout.ms': INT_MAX };

/**
 * A topic of `what` that exists: one record sent and delivered by a producer
 * of its own, as Java creates its topics before the test.
 *
 * @param {string} what
 */
async function createdTopic(what) {
  const topic = freshTopic(what);
  const warmUp = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
  try {
    await (await warmUp.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('warm-up') }))).get();
  } finally {
    await warmUp.close();
  }
  return topic;
}

// BaseProducerSendTest.testFlush, which also carries the half of
// KafkaProducerTest.testFlushCompleteSendOfInflightBatches the test above
// leaves out: none is done before flush(), all are after it. Java's messages.
(SKIP ? test.skip : test)(
  'flush sends every lingering record: none is done before flush(), all are after, 50 rounds',
  async () => {
    const topic = freshTopic('flush-linger');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, ...LINGERING } });
    try {
      const record = new ProducerRecord({ topic, value: Buffer.from('value') });
      for (let round = 0; round < 50; round++) {
        /** @type {KafkaFuture<RecordMetadata>[]} */
        const futures = [];
        for (let i = 0; i < 100; i++) futures.push(await producer.send(record));
        assert.ok(
          futures.every((f) => !f.isDone()),
          'No request is complete.',
        );
        await producer.flush();
        assert.ok(
          futures.every((f) => f.isDone()),
          'All requests are complete.',
        );
      }
    } finally {
      await producer.close();
    }
  },
  300000,
);

// BaseProducerSendTest.testCloseWithZeroTimeoutFromCallerThread, carrying
// RecordAccumulatorTest.testAbortIncompleteBatches' assertions in its first
// round: close({ timeout: 0 }) aborts every lingering record with
// RecordAccumulator#abortIncompleteBatches' bare KafkaException("Producer is
// closed forcefully."), the callbacks first, and nothing reaches the broker.
(SKIP ? test.skip : test)(
  'close({ timeout: 0 }) fails every lingering record with a bare KafkaError, 50 producers',
  async () => {
    const { KafkaError } = require('@confluentinc/kafka-javascript/common');
    const topic = await createdTopic('close-zero');
    const { highWatermark: before } = await fetchRecords(BOOTSTRAP, topic, 0);
    const record = new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('value') });
    for (let round = 0; round < 50; round++) {
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, ...LINGERING } });
      /** @type {Array<Error | null>} */
      const exceptions = [];
      /** @type {KafkaFuture<RecordMetadata>[]} */
      const futures = [];
      for (let i = 0; i < 100; i++) {
        futures.push(
          await producer.send({
            record,
            callback: ({ exception }) => {
              exceptions.push(exception);
            },
          }),
        );
      }
      assert.ok(
        futures.every((f) => !f.isDone()),
        'No request is complete.',
      );
      await producer.close({ timeout: 0 });
      assert.equal(exceptions.length, 100);
      for (const [i, future] of futures.entries()) {
        const exception = exceptions[i];
        assert.ok(exception instanceof KafkaError);
        // Java: assertEquals(classOf[KafkaException], e.getCause.getClass).
        assert.equal(exception.constructor, KafkaError);
        assert.equal(exception.message, 'Producer is closed forcefully.');
        assert.equal(future.isDone(), true);
        assert.equal(future.isCompletedExceptionally(), true);
        await assert.rejects(future.get(), (e) => {
          assert.ok(e instanceof ExecutionError);
          assert.equal(/** @type {Error} */ (e.cause).constructor, KafkaError);
          assert.equal(/** @type {Error} */ (e.cause).message, 'Producer is closed forcefully.');
          return true;
        });
      }
    }
    // Nothing reached the broker: the public view of Java's
    // assertFalse(accum.hasUndrained()) after the abort.
    const { highWatermark } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.equal(highWatermark, before);
  },
  300000,
);

// BaseProducerSendTest.testCloseWithZeroTimeoutFromSenderThread: Java's
// CloseCallback calls close(0) and then close() from every record's callback
// ("The close call will be called by all the message callbacks. This tests
// idempotence of the close call." / "Test close with non zero timeout.
// Should not block at all."); the first record's callback first sends 100
// more ("Trigger another batch in accumulator before close the producer.
// These messages should not be sent."). The flushed records reach the broker.
(SKIP ? test.skip : test)(
  'close({ timeout: 0 }) and close() from the send callbacks neither block nor lose the records sent before, 50 rounds',
  async () => {
    const topic = await createdTopic('close-in-callbacks');
    const { highWatermark: base } = await fetchRecords(BOOTSTRAP, topic, 0);
    const record = new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('value') });
    for (let round = 0; round < 50; round++) {
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, ...LINGERING } });
      /** @type {Promise<unknown>[]} */
      const closes = [];
      /** @type {Promise<unknown>[]} */
      const nested = [];
      /** @param {boolean} sendRecords @returns {Callback} */
      const closeCallback = (sendRecords) => () => {
        if (sendRecords) {
          for (let i = 0; i < 100; i++) {
            const sent = producer.send(record);
            // Handled at creation; settled below.
            sent.catch(() => {});
            nested.push(sent);
          }
        }
        for (const closed of [producer.close({ timeout: 0 }), producer.close()]) {
          // Handled at creation, settled below.
          closed.catch(() => {});
          closes.push(closed);
        }
      };
      /** @type {KafkaFuture<RecordMetadata>[]} */
      const responses = [];
      for (let i = 0; i < 100; i++) responses.push(await producer.send({ record, callback: closeCallback(i === 0) }));
      assert.ok(
        responses.every((f) => !f.isDone()),
        'No request is complete.',
      );
      await producer.flush();
      assert.ok(
        responses.every((f) => f.isDone()),
        'All requests are complete.',
      );
      // Each close settles, none rejected, within the core's 30 s bound.
      const settled = await Promise.race([
        Promise.allSettled(closes),
        new Promise((resolve) => setTimeout(() => resolve('timeout'), 30000)),
      ]);
      assert.notEqual(settled, 'timeout', 'a close() from a callback blocked');
      for (const outcome of /** @type {PromiseSettledResult<unknown>[]} */ (settled)) assert.equal(outcome.status, 'fulfilled');
      assert.equal(closes.length, 200);
      // The nested sends reached a closing producer, each handled when made.
      await Promise.allSettled(nested);
      const { highWatermark } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.ok(
        highWatermark >= base + 100 * (round + 1),
        `Consumed ${highWatermark - base} records before timeout instead of the expected ${100 * (round + 1)} records`,
      );
      await producer.close();
    }
  },
  600000,
);

// BaseProducerSendTest.testSendNonCompressedMessageWithCreateTime and
// testSendCompressedMessageWithCreateTime (gzip): 100 lingering records keep
// their CreateTime through close(20 s). Java asserts inside onCompletion; the
// callback records, the test asserts. Java checks the metadata only, so the
// gzip case needs no read-back.
for (const codec of ['none', 'gzip']) {
  (SKIP ? test.skip : test)(
    `CreateTime: 100 lingering records keep their timestamps through close({ timeout: 20000 }), compression ${codec}`,
    async () => {
      const topic = freshTopic(`create-time-${codec}`);
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'compression.type': codec, ...LINGERING } });
      const baseTimestamp = 123456;
      /** @type {string[]} */
      const failures = [];
      let offset = 0;
      let timestampDiff = 1;
      /** @param {{ metadata: RecordMetadata, exception: Error | null }} outcome */
      const callback = ({ metadata, exception }) => {
        if (exception !== null) failures.push(`Send callback returns the following exception: ${exception}`);
        else checkMetadata(metadata);
      };
      /** @param {RecordMetadata} metadata */
      const checkMetadata = (metadata) => {
        if (metadata.offset !== BigInt(offset)) failures.push(`offset ${metadata.offset}, expected ${offset}`);
        if (metadata.topic !== topic) failures.push(`topic ${metadata.topic}`);
        if (metadata.timestamp !== BigInt(baseTimestamp + timestampDiff)) failures.push(`timestamp ${metadata.timestamp}`);
        if (metadata.partition !== 0) failures.push(`partition ${metadata.partition}`);
        offset += 1;
        timestampDiff += 1;
      };
      /** @type {Array<[KafkaFuture<RecordMetadata>, number]>} */
      const sent = [];
      for (let i = 1; i <= 100; i++) {
        const timestamp = baseTimestamp + i;
        const record = new ProducerRecord({
          topic,
          partition: 0,
          timestamp,
          key: Buffer.from(`key${i}`),
          value: Buffer.from(`value${i}`),
        });
        sent.push([await producer.send({ record, callback }), timestamp]);
      }
      await producer.close({ timeout: 20000 });
      for (const [future, timestamp] of sent) assert.equal((await future.get()).timestamp, BigInt(timestamp));
      assert.deepEqual(failures, []);
      assert.equal(offset, 100, `Should have offset 100 but only successfully sent ${offset}`);
    },
    120000,
  );
}

// PlaintextProducerSendTest.testBatchSizeZero: batch.size 0 with lingering
// records; close(20 s) sends them all, in order (sendAndVerify).
(SKIP ? test.skip : test)(
  'batch.size 0 with linger.ms Int.MaxValue: close({ timeout: 20000 }) delivers every record in order',
  async () => {
    const topic = freshTopic('batch-zero');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, ...LINGERING, 'batch.size': 0 } });
    /** @type {KafkaFuture<RecordMetadata>[]} */
    const futures = [];
    for (let i = 1; i <= 100; i++) {
      futures.push(
        await producer.send(new ProducerRecord({ topic, partition: 0, key: Buffer.from(`key${i}`), value: Buffer.from(`value${i}`) })),
      );
    }
    await producer.close({ timeout: 20000 });
    let lastOffset = 0;
    for (const [index, future] of futures.entries()) {
      const m = await future.get();
      assert.equal(m.topic, topic);
      assert.equal(m.partition, 0);
      assert.equal(m.offset, BigInt(index));
      lastOffset += 1;
    }
    assert.equal(lastOffset, 100);
  },
  120000,
);

/**
 * Java's TestUtils.computeUntilTrue (15 000 ms, 100 ms pause) over a send:
 * until its future is pending, or done and successful. Returns the last one.
 *
 * @param {() => Promise<KafkaFuture<RecordMetadata>>} send
 */
async function sendUntilQueued(send) {
  const deadline = Date.now() + 15000;
  for (;;) {
    const future = await send();
    if (!future.isDone() || !future.isCompletedExceptionally()) return future;
    if (Date.now() >= deadline) return future;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/** @param {KafkaFuture<RecordMetadata>} future @param {string} topic */
async function verifySendSuccess(future, topic) {
  const m = await future.get({ timeout: 30000 });
  assert.equal(m.topic, topic);
  assert.equal(m.partition, 0);
  assert.ok(m.offset >= 0n, `Invalid offset ${m}`);
}

// PlaintextProducerSendTest.testNonBlockingProducer: with max.block.ms 0 a
// send fails at once, with Java's exact classes: Kafka's TimeoutException
// without metadata, BufferExhaustedException with a full buffer. Both are
// ApiExceptions, so send() resolves with a done, failed future.
(SKIP ? test.skip : test)(
  'max.block.ms 0: a send fails at once without metadata, and with a full buffer',
  async () => {
    const { BufferExhaustedError } = require('@confluentinc/kafka-javascript/producer');
    const topic = freshTopic('non-blocking');
    /** @param {KafkaProducer<Buffer, Buffer>} p */
    const send = (p) => p.send(new ProducerRecord({ topic, partition: 0, key: Buffer.from('key'), value: Buffer.alloc(1000) }));
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'max.block.ms': 0 } });
    const producer2 = new KafkaProducer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'max.block.ms': 0, 'linger.ms': 15000, 'batch.size': 1100, 'buffer.memory': 1500 },
    });
    try {
      // The topic has no metadata yet: the first send fails at once.
      const f = await send(producer);
      assert.equal(f.isDone(), true, 'Send should be completed immediately');
      await assert.rejects(f.get(), (e) => {
        assert.ok(e instanceof ExecutionError);
        assert.equal(/** @type {Error} */ (e.cause).constructor, TimeoutError);
        return true;
      });
      await verifySendSuccess(await sendUntilQueued(() => send(producer)), topic);
      // The first record fills the buffer: the second needs a new batch that
      // does not fit.
      const future2 = await sendUntilQueued(() => send(producer2));
      const f2 = await send(producer2);
      assert.equal(f2.isDone(), true, 'Send should be completed immediately');
      await assert.rejects(f2.get(), (e) => {
        assert.ok(e instanceof ExecutionError);
        assert.equal(/** @type {Error} */ (e.cause).constructor, BufferExhaustedError);
        assert.ok(/** @type {Error} */ (e.cause).message.startsWith('Failed to allocate '), /** @type {Error} */ (e.cause).message);
        return true;
      });
      await producer2.flush();
      await verifySendSuccess(future2, topic);
    } finally {
      await producer.close();
      await producer2.close();
    }
  },
  120000,
);

/**
 * A producer of `configs` whose metadata cache holds `topic`, which exists:
 * partitionsFor retried until it resolves, as each try has only max.block.ms.
 *
 * @param {Record<string, unknown>} configs
 * @param {string} topic
 */
async function producerKnowing(configs, topic) {
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, ...configs } });
  const deadline = Date.now() + 30000;
  for (;;) {
    try {
      await producer.partitionsFor(topic);
      return producer;
    } catch (e) {
      if (Date.now() >= deadline) {
        // The caller gets no producer to close.
        await producer.close({ timeout: 0 });
        throw e;
      }
    }
  }
}

/**
 * A producer of `configs` that knows `topic` and has its producer id: one
 * record delivered to partition 0. An idempotent producer, the default, sends
 * no record before its InitProducerId completes, and so expires none either
 * (org.apache.kafka.clients.producer.internals.Sender#runOnce returns after
 * maybeSendAndPollTransactionalRequest while the id is pending, before
 * sendProducerData): a broker paused before then holds every record until it
 * is back, in Java as here. The tests that pause the broker start from this.
 *
 * @param {Record<string, unknown>} configs
 * @param {string} topic
 */
async function producerWithId(configs, topic) {
  const producer = await producerKnowing(configs, topic);
  try {
    await (
      await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('first') }))
    ).get({ timeout: 30000 });
  } catch (e) {
    await producer.close({ timeout: 0 });
    throw e;
  }
  return producer;
}

/** @param {string} topic */
const kiloRecord = (topic) => new ProducerRecord({ topic, partition: 0, key: Buffer.from('key'), value: Buffer.alloc(1000) });

// BufferPoolTest.testBufferExhaustedExceptionIsThrown and testBlockTimeout,
// through the public API with testNonBlockingProducer's sizes: a send that
// cannot get a 1100-byte batch from the 400 bytes left fails after
// max.block.ms with BufferPool#allocate's message. BufferExhaustedException
// extends TimeoutException, an ApiException: send() resolves with a failed
// future and calls the callback.
//
// While a send waits for memory the pool has a waiter, which makes the
// lingering batch sendable (RecordAccumulator#ready's `exhausted`, in Java as
// in the core): a Sender that checks it within max.block.ms drains the batch,
// the memory is freed and the send succeeds. Java's own public-API test uses
// max.block.ms 0 to avoid that race. Here the second send comes once the
// Sender, woken by the first batch, is back in its linger wait; a try that
// still drains is retried with a new producer (Java's computeUntilTrue idea),
// and the assertions run on the try that waited.
/**
 * The flow of BufferPoolTest.testBufferExhaustedExceptionIsThrown and
 * testBlockTimeout (above): asserts all of it but the metric, and returns
 * buffer-exhausted-total as read after the drop.
 */
async function exhaustTheBuffer() {
  const { BufferExhaustedError } = require('@confluentinc/kafka-javascript/producer');
  const { MetricName } = require('@confluentinc/kafka-javascript/common');
  const topic = await createdTopic('exhausted');
  for (let attempt = 1; ; attempt++) {
    const producer = await producerKnowing(
      { 'client.id': 'js-it-exhausted', 'max.block.ms': 500, 'linger.ms': 15000, 'batch.size': 1100, 'buffer.memory': 1500 },
      topic,
    );
    try {
      const first = await producer.send(kiloRecord(topic));
      assert.equal(first.isDone(), false);
      // The Sender, woken by the new batch, goes back to its linger wait.
      await new Promise((resolve) => setTimeout(resolve, 300));
      /** @type {Array<Error | null>} */
      const calls = [];
      const started = Date.now();
      const second = await producer.send({ record: kiloRecord(topic), callback: ({ exception }) => calls.push(exception) });
      const waited = Date.now() - started;
      if (!second.isDone() && attempt < 20) continue; // the batch drained: memory was freed (see above)
      assert.equal(second.isDone(), true);
      assert.equal(second.isCompletedExceptionally(), true);
      assert.equal(calls.length, 1);
      await assert.rejects(second.get(), (e) => {
        assert.ok(e instanceof ExecutionError);
        const cause = /** @type {Error} */ (e.cause);
        assert.equal(cause.constructor, BufferExhaustedError);
        assert.ok(cause instanceof TimeoutError);
        // Kafka's TimeoutError of common/errors, never the root one, which a
        // timed get() rejects with.
        assert.ok(!(cause instanceof RootTimeoutError));
        assert.equal(cause, calls[0]);
        assert.equal(
          cause.message,
          'Failed to allocate 1100 bytes within the configured max blocking time 500 ms. Total memory: 1500 bytes. Available memory: 400 bytes. Poolable size: 1100 bytes',
        );
        return true;
      });
      assert.ok(waited >= 500, 'BufferExhaustedException should not throw before maxBlockTimeMs');
      assert.ok(waited < 500 + 1000, 'BufferExhaustedException should throw soon after maxBlockTimeMs');
      const exhausted = new MetricName({
        name: 'buffer-exhausted-total',
        group: 'producer-metrics',
        description: '',
        tags: new Map([['client-id', 'js-it-exhausted']]),
      });
      const exhaustedTotal = producer.metrics().get(exhausted)?.metricValue();
      await producer.flush();
      await verifySendSuccess(first, topic);
      return exhaustedTotal;
    } finally {
      await producer.close({ timeout: 0 });
    }
  }
}

(SKIP ? test.skip : test)(
  'a send that cannot get buffer memory within max.block.ms fails with BufferExhaustedError, after max.block.ms',
  async () => {
    await exhaustTheBuffer();
  },
  180000,
);

// BufferPool#awaitMemory records buffer-exhausted-total once for the dropped
// send. Waits on the core (DECISIONS.md, entry 31, a core issue): its FFI
// first tries a send without waiting (KafkaProducer::try_send, Rust-only), on
// the calling thread and again on its submission task, and each try's
// zero-budget BufferPool.allocate records an exhaustion too, so one dropped
// send counts 3. Remove the skip when the core records one exhaustion per
// dropped send (its non-waiting tries record none).
test.skip('the dropped send counts once in buffer-exhausted-total, as BufferPool#awaitMemory records it', async () => {
  assert.equal(await exhaustTheBuffer(), 1);
});

// BufferPoolTest.testCloseNotifyWaiters: BufferPool#close wakes every send
// waiting for memory, which BufferPool#awaitMemory fails with a bare
// KafkaException("Producer closed while allocating memory"); not an
// ApiException, so KafkaProducer#doSend rethrows it: send() rejects and the
// callback is not called. Java's numWorkers = 2. As above, a try whose
// lingering batch drains (freeing the memory) is retried with a new producer
// until both sends are still waiting when close() runs.
(SKIP ? test.skip : test)(
  'close() releases the sends waiting for buffer memory with KafkaError "Producer closed while allocating memory"',
  async () => {
    const { KafkaError } = require('@confluentinc/kafka-javascript/common');
    const topic = await createdTopic('close-waiters');
    for (let attempt = 1; ; attempt++) {
      const producer = await producerKnowing(
        { 'max.block.ms': 60000, 'linger.ms': 15000, 'batch.size': 1100, 'buffer.memory': 1500 },
        topic,
      );
      let closed = false;
      try {
        const first = await producer.send(kiloRecord(topic));
        // The Sender, woken by the new batch, goes back to its linger wait.
        await new Promise((resolve) => setTimeout(resolve, 300));
        let callbacks = 0;
        let settled = 0;
        const waiting = [0, 1].map(() => {
          const sent = producer.send({ record: kiloRecord(topic), callback: () => callbacks++ });
          sent.then(
            () => settled++,
            () => settled++,
          );
          return sent;
        });
        await new Promise((resolve) => setTimeout(resolve, 200));
        if (settled !== 0 && attempt < 20) {
          await Promise.allSettled(waiting);
          continue; // the batch drained: memory was freed (see above)
        }
        assert.equal(settled, 0);
        const started = Date.now();
        closed = true;
        await producer.close();
        for (const sent of waiting) {
          await assert.rejects(sent, (e) => {
            assert.ok(e instanceof KafkaError);
            assert.equal(e.constructor, KafkaError);
            assert.equal(e.message, 'Producer closed while allocating memory');
            return true;
          });
        }
        assert.equal(callbacks, 0);
        assert.ok(Date.now() - started < 30000, `close took ${Date.now() - started} ms`);
        // close() without a timeout sends what the accumulator holds.
        await first.get();
        return;
      } finally {
        // Every try's producer is closed, the retried ones and a failed one.
        if (!closed) await producer.close({ timeout: 0 });
      }
    }
  },
  600000,
);

// KafkaProducerTest.testFlushMeasureLatency: Java's MockTime(1) moves its
// clock on every read, so each of its flushes takes time, although nothing
// is pending. The real clock moves only while a flush does something, and an
// empty one can take less than one of its ticks, so each flush here sends a
// record lingering in the accumulator (linger.ms holds it well past the test)
// and waits for its produce response. metrics() is a snapshot, so it is read
// after each flush.
(SKIP ? test.skip : test)(
  'flush() records flush-time-ns-total, which grows with each flush',
  async () => {
    const { MetricName } = require('@confluentinc/kafka-javascript/common');
    const topic = await newTopic('flush-time');
    const producer = new KafkaProducer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'client.id': 'js-it-flush-time', 'linger.ms': 600000 },
    });
    const key = new MetricName({
      name: 'flush-time-ns-total',
      group: 'producer-metrics',
      description: '',
      tags: new Map([['client-id', 'js-it-flush-time']]),
    });
    try {
      assert.equal(producer.metrics().get(key)?.metricValue(), 0);
      /** @type {number[]} */
      const totals = [];
      for (const value of ['first', 'second']) {
        // send() resolves once the record lingers in the accumulator, its
        // topic's metadata fetched; the flush sends it and waits for it.
        const future = await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(value) }));
        assert.equal(future.isDone(), false, 'the record lingers until the flush');
        await producer.flush();
        assert.equal(future.isDone(), true);
        totals.push(/** @type {number} */ (producer.metrics().get(key)?.metricValue()));
      }
      assert.ok(totals[0] > 0, `the first flush recorded ${totals[0]} ns`);
      assert.ok(totals[1] > totals[0], `the total grew from ${totals[0]} to ${totals[1]} ns`);
    } finally {
      await producer.close();
    }
  },
  120000,
);

// KafkaProducerMetricsTest.shouldRecordMetadataWaitTime, through the public
// API: Java records an exact value through the internal recorder; here a
// send's wait for a fresh topic's metadata makes it grow, and a send with the
// metadata cached adds nothing (KafkaProducer#waitOnMetadata returns before
// its loop when the partition count is known).
(SKIP ? test.skip : test)(
  'metadata-wait-time-ns-total counts a send waiting for a fresh topic metadata, and only that',
  async () => {
    const { MetricName } = require('@confluentinc/kafka-javascript/common');
    const { KafkaMetric } = require('@confluentinc/kafka-javascript/common/metrics');
    const topic = freshTopic('metadata-wait');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'client.id': 'js-it-metadata-wait' } });
    const key = new MetricName({
      name: 'metadata-wait-time-ns-total',
      group: 'producer-metrics',
      description: '',
      tags: new Map([['client-id', 'js-it-metadata-wait']]),
    });
    try {
      const initial = producer.metrics().get(key);
      assert.ok(initial instanceof KafkaMetric);
      assert.equal(initial.metricValue(), 0);
      await (await producer.send(new ProducerRecord({ topic, value: Buffer.from('v') }))).get();
      const waited = /** @type {number} */ (producer.metrics().get(key)?.metricValue());
      assert.ok(waited > 0, `${waited}`);
      await (await producer.send(new ProducerRecord({ topic, value: Buffer.from('v') }))).get();
      assert.equal(producer.metrics().get(key)?.metricValue(), waited);
    } finally {
      await producer.close();
    }
  },
  120000,
);

// ProducerFailureHandlingTest.testSendAfterClosed, Java's three acks
// settings, each producer closed after a delivered send ("first send a
// message to make sure the metadata is refreshed"). The gRPC arm cannot
// show this (its server forgets a closed producer).
(SKIP ? test.skip : test)(
  'send() after close() rejects with IllegalStateError, for acks 0, 1 and -1',
  async () => {
    const { IllegalStateError } = require('@confluentinc/kafka-javascript');
    const { KafkaError } = require('@confluentinc/kafka-javascript/common');
    const topic = freshTopic('send-after-closed');
    const producers = ['0', '1', '-1'].map(
      (acks) =>
        new KafkaProducer({
          configs: {
            'bootstrap.servers': BOOTSTRAP,
            acks,
            retries: 0,
            'request.timeout.ms': 30000,
            'max.block.ms': 10000,
            'buffer.memory': 30000,
          },
        }),
    );
    const record = new ProducerRecord({ topic, partition: null, key: Buffer.from('key'), value: Buffer.from('value') });
    for (const p of producers) await (await p.send(record)).get();
    for (const p of producers) {
      await p.close();
      await assert.rejects(p.send(record), (e) => {
        assert.ok(e instanceof IllegalStateError && !(e instanceof KafkaError), `${e}`);
        assert.equal(e.message, 'Cannot perform operation after producer has been closed');
        return true;
      });
    }
  },
  120000,
);

// FutureRecordMetadataTest.testFutureGetWithSeconds: a Temporal.Duration of
// one second is passed on as 1000 ms, and no JavaScript timer runs (the core
// enforces the deadline). The message is the client's recorded deviation
// (src/common/kafka_future.ts remarks: Java's is "Timeout after waiting for
// 1000 ms."). The chained half needs a mocked ProduceRequestResult: N/A.
(SKIP ? test.skip : test)(
  "a record's future takes a Temporal.Duration timeout: one second waits 1000 ms",
  async () => {
    const { Temporal } = require('@js-temporal/polyfill');
    const topic = freshTopic('future-seconds');
    // Well under the test's 120 s: a stalled metadata wait or delivery fails
    // with the client's own error, not the test's timeout.
    const producer = new KafkaProducer({
      configs: {
        'bootstrap.servers': BOOTSTRAP,
        'linger.ms': 5000,
        'max.block.ms': 30000,
        'request.timeout.ms': 10000,
        'delivery.timeout.ms': 30000,
      },
    });
    try {
      await (await producer.send(new ProducerRecord({ topic, value: Buffer.from('warm-up') }))).get();
      const future = await producer.send(new ProducerRecord({ topic, value: Buffer.from('v') }));
      assert.equal(future.isDone(), false);
      const realSetTimeout = globalThis.setTimeout;
      let timers = 0;
      globalThis.setTimeout = /** @type {typeof setTimeout} */ (
        /** @type {unknown} */ (
          (/** @type {Parameters<typeof setTimeout>} */ ...args) => {
            timers++;
            return realSetTimeout(...args);
          }
        )
      );
      try {
        const started = Date.now();
        await assert.rejects(future.get({ timeout: Temporal.Duration.from({ seconds: 1 }) }), (e) => {
          assert.equal(/** @type {Error} */ (e).constructor, RootTimeoutError);
          assert.equal(/** @type {Error} */ (e).message, 'Timed out waiting for KafkaFuture after 1000 ms');
          return true;
        });
        assert.ok(Date.now() - started >= 900, `${Date.now() - started} ms`);
      } finally {
        globalThis.setTimeout = realSetTimeout;
      }
      assert.equal(timers, 0);
      const metadata = await future.get();
      assert.equal(await future.get(Temporal.Duration.from({ seconds: 1 })), metadata);
    } finally {
      await producer.close();
    }
  },
  120000,
);

// The situations below go beyond Java's own tests: what a JS caller can do
// to the bytes, the headers, the callbacks and the producer's lifetime, and
// what a failing broker does to them. They use the protocol helper's topic
// creation and container control (_fetch_records.js); a test that pauses or
// restarts the broker runs only when the Makefile started it.

const broker = require('./_fetch_records');
const CONTAINER = broker.brokerContainer();
const SKIP_CONTAINER = SKIP || CONTAINER === null;

/**
 * A new topic with `partitions` partitions and `configs`, every partition led.
 *
 * @param {string} what
 * @param {{ partitions?: number, configs?: Record<string, string> }} [options]
 */
async function newTopic(what, options) {
  const topic = freshTopic(what);
  await broker.createTopic(BOOTSTRAP, topic, options);
  return topic;
}

/**
 * A promise that resolves after `ms` milliseconds.
 *
 * @param {number} ms
 */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

(SKIP ? test.skip : test)(
  'an empty key and value reach the broker as empty, not null; a null key and value as null',
  async () => {
    const topic = await newTopic('empty-null');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      const empty = await (
        await producer.send(new ProducerRecord({ topic, partition: 0, key: Buffer.alloc(0), value: new Uint8Array(0) }))
      ).get();
      const nulls = await (await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: null }))).get();
      assert.equal(empty.serializedKeySize, 0);
      assert.equal(empty.serializedValueSize, 0);
      assert.equal(nulls.serializedKeySize, -1);
      assert.equal(nulls.serializedValueSize, -1);
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.equal(records.length, 2);
      assert.ok(records[0].key !== null && records[0].value !== null, 'the empty key and value stay present');
      assert.equal(records[0].key.length, 0);
      assert.equal(records[0].value.length, 0);
      assert.equal(records[1].key, null);
      assert.equal(records[1].value, null, 'a null value is a tombstone');
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'acks 0 gives metadata with no offset; acks 1 and all give the offset',
  async () => {
    const topic = await newTopic('acks');
    for (const acks of ['0', '1', 'all']) {
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks, 'enable.idempotence': acks === 'all' } });
      try {
        const metadata = await (
          await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(acks) }))
        ).get();
        assert.equal(metadata.topic, topic);
        assert.equal(metadata.partition, 0, `acks ${acks}`);
        if (acks === '0') {
          assert.equal(metadata.hasOffset, false);
          assert.equal(metadata.offset, -1n);
        } else {
          assert.equal(metadata.hasOffset, true, `acks ${acks}`);
          assert.ok(metadata.offset >= 0n, `acks ${acks}: offset ${metadata.offset}`);
        }
      } finally {
        await producer.close();
      }
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a record over max.request.size or buffer.memory goes to the callback with RecordTooLargeError and Java's text; send() resolves",
  async () => {
    const { RecordTooLargeError } = require('@confluentinc/kafka-javascript/common/errors');
    const topic = await newTopic('too-large');
    const cases = [
      {
        configs: { 'max.request.size': 1048576 },
        message:
          /^The message is (\d+) bytes when serialized which is larger than 1048576, which is the value of the max\.request\.size configuration\.$/,
      },
      {
        configs: { 'max.request.size': 4194304, 'buffer.memory': 1048576 },
        message:
          /^The message is (\d+) bytes when serialized which is larger than the total memory buffer you have configured with the buffer\.memory configuration\.$/,
      },
    ];
    for (const { configs, message } of cases) {
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, ...configs } });
      try {
        /** @type {Array<Error | null>} */
        const calls = [];
        const future = await producer.send({
          record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.alloc(1048577) }),
          callback: ({ exception }) => void calls.push(exception),
        });
        await assert.rejects(future.get(), (e) => {
          assert.ok(e instanceof ExecutionError);
          const cause = /** @type {Error} */ (e.cause);
          assert.equal(cause.constructor, RecordTooLargeError);
          const match = message.exec(cause.message);
          assert.ok(match, cause.message);
          assert.ok(Number(match[1]) > 1048577, cause.message);
          assert.equal(calls.length, 1);
          assert.equal(calls[0], cause);
          return true;
        });
        assert.equal(future.isDone(), true);
        assert.equal(future.isCompletedExceptionally(), true);
      } finally {
        await producer.close();
      }
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  '10,000 sends started without awaiting each, awaited together: each record lands once and each callback runs once',
  async () => {
    const topic = await newTopic('together', { partitions: 3 });
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      const count = 10000;
      const callbacks = new Array(count).fill(0);
      const sends = [];
      for (let i = 0; i < count; i++) {
        sends.push(
          producer.send({
            record: new ProducerRecord({ topic, key: null, value: Buffer.from(`v${i}`) }),
            callback: () => void callbacks[i]++,
          }),
        );
      }
      const futures = await Promise.all(sends);
      const metadata = await Promise.all(futures.map((f) => f.get()));
      await producer.flush();
      assert.ok(futures.every((f) => f.isDone() && !f.isCompletedExceptionally()));
      assert.deepEqual(
        callbacks.filter((n) => n !== 1),
        [],
      );
      const values = [];
      for (let partition = 0; partition < 3; partition++) {
        for (const r of (await fetchRecords(BOOTSTRAP, topic, partition)).records) values.push(String(r.value));
      }
      assert.equal(values.length, count);
      assert.equal(new Set(values).size, count);
      assert.equal(new Set(metadata.map((m) => `${m.partition}:${m.offset}`)).size, count);
    } finally {
      await producer.close();
    }
  },
  180000,
);

(SKIP ? test.skip : test)(
  "LogAppendTime: the metadata's timestamp is the broker's append time, and the batch carries the LogAppendTime attribute",
  async () => {
    const topic = await newTopic('log-append', { configs: { 'message.timestamp.type': 'LogAppendTime' } });
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      const before = BigInt(Date.now());
      const metadata = await (
        await producer.send(new ProducerRecord({ topic, partition: 0, timestamp: 1000, key: null, value: Buffer.from('v') }))
      ).get();
      const after = BigInt(Date.now());
      assert.equal(metadata.hasTimestamp, true);
      assert.notEqual(metadata.timestamp, 1000n);
      assert.ok(metadata.timestamp >= before - 5000n && metadata.timestamp <= after + 5000n, `timestamp ${metadata.timestamp}`);
      const { batches } = await broker.fetchBatches(BOOTSTRAP, topic, 0);
      assert.equal(batches.length, 1);
      assert.equal(batches[0].logAppendTime, true);
      assert.equal(batches[0].maxTimestamp, metadata.timestamp);
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'a send waiting for a partition the metadata lacks does not hold back a later send; it lands once the topic grows',
  async () => {
    const topic = await newTopic('grow');
    const producer = await producerKnowing({ 'max.block.ms': 60000 }, topic);
    try {
      let firstCallbacks = 0;
      let secondCallbacks = 0;
      let firstSettled = false;
      const first = producer.send({
        record: new ProducerRecord({ topic, partition: 1, key: null, value: Buffer.from('to-partition-1') }),
        callback: () => void firstCallbacks++,
      });
      void first.then(() => (firstSettled = true));
      const second = producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('to-partition-0') }),
        callback: () => void secondCallbacks++,
      });
      const secondMetadata = await (await second).get({ timeout: 10000 });
      assert.equal(secondMetadata.partition, 0);
      assert.equal(firstSettled, false, 'the send to partition 1 still waits for its metadata');
      await broker.createPartitions(BOOTSTRAP, topic, 2);
      const [firstFuture] = await Promise.all([first, second]);
      const firstMetadata = await firstFuture.get({ timeout: 60000 });
      assert.equal(firstMetadata.partition, 1);
      assert.equal(firstCallbacks, 1);
      assert.equal(secondCallbacks, 1);
    } finally {
      await producer.close();
    }
  },
  180000,
);

(SKIP ? test.skip : test)(
  'a view at an offset of a large ArrayBuffer and a Buffer over a SharedArrayBuffer reach the broker as they were at the call',
  () => {
    const topic = freshTopic('views');
    const script = `
    const { Worker } = require('node:worker_threads');
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 5000 } });
    const settle = async () => {
      for (let i = 0; i < 5; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 50));
      }
    };
    (async () => {
      await (await producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: null, value: Buffer.from('warm-up') }))).get();
      const backing = new ArrayBuffer(1024 * 1024);
      new Uint8Array(backing).fill(0x2e);
      const view = new Uint8Array(backing, 700000, 1000);
      view.fill(0x56);
      const shared = new SharedArrayBuffer(4096);
      const refs = [];
      const future = await (async () => {
        const sharedBuffer = Buffer.from(shared, 0, 4096);
        sharedBuffer.fill(0x41);
        refs.push(new WeakRef(sharedBuffer));
        return producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: view, value: sharedBuffer }));
      })();
      // send() settled: another thread may now change the shared memory, and
      // this one the view.
      await new Promise((resolve, reject) => {
        const w = new Worker('const { workerData } = require("node:worker_threads"); new Uint8Array(workerData).fill(0x42);', { eval: true, workerData: shared });
        w.on('exit', resolve);
        w.on('error', reject);
      });
      view.fill(0x57);
      await settle();
      console.log('done before delivery', future.isDone());
      console.log('shared Buffer alive', refs.filter((r) => r.deref() !== undefined).length);
      const metadata = await future.get();
      console.log('sizes', metadata.serializedKeySize, metadata.serializedValueSize);
      await producer.close();
    })();
  `;
    const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 90000,
      env: { ...process.env, ...POISON, BOOTSTRAP, TOPIC: topic },
    });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, 'done before delivery false\nshared Buffer alive 0\nsizes 1000 4096\n');
    return fetchRecords(BOOTSTRAP, topic, 0).then(({ records }) => {
      assert.equal(records.length, 2);
      assert.deepEqual(records[1].key, Buffer.alloc(1000, 0x56));
      assert.deepEqual(records[1].value, Buffer.alloc(4096, 0x41));
    });
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a serializer's fresh Buffer is borrowed until send() resolves and released before delivery; the broker gets its bytes",
  () => {
    const topic = freshTopic('serializer-borrow');
    const script = `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    let collected = 0;
    const registry = new FinalizationRegistry(() => collected++);
    let made = 0;
    const valueSerializer = {
      serialize: ({ data }) => {
        if (data === null) return null;
        const out = Buffer.alloc(1000, String(data).charCodeAt(0));
        registry.register(out, made++);
        return out;
      },
    };
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 5000 }, keySerializer: null, valueSerializer });
    const settle = async () => {
      for (let i = 0; i < 5; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 50));
      }
    };
    (async () => {
      await (await producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: null, value: 'w' }))).get();
      await settle();
      const base = collected;
      const futures = [];
      for (const v of ['a', 'b', 'c']) futures.push(await producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: null, value: v })));
      await settle();
      console.log('done before delivery', futures.some((f) => f.isDone()));
      console.log('collected', collected - base, 'of', made - 1);
      for (const f of futures) await f.get();
      await producer.close();
    })();
  `;
    const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 90000,
      env: { ...process.env, ...POISON, BOOTSTRAP, TOPIC: topic },
    });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, 'done before delivery false\ncollected 3 of 3\n');
    return fetchRecords(BOOTSTRAP, topic, 0).then(({ records }) => {
      assert.deepEqual(
        records.slice(1).map((rec) => rec.value),
        ['a', 'b', 'c'].map((v) => Buffer.alloc(1000, v.charCodeAt(0))),
      );
    });
  },
  120000,
);

(SKIP ? test.skip : test)(
  "50 headers with repeated keys, a null value, an empty value, an emoji key and a lone surrogate key arrive in order, the keys as Java's UTF-8",
  async () => {
    const topic = await newTopic('headers-fidelity');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      /** @type {Array<[string, Buffer | null]>} */
      const given = [];
      for (let i = 0; i < 44; i++) given.push([`k${i % 5}`, Buffer.from(`v${i}`)]);
      given.push(['null-value', null], ['empty-value', Buffer.alloc(0)], ['\u{1F680}', Buffer.from('rocket')]);
      given.push(['a\uD800b', Buffer.from('lone')], ['k0', Buffer.from('last')], ['été', Buffer.from('accents')]);
      assert.equal(given.length, 50);
      const headers = given.map(([key, value]) => new Header({ key, value }));
      await (await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('v'), headers }))).get();
      const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
      assert.equal(records.length, 1);
      const got = records[0].headers;
      assert.deepEqual(
        got.map((h) => h.keyBytes),
        given.map(([key]) => Buffer.from(key.replace('\uD800', '?'), 'utf8')),
      );
      assert.deepEqual(
        got.map((h) => h.value),
        given.map(([, value]) => value),
      );
      assert.equal(got[44].value, null, 'a null header value arrives with length -1');
      assert.ok(got[45].value !== null && got[45].value.length === 0, 'an empty header value arrives with length 0');
    } finally {
      await producer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'an async serializer keeps the order of awaited sends; 500 sends in flight at once all deliver once',
  async () => {
    const topic = await newTopic('async-serializer');
    let seed = 42;
    const random = () => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return seed / 2147483648;
    };
    /** @type {Serializer<string>} */
    const valueSerializer = {
      serialize: ({ data }) =>
        new Promise((resolve) => setTimeout(() => resolve(data === null ? null : Buffer.from(data)), Math.floor(random() * 6))),
    };
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP }, keySerializer: null, valueSerializer });
    try {
      const futures = [];
      for (let i = 0; i < 500; i++)
        futures.push(await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: `a${i}` })));
      await Promise.all(futures.map((f) => f.get()));
      const inFlight = await Promise.all(
        Array.from({ length: 500 }, (_, i) => producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: `b${i}` }))),
      );
      await Promise.all(inFlight.map((f) => f.get()));
      const values = (await fetchRecords(BOOTSTRAP, topic, 0)).records.map((r) => String(r.value));
      assert.deepEqual(
        values.slice(0, 500),
        Array.from({ length: 500 }, (_, i) => `a${i}`),
      );
      assert.deepEqual(values.slice(500).sort(), Array.from({ length: 500 }, (_, i) => `b${i}`).sort());
    } finally {
      await producer.close();
    }
  },
  180000,
);

// Java runs a partition's callbacks in order, one after another, on its one
// I/O thread. A JS callback may return a Promise, which the core waits for
// before completing that record's future, and the next record's callback
// waits for it before it starts, as Python's and .NET's async callbacks run
// one at a time. This pins both against real deliveries.
(SKIP ? test.skip : test)(
  "callbacks returning Promises that settle after random delays run one at a time, in offset order, and each record's future waits for its own callback",
  async () => {
    const topic = await newTopic('promise-callbacks');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      let seed = 7;
      const random = () => {
        seed = (seed * 1103515245 + 12345) % 2147483648;
        return seed / 2147483648;
      };
      /** @type {bigint[]} */
      const entered = [];
      /** @type {string[]} */
      const events = [];
      let running = 0;
      let maxRunning = 0;
      /** @type {number[]} */
      const doneBeforeItsCallbackEnded = [];
      /** @type {KafkaFuture<RecordMetadata>[]} */
      const futures = [];
      for (let i = 0; i < 500; i++) {
        const index = i;
        futures.push(
          await producer.send({
            record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(`v${i}`) }),
            callback: async ({ metadata }) => {
              entered.push(metadata.offset);
              events.push(`start ${metadata.offset}`);
              running++;
              maxRunning = Math.max(maxRunning, running);
              await sleep(Math.floor(random() * 4));
              running--;
              events.push(`end ${metadata.offset}`);
              if (futures[index].isDone()) doneBeforeItsCallbackEnded.push(index);
            },
          }),
        );
      }
      await Promise.all(futures.map((f) => f.get()));
      assert.equal(entered.length, 500);
      assert.deepEqual(
        entered.filter((offset, i) => i > 0 && offset <= entered[i - 1]),
        [],
        'callbacks entered in offset order',
      );
      assert.equal(maxRunning, 1, "a callback starts only once the previous one's Promise settled");
      // Each callback starts after the one before it ended, in offset order.
      assert.deepEqual(
        events,
        entered.flatMap((offset) => [`start ${offset}`, `end ${offset}`]),
      );
      assert.deepEqual(doneBeforeItsCallbackEnded, [], "no future completes before its own callback's Promise settled");
    } finally {
      await producer.close();
    }
  },
  180000,
);

(SKIP ? test.skip : test)(
  'a callback may send, call partitionsFor() and metrics() before its first await, then await and send again',
  async () => {
    const topic = await newTopic('reentrant');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      /** @type {Record<string, number>} */
      const callbacks = {};
      /** @param {string} name */
      const count = (name) => () => void (callbacks[name] = (callbacks[name] ?? 0) + 1);
      /** @type {Promise<KafkaFuture<RecordMetadata>>[]} */
      const nested = [];
      /** @type {Promise<unknown> | undefined} */
      let partitions;
      /** @type {unknown} */
      let metricsSeen;
      /** @type {Promise<KafkaFuture<RecordMetadata>> | undefined} */
      let afterAwait;
      /** @type {Promise<void>} */
      let callbackDone = Promise.resolve();
      const outer = await producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('outer') }),
        callback: ({ exception }) => {
          assert.equal(exception, null);
          nested.push(
            producer.send({
              record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('n1') }),
              callback: count('n1'),
            }),
          );
          nested.push(
            producer.send({
              record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('n2') }),
              callback: count('n2'),
            }),
          );
          partitions = producer.partitionsFor(topic);
          metricsSeen = producer.metrics();
          callbackDone = (async () => {
            await sleep(10);
            afterAwait = producer.send({
              record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('after-await') }),
              callback: count('after-await'),
            });
          })();
          return callbackDone;
        },
      });
      await outer.get();
      await callbackDone;
      assert.equal(nested.length, 2);
      assert.ok(afterAwait);
      const futures = await Promise.all([...nested, afterAwait]);
      await Promise.all(futures.map((f) => f.get()));
      assert.deepEqual(callbacks, { n1: 1, n2: 1, 'after-await': 1 });
      assert.ok(Array.isArray(await partitions));
      assert.ok(metricsSeen);
      const values = (await fetchRecords(BOOTSTRAP, topic, 0)).records.map((r) => String(r.value));
      assert.deepEqual(values, ['outer', 'n1', 'n2', 'after-await']);
    } finally {
      await producer.close();
    }
  },
  120000,
);

// Awaiting a later record's future inside a callback is an anti-pattern the
// docs name: the later record's callback waits for its turn behind this one,
// and its future for that callback, as Java's get(timeout) on the I/O thread
// times out on a record whose callback that thread runs next. Its bounded
// form must recover: the wait rejects with the root TimeoutError once 500 ms
// passed, the callback ends, and the later record completes once, with its
// own callback, which runs after it.
(SKIP ? test.skip : test)(
  "a callback's bounded wait on a later record's future rejects with TimeoutError, and that record completes once, after it",
  async () => {
    const topic = await newTopic('bounded-wait');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 50 } });
    try {
      /** @type {KafkaFuture<RecordMetadata> | undefined} */
      let later;
      /** @type {unknown} */
      let waitOutcome;
      let laterCallbacks = 0;
      let waited = -1;
      let firstCallbackEnded = false;
      let laterRanBeforeFirstEnded = false;
      const first = await producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('first') }),
        callback: async () => {
          await sleep(0);
          const started = Date.now();
          try {
            waitOutcome = await /** @type {KafkaFuture<RecordMetadata>} */ (later).get({ timeout: 500 });
          } catch (e) {
            waitOutcome = e;
          }
          waited = Date.now() - started;
          firstCallbackEnded = true;
        },
      });
      later = await producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('later') }),
        callback: () => {
          laterCallbacks++;
          if (!firstCallbackEnded) laterRanBeforeFirstEnded = true;
        },
      });
      await first.get({ timeout: 30000 });
      const laterMetadata = await later.get({ timeout: 30000 });
      assert.equal(firstCallbackEnded, true);
      assert.ok(waitOutcome instanceof RootTimeoutError, `the bounded wait answered ${String(waitOutcome)}`);
      assert.equal(waitOutcome.constructor, RootTimeoutError);
      // The full timeout, a timer's early firing aside.
      assert.ok(waited >= 490 && waited < 500 + 1000, `waited ${waited} ms`);
      assert.equal(laterMetadata.offset, 1n);
      assert.equal(laterCallbacks, 1);
      assert.equal(laterRanBeforeFirstEnded, false, "the later record's callback waited for its turn");
    } finally {
      await producer.close();
    }
  },
  120000,
);

// Where a keyed record lands, against the partitions Java's kafka-clients
// computes (test/java_vectors.json, written by
// tools/java-vectors/JavaVectors.java): Murmur2RandomPartitioner is Java's
// BuiltInPartitioner#partitionForKey, toPositive(murmur2(key)) % n; the
// default, ConsistentRandomPartitioner, is librdkafka's consistent_random,
// CRC-32(key) % n, as MIGRATION.md promises; RoundRobinPartitioner ignores the
// key and cycles. Keys sent one at a time, each send awaited.
(SKIP ? test.skip : test)(
  'keyed records land where the configured partitioner puts them: CRC-32 by default, murmur2 on request, round robin cycling',
  async () => {
    const zlib = require('node:zlib');
    /** @type {{ murmur2: Array<{ key: string, partition12: number }> }} */
    const vectors = require('../java_vectors.json');
    const topic = await newTopic('placement', { partitions: 12 });
    /** @type {Record<string, (key: string, i: number, first: number) => number>} */
    const expected = {
      default: (key) => zlib.crc32(Buffer.from(key, 'utf8')) % 12,
      ConsistentRandomPartitioner: (key) => zlib.crc32(Buffer.from(key, 'utf8')) % 12,
      Murmur2RandomPartitioner: (key) => /** @type {{ partition12: number }} */ (vectors.murmur2.find((v) => v.key === key)).partition12,
      // Checked below: Java's RoundRobinPartitioner walks the topic's
      // available partitions in the metadata's order, not by number.
      RoundRobinPartitioner: () => -1,
    };
    for (const [type, partitionOf] of Object.entries(expected)) {
      const configs = type === 'default' ? {} : { 'partitioner.type': type };
      const producer = await producerKnowing(configs, topic);
      try {
        /** @type {Array<[string, number]>} */
        const landed = [];
        for (const { key } of vectors.murmur2) {
          const m = await (
            await producer.send(new ProducerRecord({ topic, key: Buffer.from(key, 'utf8'), value: Buffer.from(type) }))
          ).get();
          landed.push([key, m.partition]);
        }
        if (type === 'RoundRobinPartitioner') {
          // Every 12 records in a row visit each partition once, in one order.
          const cycle = landed.slice(0, 12).map(([, partition]) => partition);
          assert.deepEqual(
            [...cycle].sort((a, b) => a - b),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            `cycle ${cycle}`,
          );
          assert.deepEqual(
            landed.filter(([, partition], i) => partition !== cycle[i % 12]),
            [],
            `${type}: the records that left the cycle ${cycle}`,
          );
        } else {
          assert.deepEqual(
            landed.filter(([key, partition], i) => partition !== partitionOf(key, i, 0)),
            [],
            `${type}: the records that landed elsewhere`,
          );
        }
      } finally {
        await producer.close();
      }
    }
    // Null keys: the built-in partitioner's sticky choice moves to another
    // partition between batches, so many small batches use many partitions.
    const producer = await producerKnowing({ 'batch.size': 1, 'linger.ms': 0 }, topic);
    try {
      const partitions = new Set();
      for (let i = 0; i < 200; i++) {
        partitions.add((await (await producer.send(new ProducerRecord({ topic, key: null, value: Buffer.from('n') }))).get()).partition);
      }
      assert.ok(partitions.size > 1, `null keys used partitions ${[...partitions]}`);
    } finally {
      await producer.close();
    }
  },
  300000,
);

/**
 * Runs `body` with the broker container paused, unpausing it afterwards
 * whatever happens.
 *
 * @template T
 * @param {() => Promise<T>} body
 * @returns {Promise<T>}
 */
async function withBrokerPaused(body) {
  const container = /** @type {NonNullable<typeof CONTAINER>} */ (CONTAINER);
  container.pause();
  try {
    return await body();
  } finally {
    container.unpause();
    await broker.waitForBroker(BOOTSTRAP);
  }
}

// Sender#sendProducerData fails each batch past delivery.timeout.ms with
// Kafka's TimeoutException("Expiring N record(s) for <tp>:<ms> ms has passed
// since batch creation. The request has not been sent, or no server response
// has been received yet."), once, through the callback and the future.
(SKIP_CONTAINER ? test.skip : test)(
  "with the broker paused, each pending record expires once with Java's TimeoutError text, flush() resolves, and later sends deliver",
  async () => {
    const topic = await newTopic('expiry');
    const producer = await producerWithId({ 'delivery.timeout.ms': 5000, 'request.timeout.ms': 2000, 'linger.ms': 0 }, topic);
    try {
      /** @type {Array<Array<Error | null>>} */
      const calls = Array.from({ length: 50 }, () => []);
      /** @type {KafkaFuture<RecordMetadata>[]} */
      const futures = [];
      await withBrokerPaused(async () => {
        for (let i = 0; i < 50; i++) {
          futures.push(
            await producer.send({
              record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(`expiring-${i}`) }),
              callback: ({ exception }) => void calls[i].push(exception),
            }),
          );
        }
        await sleep(10000);
        await producer.flush();
      });
      for (const [i, future] of futures.entries()) {
        assert.equal(calls[i].length, 1, `record ${i}'s callback ran ${calls[i].length} times`);
        const exception = calls[i][0];
        assert.ok(exception instanceof TimeoutError, String(exception));
        assert.match(
          exception.message,
          new RegExp(
            `^Expiring \\d+ record\\(s\\) for ${topic}-0:\\d+ ms has passed since batch creation\\. The request has not been sent, or no server response has been received yet\\.$`,
          ),
        );
        assert.equal(future.isCompletedExceptionally(), true);
        await assert.rejects(future.get(), (e) => /** @type {Error} */ (e).cause === exception);
      }
      const after = await (
        await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('after') }))
      ).get({ timeout: 30000 });
      assert.ok(after.offset >= 0n);
    } finally {
      await producer.close();
    }
  },
  120000,
);

// KafkaProducer#close(Duration): waits up to the timeout for the records it
// can send, then forces the close (Sender#forceClose aborts every incomplete
// batch with KafkaException "Producer is closed forcefully.").
(SKIP_CONTAINER ? test.skip : test)(
  'a timed close with records the broker cannot take resolves after about its timeout and fails them, each callback once',
  async () => {
    const { KafkaError } = require('@confluentinc/kafka-javascript/common');
    const topic = await newTopic('timed-close');
    const producer = await producerWithId({ 'linger.ms': 0 }, topic);
    /** @type {Array<Array<Error | null>>} */
    const calls = Array.from({ length: 20 }, () => []);
    await withBrokerPaused(async () => {
      /** @type {KafkaFuture<RecordMetadata>[]} */
      const futures = [];
      for (let i = 0; i < 20; i++) {
        futures.push(
          await producer.send({
            record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(`v${i}`) }),
            callback: ({ exception }) => void calls[i].push(exception),
          }),
        );
      }
      const started = Date.now();
      await producer.close({ timeout: 2000 });
      const took = Date.now() - started;
      assert.ok(took >= 1900 && took < 2000 + 3000, `close took ${took} ms`);
      for (const [i, future] of futures.entries()) {
        assert.equal(calls[i].length, 1, `record ${i}`);
        const exception = calls[i][0];
        assert.ok(exception instanceof KafkaError);
        assert.equal(/** @type {Error} */ (exception).message, 'Producer is closed forcefully.');
        assert.equal(future.isCompletedExceptionally(), true);
      }
    });
  },
  120000,
);

// Q21/Q61: no closed flag; close() and [Symbol.asyncDispose] at once each
// go to the core, which tears down once.
(SKIP_CONTAINER ? test.skip : test)(
  'close(), close() and [Symbol.asyncDispose]() at once, with sends, a flush() and a partitionsFor() in flight, all settle once',
  async () => {
    const topic = await newTopic('concurrent-close');
    const producer = await producerWithId(
      { 'linger.ms': 1000, 'delivery.timeout.ms': 3000, 'request.timeout.ms': 1000, 'max.block.ms': 60000 },
      topic,
    );
    const callbacks = new Array(100).fill(0);
    /** @type {KafkaFuture<RecordMetadata>[]} */
    const futures = [];
    for (let i = 0; i < 100; i++) {
      futures.push(
        await producer.send({
          record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(`v${i}`) }),
          callback: () => void callbacks[i]++,
        }),
      );
    }
    /** @type {PromiseSettledResult<unknown>[]} */
    let settled = [];
    await withBrokerPaused(async () => {
      const flushing = producer.flush();
      const partitions = producer.partitionsFor(freshTopic('unknown'));
      const closes = [producer.close(), producer.close(), producer[Symbol.asyncDispose]()];
      settled = await Promise.allSettled([...closes, flushing, partitions]);
    });
    assert.deepEqual(
      settled.slice(0, 4).map((s) => s.status),
      ['fulfilled', 'fulfilled', 'fulfilled', 'fulfilled'],
      JSON.stringify(settled.map((s) => (s.status === 'rejected' ? String(s.reason) : 'ok'))),
    );
    assert.equal(settled[4].status, 'rejected', 'partitionsFor() of an unknown topic rejects at the close');
    const reason = /** @type {PromiseRejectedResult} */ (settled[4]).reason;
    assert.ok(reason instanceof Error);
    console.log(`partitionsFor() rejected at the close with ${reason.constructor.name}: ${reason.message}`);
    assert.deepEqual(
      callbacks.filter((n) => n !== 1),
      [],
    );
    assert.ok(futures.every((f) => f.isDone()));
    await assert.rejects(producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('late') })), {
      message: 'Cannot perform operation after producer has been closed',
    });
  },
  120000,
);

// The idempotent producer across a broker restart: every awaited send lands
// once, in send order, under one producer id with consecutive sequences.
(SKIP_CONTAINER ? test.skip : test)(
  'an idempotent producer sending through a broker restart writes every record once, in send order, with consecutive sequences',
  async () => {
    const topic = await newTopic('restart');
    const producer = await producerKnowing({ 'enable.idempotence': true, acks: 'all', 'max.in.flight.requests.per.connection': 5 }, topic);
    const container = /** @type {NonNullable<typeof CONTAINER>} */ (CONTAINER);
    try {
      const count = 10000;
      const callbacks = new Array(count).fill(0);
      /** @type {KafkaFuture<RecordMetadata>[]} */
      const futures = [];
      /** @type {Promise<void> | undefined} */
      let restarting;
      for (let i = 0; i < count; i++) {
        if (i === 3000) restarting = container.restart(BOOTSTRAP);
        futures.push(
          await producer.send({
            record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(String(i)) }),
            callback: () => void callbacks[i]++,
          }),
        );
      }
      await restarting;
      const metadata = await Promise.all(futures.map((f) => f.get({ timeout: 120000 })));
      assert.deepEqual(
        callbacks.filter((n) => n !== 1),
        [],
      );
      assert.deepEqual(
        metadata.filter((m, i) => m.offset !== BigInt(i)),
        [],
        'contiguous offsets in send order',
      );
      const { batches } = await broker.fetchBatches(BOOTSTRAP, topic, 0);
      const values = batches.flatMap((b) => (b.records ?? []).map((r) => String(r.value)));
      assert.deepEqual(
        values.filter((v, i) => v !== String(i)),
        [],
      );
      assert.equal(values.length, count);
      assert.equal(new Set(batches.map((b) => b.producerId)).size, 1, 'one producer id');
      let next = 0;
      for (const batch of batches) {
        assert.equal(batch.baseSequence, next, `batch at offset ${batch.baseOffset}`);
        next += batch.count;
      }
    } finally {
      await producer.close();
    }
  },
  300000,
);

// close({ timeout: 0 }) from ordinary code is Java's close(Duration.ZERO): no
// "Overriding close timeout" warning, which Java's KafkaProducer#close logs
// only for a close made from its own I/O thread (a callback).
(SKIP ? test.skip : test)(
  'close({ timeout: 0 }) from outside a callback fails the lingering records and logs no close-timeout override',
  () => {
    const topic = freshTopic('forced-outside');
    const script = `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 60000, 'delivery.timeout.ms': 120000 } });
    (async () => {
      await (await producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: null, value: Buffer.from('warm-up') }))).get();
      const errors = [];
      const futures = [];
      for (let i = 0; i < 50; i++) {
        futures.push(await producer.send({ record: new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: null, value: Buffer.from('v') }), callback: ({ exception }) => errors.push(exception && exception.message) }));
      }
      await producer.close({ timeout: 0 });
      console.log('callbacks', errors.length, [...new Set(errors)].join('|'));
      console.log('failed', futures.filter((f) => f.isCompletedExceptionally()).length);
    })();
  `;
    const r = spawnSync(process.execPath, ['--no-warnings', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 90000,
      env: { ...process.env, ...POISON, BOOTSTRAP, TOPIC: topic, RUST_LOG: 'warn' },
    });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, 'callbacks 50 Producer is closed forcefully.\nfailed 50\n');
    assert.doesNotMatch(r.stderr, /Overriding close timeout/);
  },
  120000,
);

// The errors a producer meets in practice, each provoked for real: its class,
// its parents (the instanceof chain of Java's extends), its name, its message,
// its payload getters and the path it takes. An ApiError reaches the callback
// once and fails the future while send() resolves (KafkaProducer#doSend);
// anything else rejects the call. RecordTooLargeError, BufferExhaustedError,
// InvalidTopicError, delivery expiry, the TLS and SASL errors and
// BootstrapResolutionError have their own tests above and in ssl.test.js,
// sasl.test.js and kafka_producer.test.js.
(SKIP ? test.skip : test)(
  'the errors a producer meets in practice keep their class, parents, name, message and path',
  async () => {
    const { KafkaError } = require('@confluentinc/kafka-javascript/common');
    const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
    const errors = require('@confluentinc/kafka-javascript/common/errors');
    const { IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
    /**
     * The outcome of one send: the error, the path it took and the callbacks.
     *
     * @param {KafkaProducer<any, any>} producer
     * @param {ProducerRecord<any, any>} record
     */
    const sendOutcome = async (producer, record) => {
      /** @type {Array<Error | null>} */
      const calls = [];
      try {
        const future = await producer.send({ record, callback: ({ exception }) => void calls.push(exception) });
        try {
          await future.get({ timeout: 60000 });
          return { path: 'delivered', error: null, calls };
        } catch (e) {
          assert.ok(e instanceof ExecutionError);
          assert.equal(calls[0], e.cause);
          return { path: 'callback and future', error: /** @type {Error} */ (e.cause), calls };
        }
      } catch (e) {
        return { path: 'send() rejects', error: /** @type {Error} */ (e), calls };
      }
    };
    /**
     * @typedef {{ name: string, run: () => Promise<{ path: string, error: Error | null, calls: Array<Error | null> }>, cls: Function, parents: Function[], path: string, message: string | RegExp, check?: (e: any) => void }} Case
     */
    /** @type {Case[]} */
    const cases = [
      {
        name: 'metadata timeout for a partition the topic lacks',
        run: async () => {
          const topic = await newTopic('err-partition');
          const producer = await producerKnowing({ 'max.block.ms': 500 }, topic);
          try {
            return await sendOutcome(producer, new ProducerRecord({ topic, partition: 5, key: null, value: Buffer.from('v') }));
          } finally {
            await producer.close();
          }
        },
        cls: errors.TimeoutError,
        parents: [errors.RetriableError, errors.ApiError, KafkaError, Error],
        path: 'callback and future',
        message: /^Partition 5 of topic js-it-err-partition-\S+ with partition count 1 is not present in metadata after 500 ms\.$/,
      },
      {
        name: 'a timestamp the topic refuses',
        run: async () => {
          const topic = await newTopic('err-timestamp', { configs: { 'message.timestamp.before.max.ms': '60000' } });
          const producer = await producerKnowing({}, topic);
          try {
            return await sendOutcome(
              producer,
              new ProducerRecord({ topic, partition: 0, timestamp: 1000, key: null, value: Buffer.from('v') }),
            );
          } finally {
            await producer.close();
          }
        },
        cls: errors.InvalidTimestampError,
        parents: [errors.ApiError, KafkaError, Error],
        path: 'callback and future',
        message: /^Timestamp 1000 of message with offset 0 is out of range\./,
      },
      {
        name: 'a value the configured stringSerializer cannot convert',
        run: async () => {
          const producer = new KafkaProducer({
            configs: { 'bootstrap.servers': BOOTSTRAP },
            keySerializer: stringSerializer(),
            valueSerializer: stringSerializer(),
          });
          try {
            return await sendOutcome(
              producer,
              new ProducerRecord({ topic: freshTopic('err-serde'), partition: 0, key: 'k', value: /** @type {any} */ (Buffer.from('v')) }),
            );
          } finally {
            await producer.close();
          }
        },
        cls: errors.SerializationError,
        parents: [KafkaError, Error],
        path: 'send() rejects',
        message: /^Can't convert value of class /,
        check: (e) => assert.ok(e.cause instanceof TypeError),
      },
      {
        name: 'a forced close of a lingering record',
        run: async () => {
          const topic = await newTopic('err-forced');
          const producer = await producerKnowing({ 'linger.ms': 60000 }, topic);
          const pending = sendOutcome(producer, new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('v') }));
          await sleep(100);
          await producer.close({ timeout: 0 });
          return pending;
        },
        cls: KafkaError,
        parents: [Error],
        path: 'callback and future',
        message: 'Producer is closed forcefully.',
      },
      {
        name: 'a send after close',
        run: async () => {
          const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
          await producer.close();
          return sendOutcome(
            producer,
            new ProducerRecord({ topic: freshTopic('err-closed'), partition: 0, key: null, value: Buffer.from('v') }),
          );
        },
        cls: IllegalStateError,
        parents: [Error],
        path: 'send() rejects',
        message: 'Cannot perform operation after producer has been closed',
      },
      {
        name: 'a negative close timeout',
        run: async () => {
          const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
          try {
            await producer.close({ timeout: -1 });
            return { path: 'resolved', error: null, calls: [] };
          } catch (e) {
            return { path: 'close() rejects', error: /** @type {Error} */ (e), calls: [] };
          } finally {
            await producer.close();
          }
        },
        cls: IllegalArgumentError,
        parents: [RangeError, Error],
        path: 'close() rejects',
        message: 'The timeout cannot be negative.',
      },
      {
        name: 'an acks value the core refuses',
        run: async () => {
          try {
            new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: '2' } });
            return { path: 'built', error: null, calls: [] };
          } catch (e) {
            return { path: 'the constructor throws', error: /** @type {Error} */ (e), calls: [] };
          }
        },
        cls: ConfigError,
        parents: [KafkaError, Error],
        path: 'the constructor throws',
        message: 'Invalid value 2 for configuration acks: String must be one of: all, -1, 0, 1',
      },
    ];
    /** @type {string[]} */
    const wrong = [];
    for (const c of cases) {
      const { path: taken, error, calls } = await c.run();
      const problems = [];
      if (!error) problems.push(`no error (${taken})`);
      else {
        if (error.constructor !== c.cls) problems.push(`class ${error.constructor.name}: ${error.message}`);
        for (const parent of c.parents) if (!(error instanceof parent)) problems.push(`not an instance of ${parent.name}`);
        if (error.name !== c.cls.name) problems.push(`name ${error.name}`);
        if (typeof c.message === 'string' ? error.message !== c.message : !c.message.test(error.message))
          problems.push(`message ${JSON.stringify(error.message)}`);
        if (taken !== c.path) problems.push(`path ${taken}`);
        if (taken === 'callback and future' && calls.length !== 1) problems.push(`callbacks ${calls.length}`);
        if (taken === 'send() rejects' && calls.length !== 0) problems.push(`callbacks ${calls.length}`);
        if (c.check) {
          try {
            c.check(error);
          } catch (e) {
            problems.push(String(e));
          }
        }
      }
      if (problems.length > 0) wrong.push(`${c.name}: ${problems.join('; ')}`);
    }
    assert.deepEqual(wrong, []);
  },
  300000,
);
