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

// Three measurements of the consumer, reported and not acted on, run by hand
// against a broker:
//
//   make -C javascript build
//   node --expose-gc javascript/tools/consumer-measurements/measure.js <bootstrap> [records]
//
// 1. What freezing a record costs: the time per record of building a
//    ConsumerRecord as the addon's factory does (ConsumerRecord.internalCreate),
//    with and without its Object.freeze, over the same fields.
// 2. External Buffers against copies: the time per record of polling the
//    same records with external Buffers, as Node gives them, and with copies,
//    through the addon's test switch (consumerRefuseExternalBuffers), which
//    copies as Electron does.
// 3. What one kept value holds: the RSS a consumer's process keeps
//    when it keeps one value of each poll, against keeping a copy of it, and
//    the batches still alive.
//
// It writes `records` records (default 1,000,000) of a 100-byte value to a
// one-partition topic, and prints one JSON object: the machine, the
// settings, and each measurement.

const assert = require('node:assert/strict');
const os = require('node:os');
const path = require('node:path');

const DIST = path.join(__dirname, '..', '..', 'dist');
const { TopicPartition } = require(path.join(DIST, 'common'));
const { Headers } = require(path.join(DIST, 'common', 'header'));
const { TimestampType } = require(path.join(DIST, 'common', 'record'));
const { CloseOptions, ConsumerRecord, KafkaConsumer } = require(path.join(DIST, 'consumer'));
const { KafkaProducer, ProducerRecord } = require(path.join(DIST, 'producer'));
const native = require(path.join(DIST, 'internal', 'native'));
const { createTopic } = require(path.join(__dirname, '..', '..', 'test', 'integration', '_fetch_records'));

const [bootstrap, recordsArg] = process.argv.slice(2);
assert.ok(bootstrap, 'usage: node --expose-gc measure.js <bootstrap> [records]');
assert.ok(typeof global.gc === 'function', 'run with --expose-gc');
const RECORDS = Number(recordsArg ?? 1_000_000);
const VALUE_BYTES = 100;
const RUNS = 5;
const MIB = 1024 * 1024;

/** The median of `xs`. */
function median(xs) {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}

/** Collections until the heap settles, with turns of the event loop between, so the Buffers' finalizers run. */
async function settle() {
  for (let i = 0; i < 6; i++) {
    global.gc();
    await new Promise((resolve) => setImmediate(resolve));
  }
}

/**
 * A record built as ConsumerRecord.internalCreate builds one, frozen or not:
 * the same fields, in the same order, on ConsumerRecord's prototype.
 */
function build(freeze, topic, i, key, value) {
  const record = Object.create(ConsumerRecord.prototype);
  record.topic = topic;
  record.partition = 0;
  record.offset = BigInt(i);
  record.timestamp = 1700000000000n;
  record.timestampType = TimestampType.CREATE_TIME;
  record.serializedKeySize = key.length;
  record.serializedValueSize = value.length;
  record.headers = new Headers();
  record.key = key;
  record.value = value;
  record.leaderEpoch = 0;
  record.deliveryCount = null;
  return freeze ? Object.freeze(record) : record;
}

/** Nanoseconds per record of building RECORDS records, frozen and not, RUNS runs each, interleaved. */
function freezeCost() {
  const key = Buffer.from('k');
  const value = Buffer.alloc(VALUE_BYTES);
  const out = { frozen: [], notFrozen: [], factory: [] };
  let sink = 0;
  for (let run = 0; run < RUNS + 1; run++) {
    for (const [name, make] of [
      ['frozen', (i) => build(true, 't', i, key, value)],
      ['notFrozen', (i) => build(false, 't', i, key, value)],
      ['factory', (i) => ConsumerRecord.internalCreate('t', 0, BigInt(i), 1700000000000n, 0, 1, VALUE_BYTES, null, key, value, 0, -1)],
    ]) {
      const start = process.hrtime.bigint();
      for (let i = 0; i < RECORDS; i++) sink += make(i).partition;
      const ns = Number(process.hrtime.bigint() - start) / RECORDS;
      // The first run warms the JIT up.
      if (run > 0) out[name].push(ns);
    }
  }
  assert.equal(sink, 0);
  const frozen = median(out.frozen);
  const notFrozen = median(out.notFrozen);
  return {
    nsPerRecordFrozen: frozen,
    nsPerRecordNotFrozen: notFrozen,
    nsPerFreeze: frozen - notFrozen,
    nsPerRecordInternalCreate: median(out.factory),
  };
}

/** Writes RECORDS records to partition 0 of `topic`: key `i`, a VALUE_BYTES value. */
async function produce(topic) {
  const producer = new KafkaProducer({ 'bootstrap.servers': bootstrap, 'linger.ms': 5, 'batch.size': 256 * 1024 });
  try {
    const value = Buffer.alloc(VALUE_BYTES, 7);
    for (let from = 0; from < RECORDS; from += 10_000) {
      const sends = [];
      for (let i = from; i < Math.min(from + 10_000, RECORDS); i++) {
        sends.push(producer.send(new ProducerRecord({ topic, partition: 0, key: Buffer.from(String(i)), value })));
      }
      await Promise.all((await Promise.all(sends)).map((f) => f.get()));
    }
  } finally {
    await producer.close();
  }
}

/**
 * Polls the whole topic with a new consumer, copies refused or not, and hands
 * each poll's records to `each`; returns the nanoseconds per record of the
 * polls after the first one that returned records (the first waits for the
 * fetch to start), and the polls that returned records.
 */
async function pollAll(topic, refuse, each) {
  const consumer = new KafkaConsumer({ 'bootstrap.servers': bootstrap, 'group.protocol': 'consumer', 'auto.offset.reset': 'earliest' });
  native.consumerRefuseExternalBuffers(refuse);
  let total = 0;
  let timed = 0;
  let polls = 0;
  let ns = 0n;
  try {
    await consumer.assign([new TopicPartition({ topic, partition: 0 })]);
    while (total < RECORDS) {
      const start = process.hrtime.bigint();
      const records = await consumer.poll(1000);
      if (records.count() === 0) continue;
      if (polls > 0) {
        ns += process.hrtime.bigint() - start;
        timed += records.count();
      }
      polls++;
      total += records.count();
      each(records);
    }
  } finally {
    native.consumerRefuseExternalBuffers(false);
    await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  }
  return { nsPerRecord: Number(ns) / timed, polls };
}

/** Nanoseconds per record of polling the topic with external Buffers and with copies, RUNS runs each, interleaved. */
async function externalAgainstCopies(topic) {
  const out = { external: [], copies: [] };
  let bytes = 0;
  const touch = (records) => {
    for (const r of records) bytes += r.value.length;
  };
  for (let run = 0; run < RUNS; run++) {
    out.external.push((await pollAll(topic, false, touch)).nsPerRecord);
    out.copies.push((await pollAll(topic, true, touch)).nsPerRecord);
  }
  assert.equal(bytes, 2 * RUNS * RECORDS * VALUE_BYTES);
  return { nsPerRecordExternal: median(out.external), nsPerRecordCopies: median(out.copies), runs: out };
}

/** What keeping one value of each poll holds, against keeping a copy of it. */
async function keptValue(topic) {
  const stats = () => native.consumerStats();
  const result = {};
  for (const [name, keep] of [
    ['copy', (v) => Buffer.from(v)],
    ['value', (v) => v],
  ]) {
    await settle();
    const before = process.memoryUsage().rss;
    const batchesBefore = stats().batchesMade - stats().batchesDestroyed;
    const kept = [];
    const { polls } = await pollAll(topic, false, (records) => {
      for (const r of records) {
        kept.push(keep(r.value));
        break;
      }
    });
    await settle();
    const after = process.memoryUsage().rss;
    result[name] = {
      polls,
      keptValues: kept.length,
      keptBytes: kept.reduce((n, v) => n + v.length, 0),
      batchesAlive: stats().batchesMade - stats().batchesDestroyed - batchesBefore,
      rssHeldMiB: (after - before) / MIB,
      rssHeldPerKeptValueKiB: (after - before) / 1024 / kept.length,
    };
    kept.length = 0;
    await settle();
  }
  result.recordsPerPoll = RECORDS / result.value.polls;
  result.recordBytesPerPoll = (RECORDS / result.value.polls) * (VALUE_BYTES + String(RECORDS).length);
  return result;
}

(async () => {
  const topic = `js-measure-consumer-${process.pid}-${Date.now()}`;
  await createTopic(bootstrap, topic);
  await produce(topic);
  const report = {
    machine: {
      cpu: os.cpus()[0].model,
      cpus: os.cpus().length,
      memoryGiB: Math.round(os.totalmem() / 1024 ** 3),
      platform: `${os.platform()} ${os.release()} ${os.arch()}`,
      node: process.version,
    },
    settings: { records: RECORDS, valueBytes: VALUE_BYTES, runs: RUNS, partitions: 1, consumerConfigs: 'defaults' },
    freezeCost: freezeCost(),
    externalAgainstCopies: await externalAgainstCopies(topic),
    keptValue: await keptValue(topic),
  };
  console.log(JSON.stringify(report, null, 2));
})().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
