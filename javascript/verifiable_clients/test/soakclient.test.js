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

// The producer soak. The SoakRecord, configuration-routing and metric tests
// translate python/soak/test/test_soakclient.py's for the parts this soak
// has (it has no consumer, no admin client and no OTLP pipeline); the
// delivery accounting runs over the MockProducer stand-in; and the script
// runs end to end against an unreachable broker. Needs the compiled library
// (dist/).

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { TimeoutError } = require('@confluentinc/kafka-javascript/common/errors');

const { LatencyBucket, Bucket, SoakMetrics, percentileFromHist } = require('../soak/soak_metrics');
const { EXIT_FATAL, EXIT_OK, METRIC_PFX, SoakClient, SoakRecord, parseArgs, producerConfig } = require('../soak/soakclient');
const { MockProducer } = require('./_mock_producer');

const SCRIPT = path.join(__dirname, '..', 'soak', 'soakclient.js');

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'js-soak-'));
}

// ---------------------------------------------------------------------------
// SoakRecord
// ---------------------------------------------------------------------------

test('serialize / deserialize round trip', () => {
  const parsed = SoakRecord.deserialize(new SoakRecord(42, 1765432100123, 3).serialize(0));
  assert.deepEqual([parsed.msgid, parsed.sendTimeMs, parsed.txcnt], [42, 1765432100123, 3]);
});

test('serialize defaults the send time to now', () => {
  const record = new SoakRecord(0);
  assert.ok(record.sendTimeMs > 1_700_000_000_000);
  assert.equal(record.txcnt, 1);
});

test('the round trip survives padding', () => {
  const payload = new SoakRecord(7, 1765432100123, 1).serialize(10240);
  assert.equal(payload.length, 10240);
  const parsed = SoakRecord.deserialize(payload);
  assert.deepEqual([parsed.msgid, parsed.sendTimeMs, parsed.txcnt], [7, 1765432100123, 1]);
});

test('the padding target is exact across msgid widths', () => {
  for (const msgid of [0, 9, 10, 999999, 12345678901234]) {
    assert.equal(new SoakRecord(msgid, 1765432100123).serialize(50).length, 50);
  }
});

test('no padding when the prefix exceeds the target', () => {
  assert.equal(new SoakRecord(123456, 1765432100123, 1).serialize(4).toString('latin1'), '123456|1765432100123|1|');
});

test('the padding grows beyond the 10 KB profile', () => {
  assert.equal(new SoakRecord(1, 1765432100123).serialize(100000).length, 100000);
});

test('the padding is the Python soak\'s', () => {
  const payload = new SoakRecord(1, 2, 3).serialize(30).toString('latin1');
  assert.equal(payload, '1|2|3| SoakRecord nr #0 SoakRe');
});

test('deserialize accepts a Uint8Array', () => {
  const payload = new SoakRecord(5, 1765432100123).serialize(50);
  assert.equal(SoakRecord.deserialize(new Uint8Array(payload)).msgid, 5);
});

test('deserialize rejects a malformed payload', () => {
  for (const payload of [
    '', // empty
    'not-a-soak-record', // no separators at all
    '1|2', // too few fields
    '1|2|3', // missing the trailing separator
    'x|2|3|pad', // non-numeric msgid
    '1|y|3|pad', // non-numeric send time
    '1|2|z|pad', // non-numeric txcnt
    '\xff\xfe|2|3|pad', // non-ascii msgid
    '|2|3|pad', // empty msgid
  ]) {
    assert.throws(() => SoakRecord.deserialize(Buffer.from(payload, 'latin1')), RangeError, payload);
  }
});

test('deserialize rejects null', () => {
  assert.throws(() => SoakRecord.deserialize(null), { name: 'RangeError', message: 'empty payload (null)' });
});

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

test('the producer configuration strips its prefix and drops the other clients\'', () => {
  const [conf, routed] = producerConfig({
    'bootstrap.servers': 'x',
    'producer.linger.ms': '5',
    'consumer.group.id': 'g',
    'admin.client.id': 'a',
  });
  assert.deepEqual(conf, { 'bootstrap.servers': 'x', 'linger.ms': '5' });
  assert.deepEqual(routed, []);
});

test('the consumer\'s unprefixed keys are routed away', () => {
  const [conf, routed] = producerConfig({ 'bootstrap.servers': 'x', 'group.id': 'g', 'linger.ms': '5', 'group.protocol': 'consumer' });
  assert.deepEqual(routed, ['group.id', 'group.protocol']);
  assert.deepEqual(conf, { 'bootstrap.servers': 'x', 'linger.ms': '5' });
});

test('client.rack stays with the producer, as in the Python soak', () => {
  const [conf, routed] = producerConfig({ 'bootstrap.servers': 'x', 'client.rack': 'r1', 'group.id': 'g' });
  assert.deepEqual(conf, { 'bootstrap.servers': 'x', 'client.rack': 'r1' });
  assert.deepEqual(routed, ['group.id']);
});

test('shared connection keys stay with the producer', () => {
  const shared = {
    'metadata.recovery.strategy': 'none',
    'metadata.recovery.rebootstrap.trigger.ms': '60000',
    'socket.connection.setup.timeout.ms': '1000',
    'socket.connection.setup.timeout.max.ms': '5000',
  };
  assert.deepEqual(producerConfig(shared), [shared, []]);
});

test('the command line takes the Python soak\'s flags and defaults', () => {
  const args = parseArgs(['-i', 'soak1', '-t', 'topic']);
  assert.deepEqual(
    [args.testid, args.topic, args.rate, args.payloadSize, args.maxSendAttempts, args.runtimeSeconds, args.shutdownTimeout],
    ['soak1', 'topic', 80, 50, 10, 0, 60],
  );
  assert.equal(args.metricsFile, `soak-metrics-${args.variant}-soak1.jsonl`);
  assert.throws(() => parseArgs(['-i', 's', '-t', 't', '-r', '0']), { message: '-r must be greater than zero' });
  assert.throws(() => parseArgs(['-i', 's', '-t', 't', '-r', 'x']), { message: "argument -r: invalid float value: 'x'" });
  assert.throws(() => parseArgs(['-t', 't']), { message: 'the following arguments are required: -i' });
});

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

test('percentiles come from the 1 ms histogram', () => {
  const hist = new Uint32Array(10);
  assert.equal(percentileFromHist(hist, 0.5), 0);
  hist[1] = 50;
  hist[9] = 50;
  assert.equal(percentileFromHist(hist, 0.5), 1);
  assert.equal(percentileFromHist(hist, 0.9), 9);
});

test('a bucket rolls over as strings and resets', () => {
  const bucket = new Bucket();
  bucket.addMeasurement(1);
  bucket.addMeasurement(3);
  assert.deepEqual(bucket.rollover(), { average: '2', max: '3', total: '4', count: '2' });
  assert.deepEqual(bucket.rollover(), { average: '0', max: '-inf', total: '0', count: '0' });
});

test('a latency bucket adds the percentiles', () => {
  const bucket = new LatencyBucket();
  for (let i = 1; i <= 100; i++) bucket.addMeasurement(i);
  const record = bucket.rollover();
  assert.deepEqual([record.p50, record.p90, record.p99, record.p999], ['50', '90', '99', '100']);
});

test('the metrics file holds one window per line in the Python soak\'s schema', () => {
  const file = path.join(tmpDir(), 'm.jsonl');
  const metrics = new SoakMetrics({ path: file, baseTags: { host: 'h', testid: 't', variant: 'v' }, prefix: METRIC_PFX });
  metrics.incrCounter('producer.send', 2);
  metrics.setGauge('producer.latency', 5, { partition: '0' });
  metrics.observeMessage(50, 5);
  metrics.writeRecord(metrics.rollover());
  metrics.incrCounter('producer.send', 1);
  metrics.close();
  const [first, second] = fs
    .readFileSync(file, 'utf8')
    .split('\n')
    .filter((l) => l !== '')
    .map((l) => JSON.parse(l));
  assert.deepEqual(Object.keys(first), [
    'rss', 'cpu', 'latency', 'bytes', 'messages', 'window_start_ms', 'window_end_ms',
    'measurement_start_ms', 'measurement_end_ms', 'prefix', 'tags', 'counters', 'gauges',
  ]);
  assert.equal(first.prefix, 'kafka.client.soak.rust_javascript.');
  assert.deepEqual(first.counters, { 'producer.send': { total: 2, delta: 2 } });
  assert.equal(first.gauges['producer.latency{partition=0}'].p50, '5');
  assert.equal(first.messages.count, '1');
  assert.deepEqual(second.counters, { 'producer.send': { total: 3, delta: 1 } });
});

// ---------------------------------------------------------------------------
// Delivery accounting
// ---------------------------------------------------------------------------

function soakOver(mock, overrides = {}) {
  const args = {
    testid: 't',
    topic: 'soak-t',
    rate: 1000,
    variant: 'test',
    payloadSize: 50,
    maxSendAttempts: 3,
    metricsFile: null,
    ...overrides,
  };
  return new SoakClient(args, { 'bootstrap.servers': 'x' }, { createProducer: () => mock });
}

test('every delivered record is counted once and the run passes', async () => {
  const soak = soakOver(new MockProducer());
  for (let i = 0; i < 5; i++) await soak.produceRecord();
  assert.deepEqual([soak.msgid, soak.drCnt, soak.drErrCnt, soak.outstanding, soak.pending.size], [5, 5, 0, 0, 0]);
  assert.equal(soak.verdict(), 'PASS');
});

test('a record whose callback never ran fails the run', async () => {
  const mock = new MockProducer({ autoComplete: false });
  const soak = soakOver(mock);
  await soak.produceRecord();
  await soak.produceRecord();
  assert.ok(mock.completeNext());
  assert.equal(soak.verdict(), 'FAIL (1 missing delivery report(s))');
});

test('a second callback for a record fails the run', async () => {
  const mock = new MockProducer({ autoComplete: false });
  const soak = soakOver(mock);
  await soak.produceRecord();
  const [completion] = mock.pending;
  assert.ok(mock.completeNext());
  completion.callback({ metadata: { topic: 'soak-t', partition: 0, offset: 7 }, exception: null });
  assert.equal(soak.duplicateReports, 1);
  assert.equal(soak.verdict(), 'FAIL (1 duplicate delivery report(s))');
});

test('a failed delivery is counted, not failed', async () => {
  const mock = new MockProducer({ autoComplete: false });
  const soak = soakOver(mock);
  await soak.produceRecord();
  assert.ok(mock.errorNext(new KafkaError('boom')));
  assert.deepEqual([soak.drCnt, soak.drErrCnt, soak.outstanding], [0, 1, 0]);
  assert.equal(soak.metrics.counter('producer.delivery.failure', { err: 'KafkaError' }), 1);
  assert.equal(soak.verdict(), 'PASS');
});

test('a retriable rejection is retried, the attempts counted in txcnt', async () => {
  const mock = new MockProducer();
  let attempts = 0;
  const send = mock.send.bind(mock);
  mock.send = (options) => {
    attempts += 1;
    if (attempts < 3) return Promise.reject(new TimeoutError('not yet'));
    return send(options);
  };
  const soak = soakOver(mock);
  await soak.produceRecord();
  assert.equal(attempts, 3);
  const [record] = mock.history();
  assert.equal(SoakRecord.deserialize(record.value).txcnt, 3);
  assert.deepEqual([soak.drCnt, soak.drErrCnt, soak.pending.size], [1, 0, 0]);
});

test('a non-retriable rejection gives the record up at once', async () => {
  const mock = new MockProducer();
  mock.setSendError(new KafkaError('refused'));
  const soak = soakOver(mock);
  await soak.produceRecord();
  assert.deepEqual([soak.drCnt, soak.drErrCnt, soak.outstanding, soak.pending.size], [0, 1, 0, 0]);
  assert.equal(soak.verdict(), 'PASS');
});

test('a delivered offset at or below the partition\'s last one is a regression', async () => {
  const mock = new MockProducer({ autoComplete: false });
  const soak = soakOver(mock);
  await soak.produceRecord();
  await soak.produceRecord();
  const [a, b] = mock.pending.splice(0);
  b.callback({ metadata: { topic: 'soak-t', partition: 0, offset: 10 }, exception: null });
  a.callback({ metadata: { topic: 'soak-t', partition: 0, offset: 9 }, exception: null });
  assert.equal(soak.offsetRegressions, 1);
  assert.equal(soak.verdict(), 'PASS');
});

test('a failed close fails the run', async () => {
  const mock = new MockProducer();
  mock.close = () => Promise.reject(new KafkaError('close failed'));
  const soak = soakOver(mock);
  await soak.terminate();
  assert.equal(soak.verdict(), 'FAIL (close failed: close failed)');
});

// ---------------------------------------------------------------------------
// The script
// ---------------------------------------------------------------------------

test('the script with a bad command line exits EXIT_FATAL', () => {
  const result = spawnSync(process.execPath, [SCRIPT, '-t', 't'], { encoding: 'utf8', timeout: 30_000 });
  assert.equal(result.status, EXIT_FATAL);
  assert.ok(result.stderr.endsWith('soakclient: error: the following arguments are required: -i\n'), result.stderr);
});

test('the script with a configuration the client rejects exits EXIT_FATAL', () => {
  const dir = tmpDir();
  const conf = path.join(dir, 'c.config');
  fs.writeFileSync(conf, 'bootstrap.servers=localhost:1\nacks=sometimes\n');
  const result = spawnSync(
    process.execPath,
    [SCRIPT, '-i', 's', '-t', 't', '-f', conf, '--metrics-file', path.join(dir, 'm.jsonl')],
    { encoding: 'utf8', timeout: 30_000 },
  );
  assert.equal(result.status, EXIT_FATAL, result.stderr);
  assert.ok(result.stderr.startsWith('soakclient: fatal startup error: ConfigError: '), result.stderr);
});

test('the script runs for --runtime-seconds against an unreachable broker and accounts for every record', () => {
  // No broker: every send fails after max.block.ms, in its callback, so the
  // run passes (each record reported once) with nothing delivered.
  const dir = tmpDir();
  const conf = path.join(dir, 'c.config');
  fs.writeFileSync(conf, 'bootstrap.servers=localhost:1\nproducer.max.block.ms=200\nconsumer.group.id=g\n');
  const metrics = path.join(dir, 'm.jsonl');
  const result = spawnSync(
    process.execPath,
    [SCRIPT, '-i', 's', '-t', 'soak-t', '-r', '10', '-f', conf, '--metrics-file', metrics, '--metrics-interval', '0.5', '--runtime-seconds', '2'],
    { encoding: 'utf8', timeout: 60_000 },
  );
  assert.equal(result.status, EXIT_OK, result.stderr);
  const summary = result.stdout.split('\n').find((line) => line.startsWith('SUMMARY '));
  assert.ok(summary, result.stdout);
  assert.match(summary, / delivered=0 /);
  assert.match(summary, / missing_reports=0 duplicate_reports=0 /);
  assert.match(summary, / verdict=PASS$/);
  const windows = fs
    .readFileSync(metrics, 'utf8')
    .split('\n')
    .filter((l) => l !== '')
    .map((l) => JSON.parse(l));
  assert.ok(windows.length >= 2, `${windows.length}`);
  const last = windows[windows.length - 1];
  assert.equal(last.tags.testid, 's');
  assert.ok(last.counters['producer.drerr'].total >= 1, JSON.stringify(last.counters));
});
