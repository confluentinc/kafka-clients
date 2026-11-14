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

// VerifiableProducer. Java has no test of its own; these are the Python
// tool's (python/verifiable_clients/test/test_verifiable_producer.py, itself
// the Rust tool's in rust/tools/verifiable-clients/src/verifiable_producer.rs)
// over a MockProducer stand-in (./_mock_producer.js), plus the script run end
// to end against an unreachable broker. Needs the compiled library (dist/).

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');

const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { TimeoutError } = require('@confluentinc/kafka-javascript/common/errors');

const common = require('../_common');
const { ArgumentParserError } = common;
const { ThroughputThrottler } = require('../throughput_throttler');
const {
  VerifiableProducer,
  argParser,
  createFromArgs,
  failedSend,
  parseArgs,
  shutdownComplete,
  startupComplete,
  successfulSend,
  toolData,
} = require('../verifiable_producer');
const { MockProducer } = require('./_mock_producer');

const SCRIPT = path.join(__dirname, '..', 'verifiable_producer.js');
const KAFKA_ERROR_CLASS = 'class @confluentinc/kafka-javascript/common.KafkaError';

function verifiable(producer = new MockProducer(), options = {}) {
  return new VerifiableProducer({
    producer,
    topic: 't',
    throughput: options.throughput ?? -1,
    maxMessages: options.maxMessages ?? -1,
    valuePrefix: options.valuePrefix ?? null,
    createTime: options.createTime ?? null,
    repeatingKeys: options.repeatingKeys ?? null,
  });
}

/** Captures the JSON lines the tool prints while `fn` runs. */
async function captured(fn) {
  const lines = [];
  const original = fs.writeSync;
  fs.writeSync = (fd, buffer, ...rest) => {
    if (fd !== 1) return original.call(fs, fd, buffer, ...rest);
    lines.push(...Buffer.from(buffer).toString('utf8').split('\n').filter((l) => l !== ''));
    return buffer.length;
  };
  try {
    await fn();
  } finally {
    fs.writeSync = original;
  }
  return lines;
}

function json(lines) {
  return lines.map((line) => JSON.parse(line));
}

function withoutTimestamp(data) {
  assert.equal(typeof data.timestamp, 'number');
  const { timestamp: _, ...rest } = data;
  return rest;
}

function tmpFile(name, contents) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'js-verifiable-'));
  const file = path.join(dir, name);
  if (contents !== undefined) fs.writeFileSync(file, contents);
  return file;
}

// ---------------------------------------------------------------------------
// Keys and values
// ---------------------------------------------------------------------------

test('getValue without a prefix is just the number', () => {
  const producer = verifiable();
  assert.equal(producer.getValue(0), '0');
  assert.equal(producer.getValue(42), '42');
});

test('getValue with a prefix uses a dot separator', () => {
  const producer = verifiable(undefined, { valuePrefix: 7 });
  assert.equal(producer.getValue(0), '7.0');
  assert.equal(producer.getValue(42), '7.42');
});

test('getKey is null without repeating keys', () => {
  const producer = verifiable();
  assert.equal(producer.getKey(), null);
  assert.equal(producer.getKey(), null);
});

test('getKey wraps around at repeatingKeys', () => {
  const producer = verifiable(undefined, { repeatingKeys: 3 });
  assert.deepEqual(
    Array.from({ length: 5 }, () => producer.getKey()),
    ['0', '1', '2', '0', '1'],
  );
});

// ---------------------------------------------------------------------------
// send() and its callback
// ---------------------------------------------------------------------------

test('send increments sent and acked through the callback', async () => {
  const producer = verifiable();
  assert.deepEqual([producer.numSent, producer.numAcked], [0, 0]);
  const lines = await captured(async () => {
    await producer.send('k', '0');
    assert.deepEqual([producer.numSent, producer.numAcked], [1, 1]);
    await producer.send(null, '1');
    assert.deepEqual([producer.numSent, producer.numAcked], [2, 2]);
  });
  assert.deepEqual(json(lines).map(withoutTimestamp), [
    { name: 'producer_send_success', key: 'k', value: '0', topic: 't', partition: 0, offset: 0 },
    { name: 'producer_send_success', key: null, value: '1', topic: 't', partition: 0, offset: 1 },
  ]);
});

test('a send failing in the callback is a failed send', async () => {
  const mock = new MockProducer({ autoComplete: false });
  const producer = verifiable(mock);
  const lines = await captured(async () => {
    await producer.send('k', '0');
    assert.ok(mock.errorNext(new TimeoutError('Expiring 1 record(s)')));
  });
  assert.deepEqual([producer.numSent, producer.numAcked], [1, 0]);
  assert.deepEqual(json(lines).map(withoutTimestamp), [
    {
      name: 'producer_send_error',
      key: 'k',
      value: '0',
      topic: 't',
      exception: 'class @confluentinc/kafka-javascript/common/errors.TimeoutError',
      message: 'Expiring 1 record(s)',
    },
  ]);
});

test('a send rejected by send() is a failed send', async () => {
  const mock = new MockProducer();
  mock.setSendError(new KafkaError('send failed'));
  const producer = verifiable(mock);
  const lines = await captured(() => producer.send(null, '0'));
  // Java counts the attempt before send() throws.
  assert.deepEqual([producer.numSent, producer.numAcked], [1, 0]);
  assert.deepEqual(json(lines).map(withoutTimestamp), [
    { name: 'producer_send_error', key: null, value: '0', topic: 't', exception: KAFKA_ERROR_CLASS, message: 'send failed' },
  ]);
});

test('the create time is the record timestamp and advances', async () => {
  const mock = new MockProducer();
  const producer = verifiable(mock, { createTime: 1_000 });
  producer.startTime -= 5; // five milliseconds since the producer started
  await captured(async () => {
    await producer.send(null, '0');
    await producer.send(null, '1');
  });
  const [first, second] = mock.history().map((record) => record.timestamp);
  // Java's long: a bigint.
  assert.equal(first, 1_000n);
  assert.ok(second >= 1_005n, `${second}`);
  assert.ok(producer.createTime >= Number(second) + 5, `${producer.createTime}`);
});

test('a record without a create time has no timestamp', async () => {
  const mock = new MockProducer();
  await captured(() => verifiable(mock).send('k', '0'));
  const [record] = mock.history();
  assert.deepEqual([record.topic, record.partition, record.timestamp], ['t', null, null]);
  assert.deepEqual([record.key, record.value], ['k', '0']);
});

test('a negative create time is a failed send, not counted', async () => {
  const mock = new MockProducer();
  const producer = verifiable(mock, { createTime: -5 });
  const lines = await captured(() => producer.send(null, '0'));
  assert.deepEqual([producer.numSent, mock.history()], [0, []]);
  const [line] = json(lines).map(withoutTimestamp);
  assert.deepEqual(
    { ...line, message: undefined },
    {
      name: 'producer_send_error',
      key: null,
      value: '0',
      topic: 't',
      exception: 'class @confluentinc/kafka-javascript.IllegalArgumentError',
      message: undefined,
    },
  );
  // Java's ProducerRecord message, its null spelled as JavaScript's.
  assert.equal(line.message, 'Invalid timestamp: -5. Timestamp should always be non-negative or null.');
});

// ---------------------------------------------------------------------------
// The producing loop
// ---------------------------------------------------------------------------

test('run produces maxMessages', async () => {
  const mock = new MockProducer();
  const producer = verifiable(mock, { maxMessages: 3 });
  const lines = await captured(() => producer.run(new ThroughputThrottler(-1, 0)));
  assert.deepEqual(
    mock.history().map((record) => record.value),
    ['0', '1', '2'],
  );
  assert.deepEqual(
    json(lines).map((line) => line.name),
    ['startup_complete', 'producer_send_success', 'producer_send_success', 'producer_send_success'],
  );
  assert.deepEqual([producer.numSent, producer.numAcked], [3, 3]);
});

test('run values carry the prefix and keys repeat', async () => {
  const mock = new MockProducer();
  const producer = verifiable(mock, { maxMessages: 3, valuePrefix: 2, repeatingKeys: 2 });
  await captured(() => producer.run(new ThroughputThrottler(-1, 0)));
  assert.deepEqual(
    mock.history().map((r) => [r.key, r.value]),
    [
      ['0', '2.0'],
      ['1', '2.1'],
      ['0', '2.2'],
    ],
  );
});

test('run stops when stopProducing is set', async () => {
  const mock = new MockProducer();
  const producer = verifiable(mock);
  producer.stopProducing = true;
  const lines = await captured(() => producer.run(new ThroughputThrottler(-1, 0)));
  assert.deepEqual(mock.history(), []);
  assert.deepEqual(
    json(lines).map((line) => line.name),
    ['startup_complete'],
  );
});

test('close closes the producer and prints shutdown_complete', async () => {
  const mock = new MockProducer();
  const lines = await captured(() => verifiable(mock).close());
  assert.ok(mock.closed());
  assert.deepEqual(json(lines).map(withoutTimestamp), [{ name: 'shutdown_complete' }]);
});

// ---------------------------------------------------------------------------
// The events
// ---------------------------------------------------------------------------

test('lifecycle events put timestamp then name first', () => {
  assert.deepEqual(Object.keys(startupComplete()), ['timestamp', 'name']);
  assert.equal(startupComplete().name, 'startup_complete');
  assert.deepEqual(Object.keys(shutdownComplete()), ['timestamp', 'name']);
  assert.equal(shutdownComplete().name, 'shutdown_complete');
});

test('successful_send fields are in Java order', () => {
  const data = successfulSend('k', 'v', { topic: 't', partition: 2, offset: 42 });
  assert.deepEqual(Object.keys(data), ['timestamp', 'name', 'key', 'value', 'topic', 'partition', 'offset']);
  assert.deepEqual(withoutTimestamp(data), {
    name: 'producer_send_success',
    key: 'k',
    value: 'v',
    topic: 't',
    partition: 2,
    offset: 42,
  });
});

test('failed_send fields are in Java order', () => {
  const data = failedSend(null, 'v', 't', new KafkaError('boom'));
  assert.deepEqual(Object.keys(data), ['timestamp', 'name', 'key', 'value', 'topic', 'exception', 'message']);
  assert.deepEqual(withoutTimestamp(data), {
    name: 'producer_send_error',
    key: null,
    value: 'v',
    topic: 't',
    exception: KAFKA_ERROR_CLASS,
    message: 'boom',
  });
});

test('failed_send with no message is null', () => {
  // Java's getMessage() is null for KafkaException(); Jackson writes null.
  assert.equal(failedSend('k', 'v', 't', new KafkaError()).message, null);
});

test('failed_send names a class the client does not export by its name', () => {
  assert.equal(failedSend('k', 'v', 't', new TypeError('bad')).exception, 'class TypeError');
});

test('tool_data fields are in Java order', () => {
  const data = toolData(10, 9, 100, 4.5);
  assert.deepEqual(Object.keys(data), ['timestamp', 'name', 'sent', 'acked', 'target_throughput', 'avg_throughput']);
  assert.deepEqual(withoutTimestamp(data), {
    name: 'tool_data',
    sent: 10,
    acked: 9,
    target_throughput: 100,
    avg_throughput: 4.5,
  });
});

// ---------------------------------------------------------------------------
// The command line
// ---------------------------------------------------------------------------

function parse(...args) {
  return parseArgs(argParser(), args);
}

function parseError(...args) {
  let message;
  assert.throws(
    () => parse(...args),
    (e) => {
      assert.ok(e instanceof ArgumentParserError, `${e}`);
      message = e.message;
      return true;
    },
  );
  return message;
}

test('parse_args minimal', () => {
  const [res, props] = parse('--topic', 't', '--bootstrap-server', 'b:9092');
  assert.deepEqual(
    [res.topic, res.bootstrapServer, res.maxMessages, res.throughput, res.acks],
    ['t', 'b:9092', -1, -1, -1],
  );
  assert.deepEqual([res.createTime, res.valuePrefix, res.repeatingKeys], [-1, null, null]);
  assert.deepEqual(props, { 'bootstrap.servers': 'b:9092', acks: '-1', retries: '0' });
});

test('parse_args every option', () => {
  const [res, props] = parse(
    '--topic', 't', '--bootstrap-server', 'b', '--max-messages', '5', '--throughput', '100', '--acks', '1',
    '--message-create-time', '1234', '--value-prefix', '3', '--repeating-keys', '4',
  );
  assert.deepEqual(
    [res.maxMessages, res.throughput, res.createTime, res.valuePrefix, res.repeatingKeys],
    [5, 100, 1234, 3, 4],
  );
  assert.equal(props.acks, '1');
});

test('parse_args takes an option as --name=value', () => {
  const [res] = parse('--topic=t', '--bootstrap-server=b', '--max-messages=-1');
  assert.deepEqual([res.topic, res.bootstrapServer, res.maxMessages], ['t', 'b', -1]);
});

test('parse_args valid acks choices', () => {
  for (const acks of ['0', '1', '-1']) {
    const [res, props] = parse('--topic', 't', '--bootstrap-server', 'b', '--acks', acks);
    assert.deepEqual([res.acks, props.acks], [Number(acks), acks]);
  }
});

test('parse_args invalid acks choice', () => {
  assert.equal(
    parseError('--topic', 't', '--bootstrap-server', 'b', '--acks', '5'),
    "argument --acks: invalid choice: '5' (choose from '0', '1', '-1')",
  );
});

test('parse_args missing topic', () => {
  assert.equal(parseError('--bootstrap-server', 'b'), 'the following arguments are required: --topic');
});

test('parse_args missing bootstrap server', () => {
  assert.equal(parseError('--topic', 't'), 'one of the arguments --bootstrap-server is required');
});

test('parse_args missing value', () => {
  assert.equal(parseError('--topic'), 'argument --topic: expected one argument');
});

test('parse_args not an integer', () => {
  assert.equal(
    parseError('--topic', 't', '--bootstrap-server', 'b', '--max-messages', 'x'),
    "argument --max-messages: invalid int value: 'x'",
  );
});

test('parse_args unrecognized argument', () => {
  assert.equal(parseError('--topic', 't', '--bootstrap-server', 'b', '--bogus'), 'unrecognized arguments: --bogus');
});

test('parse_args config files are mutually exclusive', () => {
  const config = tmpFile('p.properties', '');
  assert.equal(
    parseError('--topic', 't', '--bootstrap-server', 'b', '--producer.config', config, '--command-config', config),
    'Options --producer.config and --command-config are mutually exclusive.',
  );
});

test('the command config overrides the command line', () => {
  const config = tmpFile('p.properties', '# written by the system test\nacks=1\nretries = 5\nlinger.ms: 10\nclient.id   tool\n');
  const [, props] = parse('--topic', 't', '--bootstrap-server', 'b', '--command-config', config);
  assert.deepEqual(props, {
    'bootstrap.servers': 'b',
    acks: '1',
    retries: '5',
    'linger.ms': '10',
    'client.id': 'tool',
  });
});

test('the deprecated producer config still loads', async () => {
  const config = tmpFile('p.properties', 'linger.ms=10\n');
  let props;
  const lines = await captured(() => {
    [, props] = parse('--topic', 't', '--bootstrap-server', 'b', '--producer.config', config);
  });
  assert.equal(props['linger.ms'], '10');
  assert.deepEqual(lines, [
    'Option --producer.config has been deprecated and will be removed in a future version. Use --command-config instead.',
  ]);
});

test('an unreadable config file is an argument error', () => {
  const missing = tmpFile('missing.properties');
  assert.ok(parseError('--topic', 't', '--bootstrap-server', 'b', '--command-config', missing).includes(missing));
});

test('createFromArgs builds a KafkaProducer', async () => {
  // The configuration the tool builds is one the client accepts.
  const producer = createFromArgs(argParser(), [
    '--topic', 't', '--bootstrap-server', 'localhost:1', '--message-create-time', '-1', '--throughput', '7',
  ]);
  try {
    assert.deepEqual([producer.topic, producer.throughput, producer.createTime], ['t', 7, null]);
  } finally {
    await producer.producer.close();
  }
});

// ---------------------------------------------------------------------------
// The script, end to end, against an unreachable broker
// ---------------------------------------------------------------------------

function runScript(args, timeout = 60_000) {
  return spawnSync(process.execPath, [SCRIPT, ...args], { encoding: 'utf8', timeout });
}

test('the script without arguments prints help', () => {
  const result = runScript([]);
  assert.equal(result.status, 0, result.stderr);
  assert.ok(result.stdout.startsWith('usage: verifiable-producer'), result.stdout);
});

test('the script with bad arguments exits 1', () => {
  const result = runScript(['--topic', 't']);
  assert.equal(result.status, 1);
  assert.equal(result.stdout, '');
  assert.ok(
    result.stderr.endsWith('verifiable-producer: error: one of the arguments --bootstrap-server is required\n'),
    result.stderr,
  );
});

test('the script reports unacked sends then its summary', () => {
  // No broker: each send fails after max.block.ms.
  const config = tmpFile('p.properties', 'max.block.ms=200\n');
  const result = runScript([
    '--topic', 't', '--bootstrap-server', 'localhost:1', '--max-messages', '2', '--command-config', config,
  ]);
  assert.equal(result.status, 0, result.stderr);
  const lines = json(result.stdout.split('\n').filter((l) => l !== ''));
  assert.deepEqual(
    lines.map((line) => line.name),
    ['startup_complete', 'producer_send_error', 'producer_send_error', 'shutdown_complete', 'tool_data'],
  );
  assert.deepEqual(
    lines.slice(1, 3).map((line) => line.value),
    ['0', '1'],
  );
  const summary = lines[lines.length - 1];
  assert.deepEqual(
    { sent: summary.sent, acked: summary.acked, target_throughput: summary.target_throughput },
    { sent: 2, acked: 0, target_throughput: -1 },
  );
});

/** Starts the script; `next()` resolves to its next stdout line, parsed. */
function startScript(args) {
  const child = spawn(process.execPath, [SCRIPT, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
  let out = '';
  let err = '';
  const waiters = [];
  const lines = [];
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => {
    out += chunk;
    let nl;
    while ((nl = out.indexOf('\n')) >= 0) {
      lines.push(JSON.parse(out.slice(0, nl)));
      out = out.slice(nl + 1);
      while (waiters.length > 0 && lines.length > 0) waiters.shift()(lines.shift());
    }
  });
  child.stderr.on('data', (chunk) => {
    err += chunk;
  });
  const exited = new Promise((resolve) => child.on('exit', (code) => resolve(code)));
  return {
    child,
    next: () => (lines.length > 0 ? Promise.resolve(lines.shift()) : new Promise((resolve) => waiters.push(resolve))),
    async end(timeoutMs) {
      const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs);
      const code = await exited;
      clearTimeout(timer);
      return { code, rest: lines.splice(0), stderr: err };
    },
  };
}

test('the script shuts down cleanly on SIGTERM', async () => {
  // --throughput 0 parks the loop in throttle() once time has passed since
  // the start (each send waits max.block.ms for metadata), until the shutdown
  // hook wakes it.
  const config = tmpFile('p.properties', 'max.block.ms=200\n');
  const script = startScript([
    '--topic', 't', '--bootstrap-server', 'localhost:1', '--throughput', '0', '--command-config', config,
  ]);
  try {
    assert.equal((await script.next()).name, 'startup_complete');
    assert.equal((await script.next()).name, 'producer_send_error');
    script.child.kill('SIGTERM');
    const { code, rest, stderr } = await script.end(60_000);
    assert.equal(code, 0, stderr);
    const names = rest.map((line) => line.name);
    assert.deepEqual(names.slice(-2), ['shutdown_complete', 'tool_data'], stderr);
    // Every line before them is a send that failed, and each was counted.
    const failed = 1 + names.filter((name) => name === 'producer_send_error').length;
    assert.ok(names.slice(0, -2).every((name) => name === 'producer_send_error'), names.join(','));
    const summary = rest[rest.length - 1];
    assert.deepEqual([summary.sent, summary.acked, summary.target_throughput], [failed, 0, 0]);
  } finally {
    script.child.kill('SIGKILL');
  }
});

test('SIGTERM releases a blocked send', async () => {
  // The first send waits for metadata that never comes, for up to
  // max.block.ms. As Java's shutdown hook, the tool's hook closes the producer
  // itself, which fails that send at once instead of letting it hold the
  // shutdown until it times out.
  const config = tmpFile('p.properties', 'max.block.ms=60000\n');
  const script = startScript(['--topic', 't', '--bootstrap-server', 'localhost:1', '--command-config', config]);
  try {
    assert.equal((await script.next()).name, 'startup_complete');
    await new Promise((resolve) => setTimeout(resolve, 500));
    const signalled = Date.now();
    script.child.kill('SIGTERM');
    const { code, rest, stderr } = await script.end(60_000);
    const shutdownSeconds = (Date.now() - signalled) / 1000;
    assert.equal(code, 0, stderr);
    assert.ok(shutdownSeconds < 20, stderr);
    const names = rest.map((line) => line.name);
    // The failed send is printed by the loop while the hook closes, so it may
    // come before or after the hook's lines.
    assert.deepEqual([...names].sort(), ['producer_send_error', 'shutdown_complete', 'tool_data'], names.join(','));
    assert.ok(names.indexOf('shutdown_complete') < names.indexOf('tool_data'));
    const error = rest[names.indexOf('producer_send_error')];
    assert.deepEqual([error.value, error.message], ['0', 'Producer closed while send in progress']);
    const summary = rest[names.indexOf('tool_data')];
    assert.deepEqual([summary.sent, summary.acked], [1, 0]);
  } finally {
    script.child.kill('SIGKILL');
  }
});
