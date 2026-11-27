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

// The README's examples against a broker (JS conventions, Documentation:
// the docs carry the contract): the Example, the Headers and Errors
// blocks, and examples/producer.js, which "`send` in two steps" describes,
// each run as a user runs it, and what they wrote read back; then the
// consumer's: its example, its create(), listener, wakeup() and
// MockConsumer blocks, and examples/consumer.js, each reading back records
// the producer wrote. test/unit/producer/index.test.js runs the producer's
// blocks without a broker.

const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');

const { TopicPartition } = require('@confluentinc/kafka-javascript/common');
const { StringDeserializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { CloseOptions, KafkaConsumer } = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { jsBlock, runBlock } = require('../_doc_snippets');
const { createTopic, fetchRecords, waitForBroker } = require('./_fetch_records');

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to produce to';
if (SKIP) console.log(`skipping the integration tests: ${SKIP}`);

// The broker `make test-integration` has just started may not listen yet, and
// this file may be the first to run.
beforeAll(async () => {
  if (!SKIP) await waitForBroker(BOOTSTRAP);
}, 90_000);

let topicCount = 0;
/** @param {string} what */
function freshTopic(what) {
  return `js-it-docs-${what}-${process.pid}-${Date.now()}-${topicCount++}`;
}

(SKIP ? test.skip : test)(
  "README's Example delivers its record and prints its metadata",
  async () => {
    const topic = freshTopic('example');
    await createTopic(BOOTSTRAP, topic);
    const { run, error } = await runBlock(jsBlock('README.md', 'Example'), {
      replace: [
        ["'bootstrap.servers': 'localhost:9092'", `'bootstrap.servers': ${JSON.stringify(BOOTSTRAP)}`],
        ["topic: 'demo'", `topic: ${JSON.stringify(topic)}`],
      ],
    });
    assert.equal(error, undefined);
    assert.equal(run.created.length, 1);
    assert.equal(run.printed.length, 1);
    assert.match(run.printed[0], new RegExp(`^${topic} 0 0n \\d+n$`));
    const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(
      records.map((r) => [r.key?.toString(), r.value?.toString()]),
      [['k1', 'hello']],
    );
  },
  60_000,
);

(SKIP ? test.skip : test)(
  "README's Headers: the record built there is sent with its three headers",
  async () => {
    const topic = freshTopic('headers');
    await createTopic(BOOTSTRAP, topic);
    const { StringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
    const built = await runBlock(jsBlock('README.md', 'Headers', 'new RecordHeader'), {
      scope: { ProducerRecord },
      replace: [["topic: 'demo'", `topic: ${JSON.stringify(topic)}`]],
      result: 'record',
    });
    assert.equal(built.error, undefined);
    const producer = new KafkaProducer({
      configs: { 'bootstrap.servers': BOOTSTRAP },
      keySerializer: new StringSerializer(),
      valueSerializer: new StringSerializer(),
    });
    try {
      const record = /** @type {import('@confluentinc/kafka-javascript/producer').ProducerRecord<string, string>} */ (built.value);
      await (await producer.send(record)).get();
    } finally {
      await producer.close();
    }
    const {
      records: [fetched],
    } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(
      fetched.headers.map((h) => [h.key, h.value]),
      [
        ['trace', Buffer.from('abc')],
        ['flag', null],
        ['more', Buffer.from([1])],
      ],
    );
  },
  60_000,
);

(SKIP ? test.skip : test)(
  "README's Errors: a record the broker takes completes the block without an error",
  async () => {
    const topic = freshTopic('errors');
    await createTopic(BOOTSTRAP, topic);
    const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP });
    try {
      const record = new ProducerRecord({ topic, key: null, value: Buffer.from('v') });
      const { error } = await runBlock(jsBlock('README.md', 'Errors', 'RetriableError'), { scope: { producer, record } });
      assert.equal(error, undefined);
    } finally {
      await producer.close();
    }
    assert.deepEqual(
      (await fetchRecords(BOOTSTRAP, topic, 0)).records.map((r) => r.value?.toString()),
      ['v'],
    );
  },
  60_000,
);

/**
 * Runs `node examples/<script>` with `args`, and gives its exit code and output.
 *
 * @param {string[]} args the example's arguments
 * @param {string} [script] the example
 * @returns {Promise<{ code: number | null, stdout: string, stderr: string }>} how it ended
 */
function runExample(args, script = 'producer.js') {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.join(__dirname, '..', '..', 'examples', script), ...args], {
      env: { ...process.env, NODE_NO_WARNINGS: '1' },
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => (stdout += d));
    child.stderr.on('data', (d) => (stderr += d));
    child.on('error', reject);
    child.on('close', (code) => resolve({ code, stdout, stderr }));
  });
}

(SKIP ? test.skip : test)(
  'examples/producer.js produces its record, prints its metadata and exits',
  async () => {
    const topic = freshTopic('example-script');
    await createTopic(BOOTSTRAP, topic);
    const { code, stdout, stderr } = await runExample([BOOTSTRAP, topic, 'k1', 'hello-from-node']);
    assert.equal(code, 0, stderr);
    assert.match(stdout, new RegExp(`^RecordMetadata topic=${topic} partition=0 offset=0 timestamp=\\d+\\n$`));
    const {
      records: [record],
    } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.equal(record.key?.toString(), 'k1');
    assert.equal(record.value?.toString(), 'hello-from-node');
    assert.deepEqual(
      record.headers.map((h) => [h.key, h.value?.toString()]),
      [['source', 'examples/producer.js']],
    );
  },
  60_000,
);

(SKIP ? test.skip : test)(
  'examples/producer.js without its arguments prints its usage and exits with 2',
  async () => {
    const { code, stdout, stderr } = await runExample([]);
    assert.equal(code, 2);
    assert.equal(stdout, '');
    assert.equal(stderr, 'usage: node examples/producer.js <bootstrap-servers> <topic> [key] [value]\n');
  },
  60_000,
);

/**
 * A topic holding `values`, in order, on partition 0, each with the key
 * `k<i>`, written by the producer.
 *
 * @param {string} what
 * @param {string[]} values
 * @returns {Promise<string>} the topic
 */
async function topicWith(what, values) {
  const topic = freshTopic(what);
  await createTopic(BOOTSTRAP, topic);
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP });
  try {
    for (const [i, value] of values.entries()) {
      await (await producer.send(new ProducerRecord({ topic, partition: 0, key: Buffer.from(`k${i}`), value: Buffer.from(value) }))).get();
    }
  } finally {
    await producer.close();
  }
  return topic;
}

(SKIP ? test.skip : test)(
  "README's consumer example reads the producer's record, prints it and commits",
  async () => {
    const topic = await topicWith('consumer-example', ['hello']);
    const group = `${topic}-group`;
    const { run, error } = await runBlock(jsBlock('README.md', 'The consumer', 'new KafkaConsumer'), {
      replace: [
        ["'bootstrap.servers': 'localhost:9092'", `'bootstrap.servers': ${JSON.stringify(BOOTSTRAP)}`],
        ["'group.id': 'demo-group'", `'group.id': ${JSON.stringify(group)}`],
        ["subscribe(['demo'])", `subscribe([${JSON.stringify(topic)}])`],
      ],
    });
    assert.equal(error, undefined);
    assert.deepEqual(run.printed, [`${topic} 0 0n k0 hello`]);
    // The commit is read back by another member of the group.
    const reader = new KafkaConsumer({ 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'group.id': group });
    try {
      const committed = await reader.committed([new TopicPartition({ topic, partition: 0 })]);
      assert.deepEqual(
        [...committed.values()].map((o) => o?.offset),
        [1n],
      );
    } finally {
      await reader.close();
    }
  },
  90_000,
);

(SKIP ? test.skip : test)(
  "README's create(), listener and wakeup() blocks: a consumer made by create(), whose revoke commits, and a poll loop that wakeup() ends",
  async () => {
    const topic = await topicWith('consumer-blocks', ['v0', 'v1']);
    const configs = {
      'bootstrap.servers': BOOTSTRAP,
      'group.protocol': 'consumer',
      'group.id': `${topic}-group`,
      'auto.offset.reset': 'earliest',
    };
    const made = await runBlock(jsBlock('README.md', 'Deserializers', 'KafkaConsumer.create'), {
      scope: { configs, KafkaConsumer, StringDeserializer },
      result: 'typed',
    });
    assert.equal(made.error, undefined);
    const consumer = /** @type {KafkaConsumer<string, string>} */ (made.value);
    try {
      const subscribed = await runBlock(jsBlock('README.md', 'The rebalance listener', 'onPartitionsRevoked'), {
        scope: { consumer },
        replace: [["topics: ['orders']", `topics: [${JSON.stringify(topic)}]`]],
      });
      assert.equal(subscribed.error, undefined);
      /** @type {unknown[]} */
      const handled = [];
      const looped = await runBlock(jsBlock('README.md', '`wakeup()` and stopping the poll loop', 'WakeupError'), {
        scope: {
          consumer,
          handle: (/** @type {{ key: string, value: string }} */ record) => {
            handled.push([record.key, record.value]);
            if (handled.length === 2) consumer.wakeup();
          },
        },
        replace: [["process.once('SIGINT', () => consumer.wakeup());\n", '']],
      });
      assert.equal(looped.error, undefined);
      assert.deepEqual(handled, [
        ['k0', 'v0'],
        ['k1', 'v1'],
      ]);
      // The block closed the consumer: a second close() resolves.
    } finally {
      await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
    }
  },
  90_000,
);

(SKIP ? test.skip : test)(
  "README's MockConsumer block polls the record it added",
  async () => {
    const { run, error } = await runBlock(jsBlock('README.md', 'MockConsumer', 'new MockConsumer'));
    assert.equal(error, undefined);
    assert.deepEqual(run.printed, ['1 v']);
  },
  30_000,
);

(SKIP ? test.skip : test)(
  'examples/consumer.js reads the records, prints them, commits and exits',
  async () => {
    const topic = await topicWith('consumer-script', ['one', 'two']);
    const group = `${topic}-group`;
    const { code, stdout, stderr } = await runExample([BOOTSTRAP, topic, group, '2'], 'consumer.js');
    assert.equal(code, 0, stderr);
    assert.equal(
      stdout,
      `ConsumerRecord topic=${topic} partition=0 offset=0 key=k0 value=one\n` +
        `ConsumerRecord topic=${topic} partition=0 offset=1 key=k1 value=two\n`,
    );
    const reader = new KafkaConsumer({ 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'group.id': group });
    try {
      const committed = await reader.committed([new TopicPartition({ topic, partition: 0 })]);
      assert.deepEqual(
        [...committed.values()].map((o) => o?.offset),
        [2n],
      );
    } finally {
      await reader.close();
    }
  },
  90_000,
);

(SKIP ? test.skip : test)(
  'examples/consumer.js without its arguments prints its usage and exits with 2',
  async () => {
    const { code, stdout, stderr } = await runExample([], 'consumer.js');
    assert.equal(code, 2);
    assert.equal(stdout, '');
    assert.equal(stderr, 'usage: node examples/consumer.js <bootstrap-servers> <topic> <group-id> [max-records]\n');
  },
  60_000,
);
