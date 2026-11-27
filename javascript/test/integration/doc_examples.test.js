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
// each run as a user runs it, and what they wrote read back.
// test/unit/producer/index.test.js runs the blocks without a broker.

const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');

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
 * Runs `node examples/producer.js` with `args`, and gives its exit code and output.
 *
 * @param {string[]} args the example's arguments
 * @returns {Promise<{ code: number | null, stdout: string, stderr: string }>} how it ended
 */
function runExample(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.join(__dirname, '..', '..', 'examples', 'producer.js'), ...args], {
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
