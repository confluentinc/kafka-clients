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

// The README's examples of the producer package run (JS conventions,
// Documentation: the docs carry the contract): each one a user can run as
// written, against MockProducer or an address where no broker listens, a
// fragment with the names the text around it defines.
// test/integration/doc_examples.test.js runs them against a broker. This file
// loads no second copy of the package: the addon builds the core's errors
// with the classes of the copy loaded last.

const assert = require('node:assert/strict');
const { jsBlock, runBlock } = require('../../_doc_snippets');

/** Where no broker listens, with waits short enough for a test. */
const UNREACHABLE = "'bootstrap.servers': '127.0.0.1:9', 'max.block.ms': 300";

test("README's Example: the producer is built and hands the record over; with no broker get() rejects with the metadata TimeoutError", async () => {
  // The metadata wait's TimeoutError is an ApiError, which Java's
  // org.apache.kafka.clients.producer.KafkaProducer#doSend hands to the
  // callback and the future, so send() resolves and get() rejects.
  const { ExecutionError } = require('@confluentinc/kafka-javascript');
  const { TimeoutError } = require('@confluentinc/kafka-javascript/common/errors');
  const { run, error } = await runBlock(jsBlock('README.md', 'Example'), {
    replace: [["'bootstrap.servers': 'localhost:9092'", UNREACHABLE]],
  });
  assert.equal(run.created.length, 1);
  assert.ok(error instanceof ExecutionError, `the example ended with ${String(error)}`);
  assert.ok(error.cause instanceof TimeoutError, `get() failed with ${error.cause}`);
  assert.equal(error.cause.message, 'Topic demo not present in metadata after 300 ms.');
  assert.deepEqual(run.printed, []);
});

test("README's Serializers: both ways of giving stringSerializer() build a producer", async () => {
  const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
  const { run, error, value } = await runBlock(jsBlock('README.md', 'Serializers', 'key.serializer'), {
    producerClasses: true,
    scope: { configs: { 'bootstrap.servers': '127.0.0.1:9' } },
    result: '[producer, same, stringSerializer]',
  });
  assert.equal(error, undefined);
  assert.equal(run.created.length, 2);
  assert.deepEqual(value, [...run.created, stringSerializer]);
});

test("README's Headers: the record built there holds the headers given and the one added", async () => {
  const { ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
  const { error, value } = await runBlock(jsBlock('README.md', 'Headers', 'new Header'), { scope: { ProducerRecord }, result: 'record' });
  assert.equal(error, undefined);
  assert.ok(value instanceof ProducerRecord);
  assert.deepEqual(
    value.headers.toArray().map((h) => [h.key(), h.value()]),
    [
      ['trace', Buffer.from('abc')],
      ['flag', null],
      ['more', Buffer.from([1])],
    ],
  );
});

test("README's MockProducer: it runs as written and prints what its comment says", async () => {
  const { run, error } = await runBlock(jsBlock('README.md', 'MockProducer', 'new MockProducer'));
  assert.equal(error, undefined);
  assert.deepEqual(run.printed, ['0n true']);
});

test("README's Errors: a retriable delivery error is caught, and any other error is thrown again as it is", async () => {
  const { KafkaError } = require('@confluentinc/kafka-javascript/common');
  const { KafkaProducer, MockProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
  const block = jsBlock('README.md', 'Errors', 'RetriableError');
  const record = new ProducerRecord({ topic: 'demo', key: null, value: Buffer.from('v') });

  // No broker: get() rejects with the metadata TimeoutError, a RetriableError.
  const producer = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:9', 'max.block.ms': 300 });
  try {
    const retried = await runBlock(block, { scope: { producer, record } });
    assert.equal(retried.error, undefined);
  } finally {
    await producer.close({ timeout: 0 });
  }

  // send() itself rejects with a KafkaError, which is not retriable.
  const mock = new MockProducer();
  const boom = new KafkaError('boom');
  mock.setSendException(boom);
  try {
    const thrown = await runBlock(block, { scope: { producer: mock, record } });
    assert.equal(thrown.error, boom);
  } finally {
    await mock.close();
  }
});

test("README's worker example: the record crosses a MessagePort as plain data and is rebuilt the same", async () => {
  const { MessageChannel } = require('node:worker_threads');
  const { Header } = require('@confluentinc/kafka-javascript/common/header');
  const { ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
  const record = new ProducerRecord({
    topic: 'orders',
    partition: 2,
    timestamp: 1700000000000n,
    key: Buffer.from('k'),
    value: Buffer.from('v'),
    headers: [new Header({ key: 'h', value: Buffer.from('1') }), new Header({ key: 'n', value: null })],
  });
  const { port1, port2 } = new MessageChannel();
  try {
    const received = new Promise((resolve) => port2.once('message', resolve));
    const { error, value } = await runBlock(jsBlock('README.md', 'Limitations', 'parentPort.postMessage'), {
      scope: { record, parentPort: port1, ProducerRecord, Header },
      result: 'copy',
    });
    assert.equal(error, undefined);
    assert.deepEqual(await received, {
      topic: 'orders',
      partition: 2,
      timestamp: 1700000000000n,
      key: new Uint8Array(Buffer.from('k')),
      value: new Uint8Array(Buffer.from('v')),
      headers: [
        ['h', new Uint8Array(Buffer.from('1'))],
        ['n', null],
      ],
    });
    assert.ok(value instanceof ProducerRecord);
    assert.ok(value.equals(record), `${value} is not ${record}`);
  } finally {
    port1.close();
    port2.close();
  }
});
