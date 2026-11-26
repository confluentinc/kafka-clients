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

// The security suites' shared cases (ssl.test.js, sasl.test.js): the
// certificates `make security-certs` wrote, the producer test's core case over
// any security protocol, and the shape of a send whose connection fails its
// authentication.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { AuthenticationError } = require('@confluentinc/kafka-javascript/common/errors');
const { RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { fetchRecords } = require('./_fetch_records');

/** @typedef {import('@confluentinc/kafka-javascript/producer').ProducerConfigs} ProducerConfigs */

/**
 * The path of `name` in the certificates' directory, KAFKA_SECURITY_DIR.
 *
 * @param {string} name
 */
function certPath(name) {
  const dir = process.env.KAFKA_SECURITY_DIR;
  assert.ok(dir, 'KAFKA_SECURITY_DIR is not set');
  return path.join(dir, name);
}

/**
 * The PEM text of `name` in the certificates' directory.
 *
 * @param {string} name
 */
function pem(name) {
  return fs.readFileSync(certPath(name), 'utf8');
}

let topicCount = 0;
/** @param {string} what */
function freshTopic(what) {
  return `js-it-${what}-${process.pid}-${Date.now()}-${topicCount++}`;
}

/**
 * The producer test's core case over `configs`' security protocol: one record
 * with a key, a value and two headers, its callback run once with no error
 * and its future done with the metadata, then flush() and close(); the record
 * is read back, headers included, through `plaintextBootstrap`.
 *
 * @param {ProducerConfigs} configs
 * @param {string} plaintextBootstrap
 * @param {string} what
 */
async function produceOneRecord(configs, plaintextBootstrap, what) {
  const topic = freshTopic(what);
  const producer = new KafkaProducer({ configs: { acks: 'all', ...configs } });
  let closed = false;
  try {
    let callbacks = 0;
    /** @type {RecordMetadata | null} */
    let callbackMetadata = null;
    const future = await producer.send({
      record: new ProducerRecord({
        topic,
        partition: null,
        key: Buffer.from('k1'),
        value: Buffer.from(`hello-over-${what}`),
        headers: [new RecordHeader({ key: 'h1', value: Buffer.from('v1') }), ['h2', null]],
      }),
      callback: ({ metadata, exception }) => {
        callbacks++;
        callbackMetadata = metadata;
        assert.equal(exception, null);
      },
    });
    assert.ok(future instanceof KafkaFuture);
    const metadata = await future.get();
    assert.equal(callbacks, 1);
    assert.equal(future.isCompletedExceptionally(), false);
    assert.ok(metadata instanceof RecordMetadata);
    assert.equal(String(callbackMetadata), metadata.toString());
    assert.equal(metadata.topic, topic);
    assert.equal(metadata.hasOffset, true);
    assert.equal(metadata.serializedKeySize, 2);
    assert.equal(metadata.serializedValueSize, `hello-over-${what}`.length);
    await producer.flush();
    await producer.close();
    closed = true;

    const { records } = await fetchRecords(plaintextBootstrap, topic, metadata.partition);
    assert.deepEqual(
      records.map((r) => [
        r.offset,
        r.key && r.key.toString(),
        r.value && r.value.toString(),
        r.headers.map((h) => [h.key, h.value && h.value.toString()]),
      ]),
      [
        [
          Number(metadata.offset),
          'k1',
          `hello-over-${what}`,
          [
            ['h1', 'v1'],
            ['h2', null],
          ],
        ],
      ],
    );
  } finally {
    if (!closed) await producer.close();
  }
}

/**
 * Sends one record with `configs` and asserts the path Java's
 * KafkaProducer#doSend gives an AuthenticationException, an ApiException, from
 * the metadata wait: send() resolves, the callback runs once with the error
 * and Java's failure metadata (the record's topic, partition -1, every other
 * field -1), and the future is done, failed with it. Returns the error.
 *
 * @param {ProducerConfigs} configs
 * @param {string} what
 * @returns {Promise<Error>}
 */
async function sendFailingAuthentication(configs, what) {
  const topic = freshTopic(what);
  const producer = new KafkaProducer({ configs: { 'max.block.ms': 30000, ...configs } });
  try {
    /** @type {Array<{ metadata: RecordMetadata | null, exception: Error | null }>} */
    const calls = [];
    const future = await producer.send({
      record: new ProducerRecord({ topic, value: Buffer.from('v') }),
      callback: ({ metadata, exception }) => void calls.push({ metadata, exception }),
    });
    assert.ok(future instanceof KafkaFuture);
    /** @type {unknown} */
    let failure;
    await assert.rejects(future.get(), (e) => {
      assert.ok(e instanceof ExecutionError, String(e));
      failure = e.cause;
      return true;
    });
    assert.ok(failure instanceof AuthenticationError, String(failure));
    assert.equal(future.isDone(), true);
    assert.equal(future.isCompletedExceptionally(), true);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].exception, failure);
    const metadata = calls[0].metadata;
    assert.ok(metadata instanceof RecordMetadata);
    assert.deepEqual(
      [
        metadata.topic,
        metadata.partition,
        metadata.hasOffset,
        metadata.offset,
        metadata.hasTimestamp,
        metadata.serializedKeySize,
        metadata.serializedValueSize,
      ],
      [topic, -1, false, -1n, false, -1, -1],
    );
    return failure;
  } finally {
    await producer.close();
  }
}

module.exports = { certPath, pem, freshTopic, produceOneRecord, sendFailingAuthentication };
