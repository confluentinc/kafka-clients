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
// authentication. And the broker the consumer's broker tests run against
// (consumer.test.js, kafka_consumer_broker.test.js), over the listener
// KAFKA_TEST_LISTENER names: PLAINTEXT when unset, as `make test-integration`
// runs them, SSL in `make test-integration-ssl`, SASL_SSL in `make
// test-integration-sasl`.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { KafkaFuture, TopicPartition } = require('@confluentinc/kafka-javascript/common');
const { AuthenticationError } = require('@confluentinc/kafka-javascript/common/errors');
const { RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { CloseOptions, KafkaConsumer } = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { createTopic, fetchRecords } = require('./_fetch_records');

/** @typedef {import('@confluentinc/kafka-javascript/producer').ProducerConfigs} ProducerConfigs */
/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerConfigs} ConsumerConfigs */

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

/**
 * The configuration keys every client of a consumer broker test starts from:
 * the listener's address, and its security keys.
 *
 * @typedef {{
 *   'bootstrap.servers': string,
 *   'security.protocol'?: string,
 *   'ssl.truststore.location'?: string,
 *   'sasl.mechanism'?: string,
 *   'sasl.jaas.config'?: string,
 * }} ClientKeys
 */

/**
 * The broker the consumer's broker tests run against, over the listener
 * KAFKA_TEST_LISTENER names. `plaintext` is the broker's PLAINTEXT listener,
 * which `_fetch_records.js` speaks, to make topics and wait for the broker;
 * `clientConfigs()` gives the keys every client the tests make starts from,
 * the producers that write their records included: over SSL the truststore,
 * over SASL_SSL the truststore and SASL/PLAIN as `admin`, a super user, so the
 * StandardAuthorizer refuses nothing. `skip` is the reason the tests skip, or
 * `false`.
 *
 * @returns {{ listener: string, plaintext: string, skip: string | false, clientConfigs: () => ClientKeys }}
 */
function testBroker() {
  const listener = process.env.KAFKA_TEST_LISTENER || 'PLAINTEXT';
  /** @type {Record<string, [string | undefined, string | undefined, string[]]>} */
  const listeners = {
    PLAINTEXT: [process.env.KAFKA_BOOTSTRAP_SERVERS, process.env.KAFKA_BOOTSTRAP_SERVERS, ['KAFKA_BOOTSTRAP_SERVERS']],
    SSL: [
      process.env.KAFKA_SSL_BOOTSTRAP_SERVERS,
      process.env.KAFKA_SSL_PLAINTEXT_BOOTSTRAP_SERVERS,
      ['KAFKA_SSL_BOOTSTRAP_SERVERS', 'KAFKA_SSL_PLAINTEXT_BOOTSTRAP_SERVERS', 'KAFKA_SECURITY_DIR'],
    ],
    SASL_SSL: [
      process.env.KAFKA_SASL_SSL_BOOTSTRAP_SERVERS,
      process.env.KAFKA_SASL_ADMIN_BOOTSTRAP_SERVERS,
      ['KAFKA_SASL_SSL_BOOTSTRAP_SERVERS', 'KAFKA_SASL_ADMIN_BOOTSTRAP_SERVERS', 'KAFKA_SECURITY_DIR'],
    ],
  };
  const entry = listeners[listener];
  assert.ok(entry, `KAFKA_TEST_LISTENER is ${listener}: one of ${Object.keys(listeners).join(', ')}`);
  const [client, plaintext, needs] = entry;
  const missing = needs.filter((name) => !process.env[name]);
  const skip = missing.length === 0 ? false : `${missing.join(', ')} not set: no broker to consume from over ${listener}`;
  return {
    listener,
    plaintext: /** @type {string} */ (plaintext),
    skip,
    clientConfigs: () => {
      /** @type {ClientKeys} */
      const keys = { 'bootstrap.servers': /** @type {string} */ (client) };
      if (listener === 'PLAINTEXT') return keys;
      keys['security.protocol'] = listener;
      keys['ssl.truststore.location'] = certPath('ca.pem');
      if (listener === 'SASL_SSL') {
        keys['sasl.mechanism'] = 'PLAIN';
        keys['sasl.jaas.config'] =
          'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";';
      }
      return keys;
    },
  };
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

/**
 * Writes one record with a key, a value and a header to a new topic through
 * `plaintextBootstrap`.
 *
 * @param {string} plaintextBootstrap
 * @param {string} what
 * @returns {Promise<string>} the topic
 */
async function topicWithOneRecord(plaintextBootstrap, what) {
  const topic = freshTopic(what);
  await createTopic(plaintextBootstrap, topic);
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': plaintextBootstrap, acks: 'all' } });
  try {
    const future = await producer.send(
      new ProducerRecord({
        topic,
        partition: 0,
        key: Buffer.from('k1'),
        value: Buffer.from(`hello-over-${what}`),
        headers: [['h1', Buffer.from('v1')]],
      }),
    );
    await future.get();
  } finally {
    await producer.close();
  }
  return topic;
}

/**
 * The consumer's core case over `configs`' security protocol: a record
 * written through `plaintextBootstrap` is read back by a consumer that
 * subscribes as a group member, its key, value and header included, its
 * offset committed with commitSync() and read back with committed(), and the
 * consumer closed.
 *
 * @param {ConsumerConfigs} configs
 * @param {string} plaintextBootstrap
 * @param {string} what
 */
async function consumeOneRecord(configs, plaintextBootstrap, what) {
  const topic = await topicWithOneRecord(plaintextBootstrap, what);
  const consumer = new KafkaConsumer({
    configs: { 'group.protocol': 'consumer', 'group.id': `${topic}-group`, 'auto.offset.reset': 'earliest', ...configs },
  });
  try {
    await consumer.subscribe([topic]);
    /** @type {import('@confluentinc/kafka-javascript/consumer').ConsumerRecord<Buffer, Buffer>[]} */
    const records = [];
    const deadline = Date.now() + 60000;
    while (records.length === 0) {
      assert.ok(Date.now() < deadline, `no record consumed over ${what} within 60 s`);
      records.push(...(await consumer.poll(500)));
    }
    assert.deepEqual(
      records.map((r) => [
        r.topic,
        r.partition,
        r.offset,
        r.key && r.key.toString(),
        r.value && r.value.toString(),
        r.headers.toArray().map((h) => [h.key(), String(h.value())]),
      ]),
      [[topic, 0, 0n, 'k1', `hello-over-${what}`, [['h1', 'v1']]]],
    );
    await consumer.commitSync();
    const tp = new TopicPartition({ topic, partition: 0 });
    const committed = await consumer.committed([tp]);
    assert.equal(committed.get(tp)?.offset, 1n);
  } finally {
    await consumer.close();
  }
}

/**
 * Polls a consumer with `configs`, assigned partition 0 of a topic made
 * through `plaintextBootstrap`, until a poll rejects, and returns the error:
 * Java's AsyncKafkaConsumer#poll throws the authentication error its network
 * thread fails the poll's event with.
 *
 * @param {ConsumerConfigs} configs
 * @param {string} plaintextBootstrap
 * @param {string} what
 * @returns {Promise<unknown>}
 */
async function pollFailingAuthentication(configs, plaintextBootstrap, what) {
  const topic = await topicWithOneRecord(plaintextBootstrap, what);
  const consumer = new KafkaConsumer({ configs: { 'group.protocol': 'consumer', 'auto.offset.reset': 'earliest', ...configs } });
  try {
    await consumer.assign([new TopicPartition({ topic, partition: 0 })]);
    const deadline = Date.now() + 60000;
    for (;;) {
      assert.ok(Date.now() < deadline, `no poll over ${what} failed within 60 s`);
      try {
        const records = await consumer.poll(500);
        assert.equal(records.count(), 0, 'a record came back over a connection that should fail its authentication');
      } catch (e) {
        return e;
      }
    }
  } finally {
    await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  }
}

module.exports = {
  certPath,
  pem,
  freshTopic,
  produceOneRecord,
  sendFailingAuthentication,
  topicWithOneRecord,
  consumeOneRecord,
  pollFailingAuthentication,
  testBroker,
};
