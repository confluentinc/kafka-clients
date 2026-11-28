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

// The producer and the consumer over SASL/PLAIN, on SASL_PLAINTEXT and
// SASL_SSL, and their authorization: runs only when the SASL broker's variables are set
// (`make -C javascript test-integration-sasl` starts the broker, which runs
// the StandardAuthorizer with admin as a super user; alice has only the ACLs
// a test gives her, through kafka-acls.sh in the broker's container).

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');
const {
  AuthenticationError,
  AuthorizationError,
  GroupAuthorizationError,
  SaslAuthenticationError,
  TopicAuthorizationError,
} = require('@confluentinc/kafka-javascript/common/errors');
const { CloseOptions, KafkaConsumer } = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { createTopic, fetchRecords } = require('./_fetch_records');
const {
  certPath,
  consumeOneRecord,
  freshTopic,
  pollFailingAuthentication,
  pollUntilRejected,
  produceOneRecord,
  sendFailingAuthentication,
  topicWithOneRecord,
} = require('./_security');

const SASL_PLAINTEXT = process.env.KAFKA_SASL_PLAINTEXT_BOOTSTRAP_SERVERS;
const SASL_SSL = process.env.KAFKA_SASL_SSL_BOOTSTRAP_SERVERS;
const ADMIN = process.env.KAFKA_SASL_ADMIN_BOOTSTRAP_SERVERS;
const CONTAINER = process.env.KAFKA_SASL_CONTAINER;
const SKIP =
  SASL_PLAINTEXT && SASL_SSL && ADMIN && process.env.KAFKA_SECURITY_DIR
    ? false
    : 'KAFKA_SASL_PLAINTEXT_BOOTSTRAP_SERVERS, KAFKA_SASL_SSL_BOOTSTRAP_SERVERS, KAFKA_SASL_ADMIN_BOOTSTRAP_SERVERS or KAFKA_SECURITY_DIR is not set: no SASL broker';
if (SKIP) console.log(`skipping the SASL integration tests: ${SKIP}`);
const SKIP_ACLS = SKIP || (CONTAINER ? false : 'KAFKA_SASL_CONTAINER is not set: no broker container to run kafka-acls.sh in');
if (SKIP_ACLS && !SKIP) console.log(`skipping the authorization test: ${SKIP_ACLS}`);

// Strings in every test that runs: the tests are skipped when one is unset.
const saslPlaintext = /** @type {string} */ (SASL_PLAINTEXT);
const saslSsl = /** @type {string} */ (SASL_SSL);
const admin = /** @type {string} */ (ADMIN);

/**
 * The SASL/PLAIN keys for `username` and `password` over `protocol`.
 *
 * @param {'SASL_PLAINTEXT' | 'SASL_SSL'} protocol
 * @param {string} username
 * @param {string} password
 * @returns {Record<string, string>} keys a producer's and a consumer's configuration both take
 */
function plain(protocol, username, password) {
  return {
    'bootstrap.servers': protocol === 'SASL_SSL' ? saslSsl : saslPlaintext,
    'security.protocol': protocol,
    'sasl.mechanism': 'PLAIN',
    'sasl.jaas.config': `org.apache.kafka.common.security.plain.PlainLoginModule required username="${username}" password="${password}";`,
    ...(protocol === 'SASL_SSL' ? { 'ssl.truststore.location': certPath('ca.pem') } : {}),
  };
}

/**
 * Runs kafka-acls.sh in the broker's container, over its PLAINTEXT listener
 * (ANONYMOUS, a super user), with `args`; returns its stdout. The listener
 * has the same port inside the container as outside, so the address the
 * tests use is the one the broker advertises to the tool too.
 *
 * @param {string[]} args
 */
function kafkaAcls(args) {
  const run = spawnSync(
    'docker',
    ['exec', /** @type {string} */ (CONTAINER), '/opt/kafka/bin/kafka-acls.sh', '--bootstrap-server', admin, ...args],
    { encoding: 'utf8', timeout: 120000 },
  );
  assert.equal(run.status, 0, `kafka-acls.sh ${args.join(' ')}: ${run.stderr}`);
  return run.stdout;
}

/**
 * Gives alice `operation` on `resource` and waits until the broker lists it.
 *
 * @param {string} operation
 * @param {string[]} resource
 */
async function allowAlice(operation, resource) {
  kafkaAcls(['--add', '--allow-principal', 'User:alice', '--operation', operation, ...resource]);
  const deadline = Date.now() + 30000;
  // kafka-acls.sh lists the operation by its AclOperation name (IDEMPOTENT_WRITE for IdempotentWrite).
  const listed = `principal=User:alice, host=*, operation=${operation.replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase()},`;
  while (!kafkaAcls(['--list', ...resource]).includes(listed)) {
    assert.ok(Date.now() < deadline, `the broker does not list alice's ${operation} ACL`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

(SKIP ? test.skip : test)(
  'produces over SASL_PLAINTEXT with PLAIN: one record with headers, flush, close',
  async () => {
    await produceOneRecord(plain('SASL_PLAINTEXT', 'admin', 'admin-secret'), admin, 'sasl-plaintext');
  },
  120000,
);

(SKIP ? test.skip : test)(
  'produces over SASL_SSL with PLAIN: one record with headers, flush, close',
  async () => {
    await produceOneRecord(plain('SASL_SSL', 'admin', 'admin-secret'), admin, 'sasl-ssl');
  },
  120000,
);

// ClientAuthenticationFailureTest#testProducerWithInvalidCredentials: the
// record's future fails with SaslAuthenticationException. The broker's
// PlainSaslServer#evaluateResponse throws "Authentication failed: Invalid
// username or password", SaslServerAuthenticator sends it back in its
// SASL_AUTHENTICATION_FAILED response, and SaslClientAuthenticator raises
// SaslAuthenticationException with that message, which reaches the send
// through the metadata wait (KafkaProducer#doSend's ApiException catch).
(SKIP ? test.skip : test)(
  "a wrong SASL password fails the first send with SaslAuthenticationError, Java's broker message",
  async () => {
    const failure = await sendFailingAuthentication(plain('SASL_PLAINTEXT', 'admin', 'anotherpassword'), 'sasl-wrong-password');
    assert.equal(failure.constructor, SaslAuthenticationError);
    assert.ok(failure instanceof AuthenticationError);
    assert.equal(failure.name, 'SaslAuthenticationError');
    assert.equal(failure.message, 'Authentication failed: Invalid username or password');
  },
  120000,
);

// Sender#completeBatch: a produce response's TOPIC_AUTHORIZATION_FAILED fails
// the batch with new TopicAuthorizationException(Collections.singleton(topic)),
// whose message is "Not authorized to access topics: [<topic>]", through the
// callback and the future; send() itself took the record, as the metadata
// (DESCRIBE) was authorized. TopicAuthorizationException is not fatal to an
// idempotent producer (TransactionManager#maybeTransitionToErrorState), so
// once alice has WRITE, the same producer delivers.
(SKIP_ACLS ? test.skip : test)(
  'a user without WRITE gets TopicAuthorizationError naming the topic through the callback; once WRITE is granted, later sends deliver',
  async () => {
    const topic = freshTopic('acl');
    await createTopic(admin, topic);
    // DESCRIBE for the metadata, IdempotentWrite for the idempotent
    // producer's InitProducerId, and no WRITE.
    await allowAlice('Describe', ['--topic', topic]);
    await allowAlice('IdempotentWrite', ['--cluster']);
    const producer = new KafkaProducer({ configs: { ...plain('SASL_PLAINTEXT', 'alice', 'alice-secret'), 'max.block.ms': 30000 } });
    try {
      /** @type {Array<{ metadata: RecordMetadata | null, exception: Error | null }>} */
      const calls = [];
      const future = await producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('denied') }),
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
      assert.ok(failure instanceof TopicAuthorizationError, String(failure));
      assert.ok(failure instanceof AuthorizationError);
      assert.equal(failure.message, `Not authorized to access topics: [${topic}]`);
      assert.deepEqual(failure.unauthorizedTopics(), new Set([topic]));
      assert.equal(calls.length, 1);
      assert.equal(calls[0].exception, failure);

      await allowAlice('Write', ['--topic', topic]);
      /** @type {RecordMetadata | null} */
      let delivered = null;
      const deadline = Date.now() + 60000;
      while (delivered === null) {
        assert.ok(Date.now() < deadline, 'no send delivered within 60 s of the WRITE grant');
        try {
          delivered = await (
            await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('allowed') }))
          ).get();
        } catch (e) {
          // A send racing the ACL's propagation is still refused, with the same error.
          assert.ok(e instanceof ExecutionError && e.cause instanceof TopicAuthorizationError, String(e));
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }
      assert.equal(delivered.topic, topic);
      const { records } = await fetchRecords(admin, topic, 0);
      assert.deepEqual(
        records.map((r) => r.value && r.value.toString()),
        ['allowed'],
      );
    } finally {
      await producer.close();
    }
  },
  240000,
);

(SKIP ? test.skip : test)(
  'consumes over SASL_PLAINTEXT with PLAIN: one record with its header, committed and read back',
  async () => {
    await consumeOneRecord(plain('SASL_PLAINTEXT', 'admin', 'admin-secret'), admin, 'consumer-sasl-plaintext');
  },
  120000,
);

(SKIP ? test.skip : test)(
  'consumes over SASL_SSL with PLAIN: one record with its header, committed and read back',
  async () => {
    await consumeOneRecord(plain('SASL_SSL', 'admin', 'admin-secret'), admin, 'consumer-sasl-ssl');
  },
  120000,
);

// ClientAuthenticationFailureTest#testConsumerWithInvalidCredentials: after
// assign(), poll() throws SaslAuthenticationException, the broker's message
// (PlainSaslServer#evaluateResponse), which the metadata keeps as a fatal
// error and AsyncKafkaConsumer#poll throws, as for an SSL handshake.
(SKIP ? test.skip : test)(
  "a wrong SASL password fails the consumer's poll() with SaslAuthenticationError, Java's broker message",
  async () => {
    const failure = await pollFailingAuthentication(
      plain('SASL_PLAINTEXT', 'admin', 'anotherpassword'),
      admin,
      'consumer-sasl-wrong-password',
    );
    assert.ok(failure instanceof SaslAuthenticationError, String(failure));
    assert.equal(failure.constructor, SaslAuthenticationError);
    assert.ok(failure instanceof AuthenticationError);
    assert.equal(failure.message, 'Authentication failed: Invalid username or password');
  },
  120000,
);

// alice has DESCRIBE on the topic throughout, for its metadata, its offsets
// and the heartbeat's subscription. Without READ on the group, the broker
// answers her ConsumerGroupHeartbeat with GROUP_AUTHORIZATION_FAILED, which
// Java's AbstractHeartbeatRequestManager#onErrorResponse makes fatal, as
// GroupAuthorizationException.forGroupId(groupId)'s message: "Not authorized
// to access group: <group>", thrown by the next poll(). With READ on the
// group and not on the topic, the member joins, and the fetch's
// TOPIC_AUTHORIZATION_FAILED is FetchCollector#handleInitializeErrors' new
// TopicAuthorizationException(Collections.singleton(topic)), "Not authorized
// to access topics: [<topic>]", thrown by poll(); the consumer stays usable,
// so once READ is granted on the topic, it consumes.
(SKIP_ACLS ? test.skip : test)(
  'a user without READ on the group gets GroupAuthorizationError, then without READ on the topic TopicAuthorizationError; once READ is granted, she consumes',
  async () => {
    const topic = await topicWithOneRecord(admin, 'consumer-acl');
    const group = `${topic}-group`;
    await allowAlice('Describe', ['--topic', topic]);
    const configs = {
      ...plain('SASL_PLAINTEXT', 'alice', 'alice-secret'),
      'group.protocol': 'consumer',
      'group.id': group,
      'auto.offset.reset': 'earliest',
    };

    const denied = new KafkaConsumer({ configs });
    try {
      await denied.subscribe([topic]);
      const failure = await pollUntilRejected(denied, 'GroupAuthorizationError');
      assert.ok(failure instanceof GroupAuthorizationError, String(failure));
      assert.equal(failure.constructor, GroupAuthorizationError);
      assert.ok(failure instanceof AuthorizationError);
      assert.equal(failure.message, `Not authorized to access group: ${group}`);
    } finally {
      await denied.close({ option: CloseOptions.timeout({ timeout: 0 }) });
    }

    await allowAlice('Read', ['--group', group]);
    const consumer = new KafkaConsumer({ configs });
    try {
      await consumer.subscribe([topic]);
      const failure = await pollUntilRejected(consumer, 'TopicAuthorizationError');
      assert.ok(failure instanceof TopicAuthorizationError, String(failure));
      assert.equal(failure.constructor, TopicAuthorizationError);
      assert.equal(failure.message, `Not authorized to access topics: [${topic}]`);
      assert.deepEqual(failure.unauthorizedTopics(), new Set([topic]));

      await allowAlice('Read', ['--topic', topic]);
      /** @type {string[]} */
      const values = [];
      const deadline = Date.now() + 60000;
      while (values.length === 0) {
        assert.ok(Date.now() < deadline, 'no record consumed within 60 s of the READ grant');
        try {
          for (const r of await consumer.poll(500)) values.push(String(r.value));
        } catch (e) {
          // A fetch racing the ACL's propagation is still refused, with the same error.
          assert.ok(e instanceof TopicAuthorizationError, String(e));
        }
      }
      assert.deepEqual(values, ['hello-over-consumer-acl']);
      assert.deepEqual([...consumer.assignment()], [new TopicPartition({ topic, partition: 0 })]);
    } finally {
      await consumer.close();
    }
  },
  240000,
);
