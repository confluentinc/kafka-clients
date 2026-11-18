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

// The integration tests' protocol helper (_fetch_records.js): its Fetch
// reader against a fake server, which needs no broker, and its admin requests
// and batch decoding against the broker when KAFKA_BOOTSTRAP_SERVERS is set.

const assert = require('node:assert/strict');
const net = require('node:net');

const {
  connect,
  roundTrip,
  createTopic,
  deleteTopic,
  createPartitions,
  alterTopicConfigs,
  topicMetadata,
  fetchBatches,
  fetchRecords,
} = require('./_fetch_records');

const EVENTS = ['data', 'error', 'end', 'close', 'connect'];

/**
 * A server answering each request with `respond(socket)`; resolves to {server, port}.
 *
 * @param {(socket: net.Socket) => void} respond
 */
async function fakeServer(respond) {
  const server = net.createServer((socket) => socket.once('data', () => respond(socket)));
  await /** @type {Promise<void>} */ (new Promise((resolve) => server.listen(0, '127.0.0.1', resolve)));
  // A server listening on TCP has an address of its own.
  const address = server.address();
  assert.ok(address !== null && typeof address === 'object');
  return { server, port: address.port };
}

/**
 * A framed response: its 4-byte size, then `body`.
 *
 * @param {Buffer} body
 */
function frame(body) {
  const size = Buffer.alloc(4);
  size.writeInt32BE(body.length);
  return Buffer.concat([size, body]);
}

/** @param {net.Socket} socket */
function listenerCounts(socket) {
  return Object.fromEntries(EVENTS.map((event) => [event, socket.listenerCount(event)]));
}

test('a round trip rejects promptly when the connection closes mid-response', async () => {
  // The size announces 100 bytes; 10 arrive, then the server closes.
  const { server, port } = await fakeServer((socket) => {
    socket.end(frame(Buffer.alloc(100)).subarray(0, 14));
  });
  const socket = await connect('127.0.0.1', port);
  try {
    const before = listenerCounts(socket);
    await assert.rejects(roundTrip(socket, Buffer.from('request')), {
      message: 'the connection closed after 14 bytes of the response',
    });
    assert.deepEqual(listenerCounts(socket), before);
  } finally {
    socket.destroy();
    server.close();
  }
}, 10000);

test('a completed round trip removes every listener it added', async () => {
  const { server, port } = await fakeServer((socket) => {
    // The response arrives in two pieces.
    const response = frame(Buffer.from('hello'));
    socket.write(response.subarray(0, 3));
    setImmediate(() => socket.write(response.subarray(3)));
  });
  const socket = await connect('127.0.0.1', port);
  try {
    // connect() left none of its own.
    assert.deepEqual(listenerCounts(socket), listenerCounts(new net.Socket()));
    const before = listenerCounts(socket);
    assert.equal((await roundTrip(socket, Buffer.from('request'))).toString(), 'hello');
    assert.deepEqual(listenerCounts(socket), before);
  } finally {
    socket.destroy();
    server.close();
  }
});

test('connecting rejects when the connection is refused', async () => {
  const { server, port } = await fakeServer(() => {});
  await new Promise((resolve) => server.close(resolve));
  await assert.rejects(connect('127.0.0.1', port), { code: 'ECONNREFUSED' });
});

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = !BOOTSTRAP;

/** @param {string} what */
const helperTopic = (what) => `js-it-helper-${what}-${process.pid}-${Date.now()}`;

(SKIP ? test.skip : test)(
  'the helper creates a topic with partitions and configs, grows it, changes its configs and deletes it',
  async () => {
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const topic = helperTopic('admin');
    await createTopic(BOOTSTRAP, topic, { partitions: 3, configs: { 'message.timestamp.type': 'LogAppendTime' } });
    assert.deepEqual((await topicMetadata(BOOTSTRAP, topic)).partitions.map((p) => p.partition).sort(), [0, 1, 2]);
    await createPartitions(BOOTSTRAP, topic, 5);
    assert.equal((await topicMetadata(BOOTSTRAP, topic)).partitions.length, 5);
    await alterTopicConfigs(BOOTSTRAP, topic, { 'message.timestamp.type': 'CreateTime' });
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'compression.type': 'gzip' } });
    try {
      for (let i = 0; i < 3; i++) {
        await producer.send(new ProducerRecord({ topic, partition: 4, key: null, value: Buffer.from(`v${i}`), timestamp: 1000 + i }));
      }
      await producer.flush();
    } finally {
      await producer.close();
    }
    const { batches } = await fetchBatches(BOOTSTRAP, topic, 4);
    assert.ok(batches.length >= 1);
    assert.ok(batches.every((b) => b.codec === 'gzip' && !b.logAppendTime));
    // The producer is idempotent by default: one producer id, sequences from 0.
    assert.ok(batches.every((b) => b.producerId >= 0n && b.producerId === batches[0].producerId));
    assert.equal(batches[0].baseSequence, 0);
    const { records } = await fetchRecords(BOOTSTRAP, topic, 4);
    assert.deepEqual(
      records.map((r) => [r.offset, r.timestamp, r.value && r.value.toString()]),
      [
        [0, 1000, 'v0'],
        [1, 1001, 'v1'],
        [2, 1002, 'v2'],
      ],
    );
    await deleteTopic(BOOTSTRAP, topic);
    assert.equal((await topicMetadata(BOOTSTRAP, topic)).errorCode, 3); // UNKNOWN_TOPIC_OR_PARTITION
  },
  120000,
);

(SKIP ? test.skip : test)(
  'the helper decodes zstd batches and reports snappy and lz4 ones without their records',
  async () => {
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    for (const codec of ['zstd', 'snappy', 'lz4']) {
      const topic = helperTopic(codec);
      await createTopic(BOOTSTRAP, topic);
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'compression.type': codec } });
      try {
        await (await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.alloc(500, 7) }))).get();
      } finally {
        await producer.close();
      }
      const { batches } = await fetchBatches(BOOTSTRAP, topic, 0);
      assert.deepEqual(
        batches.map((b) => b.codec),
        [codec],
      );
      if (codec === 'zstd') {
        assert.deepEqual(
          batches[0].records?.map((r) => r.value),
          [Buffer.alloc(500, 7)],
        );
      } else {
        assert.equal(batches[0].records, null);
      }
    }
  },
  120000,
);
