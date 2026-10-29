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

// Broker-backed producer test: runs only when KAFKA_BOOTSTRAP_SERVERS is set
// (`make -C javascript test-integration` starts a scratch broker for it).

const test = require('node:test');
const assert = require('node:assert/strict');

const { KafkaProducer, ProducerRecord, RecordMetadata } = require('../..');

const BOOTSTRAP = process.env.KAFKA_BOOTSTRAP_SERVERS;
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to produce to';
if (SKIP) console.log(`skipping the integration tests: ${SKIP}`);

test('produces one record with a key, a value and headers', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = `js-it-${process.pid}-${Date.now()}`;
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, acks: 'all' });
  try {
    const before = Date.now();
    let callbacks = 0;
    let callbackMetadata;
    const metadata = await producer.send(
      new ProducerRecord({
        topic,
        key: 'k1',
        value: Buffer.from('hello-from-node'),
        headers: [
          { key: 'h1', value: 'v1' },
          { key: 'h2', value: null },
        ],
      }),
      (m, error) => {
        callbacks++;
        callbackMetadata = m;
        assert.equal(error, null);
      },
    );
    assert.equal(callbacks, 1);
    assert.ok(metadata instanceof RecordMetadata);
    assert.equal(callbackMetadata.toString(), metadata.toString());
    assert.equal(metadata.topic, topic);
    assert.equal(metadata.partition >= 0, true);
    assert.equal(metadata.hasOffset, true);
    assert.ok(metadata.offset >= 0, `offset ${metadata.offset}`);
    assert.equal(metadata.hasTimestamp, true);
    assert.ok(metadata.timestamp >= before - 60000, `timestamp ${metadata.timestamp}`);
    assert.equal(metadata.serializedKeySize, 2);
    assert.equal(metadata.serializedValueSize, 15);

    // A second record lands after the first on the same partition.
    const next = await producer.send(new ProducerRecord({ topic, partition: metadata.partition, value: 'second' }));
    assert.equal(next.partition, metadata.partition);
    assert.equal(next.offset, metadata.offset + 1);
    assert.equal(next.serializedKeySize, -1);
    await producer.flush();
  } finally {
    await producer.close();
  }
});
