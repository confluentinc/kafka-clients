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

// A short stress of the consumer against a broker, for CI (a few minutes at
// most): many records the producer wrote, polled through StringDeserializers
// by a consumer assigned the topic's partitions. It checks that nothing
// crashes, each record is read back once, with its key and value as written,
// and the RSS stays bounded once warm: the test keeps no record, so a batch's
// Buffers and their fetch are collected (JS conventions, Serialization).
// STRESS_RECORDS sets the number of records (default 1,000,000).

const assert = require('node:assert/strict');

const { TopicPartition } = require('@confluentinc/kafka-javascript/common');
const { StringDeserializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { CloseOptions, KafkaConsumer } = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { createTopic, waitForBroker } = require('../integration/_fetch_records');

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to consume from';
if (SKIP) console.log(`skipping the consumer stress test: ${SKIP}`);

// The broker the Makefile has just started may not listen yet.
beforeAll(async () => {
  if (!SKIP) await waitForBroker(BOOTSTRAP);
}, 90_000);

const RECORDS = Number(process.env.STRESS_RECORDS ?? 1_000_000);
const PARTITIONS = 4;
const BATCH = 10_000;
const MIB = 1024 * 1024;
// A poll loop that reads nothing new for this long fails the test with how
// far it got, rather than only with the test's own timeout.
const STALL_MS = 60_000;

/**
 * Writes `count` records to `topic`, round robin over its partitions: record
 * `i`'s key is `i` and its value `value-<i>`, both as UTF-8 bytes.
 *
 * @param {string} topic
 * @param {number} count
 */
async function produce(topic, count) {
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 5, 'batch.size': 256 * 1024 });
  try {
    for (let from = 0; from < count; from += BATCH) {
      const sends = [];
      for (let i = from; i < Math.min(from + BATCH, count); i++) {
        sends.push(
          producer.send(
            new ProducerRecord({ topic, partition: i % PARTITIONS, key: Buffer.from(String(i)), value: Buffer.from(`value-${i}`) }),
          ),
        );
      }
      const futures = await Promise.all(sends);
      await Promise.all(futures.map((f) => f.get()));
    }
    await producer.flush();
  } finally {
    await producer.close();
  }
}

(SKIP ? test.skip : test)(
  `${RECORDS} records polled through StringDeserializers: each read once, as written, the RSS bounded`,
  async () => {
    const topic = `js-stress-consumer-${process.pid}-${Date.now()}`;
    await createTopic(BOOTSTRAP, topic, { partitions: PARTITIONS });
    await produce(topic, RECORDS);
    const consumer = new KafkaConsumer({
      configs: {
        'bootstrap.servers': BOOTSTRAP,
        'group.protocol': 'consumer',
        'auto.offset.reset': 'earliest',
        'max.poll.records': 5000,
      },
      keyDeserializer: new StringDeserializer(),
      valueDeserializer: new StringDeserializer(),
    });
    const reads = new Uint8Array(RECORDS);
    /** @type {number[]} */
    const rss = [];
    let total = 0;
    let wrong = /** @type {string | null} */ (null);
    try {
      const partitions = [];
      for (let partition = 0; partition < PARTITIONS; partition++) partitions.push(new TopicPartition({ topic, partition }));
      await consumer.assign(partitions);
      let lastProgress = Date.now();
      let nextSample = RECORDS / 100;
      while (total < RECORDS) {
        const records = await consumer.poll(500);
        for (const record of records) {
          const i = Number(record.key);
          if (wrong === null && (!(i >= 0 && i < RECORDS) || record.value !== `value-${i}`))
            wrong = `${record.topic}-${record.partition}@${record.offset}: key ${record.key}, value ${record.value}`;
          if (i >= 0 && i < RECORDS) reads[i]++;
          total++;
        }
        if (records.count() > 0) lastProgress = Date.now();
        else assert.ok(Date.now() - lastProgress < STALL_MS, `${total} of ${RECORDS} records read, none in ${STALL_MS / 1000} s`);
        if (total >= nextSample) {
          rss.push(process.memoryUsage().rss);
          nextSample += RECORDS / 100;
        }
      }
    } finally {
      await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
    }
    assert.equal(wrong, null);
    assert.equal(total, RECORDS);
    const notOnce = reads.findIndex((n) => n !== 1);
    assert.equal(notOnce, -1, `record ${notOnce} was read ${reads[notOnce]} times`);
    // Warm after the first tenth: from there the RSS may not grow by more than 256 MiB.
    const warm = rss.slice(Math.floor(rss.length / 10));
    const growth = (Math.max(...warm) - warm[0]) / MIB;
    assert.ok(growth < 256, `the RSS grew ${growth.toFixed(0)} MiB after the warm-up`);
  },
  300_000,
);
