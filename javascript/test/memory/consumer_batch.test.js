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

// The consumer's native handle with no broker, read through the addon's
// test-only counts (consumerStats(), native/consumer.c): a consumer made and
// closed is destroyed once collected, a failed construction frees its struct,
// and an open consumer is never destroyed by a collection. Each probe runs in
// a child with --expose-gc and the allocator's poisoning, so a struct freed
// twice crashes it. The batches a poll fetches need a broker:
// test/integration/memory_probes.test.js holds them.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult: probe } = require('./_probe');

const CONFIGS = "{ 'bootstrap.servers': '127.0.0.1:1', 'group.protocol': 'consumer', 'group.id': 'g' }";

test('consumers made, used and closed are destroyed once collected, leaving nothing behind', () => {
  const out = probe(`
    const { KafkaConsumer, CloseOptions } = require(D + '/consumer');
    const { TopicPartition } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      await (async () => {
        for (let i = 0; i < 20; i++) {
          const c = new KafkaConsumer({ configs: ${CONFIGS} });
          await c.subscribe({ topics: ['t'] });
          await c.assign({ partitions: [new TopicPartition({ topic: 't', partition: 0 })] }).catch(() => {});
          await c.unsubscribe();
          await c.assign({ partitions: [new TopicPartition({ topic: 't', partition: 0 })] });
          await c.pause({ partitions: c.assignment() });
          await (await c.poll({ timeout: 0 })).count();
          c.metrics().size();
          await c.close({ option: CloseOptions.timeout({ timeout: 0 }) });
        }
      })();
      await settle();
      console.log(JSON.stringify({ leaks: leaks(), stats: native.consumerStats() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.stats.consumersMade, 20);
  assert.equal(out.stats.consumersFreed, 20);
  // An empty poll's records hold no key or value.
  assert.equal(out.stats.buffersMade, 0);
});

test('a consumer whose construction fails frees its native struct once collected, and the next one works', () => {
  const out = probe(`
    const { KafkaConsumer } = require(D + '/consumer');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const thrown = [];
      await (async () => {
        native.faultNext('notify');
        try {
          new KafkaConsumer({ configs: ${CONFIGS} });
          thrown.push('constructed');
        } catch (e) {
          thrown.push(e.name + ': ' + e.message);
        }
        // Rejected by the core before any native struct exists.
        try {
          new KafkaConsumer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g' } });
        } catch (e) {
          thrown.push(e.name);
        }
        const c = new KafkaConsumer({ configs: ${CONFIGS} });
        await c.subscribe({ topics: ['t'] });
        await c.close();
      })();
      const armed = native.faultNext(null);
      await settle();
      console.log(JSON.stringify({ thrown, armed, leaks: leaks(), stats: native.consumerStats() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.thrown, ['TypeError: self_ must not be null', 'UnsupportedOperationError']);
  assert.deepEqual(out.armed, []);
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.stats.consumersMade, 2);
});

test('an open consumer is never destroyed by a collection', () => {
  const out = probe(`
    const { KafkaConsumer } = require(D + '/consumer');
    (async () => {
      (() => {
        new KafkaConsumer({ configs: ${CONFIGS} });
      })();
      await settle();
      console.log(JSON.stringify({ stats: native.consumerStats() }));
      // The open consumer keeps Node running: the probe ends it.
      process.exit(0);
    })();
  `);
  assert.equal(out.stats.consumersMade, 1);
  assert.equal(out.stats.consumersFreed, 0);
});

test('a consumer whose close the FFI refused is still open: a collection never destroys it', () => {
  const out = probe(`
    const { KafkaConsumer } = require(D + '/consumer');
    (async () => {
      await (async () => {
        const c = new KafkaConsumer({ configs: ${CONFIGS} });
        await c.close({ option: null }).catch(() => {});
      })();
      await settle();
      console.log(JSON.stringify({ stats: native.consumerStats() }));
      // The open consumer keeps Node running: the probe ends it.
      process.exit(0);
    })();
  `);
  assert.equal(out.stats.consumersMade, 1);
  assert.equal(out.stats.consumersFreed, 0);
});

test('the test-only switch of the copy path arms and disarms, and a poll with no records makes no batch hold', () => {
  const out = probe(`
    const { KafkaConsumer } = require(D + '/consumer');
    const { TopicPartition } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      native.consumerRefuseExternalBuffers(true);
      await (async () => {
        const c = new KafkaConsumer({ configs: ${CONFIGS} });
        await c.assign({ partitions: [new TopicPartition({ topic: 't', partition: 0 })] });
        await c.poll({ timeout: 0 });
        await c.close({ option: null }).catch(() => {});
        await c.close();
      })();
      native.consumerRefuseExternalBuffers(false);
      await settle();
      console.log(JSON.stringify({ leaks: leaks(), stats: native.consumerStats() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.stats.copies, 0);
  assert.equal(out.stats.batchesMade, out.stats.batchesDestroyed);
});

test('the offset calls free every error they fail with, every answer, and the values built for the call', () => {
  // Each call fails at least once (a null, a negative timeout, an unassigned
  // partition, a closed consumer), and each answer is read; the errors' and
  // the consumers' counts must come back to zero.
  const out = probe(`
    const { KafkaConsumer, CloseOptions, OffsetAndMetadata } = require(D + '/consumer');
    const { TopicPartition, TopicPartitionMap } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      let failures = 0;
      const fail = (p) => p.then(() => {}, () => { failures++; });
      await (async () => {
        const tp = new TopicPartition({ topic: 't', partition: 0 });
        const other = new TopicPartition({ topic: 't', partition: 1 });
        const offsets = new TopicPartitionMap([[tp, new OffsetAndMetadata({ offset: 5n, leaderEpoch: 1, metadata: 'm' })]]);
        for (let i = 0; i < 5; i++) {
          const c = new KafkaConsumer({ configs: { ...${CONFIGS}, 'default.api.timeout.ms': 50 } });
          await c.assign({ partitions: [tp] });
          await c.commitSync();
          await c.commitAsync();
          await fail(c.commitSync({ timeout: -1 }));
          await fail(c.commitSync({ offsets: null }));
          await fail(c.commitSync({ offsets: new TopicPartitionMap([[tp, null]]) }));
          await fail(c.commitSync({ offsets, timeout: 20 }));
          await c.seek({ partition: tp, offset: 3 });
          await c.seek({ partition: tp, offsetAndMetadata: new OffsetAndMetadata(4) });
          await fail(c.seek({ partition: other, offset: 1 }));
          await fail(c.seek({ partition: tp, offsetAndMetadata: null }));
          await c.seekToBeginning({ partitions: [tp] });
          await fail(c.seekToEnd({ partitions: null }));
          await c.seek({ partition: tp, offset: 3 });
          await c.position({ partition: tp });
          await fail(c.position({ partition: other }));
          await fail(c.position(new TopicPartition({ topic: null, partition: 0 })));
          await c.committed({ partitions: [] });
          await fail(c.committed({ partitions: [tp], timeout: 20 }));
          await fail(c.committed({ partitions: null }));
          await c.currentLag({ topicPartition: tp });
          await fail(c.currentLag({ topicPartition: other }));
          c.groupMetadata();
          await fail(c.partitionsFor({ topic: 't', timeout: 20 }));
          await fail(c.partitionsFor(null));
          await fail(c.listTopics({ timeout: 20 }));
          await c.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap() });
          await fail(c.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[tp, 0n]]), timeout: 20 }));
          await fail(c.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[tp, -1n]]) }));
          await fail(c.offsetsForTimes(null));
          await c.beginningOffsets({ partitions: [tp], timeout: 0 });
          await fail(c.beginningOffsets({ partitions: [tp], timeout: 20 }));
          await fail(c.endOffsets({ partitions: [tp], timeout: 20 }));
          await fail(c.endOffsets(null));
          await c.close({ option: CloseOptions.timeout({ timeout: 0 }) });
          await fail(c.listTopics());
          await fail(c.commitSync());
          await fail(c.position({ partition: tp }));
          await fail(c.committed({ partitions: [tp] }));
        }
      })();
      await settle();
      console.log(JSON.stringify({ leaks: leaks(), failures }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.failures, 5 * 25);
});
