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

// MockConsumer's native handle, read through the addon's test-only counts
// (consumerStats(), native/consumer.c and native/mock_values.c): the records
// added to a mock are held until its destroy and a record the FFI refuses is
// released at once; an injected error is let go once the core holds no copy
// of it, or at the destroy; a mock's poll makes no external buffer and no
// header handle; and an open mock is never destroyed by a collection. Each
// probe runs in a child with --expose-gc and the allocator's poisoning.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult: probe } = require('./_probe');

test("a mock's records are held until its destroy, a refused one is released at once, and its polls make no external buffer", () => {
  const out = probe(`
    const { MockConsumer, ConsumerRecord } = require(D + '/consumer');
    const { Headers, RecordHeader } = require(D + '/common/header');
    const { TimestampType } = require(D + '/common/record');
    const { TopicPartition, TopicPartitionMap } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const refs = [];
      const counts = {};
      await (async () => {
        const m = new MockConsumer('earliest');
        const tp = new TopicPartition({ topic: 't', partition: 0 });
        await m.assign([tp]);
        m.updateBeginningOffsets(new TopicPartitionMap([[tp, 0n]]));
        for (let i = 0; i < 50; i++) {
          const headers = new Headers([new RecordHeader({ key: 'h', value: Buffer.from('v' + i) })]);
          const r = new ConsumerRecord({
            topic: 't', partition: 0, offset: i, timestamp: 0, timestampType: TimestampType.CREATE_TIME,
            serializedKeySize: 1, serializedValueSize: 1, key: Buffer.from('k' + i), value: Buffer.from('v' + i),
            headers, leaderEpoch: i % 2 === 0 ? null : 3,
          });
          refs.push(new WeakRef(r));
          m.addRecord(r);
        }
        // Refused: a partition not assigned. Released at once.
        try {
          m.addRecord(new ConsumerRecord({ topic: 't', partition: 9, offset: 0, key: null, value: null }));
        } catch (e) {
          counts.refused = e.name;
        }
        let s = native.consumerStats();
        counts.heldBeforePoll = s.mockRecordsMade - s.mockRecordsFreed;
        let polled = 0;
        for (let i = 0; i < 3; i++) polled += (await m.poll(0)).count();
        counts.polled = polled;
        await settle();
        s = native.consumerStats();
        // The polled records stay held: the core may still hold a copy.
        counts.heldAfterPoll = s.mockRecordsMade - s.mockRecordsFreed;
        counts.alive = refs.filter((w) => w.deref() !== undefined).length;
        await m.close();
      })();
      await settle();
      console.log(JSON.stringify({ counts, collected: refs.every((w) => w.deref() === undefined), leaks: leaks(), stats: native.consumerStats() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.counts, { refused: 'IllegalStateError', heldBeforePoll: 50, polled: 50, heldAfterPoll: 50, alive: 50 });
  assert.ok(out.collected, 'a record added to the mock outlived its destroy');
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.stats.consumersMade, 1);
  assert.equal(out.stats.mockRecordsMade, 51);
  // A mock's batch holds the app's own records: no Buffer over it, no header handle.
  assert.equal(out.stats.batchesMade, 3);
  assert.equal(out.stats.buffersMade, 0);
  assert.equal(out.stats.sourcesMade, 0);
  assert.equal(out.stats.copies, 0);
});

test('an injected error is let go once the core holds no copy of it, or at the destroy', () => {
  const out = probe(`
    const { MockConsumer } = require(D + '/consumer');
    const { KafkaError } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = {};
      let delivered;
      await (async () => {
        const m = new MockConsumer('earliest');
        delivered = new WeakRef(new KafkaError('delivered'));
        m.setPollException(delivered.deref());
        counts.same = await m.poll(0).then(() => false, (e) => e === delivered.deref());
        // Replaced before any poll: the first one's only copy is dropped.
        m.setOffsetsException(new KafkaError('replaced'));
        m.setOffsetsException(new KafkaError('pending'));
        // A mock call sweeps the errors the core let go of.
        m.scheduleNopPollTask();
        let s = native.consumerStats();
        counts.heldWhileOpen = s.injectedMade - s.injectedFreed;
        await m.close();
      })();
      await settle();
      console.log(JSON.stringify({ counts, collected: delivered.deref() === undefined, leaks: leaks(), stats: native.consumerStats() }));
      clearInterval(hold);
    })();
  `);
  // The pending offsets error is the one still held while the mock is open.
  assert.deepEqual(out.counts, { same: true, heldWhileOpen: 1 });
  assert.ok(out.collected, 'the delivered error outlived the mock');
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.stats.injectedMade, 3);
});

test('mocks made, used and closed are destroyed once collected; an open mock never is', () => {
  const out = probe(`
    const { MockConsumer, ConsumerRecord } = require(D + '/consumer');
    const { Node, PartitionInfo, TopicPartition, TopicPartitionMap } = require(D + '/common');
    (async () => {
      await (async () => {
        const tp = new TopicPartition({ topic: 't', partition: 0 });
        const node = new Node({ id: 1, host: 'h', port: 1 });
        for (let i = 0; i < 10; i++) {
          const m = new MockConsumer('earliest');
          await m.subscribe(['t']);
          await m.rebalance([tp]);
          m.updateBeginningOffsets(new TopicPartitionMap([[tp, 0n]]));
          m.updateEndOffsets(new TopicPartitionMap([[tp, 5n]]));
          m.updatePartitions({ topic: 't', partitions: [new PartitionInfo({ topic: 't', partition: 0, leader: node, replicas: [node], inSyncReplicas: [node] })] });
          (await m.partitionsFor('t')).length;
          (await m.listTopics()).size;
          m.addRecord(new ConsumerRecord({ topic: 't', partition: 0, offset: 0, key: null, value: null }));
          await m.losePartitions([tp]);
          m.setMaxPollRecords(3);
          await m.close();
        }
        new MockConsumer('earliest');
      })();
      await settle();
      console.log(JSON.stringify({ stats: native.consumerStats() }));
      // The open mock keeps Node running: the probe ends it.
      process.exit(0);
    })();
  `);
  assert.equal(out.stats.consumersMade, 11);
  assert.equal(out.stats.consumersFreed, 10);
  assert.equal(out.stats.mockRecordsMade - out.stats.mockRecordsFreed, 0);
});
