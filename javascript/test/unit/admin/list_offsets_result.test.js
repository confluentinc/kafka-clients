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

// ListOffsetsResult and ListOffsetsResultInfo, both with Java's public
// constructors. Java has no test of them: these check Java's members.

const assert = require('node:assert/strict');

const { ExecutionError, IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { ListOffsetsResult } = require('@confluentinc/kafka-javascript/admin');
const { KafkaFuture, TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');

const tp = new TopicPartition({ topic: 't', partition: 0 });

test("ListOffsetsResultInfo: the fields, Java's toString of the Optional epoch, and toJSON", () => {
  const info = new ListOffsetsResult.ListOffsetsResultInfo({ offset: 5, timestamp: 7n, leaderEpoch: 3 });
  assert.equal(info.offset(), 5n);
  assert.equal(info.timestamp(), 7n);
  assert.equal(info.leaderEpoch(), 3);
  assert.equal(info.toString(), 'ListOffsetsResultInfo(offset=5, timestamp=7, leaderEpoch=Optional[3])');
  assert.equal(JSON.stringify(info), '{"offset":"5","timestamp":"7","leaderEpoch":3}');
  const none = new ListOffsetsResult.ListOffsetsResultInfo({ offset: -1, timestamp: -1, leaderEpoch: null });
  assert.equal(none.toString(), 'ListOffsetsResultInfo(offset=-1, timestamp=-1, leaderEpoch=Optional.empty)');
});

test('ListOffsetsResult: partitionResult and all over the futures it was given', async () => {
  const info = new ListOffsetsResult.ListOffsetsResultInfo({ offset: 1, timestamp: 2, leaderEpoch: null });
  const result = new ListOffsetsResult(new TopicPartitionMap([[tp, KafkaFuture.completedFuture({ value: info })]]));
  assert.equal(await result.partitionResult(tp).get(), info);
  assert.equal(await result.partitionResult({ partition: new TopicPartition({ topic: 't', partition: 0 }) }).get(), info);
  const all = await result.all().get();
  assert.equal(all.get(tp), info);
  assert.throws(
    () => result.partitionResult(new TopicPartition({ topic: 'u', partition: 1 })),
    (e) => e instanceof IllegalArgumentError && e.message === 'List Offsets for partition "u-1" was not attempted',
  );
  // Java's constructor stores a null map; its methods dereference it.
  // @ts-expect-error a null the types forbid: Java stores it
  const empty = new ListOffsetsResult(null);
  assert.throws(() => empty.partitionResult(tp), { name: 'TypeError', message: 'futures must not be null' });
  assert.throws(() => empty.all(), { name: 'TypeError', message: 'futures must not be null' });
  // A Map, bare or by name, is refused with a hint.
  for (const make of [
    // @ts-expect-error a wrong argument: Java's Map<TopicPartition, ...> is a TopicPartitionMap
    () => new ListOffsetsResult(new Map()),
    // @ts-expect-error a wrong argument: Java's Map<TopicPartition, ...> is a TopicPartitionMap
    () => new ListOffsetsResult({ futures: new Map() }),
  ]) {
    assert.throws(make, {
      name: 'TypeError',
      message: 'futures must be a TopicPartitionMap, not a Map: build one with new TopicPartitionMap()',
    });
  }
});

test("ListOffsetsResult.all fails as the first failed partition's future", async () => {
  const failed = new Error('boom');
  /** @type {KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>} */
  const future = KafkaFuture.completedFuture({
    value: new ListOffsetsResult.ListOffsetsResultInfo({ offset: 1, timestamp: 2, leaderEpoch: null }),
  }).thenApply(() => {
    throw failed;
  });
  const result = new ListOffsetsResult({ futures: new TopicPartitionMap([[tp, future]]) });
  await assert.rejects(result.all().get(), (e) => e instanceof ExecutionError && e.cause === failed);
});
