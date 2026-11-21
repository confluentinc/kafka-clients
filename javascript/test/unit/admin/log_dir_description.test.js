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

// LogDirDescription, an admin data class (design D3), and ReplicaInfo. Java
// has no test of its own for them; KafkaAdminClientTopicTest's isCordoned
// assertions are ported in kafka_admin_client.test.js. These check the
// frozen readonly fields, Java's three constructors as one, the read-only
// map of replicas, toString and toJSON.

const assert = require('node:assert/strict');

const { IllegalArgumentError, UnsupportedOperationError } = require('@confluentinc/kafka-javascript');
const { LogDirDescription, ReplicaInfo } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { KafkaStorageError } = require('@confluentinc/kafka-javascript/common/errors');

test("ReplicaInfo keeps Java's getters and toString, a long a bigint", () => {
  const info = new ReplicaInfo({ size: 123, offsetLag: 4n, isFuture: false });
  assert.equal(info.size(), 123n);
  assert.equal(info.offsetLag(), 4n);
  assert.equal(info.isFuture(), false);
  assert.equal(info.toString(), 'ReplicaInfo(size=123, offsetLag=4, isFuture=false)');
  assert.equal(JSON.stringify(info), '{"size":"123","offsetLag":"4","isFuture":false}');
  assert.throws(() => new ReplicaInfo({ size: 2 ** 53, offsetLag: 0, isFuture: false }), RangeError);
});

test("LogDirDescription: Java's three constructors, -1 an unknown size, frozen fields", () => {
  const tp = new TopicPartition({ topic: 'test', partition: 0 });
  const infos = new TopicPartitionMap([[tp, new ReplicaInfo({ size: 123, offsetLag: -1, isFuture: false })]]);
  const unknown = new LogDirDescription({ error: null, replicaInfos: infos });
  assert.equal(unknown.error, null);
  assert.equal(unknown.totalBytes, null);
  assert.equal(unknown.usableBytes, null);
  assert.equal(unknown.isCordoned, false);
  assert.equal(unknown.replicaInfos.get(tp)?.size(), 123n);
  assert.ok(Object.isFrozen(unknown));
  // The map is copied: a change to the one given is not seen.
  infos.clear();
  assert.equal(unknown.replicaInfos.size(), 1);
  // Java's unmodifiable view.
  assert.throws(() => unknown.replicaInfos.clear(), UnsupportedOperationError);
  assert.throws(() => unknown.replicaInfos.set(tp, new ReplicaInfo({ size: 1, offsetLag: 1, isFuture: true })), UnsupportedOperationError);
  assert.throws(() => unknown.replicaInfos.delete(tp), UnsupportedOperationError);

  const sized = new LogDirDescription({ error: null, replicaInfos: new TopicPartitionMap(), totalBytes: 1000, usableBytes: 500n });
  assert.equal(sized.totalBytes, 1000n);
  assert.equal(sized.usableBytes, 500n);
  assert.equal(sized.isCordoned, false);
  const error = new KafkaStorageError('offline');
  const cordoned = new LogDirDescription({
    error,
    replicaInfos: new TopicPartitionMap(),
    totalBytes: -1,
    usableBytes: -1,
    isCordoned: true,
  });
  assert.equal(cordoned.error, error);
  assert.equal(cordoned.isCordoned, true);
  assert.equal(cordoned.totalBytes, null);
  assert.equal(
    sized.toString(),
    'LogDirDescription(replicaInfos={}, error=null, totalBytes=OptionalLong[1000], usableBytes=OptionalLong[500], isCordoned=false)',
  );
  assert.equal(
    cordoned.toString(),
    `LogDirDescription(replicaInfos={}, error=${String(error)}, totalBytes=OptionalLong.empty, usableBytes=OptionalLong.empty, isCordoned=true)`,
  );
  assert.equal(JSON.stringify(sized), '{"error":null,"replicaInfos":{},"totalBytes":"1000","usableBytes":"500","isCordoned":false}');
  // @ts-expect-error a key no form has: Java has no (error, replicaInfos, totalBytes) constructor
  assert.throws(() => new LogDirDescription({ error: null, replicaInfos: new TopicPartitionMap(), totalBytes: 1 }), IllegalArgumentError);
  // @ts-expect-error a null map, which the types forbid: the copy dereferences it
  assert.throws(() => new LogDirDescription({ error: null, replicaInfos: null }), {
    name: 'TypeError',
    message: 'replicaInfos must not be null',
  });
  // @ts-expect-error a Map: a map keyed by TopicPartition is a TopicPartitionMap
  assert.throws(() => new LogDirDescription({ error: null, replicaInfos: new Map() }), {
    name: 'TypeError',
    message: 'replicaInfos must be a TopicPartitionMap, not a Map: build one with new TopicPartitionMap()',
  });
});
