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

// ElectLeadersResult over Java's constructor (the library's internalCreate).
// Java ships no test of the class, so these are the Rust core's tests of its
// translation (rust/src/admin/elect_leaders_result.rs).

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { ClusterAuthorizationError } = require('@confluentinc/kafka-javascript/common/errors');
const { createFuture, electLeadersResult } = require('../../_internal');

const tp = (/** @type {number} */ partition) => new TopicPartition({ topic: 't', partition });

test('all_succeeds_when_no_partition_has_error', async () => {
  /** @type {import('../../_internal').InternalFuture<TopicPartitionMap<unknown>>} */
  const future = createFuture(false);
  const result = electLeadersResult(future);
  future.internalComplete(
    new TopicPartitionMap([
      [tp(0), null],
      [tp(1), null],
    ]),
    null,
  );
  assert.equal(await result.all().get(), null);
});

test('all_fails_when_any_partition_has_error', async () => {
  /** @type {import('../../_internal').InternalFuture<TopicPartitionMap<unknown>>} */
  const future = createFuture(false);
  const result = electLeadersResult(future);
  const error = new ClusterAuthorizationError('Cluster authorization failed.');
  future.internalComplete(
    new TopicPartitionMap([
      [tp(0), null],
      [tp(1), error],
    ]),
    null,
  );
  // The partition's very error, as Java's whenComplete completes with it.
  await assert.rejects(result.all().get(), (e) => e instanceof ExecutionError && e.cause === error);
});

test('all fails as the election future fails, before it completes and after', async () => {
  /** @type {import('../../_internal').InternalFuture<TopicPartitionMap<unknown>>} */
  const future = createFuture(false);
  const result = electLeadersResult(future);
  const all = result.all();
  assert.equal(all.isDone(), false);
  const failed = new Error('boom');
  future.internalComplete(/** @type {TopicPartitionMap<unknown>} */ (/** @type {unknown} */ (null)), failed);
  await assert.rejects(all.get(), (e) => e instanceof ExecutionError && e.cause === failed);
  await assert.rejects(result.all().get(), (e) => e instanceof ExecutionError && e.cause === failed);
});

test('partitions_exposes_per_partition_result', async () => {
  /** @type {import('../../_internal').InternalFuture<TopicPartitionMap<unknown>>} */
  const future = createFuture(false);
  const result = electLeadersResult(future);
  const error = new ClusterAuthorizationError('Cluster authorization failed.');
  future.internalComplete(new TopicPartitionMap([[tp(0), error]]), null);
  const partitions = await result.partitions().get();
  assert.equal(partitions.get(tp(0)), error);
  // The same future on every call, Java's field.
  assert.equal(result.partitions(), result.partitions());
});
