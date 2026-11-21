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

// KafkaAdminClientTopicTest's isCordoned assertions (KIP-1066), ported as
// Python's test_admin_java_ports.py ports them: Java's MockClient response is
// the Rust core's (its test_describe_log_dirs_with_cordoned_dir), so these
// check the conversion of the rows the addon reads for each log directory
// into Java's Map<String, LogDirDescription> (src/internal/admin_config_calls.ts).

const assert = require('node:assert/strict');

const { TopicPartition } = require('@confluentinc/kafka-javascript/common');

const untyped = require('../../../dist/internal/admin_config_calls');

// The conversions' signatures, from src/ (dist/internal/ has no declarations).
const calls = {
  logDirDescriptionsOf: /** @type {typeof import('../../../src/internal/admin_config_calls').logDirDescriptionsOf} */ (
    /** @type {unknown} */ (untyped.logDirDescriptionsOf)
  ),
  configResourceOf: /** @type {typeof import('../../../src/internal/admin_config_calls').configResourceOf} */ (
    /** @type {unknown} */ (untyped.configResourceOf)
  ),
  topicPartitionReplicaOf: /** @type {typeof import('../../../src/internal/admin_config_calls').topicPartitionReplicaOf} */ (
    /** @type {unknown} */ (untyped.topicPartitionReplicaOf)
  ),
};

test('testDescribeLogDirsWithCordonedDir: a cordoned directory says so through isCordoned', () => {
  const tp = new TopicPartition({ topic: 'test', partition: 0 });
  const described = calls.logDirDescriptionsOf([
    [
      '/data',
      {
        error: null,
        replicaInfos: [[['test', 0], { size: 123n, offsetLag: -1n, isFuture: false }]],
        totalBytes: -1n,
        usableBytes: -1n,
        isCordoned: true,
      },
    ],
  ]);
  assert.deepEqual([...described.keys()], ['/data']);
  const description = described.get('/data');
  assert.ok(description);
  assert.equal(description.isCordoned, true);
  assert.deepEqual([...description.replicaInfos.keys()].map(String), [tp.toString()]);
  assert.equal(description.totalBytes, null);
  assert.equal(description.usableBytes, null);
});

test("assertDescriptionContains' assertFalse(isCordoned()): a directory the broker does not cordon", () => {
  const description = calls
    .logDirDescriptionsOf([
      [
        '/data',
        {
          error: null,
          replicaInfos: [[['test', 0], { size: 123n, offsetLag: 4n, isFuture: false }]],
          totalBytes: 1000n,
          usableBytes: 500n,
          isCordoned: false,
        },
      ],
    ])
    .get('/data');
  assert.ok(description);
  assert.equal(description.error, null);
  assert.equal(description.totalBytes, 1000n);
  assert.equal(description.usableBytes, 500n);
  assert.equal(description.isCordoned, false);
});

test('a resource and a replica the addon reads build their classes', () => {
  assert.equal(calls.configResourceOf([4, 't']).toString(), "ConfigResource(type=TOPIC, name='t')");
  assert.equal(calls.topicPartitionReplicaOf(['t', 1, 2]).toString(), 't-1-2');
});
