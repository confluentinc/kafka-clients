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

// ListConsumerGroupOffsetsSpec: Java has no test of it. These check the
// keyed constructor, Java's getter / setter pair (null: every partition),
// equals, hashCode, toString and toJSON.

const assert = require('node:assert/strict');

const { ListConsumerGroupOffsetsSpec } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');

const tp = new TopicPartition({ topic: 't', partition: 0 });

test('the spec: null by default, any iterable copied, a new array read each time', () => {
  const spec = new ListConsumerGroupOffsetsSpec();
  assert.equal(spec.topicPartitions(), null);
  assert.equal(String(spec), 'ListConsumerGroupOffsetsSpec(topicPartitions=null)');
  assert.equal(spec.topicPartitions(new Set([tp])), spec);
  assert.deepEqual(spec.topicPartitions(), [tp]);
  assert.notEqual(spec.topicPartitions(), spec.topicPartitions());
  assert.equal(String(spec), 'ListConsumerGroupOffsetsSpec(topicPartitions=[t-0])');
  const keyed = new ListConsumerGroupOffsetsSpec({ topicPartitions: [tp] });
  assert.ok(keyed.equals(spec));
  assert.equal(keyed.hashCode(), spec.hashCode());
  assert.ok(!keyed.equals(new ListConsumerGroupOffsetsSpec()));
  assert.equal(JSON.stringify(keyed), '{"topicPartitions":[{"topic":"t","partition":0}]}');
  keyed.topicPartitions({ topicPartitions: null });
  assert.equal(keyed.topicPartitions(), null);
  // @ts-expect-error a bare string where Java takes Collection<TopicPartition>
  assert.throws(() => keyed.topicPartitions('t'), {
    name: 'TypeError',
    message: 'topicPartitions must be an iterable of TopicPartition or null, not a string',
  });
});
