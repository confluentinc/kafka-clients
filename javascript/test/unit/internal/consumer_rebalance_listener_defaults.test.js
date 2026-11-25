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

// src/internal/consumer_rebalance_listener_defaults.ts: Java's default
// ConsumerRebalanceListener.onPartitionsLost, run with the user's listener as
// `this`, calls the listener's own onPartitionsRevoked.

const assert = require('node:assert/strict');

const { TopicPartition, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const { ConsumerRebalanceListenerDefaults } = require('../../../dist/internal/consumer_rebalance_listener_defaults');

test("the default onPartitionsLost calls the user's onPartitionsRevoked", () => {
  /** @type {Array<[string, import('@confluentinc/kafka-javascript/common').TopicPartitionSet]>} */
  const calls = [];
  /** @type {import('@confluentinc/kafka-javascript/consumer').ConsumerRebalanceListener} */
  const listener = {
    onPartitionsRevoked({ partitions }) {
      calls.push(['revoked', partitions]);
      return 'revoked';
    },
    onPartitionsAssigned({ partitions }) {
      calls.push(['assigned', partitions]);
    },
  };
  const partitions = new TopicPartitionSet([new TopicPartition({ topic: 't', partition: 0 })]);
  const result = ConsumerRebalanceListenerDefaults.prototype.onPartitionsLost.call(listener, { partitions });
  assert.equal(result, 'revoked');
  assert.deepEqual(calls, [['revoked', partitions]]);
  assert.equal(calls[0][1], partitions);
});
