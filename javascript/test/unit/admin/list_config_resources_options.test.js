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

// The option classes of the config and log-dir calls with no field of their
// own (ListConfigResourcesOptions, DescribeLogDirsOptions,
// AlterReplicaLogDirsOptions, DescribeReplicaLogDirsOptions): the keyed
// constructor takes timeoutMs only. Java has no test of them.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const {
  AlterReplicaLogDirsOptions,
  DescribeLogDirsOptions,
  DescribeReplicaLogDirsOptions,
  ListConfigResourcesOptions,
} = require('@confluentinc/kafka-javascript/admin');

test('the options with no field of their own take a timeoutMs only', () => {
  for (const Options of [ListConfigResourcesOptions, DescribeLogDirsOptions, AlterReplicaLogDirsOptions, DescribeReplicaLogDirsOptions]) {
    assert.equal(new Options({ timeoutMs: 5 }).timeoutMs(), 5, Options.name);
    // @ts-expect-error a key the class has not
    assert.throws(() => new Options({ validateOnly: true }), IllegalArgumentError, Options.name);
  }
});
