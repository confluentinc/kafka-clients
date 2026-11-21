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

// AlterPartitionReassignmentsOptions: Java's getter / setter pair
// allowReplicationFactorChange, its setter's parameter `allow`. Java has no
// test of it.

const assert = require('node:assert/strict');

const { AlterPartitionReassignmentsOptions } = require('@confluentinc/kafka-javascript/admin');

test('AlterPartitionReassignmentsOptions: allowReplicationFactorChange reads, and sets bare or as { allow }', () => {
  const options = new AlterPartitionReassignmentsOptions();
  assert.equal(options.allowReplicationFactorChange(), true);
  assert.equal(options.allowReplicationFactorChange(false), options);
  assert.equal(options.allowReplicationFactorChange(), false);
  assert.equal(options.allowReplicationFactorChange({ allow: true }).allowReplicationFactorChange(), true);
  assert.equal(new AlterPartitionReassignmentsOptions({ allowReplicationFactorChange: false }).allowReplicationFactorChange(), false);
  // @ts-expect-error a wrong argument: the setter takes a boolean
  assert.throws(() => options.allowReplicationFactorChange('no'), {
    name: 'IllegalArgumentError',
    message: 'allowReplicationFactorChange() takes one of (), (allow); got a string',
  });
});
