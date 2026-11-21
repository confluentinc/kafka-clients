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

// DescribeFeaturesOptions, which Java ships without tests: nodeId's getter /
// setter pair over Java's OptionalInt.

const assert = require('node:assert/strict');

const { DescribeFeaturesOptions } = require('@confluentinc/kafka-javascript/admin');

test('nodeId reads null until set, and sets a Java int', () => {
  const o = new DescribeFeaturesOptions();
  assert.equal(o.nodeId(), null);
  assert.equal(o.nodeId(3), o);
  assert.equal(o.nodeId(), 3);
  assert.equal(o.nodeId({ nodeId: 4 }).nodeId(), 4);
  assert.equal(new DescribeFeaturesOptions({ nodeId: 5, timeoutMs: 10 }).nodeId(), 5);
  // @ts-expect-error Java's int cannot be null: no Java form takes it
  assert.throws(() => o.nodeId(null), { name: 'IllegalArgumentError', message: 'nodeId() takes one of (), (nodeId); got null' });
  assert.throws(() => o.nodeId(1.5), { name: 'TypeError', message: 'nodeId must be an integer' });
  assert.throws(() => o.nodeId(2 ** 31), { name: 'RangeError', message: 'nodeId must be from -2147483648 to 2147483647' });
});
