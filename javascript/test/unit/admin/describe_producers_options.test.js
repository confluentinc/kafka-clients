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

// DescribeProducersOptions, which Java ships without tests: brokerId's
// getter / setter pair over Java's OptionalInt, Java's equals and toString.

const assert = require('node:assert/strict');

const { DescribeProducersOptions } = require('@confluentinc/kafka-javascript/admin');

test("brokerId reads null until set; toString writes Java's OptionalInt", () => {
  const o = new DescribeProducersOptions();
  assert.equal(o.brokerId(), null);
  assert.equal(String(o), 'DescribeProducersOptions(brokerId=OptionalInt.empty, timeoutMs=null)');
  assert.equal(o.brokerId(3), o);
  assert.equal(o.brokerId(), 3);
  assert.equal(String(o.timeoutMs(10)), 'DescribeProducersOptions(brokerId=OptionalInt[3], timeoutMs=10)');
  assert.equal(new DescribeProducersOptions({ brokerId: 4 }).brokerId(), 4);
});

test("equals and hashCode compare the broker id and the timeout, as Java's", () => {
  assert.ok(new DescribeProducersOptions({ brokerId: 1 }).equals(new DescribeProducersOptions({ brokerId: 1 })));
  assert.ok(!new DescribeProducersOptions({ brokerId: 1 }).equals(new DescribeProducersOptions({ brokerId: 1, timeoutMs: 5 })));
  assert.equal(new DescribeProducersOptions().hashCode(), new DescribeProducersOptions().hashCode());
});

test("brokerId's value is a Java int", () => {
  // @ts-expect-error Java's int cannot be null: no Java form takes it
  assert.throws(() => new DescribeProducersOptions().brokerId(null), {
    name: 'IllegalArgumentError',
    message: 'brokerId() takes one of (), (brokerId); got null',
  });
  assert.throws(() => new DescribeProducersOptions().brokerId(1.5), { name: 'TypeError', message: 'brokerId must be an integer' });
});
