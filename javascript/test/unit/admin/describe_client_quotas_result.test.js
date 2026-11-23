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

// DescribeClientQuotasResult, whose Java constructor is public and stores its
// future as given: Java ships no test of it.

const assert = require('node:assert/strict');

const { DescribeClientQuotasResult } = require('@confluentinc/kafka-javascript/admin');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');

test('entities() hands out the future given, a null included', () => {
  const future = KafkaFuture.completedFuture(null);
  // @ts-expect-error a null the types forbid: Java stores it
  assert.equal(new DescribeClientQuotasResult({ entities: null }).entities(), null);
  assert.equal(
    new DescribeClientQuotasResult(/** @type {import('@confluentinc/kafka-javascript/common').KafkaFuture<never>} */ (future)).entities(),
    future,
  );
  // @ts-expect-error not a KafkaFuture
  assert.throws(() => new DescribeClientQuotasResult({ entities: 1 }), {
    name: 'TypeError',
    message: 'entities must be a KafkaFuture, not a number',
  });
});
