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

// UpdateFeaturesResult, which Java ships without tests: values() is the
// futures by feature name, and all() is Java's allOf over them.

const assert = require('node:assert/strict');

const { UpdateFeaturesResult } = require('@confluentinc/kafka-javascript/admin');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { createFuture, internalResult } = require('../../_internal');

test('all() succeeds once every update has, and fails with the first failure', async () => {
  /** @type {import('../../_internal').InternalFuture<void>} */
  const a = createFuture(false);
  /** @type {import('../../_internal').InternalFuture<void>} */
  const b = createFuture(false);
  const result = internalResult(
    UpdateFeaturesResult,
    new Map([
      ['a', a],
      ['b', b],
    ]),
  );
  assert.deepEqual([...result.values().keys()], ['a', 'b']);
  const all = result.all();
  a.internalComplete(undefined, null);
  assert.equal(all.isDone(), false);
  b.internalComplete(/** @type {never} */ (undefined), new KafkaError('b failed'));
  await assert.rejects(all.get(), (/** @type {unknown} */ e) => {
    assert.ok(e instanceof ExecutionError);
    assert.ok(e.cause instanceof KafkaError);
    assert.equal(e.cause.message, 'b failed');
    return true;
  });
  // @ts-expect-error a reader takes no argument
  assert.throws(() => result.values(1), { name: 'TypeError', message: 'values() only reads: it takes no argument' });
  // @ts-expect-error a reader takes no argument
  assert.throws(() => result.all(1), { name: 'TypeError', message: 'all() only reads: it takes no argument' });
});
