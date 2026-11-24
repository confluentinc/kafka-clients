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

// AlterUserScramCredentialsResult, whose Java constructor is public: Java
// ships no test of it; this checks what Java's source does (requireNonNull,
// an unmodifiable copy, all() over the futures).

const assert = require('node:assert/strict');

const { AlterUserScramCredentialsResult } = require('@confluentinc/kafka-javascript/admin');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');

/** @returns {import('@confluentinc/kafka-javascript/common').KafkaFuture<void>} a done Void future */
const voidDone = () =>
  /** @type {import('@confluentinc/kafka-javascript/common').KafkaFuture<void>} */ (
    /** @type {unknown} */ (KafkaFuture.completedFuture(null))
  );

test("a copy of the map, Java's requireNonNull, all() over the futures", async () => {
  /** @type {Map<string, import('@confluentinc/kafka-javascript/common').KafkaFuture<void>>} */
  const futures = new Map([['u', voidDone()]]);
  const result = new AlterUserScramCredentialsResult(futures);
  futures.clear();
  assert.deepEqual([...result.values().keys()], ['u']);
  assert.notEqual(result.values(), result.values());
  await result.all().get();
  assert.deepEqual([...new AlterUserScramCredentialsResult({ futures: { v: voidDone() } }).values().keys()], ['v']);
  // @ts-expect-error a null map
  assert.throws(() => new AlterUserScramCredentialsResult(null), { name: 'TypeError', message: 'futures must not be null' });
});

test("a null user or a null future is kept, as Java's constructor keeps it; all() fails on the null future", () => {
  // Java only stores the nulls (its requireNonNull checks the map alone), so
  // the type is the non-null one and the run time keeps them.
  const result = new AlterUserScramCredentialsResult(
    // @ts-expect-error a null future, which Java stores
    new Map([
      ['u', null],
      [null, voidDone()],
    ]),
  );
  assert.deepEqual(
    [...result.values().entries()].map(([k, v]) => [k, v === null ? null : 'future']),
    [
      ['u', null],
      [null, 'future'],
    ],
  );
  assert.throws(() => result.all(), { name: 'TypeError', message: 'futures must not contain null' });
});
