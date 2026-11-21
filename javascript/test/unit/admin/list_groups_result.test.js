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

// ListGroupsResult: Java has no test of it. These check Java's constructor
// (the library's internalCreate), which splits one future of the listings and
// the errors into all(), valid() and errors().

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { GroupListing } = require('@confluentinc/kafka-javascript/admin');
const { GroupState, GroupType } = require('@confluentinc/kafka-javascript/common');
const { CoordinatorNotAvailableError } = require('@confluentinc/kafka-javascript/common/errors');
const { createFuture, listGroupsResult } = require('../../_internal');

const listing = new GroupListing({ groupId: 'g', type: GroupType.CONSUMER, protocol: 'consumer', groupState: GroupState.STABLE });

test('the listings only: all() and valid() yield them, errors() none', async () => {
  /** @type {import('../../_internal').InternalFuture<Iterable<unknown>>} */
  const future = createFuture(false);
  const result = listGroupsResult(future);
  future.internalComplete([listing], null);
  assert.deepEqual(await result.all().get(), [listing]);
  assert.deepEqual(await result.valid().get(), [listing]);
  assert.deepEqual(await result.errors().get(), []);
});

test('an error among the listings: all() fails with the first, valid() and errors() split them', async () => {
  /** @type {import('../../_internal').InternalFuture<Iterable<unknown>>} */
  const future = createFuture(false);
  const result = listGroupsResult(future);
  const error = new CoordinatorNotAvailableError('nope');
  future.internalComplete([listing, error], null);
  await assert.rejects(result.all().get(), (e) => e instanceof ExecutionError && e.cause === error);
  assert.deepEqual(await result.valid().get(), [listing]);
  assert.deepEqual(await result.errors().get(), [error]);
  // Called with an argument tsc refuses, as a plain JavaScript caller calls them.
  const loose = /** @type {Record<string, (x: unknown) => unknown>} */ (/** @type {unknown} */ (result));
  for (const name of ['all', 'valid', 'errors']) {
    assert.throws(() => loose[name](1), { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
});
