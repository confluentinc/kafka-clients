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

// CreateAclsResult over Java's package-private constructor (the library's
// internalCreate). Java and the Rust core ship no test of it, so this ports
// Python's test_all_waits_for_pending_futures
// (python/test/unit/test_admin_results.py).

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { CreateAclsResult } = require('@confluentinc/kafka-javascript/admin');
const {
  AccessControlEntry,
  AclBinding,
  AclBindingMap,
  AclOperation,
  AclPermissionType,
} = require('@confluentinc/kafka-javascript/common/acl');
const { TopicAuthorizationError } = require('@confluentinc/kafka-javascript/common/errors');
const { PatternType, ResourcePattern, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');
const { createFuture, internalResult } = require('../../_internal');

const binding = (/** @type {string} */ name) =>
  new AclBinding({
    pattern: new ResourcePattern({ resourceType: ResourceType.TOPIC, name, patternType: PatternType.LITERAL }),
    entry: new AccessControlEntry({
      principal: 'User:alice',
      host: '*',
      operation: AclOperation.WRITE,
      permissionType: AclPermissionType.ALLOW,
    }),
  });

test('test_all_waits_for_pending_futures', async () => {
  /** @type {import('../../_internal').InternalFuture<void>} */
  const pending = createFuture(false);
  const result = internalResult(CreateAclsResult, new AclBindingMap([[binding('a'), pending]]));
  const everything = result.all();
  assert.equal(everything.isDone(), false);
  pending.internalComplete(undefined, null);
  assert.equal(await everything.get(), null);
});

test("all() fails with a binding's error once every future is done", async () => {
  /** @type {import('../../_internal').InternalFuture<void>} */
  const ok = createFuture(false);
  /** @type {import('../../_internal').InternalFuture<void>} */
  const failed = createFuture(false);
  const error = new TopicAuthorizationError({ message: 'denied' });
  const result = internalResult(
    CreateAclsResult,
    new AclBindingMap([
      [binding('a'), ok],
      [binding('b'), failed],
    ]),
  );
  const values = result.values();
  assert.equal(values.get(binding('b')), failed);
  const everything = result.all();
  failed.internalComplete(undefined, error);
  ok.internalComplete(undefined, null);
  await assert.rejects(everything.get(), (e) => e instanceof ExecutionError && e.cause === error);
});
