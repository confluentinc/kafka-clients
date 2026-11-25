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

// DeleteAclsResult, its nested FilterResult and FilterResults over Java's
// package-private constructors (the library's internalCreate). Java and the
// Rust core ship no test of them, so these port Python's
// TestDeleteAclsResult (python/test/unit/test_admin_results_hand.py), and pin
// the js-only toJSON() output, which is frozen once released. Python's
// test_get_acl_bindings_internal_error is not ported: it calls a private
// helper Java's all() reaches only after allOf has failed, and the binding
// has no such helper to call.

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { DeleteAclsResult } = require('@confluentinc/kafka-javascript/admin');
const {
  AccessControlEntry,
  AccessControlEntryFilter,
  AclBinding,
  AclBindingFilter,
  AclBindingFilterMap,
  AclOperation,
  AclPermissionType,
} = require('@confluentinc/kafka-javascript/common/acl');
const { TopicAuthorizationError } = require('@confluentinc/kafka-javascript/common/errors');
const { PatternType, ResourcePattern, ResourcePatternFilter, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');
const { createFuture, internalResult } = require('../../_internal');

/** @typedef {import('@confluentinc/kafka-javascript/common').KafkaFuture<DeleteAclsResult.FilterResults>} ResultsFuture */

const binding = (/** @type {string} */ name) =>
  new AclBinding({
    pattern: new ResourcePattern({ resourceType: ResourceType.TOPIC, name, patternType: PatternType.LITERAL }),
    entry: new AccessControlEntry({
      principal: 'User:alice',
      host: '*',
      operation: AclOperation.READ,
      permissionType: AclPermissionType.ALLOW,
    }),
  });
const filter = (/** @type {string} */ name) =>
  new AclBindingFilter({
    patternFilter: new ResourcePatternFilter({ resourceType: ResourceType.TOPIC, name, patternType: PatternType.LITERAL }),
    entryFilter: AccessControlEntryFilter.ANY,
  });

const filterResult = /** @type {{ internalCreate(b: AclBinding | null, e: Error | null): DeleteAclsResult.FilterResult }} */ (
  /** @type {unknown} */ (DeleteAclsResult.FilterResult)
);
const filterResults = /** @type {{ internalCreate(v: DeleteAclsResult.FilterResult[]): DeleteAclsResult.FilterResults }} */ (
  /** @type {unknown} */ (DeleteAclsResult.FilterResults)
);

/**
 * A future completed with `value`, or failed with `error`.
 *
 * @param {DeleteAclsResult.FilterResults | null} value the value
 * @param {Error | null} error the error
 * @returns {ResultsFuture} the future
 */
function done(value, error = null) {
  /** @type {import('../../_internal').InternalFuture<DeleteAclsResult.FilterResults>} */
  const f = createFuture(false);
  f.internalComplete(/** @type {DeleteAclsResult.FilterResults} */ (value), error);
  return f;
}

/**
 * The Result over `entries`.
 *
 * @param {Array<[AclBindingFilter, ResultsFuture]>} entries the futures by filter
 * @returns {DeleteAclsResult} the result
 */
const result = (entries) => internalResult(DeleteAclsResult, new AclBindingFilterMap(entries));

test('test_filter_result', () => {
  const b = binding('a');
  const ok = filterResult.internalCreate(b, null);
  assert.equal(ok.binding(), b);
  assert.equal(ok.exception(), null);
  const error = new TopicAuthorizationError({ message: 'denied' });
  const failed = filterResult.internalCreate(null, error);
  assert.equal(failed.binding(), null);
  assert.equal(failed.exception(), error);
  const results = filterResults.internalCreate([ok, failed]);
  assert.deepEqual(results.values(), [ok, failed]);
  // values() hands out a new array each call.
  assert.notEqual(results.values(), results.values());
  // @ts-expect-error a reader takes no argument
  assert.throws(() => ok.exception(1), { name: 'TypeError', message: 'exception() only reads: it takes no argument' });
});

test("toJSON(): each result's binding and error, the error as its text (frozen once released)", () => {
  const ok = filterResult.internalCreate(binding('a'), null);
  const failed = filterResult.internalCreate(null, new TopicAuthorizationError({ message: 'denied' }));
  assert.equal(
    JSON.stringify(filterResults.internalCreate([ok, failed])),
    '[{"binding":{"pattern":{"resourceType":"TOPIC","name":"a","patternType":"LITERAL"},' +
      '"entry":{"principal":"User:alice","host":"*","operation":"READ","permissionType":"ALLOW"}},"exception":null},' +
      '{"binding":null,"exception":"TopicAuthorizationError: denied"}]',
  );
});

test('test_values_and_all', async () => {
  const a = filterResults.internalCreate([
    filterResult.internalCreate(binding('a'), null),
    filterResult.internalCreate(binding('b'), null),
  ]);
  const empty = filterResults.internalCreate([]);
  const fa = done(a);
  const fx = done(empty);
  const r = result([
    [filter('a'), fa],
    [filter('x'), fx],
  ]);
  const values = r.values();
  assert.equal(values.size(), 2);
  assert.equal(values.get(filter('a')), fa);
  assert.equal(values.get(filter('x')), fx);
  const all = await r.all().get();
  assert.equal(all.length, 2);
  assert.ok(all[0].equals(binding('a')));
  assert.ok(all[1].equals(binding('b')));
  assert.deepEqual(await result([]).all().get(), []);
});

test('test_all_throws_the_filter_result_exception', async () => {
  const error = new TopicAuthorizationError({ message: 'denied' });
  const results = filterResults.internalCreate([filterResult.internalCreate(binding('a'), null), filterResult.internalCreate(null, error)]);
  await assert.rejects(
    result([[filter('a'), done(results)]])
      .all()
      .get(),
    (e) => e instanceof ExecutionError && e.cause === error,
  );
});

test('test_all_fails_with_a_failed_filter_future', async () => {
  const error = new TopicAuthorizationError({ message: 'denied' });
  await assert.rejects(
    result([[filter('a'), done(null, error)]])
      .all()
      .get(),
    (e) => e instanceof ExecutionError && e.cause === error,
  );
});
