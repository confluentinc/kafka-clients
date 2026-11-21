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

// Java's DescribeUserScramCredentialsResultTest, over Java's constructor (the
// library's internalCreate, the future of the response's per-user results),
// plus the Rust core's test of a null user
// (rust/src/admin/describe_user_scram_credentials_result.rs). The admin client
// builds the Result over the core's own futures instead (internalOfClient), as
// Python's does; the last test covers that path: it hands out the core's
// futures, a new one each call, and passes description's name, null included.

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { DuplicateResourceError, ResourceNotFoundError } = require('@confluentinc/kafka-javascript/common/errors');
const { describeUserScramCredentialsResult, describeUserScramCredentialsResultOfData, createFuture } = require('../../_internal');

/**
 * A future completed with `value`.
 *
 * @template T
 * @param {T} value the value
 * @returns {import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} the future
 */
function done(value) {
  /** @type {import('../../_internal').InternalFuture<T>} */
  const f = createFuture(false);
  f.internalComplete(value, null);
  return f;
}

const DUPLICATE = 'A request illegally referred to the same resource twice.';
// Errors.RESOURCE_NOT_FOUND's default message: Java's exception(null) for a
// response that carries no message.
const NOT_FOUND = 'A request illegally referred to a resource that does not exist.';

/**
 * Asserts that `future` fails with an `ExecutionError` whose cause is of
 * class `cls` with `message`.
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} future the future
 * @param {Function} cls the cause's class
 * @param {string} message the cause's message
 * @returns {Promise<void>} when checked
 */
async function assertFails(future, cls, message) {
  await assert.rejects(future.get(), (/** @type {unknown} */ e) => {
    assert.ok(e instanceof ExecutionError);
    assert.equal(Object.getPrototypeOf(e.cause), cls.prototype);
    assert.equal(/** @type {Error} */ (e.cause).message, message);
    return true;
  });
}

/**
 * The Java test's user result.
 *
 * @param {string} user the user
 * @param {Error | null} error null for Java's Errors.NONE
 * @param {Array<import('@confluentinc/kafka-javascript/admin').ScramCredentialInfo>} credentialInfos the credentials
 * @returns {{ user: string, error: Error | null, credentialInfos: Array<import('@confluentinc/kafka-javascript/admin').ScramCredentialInfo> }} the result
 */
const userResult = (user, error, credentialInfos = []) => ({ user, error, credentialInfos });

test('testTopLevelError', async () => {
  /** @type {import('../../_internal').InternalFuture<ReturnType<typeof userResult>[]>} */
  const dataFuture = createFuture(false);
  const failure = new Error('top-level');
  dataFuture.internalComplete(/** @type {never} */ (null), failure);
  const results = describeUserScramCredentialsResultOfData(dataFuture);
  for (const future of [results.all(), results.users(), results.description('whatever')]) {
    await assert.rejects(future.get(), (/** @type {unknown} */ e) => e instanceof ExecutionError && e.cause === failure);
  }
});

test('testUserLevelErrors', async () => {
  const { ScramCredentialInfo, ScramMechanism, UserScramCredentialsDescription } = require('@confluentinc/kafka-javascript/admin');
  const goodUser = 'goodUser';
  const unknownUser = 'unknownUser';
  const failedUser = 'failedUser';
  const scramSha256 = ScramMechanism.SCRAM_SHA_256;
  const iterations = 4096;
  const results = describeUserScramCredentialsResultOfData(
    done([
      userResult(goodUser, null, [new ScramCredentialInfo({ mechanism: scramSha256, iterations })]),
      userResult(unknownUser, new ResourceNotFoundError(NOT_FOUND)),
      userResult(failedUser, new DuplicateResourceError(DUPLICATE)),
    ]),
  );
  await assertFails(results.all(), DuplicateResourceError, DUPLICATE);
  assert.deepEqual(await results.users().get(), [goodUser, failedUser], 'Expected 2 users with credentials');
  const goodUserDescription = await results.description(goodUser).get();
  assert.ok(
    goodUserDescription.equals(
      new UserScramCredentialsDescription({
        name: goodUser,
        credentialInfos: [new ScramCredentialInfo({ mechanism: scramSha256, iterations })],
      }),
    ),
  );
  await assertFails(results.description(failedUser), DuplicateResourceError, DUPLICATE);
  await assertFails(results.description(unknownUser), ResourceNotFoundError, NOT_FOUND);
});

test('testSuccessfulDescription', async () => {
  const { ScramCredentialInfo, ScramMechanism, UserScramCredentialsDescription } = require('@confluentinc/kafka-javascript/admin');
  const goodUser = 'goodUser';
  const unknownUser = 'unknownUser';
  const scramSha256 = ScramMechanism.SCRAM_SHA_256;
  const iterations = 4096;
  const results = describeUserScramCredentialsResultOfData(
    done([userResult(goodUser, null, [new ScramCredentialInfo({ mechanism: scramSha256, iterations })])]),
  );
  assert.deepEqual(await results.users().get(), [goodUser], 'Expected 1 user with credentials');
  const allResults = await results.all().get();
  assert.equal(allResults.size, 1);
  const goodUserDescriptionViaAll = allResults.get(goodUser);
  assert.ok(goodUserDescriptionViaAll !== undefined);
  assert.ok(
    goodUserDescriptionViaAll.equals(
      new UserScramCredentialsDescription({
        name: goodUser,
        credentialInfos: [new ScramCredentialInfo({ mechanism: scramSha256, iterations })],
      }),
    ),
  );
  assert.ok(goodUserDescriptionViaAll.equals(await results.description(goodUser).get()), 'Expected same thing via all() and description()');
  await assertFails(results.description(unknownUser), ResourceNotFoundError, 'No such user: unknownUser');
});

test('test_description_of_a_null_user_fails_with_resource_not_found', async () => {
  const results = describeUserScramCredentialsResultOfData(done([userResult('goodUser', null)]));
  await assertFails(results.description(null), ResourceNotFoundError, 'No such user: null');
});

test("the admin client's path: all(), users() and description(userName) read the core's futures, each call a new one", async () => {
  /** @type {Array<string | null>} */
  const asked = [];
  const { ScramCredentialInfo, ScramMechanism, UserScramCredentialsDescription } = require('@confluentinc/kafka-javascript/admin');
  const description = new UserScramCredentialsDescription({
    name: 'u',
    credentialInfos: [new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 })],
  });
  const result = describeUserScramCredentialsResult(
    () => done(new Map([['u', description]])),
    () => done(['u']),
    (name) => {
      asked.push(name);
      return done(description);
    },
  );
  assert.notEqual(result.users(), result.users());
  assert.deepEqual(await result.users().get(), ['u']);
  assert.equal((await result.all().get()).get('u'), description);
  assert.equal(await result.description('u').get(), description);
  result.description({ userName: null });
  assert.deepEqual(asked, ['u', null]);
  // @ts-expect-error a reader takes no argument
  assert.throws(() => result.users(1), { name: 'TypeError', message: 'users() only reads: it takes no argument' });
});
