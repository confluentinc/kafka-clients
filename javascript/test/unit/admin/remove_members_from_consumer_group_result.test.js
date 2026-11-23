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

// Java's RemoveMembersFromConsumerGroupResultTest, over Java's constructor
// shape (the library's internalCreate, the errors keyed by group instance id,
// null for Java's Errors.NONE).

const assert = require('node:assert/strict');

const { ExecutionError, IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { MemberToRemove, MemberToRemoveSet } = require('@confluentinc/kafka-javascript/admin');
const { FencedInstanceIdError, GroupAuthorizationError } = require('@confluentinc/kafka-javascript/common/errors');
const { createFuture, removeMembersResult } = require('../../_internal');

const instanceOne = new MemberToRemove('instance-1');
const instanceTwo = new MemberToRemove('instance-2');

/** @returns {{ memberFutures: import('../../_internal').InternalFuture<Map<string | null, Error | null>>, membersToRemove: MemberToRemoveSet, errorsMap: Map<string | null, Error | null> }} */
function setUp() {
  /** @type {import('../../_internal').InternalFuture<Map<string | null, Error | null>>} */
  const memberFutures = createFuture(false);
  const membersToRemove = new MemberToRemoveSet([instanceOne, instanceTwo]);
  /** @type {Map<string | null, Error | null>} */
  const errorsMap = new Map([
    ['instance-1', null],
    ['instance-2', new FencedInstanceIdError('fenced')],
  ]);
  return { memberFutures, membersToRemove, errorsMap };
}

/**
 * Java's TestUtils.assertFutureThrows: the cause of the future's ExecutionError.
 *
 * @param {Function} cls
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} future
 */
async function assertFutureThrows(cls, future) {
  await assert.rejects(future.get(), (e) => e instanceof ExecutionError && e.cause instanceof cls);
}

test('testTopLevelErrorConstructor', async () => {
  const { memberFutures, membersToRemove } = setUp();
  memberFutures.internalComplete(new Map(), new GroupAuthorizationError('auth'));
  await assertFutureThrows(GroupAuthorizationError, removeMembersResult(memberFutures, membersToRemove).all());
});

/**
 * @param {ReturnType<typeof setUp>} s
 */
async function createAndVerifyMemberLevelError(s) {
  s.memberFutures.internalComplete(s.errorsMap, null);
  assert.ok(!s.memberFutures.isCompletedExceptionally());
  const result = removeMembersResult(s.memberFutures, s.membersToRemove);
  await assertFutureThrows(FencedInstanceIdError, result.all());
  assert.equal(await result.memberResult(instanceOne).get(), null);
  await assertFutureThrows(FencedInstanceIdError, result.memberResult(instanceTwo));
  return result;
}

test('testMemberLevelErrorConstructor', async () => {
  await createAndVerifyMemberLevelError(setUp());
});

test('testMemberMissingErrorInRequestConstructor', async () => {
  const s = setUp();
  s.errorsMap.delete('instance-2');
  s.memberFutures.internalComplete(s.errorsMap, null);
  assert.ok(!s.memberFutures.isCompletedExceptionally());
  const result = removeMembersResult(s.memberFutures, s.membersToRemove);
  await assertFutureThrows(IllegalArgumentError, result.all());
  assert.equal(await result.memberResult(instanceOne).get(), null);
  await assert.rejects(
    result.memberResult(instanceTwo).get(),
    (e) =>
      e instanceof ExecutionError &&
      e.cause instanceof IllegalArgumentError &&
      e.cause.message ===
        "Member \"MemberIdentity(memberId='', groupInstanceId='instance-2', reason=null)\" was not included in the removal response",
  );
});

test('testMemberLevelErrorInResponseConstructor', async () => {
  const result = await createAndVerifyMemberLevelError(setUp());
  // Java prints the member as Object.toString(): its class and hash code.
  assert.throws(() => result.memberResult(new MemberToRemove('invalid-instance-id')), {
    name: 'IllegalArgumentError',
    message: 'Member org.apache.kafka.clients.admin.MemberToRemove@cc9269c was not included in the original request',
  });
  // A null member: Java's HashSet holds none.
  // @ts-expect-error a null the types forbid: Java's contains(null) is false
  assert.throws(() => result.memberResult(null), {
    name: 'IllegalArgumentError',
    message: 'Member null was not included in the original request',
  });
});

test('testNoErrorConstructor', async () => {
  const s = setUp();
  const result = removeMembersResult(s.memberFutures, s.membersToRemove);
  s.memberFutures.internalComplete(
    new Map([
      ['instance-1', null],
      ['instance-2', null],
    ]),
    null,
  );
  assert.equal(await result.all().get(), null);
  assert.equal(await result.memberResult(instanceOne).get(), null);
  assert.equal(await result.memberResult(instanceTwo).get(), null);
});

test("removeAll mode: memberResult refused, all() wraps the first member's error as Java does", async () => {
  /** @type {import('../../_internal').InternalFuture<Map<string | null, Error | null>>} */
  const future = createFuture(false);
  const result = removeMembersResult(future, new MemberToRemoveSet());
  assert.throws(() => result.memberResult(instanceOne), {
    name: 'IllegalArgumentError',
    message: "The method: memberResult is not applicable in 'removeAll' mode",
  });
  // @ts-expect-error a null the types forbid: the removeAll check comes first
  assert.throws(() => result.memberResult(null), {
    name: 'IllegalArgumentError',
    message: "The method: memberResult is not applicable in 'removeAll' mode",
  });
  const fenced = new FencedInstanceIdError('fenced');
  future.internalComplete(new Map([['instance-1', fenced]]), null);
  await assert.rejects(
    result.all().get(),
    (e) =>
      e instanceof ExecutionError &&
      e.cause instanceof Error &&
      e.cause.name === 'KafkaError' &&
      e.cause.message ===
        "Encounter exception when trying to remove: MemberIdentity(memberId='', groupInstanceId='instance-1', reason=null)" &&
      e.cause.cause === fenced,
  );
});
