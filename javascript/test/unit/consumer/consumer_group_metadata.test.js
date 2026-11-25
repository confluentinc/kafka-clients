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

// Java's ConsumerGroupMetadataTest, 2 of its 5 tests, through the library's
// own factory, and the core's consumer_group_metadata.rs test_display.
// Not translated: testInvalidGroupId, testInvalidMemberId and
// testInvalidInstanceId, which test the null checks of Java's deprecated
// public constructors: the class has no public constructor, and the core
// never passes null. The core's other four tests: test_assignment_constructor
// and test_group_id_constructor repeat Java's, and test_display_through_trait_object
// and test_copy_of test Rust-only code.

const assert = require('node:assert/strict');

const { createConsumerGroupMetadata } = require('../../_internal');

const groupId = 'group';
// Java's JoinGroupRequest.UNKNOWN_GENERATION_ID and UNKNOWN_MEMBER_ID, which
// the deprecated ConsumerGroupMetadata(String) passes.
const UNKNOWN_GENERATION_ID = -1;
const UNKNOWN_MEMBER_ID = '';

test('testAssignmentConstructor', () => {
  const memberId = 'member';
  const generationId = 2;
  const groupInstanceId = 'instance';

  const groupMetadata = createConsumerGroupMetadata(groupId, generationId, memberId, groupInstanceId);

  assert.equal(groupMetadata.groupId(), groupId);
  assert.equal(groupMetadata.generationId(), generationId);
  assert.equal(groupMetadata.memberId(), memberId);
  assert.notEqual(groupMetadata.groupInstanceId(), null);
  assert.equal(groupMetadata.groupInstanceId(), groupInstanceId);
});

test("testGroupIdConstructor, with the defaults Java's short constructor passes", () => {
  const groupMetadata = createConsumerGroupMetadata(groupId, UNKNOWN_GENERATION_ID, UNKNOWN_MEMBER_ID, null);

  assert.equal(groupMetadata.groupId(), groupId);
  assert.equal(groupMetadata.generationId(), UNKNOWN_GENERATION_ID);
  assert.equal(groupMetadata.memberId(), UNKNOWN_MEMBER_ID);
  assert.equal(groupMetadata.groupInstanceId(), null);
});

test('test_display', () => {
  const m = createConsumerGroupMetadata('g', 2, 'member', 'instance');
  assert.equal(String(m), 'GroupMetadata(groupId = g, generationId = 2, memberId = member, groupInstanceId = instance)');
  // groupInstanceId rendered as empty string (matches Java)
  const n = createConsumerGroupMetadata('g', UNKNOWN_GENERATION_ID, UNKNOWN_MEMBER_ID, null);
  assert.equal(String(n), 'GroupMetadata(groupId = g, generationId = -1, memberId = , groupInstanceId = )');
});

test("equals and hashCode are Java's", () => {
  const a = createConsumerGroupMetadata('g', 2, 'm', 'i');
  assert.ok(a.equals(createConsumerGroupMetadata('g', 2, 'm', 'i')));
  assert.ok(a.equals({ o: a }));
  assert.equal(a.equals(createConsumerGroupMetadata('g', 2, 'm', null)), false);
  assert.equal(a.equals(createConsumerGroupMetadata('g', 3, 'm', 'i')), false);
  assert.equal(a.equals(null), false);
  // Java's Objects.hash("g", 2, "m", Optional.of("i")) and
  // Objects.hash("g", -1, "", Optional.empty()).
  assert.equal(a.hashCode(), 3997400);
  assert.equal(createConsumerGroupMetadata('g', -1, '', null).hashCode(), 3991033);
  assert.equal(a.hashCode(), createConsumerGroupMetadata('g', 2, 'm', 'i').hashCode());
  assert.equal(JSON.stringify(a), '{"groupId":"g","generationId":2,"memberId":"m","groupInstanceId":"i"}');
  // @ts-expect-error a wrong argument: groupId() only reads
  assert.throws(() => a.groupId(1), { constructor: TypeError, message: 'groupId() only reads: it takes no argument' });
});
