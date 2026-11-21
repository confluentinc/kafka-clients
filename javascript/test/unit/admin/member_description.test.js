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

// Java's MemberDescriptionTest (its deprecated constructors are not
// translated, root CLAUDE.md §3), then the binding's own: the data class's
// fields, Java's defaults for null, toString and toJSON.

const assert = require('node:assert/strict');

const { MemberAssignment, MemberDescription } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');

const MEMBER_ID = 'member_id';
const INSTANCE_ID = 'instanceId';
const RACK_ID = 'rackId';
const CLIENT_ID = 'client_id';
const HOST = 'host';
const ASSIGNMENT = new MemberAssignment([new TopicPartition({ topic: 'topic', partition: 1 })]);

/**
 * A description of MEMBER_ID's, with the given changes.
 *
 * @param {Partial<{ memberId: string, groupInstanceId: string | null, rackId: string | null, targetAssignment: MemberAssignment | null, memberEpoch: number | null, upgraded: boolean | null }>} changes
 */
function description(changes) {
  return new MemberDescription({
    memberId: MEMBER_ID,
    groupInstanceId: INSTANCE_ID,
    rackId: RACK_ID,
    clientId: CLIENT_ID,
    host: HOST,
    assignment: ASSIGNMENT,
    targetAssignment: null,
    memberEpoch: null,
    upgraded: null,
    ...changes,
  });
}

const STATIC_MEMBER_DESCRIPTION = description({});

test('testEqualsWithoutGroupInstanceId', () => {
  const dynamicMemberDescription = description({ groupInstanceId: null, rackId: null });
  const identityDescription = description({ groupInstanceId: null, rackId: null });
  assert.ok(!STATIC_MEMBER_DESCRIPTION.equals(dynamicMemberDescription));
  assert.notEqual(STATIC_MEMBER_DESCRIPTION.hashCode(), dynamicMemberDescription.hashCode());
  assert.ok(dynamicMemberDescription.equals(dynamicMemberDescription));
  assert.ok(dynamicMemberDescription.equals(identityDescription));
  assert.equal(dynamicMemberDescription.hashCode(), identityDescription.hashCode());
});

test('testEqualsWithGroupInstanceId', () => {
  assert.ok(STATIC_MEMBER_DESCRIPTION.equals(STATIC_MEMBER_DESCRIPTION));
  const identityDescription = description({});
  assert.ok(STATIC_MEMBER_DESCRIPTION.equals(identityDescription));
  assert.equal(STATIC_MEMBER_DESCRIPTION.hashCode(), identityDescription.hashCode());
});

test('testNonEqual', () => {
  for (const other of [
    description({ memberId: 'new_member' }),
    description({ groupInstanceId: 'new_instance' }),
    description({ targetAssignment: ASSIGNMENT }),
    description({ memberEpoch: 1 }),
    description({ upgraded: false }),
  ]) {
    assert.ok(!STATIC_MEMBER_DESCRIPTION.equals(other));
    assert.notEqual(STATIC_MEMBER_DESCRIPTION.hashCode(), other.hashCode());
  }
});

test("a data class: frozen readonly fields, Java's empty defaults for null, toString and toJSON", () => {
  const d = new MemberDescription({
    memberId: null,
    groupInstanceId: null,
    rackId: null,
    clientId: null,
    host: null,
    assignment: null,
    targetAssignment: ASSIGNMENT,
    memberEpoch: 3,
    upgraded: true,
  });
  assert.ok(Object.isFrozen(d));
  assert.equal(d.consumerId, '');
  assert.equal(d.clientId, '');
  assert.equal(d.host, '');
  assert.ok(d.assignment.equals(new MemberAssignment(null)));
  assert.equal(
    String(d),
    '(memberId=, groupInstanceId=null, rackId=null, clientId=, host=, assignment=(topicPartitions=), ' +
      'targetAssignment=Optional[(topicPartitions=topic-1)], memberEpoch=3, upgraded=true)',
  );
  assert.equal(
    String(STATIC_MEMBER_DESCRIPTION),
    '(memberId=member_id, groupInstanceId=instanceId, rackId=rackId, clientId=client_id, host=host, ' +
      'assignment=(topicPartitions=topic-1), targetAssignment=Optional.empty, memberEpoch=null, upgraded=null)',
  );
  assert.equal(
    JSON.stringify(d),
    '{"consumerId":"","groupInstanceId":null,"rackId":null,"clientId":"","host":"","assignment":{"topicPartitions":[]},' +
      '"targetAssignment":{"topicPartitions":[{"topic":"topic","partition":1}]},"memberEpoch":3,"upgraded":true}',
  );
});
