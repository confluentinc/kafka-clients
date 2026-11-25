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

// Java's ConsumerGroupDescriptionTest (its deprecated constructors and
// state() are not translated, root CLAUDE.md §3), then the binding's own: the
// data class's fields, Java's defaults for null, equals, toString and toJSON.

const assert = require('node:assert/strict');

const { ConsumerGroupDescription, MemberAssignment, MemberDescription } = require('@confluentinc/kafka-javascript/admin');
const { GroupState, GroupType, Node } = require('@confluentinc/kafka-javascript/common');
const { AclOperation } = require('@confluentinc/kafka-javascript/common/acl');

test('testGroupState', () => {
  for (const groupState of GroupState.values()) {
    const description = new ConsumerGroupDescription({
      groupId: 'groupId',
      isSimpleConsumerGroup: false,
      members: [],
      partitionAssignor: 'assignor',
      type: GroupType.CONSUMER,
      groupState,
      coordinator: null,
      authorizedOperations: [],
      groupEpoch: null,
      targetAssignmentEpoch: null,
    });
    assert.equal(description.groupState, groupState);
  }
});

const member = new MemberDescription({
  memberId: 'm',
  groupInstanceId: null,
  rackId: null,
  clientId: 'c',
  host: 'h',
  assignment: new MemberAssignment(null),
  targetAssignment: null,
  memberEpoch: 2,
  upgraded: null,
});

test("a data class: frozen fields and collections, Java's defaults, equals, toString and toJSON", () => {
  const d = new ConsumerGroupDescription({
    groupId: null,
    isSimpleConsumerGroup: false,
    members: [member],
    partitionAssignor: null,
    type: GroupType.CONSUMER,
    groupState: GroupState.STABLE,
    coordinator: new Node({ id: 1, host: 'b', port: 9092 }),
    authorizedOperations: [AclOperation.READ],
    groupEpoch: 5,
    targetAssignmentEpoch: 5,
  });
  assert.ok(Object.isFrozen(d));
  assert.ok(Object.isFrozen(d.members));
  assert.equal(d.groupId, '');
  assert.equal(d.partitionAssignor, '');
  assert.ok(d.authorizedOperations !== null && d.authorizedOperations.has(AclOperation.READ));
  const loose = /** @type {Set<unknown>} */ (/** @type {unknown} */ (d.authorizedOperations));
  assert.throws(() => loose.add(AclOperation.WRITE), { name: 'UnsupportedOperationError' });
  const same = new ConsumerGroupDescription({
    groupId: '',
    isSimpleConsumerGroup: false,
    members: [member],
    partitionAssignor: '',
    type: GroupType.CONSUMER,
    groupState: GroupState.STABLE,
    coordinator: new Node({ id: 1, host: 'b', port: 9092 }),
    authorizedOperations: [AclOperation.READ],
    groupEpoch: 5,
    targetAssignmentEpoch: 5,
  });
  assert.ok(d.equals(same));
  assert.equal(d.hashCode(), same.hashCode());
  assert.equal(
    String(d),
    '(groupId=, isSimpleConsumerGroup=false, members=' +
      String(member) +
      ', partitionAssignor=, type=Consumer, groupState=Stable, coordinator=' +
      String(d.coordinator) +
      ', authorizedOperations=[READ], groupEpoch=5, targetAssignmentEpoch=5)',
  );
  const json = JSON.parse(JSON.stringify(d));
  assert.deepEqual(Object.keys(json), [
    'groupId',
    'isSimpleConsumerGroup',
    'members',
    'partitionAssignor',
    'type',
    'groupState',
    'coordinator',
    'authorizedOperations',
    'groupEpoch',
    'targetAssignmentEpoch',
  ]);
  assert.deepEqual(json.authorizedOperations, ['READ']);
  assert.equal(json.groupState, 'STABLE');
  // @ts-expect-error a null member, which Java's List.copyOf refuses
  assert.throws(() => new ConsumerGroupDescription({ ...fields(d), members: [null] }), {
    name: 'TypeError',
    message: 'members must not contain null',
  });
});

/**
 * The constructor's fields of a description.
 *
 * @param {ConsumerGroupDescription} d
 */
function fields(d) {
  return {
    groupId: d.groupId,
    isSimpleConsumerGroup: d.isSimpleConsumerGroup,
    members: d.members,
    partitionAssignor: d.partitionAssignor,
    type: d.type,
    groupState: d.groupState,
    coordinator: d.coordinator,
    authorizedOperations: d.authorizedOperations,
    groupEpoch: d.groupEpoch,
    targetAssignmentEpoch: d.targetAssignmentEpoch,
  };
}
