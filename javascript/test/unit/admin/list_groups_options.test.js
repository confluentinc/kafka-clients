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

// Java's ListGroupsOptionsTest, then the binding's own: the keyed constructor
// (one key per setter field, named after the field) and the setters' checks.

const assert = require('node:assert/strict');

const { ListGroupsOptions } = require('@confluentinc/kafka-javascript/admin');
const { GroupState, GroupType } = require('@confluentinc/kafka-javascript/common');

// Java's ConsumerProtocol.PROTOCOL_TYPE.
const PROTOCOL_TYPE = 'consumer';

/**
 * Java's options.inGroupStates / withTypes / withProtocolTypes, then the readers.
 *
 * @param {ListGroupsOptions} options
 */
function setAndCheck(options) {
  options.inGroupStates(new Set([GroupState.STABLE]));
  options.withTypes(new Set([GroupType.CONSUMER]));
  options.withProtocolTypes(new Set([PROTOCOL_TYPE]));
  assert.deepEqual(options.groupStates(), new Set([GroupState.STABLE]));
  assert.deepEqual(options.types(), new Set([GroupType.CONSUMER]));
  assert.deepEqual(options.protocolTypes(), new Set([PROTOCOL_TYPE]));
}

test('testForConsumerGroups', () => {
  const options = ListGroupsOptions.forConsumerGroups();
  assert.equal(options.groupStates().size, 0);
  assert.deepEqual(options.types(), new Set([GroupType.CLASSIC, GroupType.CONSUMER]));
  assert.deepEqual(options.protocolTypes(), new Set(['', PROTOCOL_TYPE]));
  setAndCheck(options);
});

test('testForShareGroups', () => {
  const options = ListGroupsOptions.forShareGroups();
  assert.equal(options.groupStates().size, 0);
  assert.deepEqual(options.types(), new Set([GroupType.SHARE]));
  assert.equal(options.protocolTypes().size, 0);
  setAndCheck(options);
});

test('testForStreamsGroups', () => {
  const options = ListGroupsOptions.forStreamsGroups();
  assert.equal(options.groupStates().size, 0);
  assert.deepEqual(options.types(), new Set([GroupType.STREAMS]));
  assert.equal(options.protocolTypes().size, 0);
  setAndCheck(options);
});

test('testGroupStates', () => {
  let options = new ListGroupsOptions();
  assert.equal(options.groupStates().size, 0);
  options.inGroupStates(new Set([GroupState.DEAD]));
  assert.deepEqual(options.groupStates(), new Set([GroupState.DEAD]));
  const groupStates = new Set(GroupState.values());
  options = new ListGroupsOptions().inGroupStates(groupStates);
  assert.deepEqual(options.groupStates(), groupStates);
});

test('testConsumerGroupStates', () => {
  const groupStates = GroupState.groupStatesForType(GroupType.CONSUMER);
  const options = new ListGroupsOptions().inGroupStates(groupStates);
  assert.deepEqual(options.groupStates(), groupStates);
});

test('testProtocolTypes', () => {
  let options = new ListGroupsOptions();
  assert.equal(options.protocolTypes().size, 0);
  options.withProtocolTypes(new Set([PROTOCOL_TYPE]));
  assert.deepEqual(options.protocolTypes(), new Set([PROTOCOL_TYPE]));
  const protocolTypes = new Set(['', 'consumer', 'share']);
  options = new ListGroupsOptions().withProtocolTypes(protocolTypes);
  assert.deepEqual(options.protocolTypes(), protocolTypes);
});

test('testTypes', () => {
  let options = new ListGroupsOptions();
  assert.equal(options.types().size, 0);
  options.withTypes(new Set([GroupType.CLASSIC]));
  assert.deepEqual(options.types(), new Set([GroupType.CLASSIC]));
  const groupTypes = new Set(GroupType.values());
  options = new ListGroupsOptions().withTypes(groupTypes);
  assert.deepEqual(options.types(), groupTypes);
});

test("the keyed constructor, null read as empty, Java's Set.copyOf refusing a null element, a bare string refused", () => {
  const options = new ListGroupsOptions({
    groupStates: [GroupState.EMPTY],
    types: [GroupType.CLASSIC],
    protocolTypes: [''],
    timeoutMs: 100,
  });
  assert.deepEqual(options.groupStates(), new Set([GroupState.EMPTY]));
  assert.equal(options.timeoutMs(), 100);
  options.inGroupStates(null);
  assert.equal(options.groupStates().size, 0);
  // @ts-expect-error a null element, which Java's Set.copyOf refuses
  assert.throws(() => options.withTypes([null]), { name: 'TypeError', message: 'types must not contain null' });
  // A string is an Iterable<string> to tsc: only the run time refuses it.
  assert.throws(() => options.withProtocolTypes('consumer'), {
    name: 'TypeError',
    message: 'protocolTypes must be an iterable of strings, not string',
  });
  // @ts-expect-error the setter's value left out
  assert.throws(() => options.inGroupStates(), {
    name: 'TypeError',
    message: 'inGroupStates() takes the value to set: read it with groupStates()',
  });
  assert.notEqual(options.types(), options.types());
});
