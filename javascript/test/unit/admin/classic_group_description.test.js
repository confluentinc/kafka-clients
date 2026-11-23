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

// ClassicGroupDescription: Java has no test of it. These check Java's two
// constructors as one (the shorter's empty authorized operations), the data
// class's fields, isSimpleConsumerGroup, equals, toString and toJSON.

const assert = require('node:assert/strict');

const { ClassicGroupDescription } = require('@confluentinc/kafka-javascript/admin');
const { ClassicGroupState } = require('@confluentinc/kafka-javascript/common');

test("Java's shorter constructor passes the empty set of operations; a null one is unknown", () => {
  const short = new ClassicGroupDescription({
    groupId: 'g',
    protocol: '',
    protocolData: null,
    members: null,
    state: ClassicGroupState.EMPTY,
    coordinator: null,
  });
  assert.ok(Object.isFrozen(short));
  assert.deepEqual([...(short.authorizedOperations ?? [])], []);
  assert.equal(short.protocolData, '');
  assert.deepEqual(short.members, []);
  assert.ok(short.isSimpleConsumerGroup);
  const unknown = new ClassicGroupDescription({
    groupId: 'g',
    protocol: 'consumer',
    protocolData: 'range',
    members: [],
    state: ClassicGroupState.STABLE,
    coordinator: null,
    authorizedOperations: null,
  });
  assert.equal(unknown.authorizedOperations, null);
  assert.ok(!unknown.isSimpleConsumerGroup);
  assert.equal(
    String(unknown),
    "(groupId=g, protocol='consumer', protocolData=range, members=, state=Stable, coordinator=null, authorizedOperations=null)",
  );
  assert.ok(!short.equals(unknown));
  assert.ok(
    unknown.equals(
      new ClassicGroupDescription({
        groupId: 'g',
        protocol: 'consumer',
        protocolData: 'range',
        members: [],
        state: ClassicGroupState.STABLE,
        coordinator: null,
        authorizedOperations: null,
      }),
    ),
  );
  assert.equal(JSON.parse(JSON.stringify(unknown)).state, 'STABLE');
});
