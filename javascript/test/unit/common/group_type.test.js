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

// GroupType, a Java enum whose toString() is its display name, with Java's
// parse, which reads null as UNKNOWN. Java has no test of it: these check
// Java's members.

const assert = require('node:assert/strict');

const { GroupType } = require('@confluentinc/kafka-javascript/common');

test("GroupType: the constants in Java's order, toString the display name, toJSON the name", () => {
  assert.deepEqual(
    GroupType.values().map((t) => [t.name(), t.ordinal(), String(t), JSON.stringify(t)]),
    [
      ['UNKNOWN', 0, 'Unknown', '"UNKNOWN"'],
      ['CONSUMER', 1, 'Consumer', '"CONSUMER"'],
      ['CLASSIC', 2, 'Classic', '"CLASSIC"'],
      ['SHARE', 3, 'Share', '"SHARE"'],
      ['STREAMS', 4, 'Streams', '"STREAMS"'],
    ],
  );
});

test('GroupType.parse: case-insensitive, UNKNOWN for null or an unknown name', () => {
  assert.equal(GroupType.parse('consumer'), GroupType.CONSUMER);
  assert.equal(GroupType.parse({ name: 'CLASSIC' }), GroupType.CLASSIC);
  assert.equal(GroupType.parse(null), GroupType.UNKNOWN);
  assert.equal(GroupType.parse('nope'), GroupType.UNKNOWN);
  assert.equal(GroupType.valueOf('SHARE'), GroupType.SHARE);
});
