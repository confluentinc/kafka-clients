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

// Java's GroupListingTest, then the binding's own: Java's toString, equals,
// hashCode and toJSON.

const assert = require('node:assert/strict');

const { GroupListing } = require('@confluentinc/kafka-javascript/admin');
const { GroupState, GroupType } = require('@confluentinc/kafka-javascript/common');

const GROUP_ID = 'mygroup';
// Java's ConsumerProtocol.PROTOCOL_TYPE.
const CONSUMER_PROTOCOL_TYPE = 'consumer';

test('testSimpleConsumerGroup', () => {
  let gl = new GroupListing({ groupId: GROUP_ID, type: GroupType.CLASSIC, protocol: '', groupState: GroupState.EMPTY });
  assert.ok(gl.isSimpleConsumerGroup());
  gl = new GroupListing({ groupId: GROUP_ID, type: GroupType.CLASSIC, protocol: CONSUMER_PROTOCOL_TYPE, groupState: GroupState.STABLE });
  assert.ok(!gl.isSimpleConsumerGroup());
  gl = new GroupListing({ groupId: GROUP_ID, type: GroupType.CONSUMER, protocol: '', groupState: GroupState.EMPTY });
  assert.ok(!gl.isSimpleConsumerGroup());
  gl = new GroupListing({ groupId: GROUP_ID, type: null, protocol: '', groupState: null });
  assert.ok(!gl.isSimpleConsumerGroup());
});

test("toString, equals, hashCode and toJSON are Java's", () => {
  const a = new GroupListing({ groupId: 'g', type: GroupType.CONSUMER, protocol: 'consumer', groupState: GroupState.STABLE });
  const b = new GroupListing({ groupId: 'g', type: GroupType.CONSUMER, protocol: 'consumer', groupState: GroupState.STABLE });
  const none = new GroupListing({ groupId: 'g', type: null, protocol: '', groupState: null });
  assert.equal(String(a), "(groupId='g', type=Consumer, protocol='consumer', groupState=Stable)");
  assert.equal(String(none), "(groupId='g', type=none, protocol='', groupState=none)");
  assert.ok(a.equals(b));
  assert.equal(a.hashCode(), b.hashCode());
  assert.ok(!a.equals(none));
  assert.equal(JSON.stringify(a), '{"groupId":"g","type":"CONSUMER","protocol":"consumer","groupState":"STABLE"}');
  assert.equal(a.groupId(), 'g');
  assert.equal(a.type(), GroupType.CONSUMER);
  assert.equal(a.protocol(), 'consumer');
  assert.equal(none.groupState(), null);
  // Called with an argument tsc refuses, as a plain JavaScript caller calls it.
  const loose = /** @type {Record<string, (x: unknown) => unknown>} */ (/** @type {unknown} */ (a));
  assert.throws(() => loose.groupId(1), { name: 'TypeError', message: 'groupId() only reads: it takes no argument' });
});

test("a classic listing with a null protocol: Java's protocol.isEmpty() dereference", () => {
  const gl = new GroupListing({ groupId: GROUP_ID, type: GroupType.CLASSIC, protocol: null, groupState: null });
  assert.throws(() => gl.isSimpleConsumerGroup(), { name: 'TypeError', message: 'protocol must not be null' });
  // @ts-expect-error a type of the wrong class
  assert.throws(() => new GroupListing({ groupId: 'g', type: 'CONSUMER', protocol: '', groupState: null }), {
    name: 'TypeError',
    message: 'type must be a GroupType or null, not a string',
  });
});
