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

// MemberToRemove and the MemberToRemoveSet that holds it by its group
// instance id: Java has no test of either. These check Java's equals and
// hashCode, and the set's value equality.

const assert = require('node:assert/strict');

const { MemberToRemove, MemberToRemoveSet } = require('@confluentinc/kafka-javascript/admin');

test("equals and hashCode are Java's: by group instance id", () => {
  const a = new MemberToRemove('instance-1');
  const b = new MemberToRemove({ groupInstanceId: 'instance-1' });
  assert.ok(a.equals(b));
  assert.equal(a.hashCode(), b.hashCode());
  assert.ok(!a.equals(new MemberToRemove('instance-2')));
  assert.equal(a.groupInstanceId(), 'instance-1');
  assert.equal(JSON.stringify(a), '{"groupInstanceId":"instance-1"}');
  // Java's equals dereferences the id.
  assert.throws(() => new MemberToRemove(null).equals(a), { name: 'TypeError', message: 'groupInstanceId must not be null' });
});

test('MemberToRemoveSet: equal members once, the original kept, a null member held', () => {
  const a = new MemberToRemove('instance-1');
  const set = new MemberToRemoveSet([a, new MemberToRemove('instance-1'), new MemberToRemove('instance-2')]);
  assert.equal(set.size(), 2);
  assert.ok(set.has(new MemberToRemove('instance-1')));
  assert.equal([...set][0], a);
  set.add(null);
  assert.ok(set.has(null));
  // Java's AbstractCollection.toString(), each member as Object.toString().
  assert.equal(
    String(set),
    '[org.apache.kafka.clients.admin.MemberToRemove@35c3c918, org.apache.kafka.clients.admin.MemberToRemove@35c3c919, null]',
  );
  assert.ok(set.delete(new MemberToRemove('instance-2')));
  assert.equal(JSON.stringify(set), '[{"groupInstanceId":"instance-1"},null]');
  assert.ok(!(set instanceof Set));
});
