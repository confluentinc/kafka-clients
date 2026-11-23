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

// Java's RemoveMembersFromConsumerGroupOptionsTest, then the binding's own:
// the removeAll mode, the reason setter (Java's void) and the forms.

const assert = require('node:assert/strict');

const { MemberToRemove, MemberToRemoveSet, RemoveMembersFromConsumerGroupOptions } = require('@confluentinc/kafka-javascript/admin');

test('testConstructor', () => {
  const options = new RemoveMembersFromConsumerGroupOptions([new MemberToRemove('instance-1')]);
  const members = options.members();
  assert.ok(members instanceof MemberToRemoveSet);
  assert.equal(members.size(), 1);
  assert.ok(members.has(new MemberToRemove('instance-1')));
  assert.throws(() => new RemoveMembersFromConsumerGroupOptions([]), {
    name: 'IllegalArgumentError',
    message: 'Invalid empty members has been provided',
  });
});

test("removeAll mode, the reason, Java's null members dereference and the forms", () => {
  const all = new RemoveMembersFromConsumerGroupOptions();
  assert.ok(all.removeAll());
  assert.equal(all.members().size(), 0);
  assert.equal(all.reason(), null);
  assert.equal(all.reason('because'), undefined);
  assert.equal(all.reason(), 'because');
  all.reason({ reason: null });
  assert.equal(all.reason(), null);
  const some = new RemoveMembersFromConsumerGroupOptions({ members: new Set([new MemberToRemove('a'), new MemberToRemove('a')]) });
  assert.ok(!some.removeAll());
  assert.equal(some.members().size(), 1);
  // @ts-expect-error null members, which Java dereferences
  assert.throws(() => new RemoveMembersFromConsumerGroupOptions(null), { name: 'TypeError', message: 'members must not be null' });
  // @ts-expect-error a bare string where Java takes Collection<MemberToRemove>
  assert.throws(() => new RemoveMembersFromConsumerGroupOptions('a'), {
    name: 'TypeError',
    message: 'members must be an iterable of MemberToRemove, not a string',
  });
});
