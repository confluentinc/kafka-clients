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

// TokenInformation: Java ships no test of it; this checks what Java's source
// does (equals without the expiry, toString, ownerOrRenewer, the setter).

const assert = require('node:assert/strict');

const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
const { TokenInformation } = require('@confluentinc/kafka-javascript/common/security/token/delegation');

const alice = new KafkaPrincipal({ principalType: 'User', name: 'alice' });
const bob = new KafkaPrincipal({ principalType: 'User', name: 'bob' });
/** @param {number} expiry */
const info = (expiry) =>
  new TokenInformation({ tokenId: 'id', owner: alice, renewers: [bob], issueTimestamp: 1, maxTimestamp: 2, expiryTimestamp: expiry });

test("the six-parameter form makes the owner the requester; equals leaves out the expiry, as Java's does", () => {
  const i = info(3);
  assert.equal(i.tokenRequester(), alice);
  assert.ok(i.equals(info(4)));
  assert.ok(
    i.equals(
      TokenInformation.fromRecord({
        tokenId: 'id',
        owner: alice,
        tokenRequester: alice,
        renewers: [bob],
        issueTimestamp: 1n,
        maxTimestamp: 2n,
        expiryTimestamp: 9n,
      }),
    ),
  );
  assert.equal(
    i.toString(),
    "TokenInformation{owner=User:alice, tokenRequester=User:alice, renewers=[User:bob], issueTimestamp=1, maxTimestamp=2, expiryTimestamp=3, tokenId='id'}",
  );
  assert.equal(i.expiryTimestamp(), 3n);
  i.setExpiryTimestamp(5n);
  assert.equal(i.expiryTimestamp(), 5n);
  assert.deepEqual(i.renewersAsString(), ['User:bob']);
  assert.equal(i.ownerAsString(), 'User:alice');
  assert.equal(i.ownerOrRenewer(bob), true);
  assert.equal(i.ownerOrRenewer(new KafkaPrincipal({ principalType: 'User', name: 'carol' })), false);
  assert.notEqual(i.renewers(), i.renewers());
  assert.deepEqual(JSON.parse(JSON.stringify(i)).expiryTimestamp, '5');
});

test('a null is stored, as Java stores it, and fails where Java reads it', () => {
  // @ts-expect-error a null the types forbid: Java stores it
  const i = new TokenInformation({ tokenId: null, owner: null, renewers: null, issueTimestamp: 0, maxTimestamp: 0, expiryTimestamp: 0 });
  assert.equal(i.owner(), null);
  assert.throws(() => i.ownerAsString(), { name: 'TypeError', message: 'owner must not be null' });
  assert.throws(() => i.renewersAsString(), { name: 'TypeError', message: 'renewers must not be null' });
});
