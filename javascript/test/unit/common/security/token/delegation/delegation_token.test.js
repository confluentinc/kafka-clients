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

// DelegationToken: Java ships no test of it; this checks what Java's source
// does (equals over the HMAC bytes, the hidden HMAC in toString, base64).

const assert = require('node:assert/strict');

const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
const { DelegationToken, TokenInformation } = require('@confluentinc/kafka-javascript/common/security/token/delegation');

const alice = new KafkaPrincipal({ principalType: 'User', name: 'alice' });
const tokenInformation = new TokenInformation({
  tokenId: 'id',
  owner: alice,
  renewers: [],
  issueTimestamp: 1,
  maxTimestamp: 2,
  expiryTimestamp: 3,
});

test('equal details and HMAC bytes are equal; toString hides the HMAC', () => {
  const t = new DelegationToken({ tokenInformation, hmac: new Uint8Array([1, 2]) });
  assert.ok(t.equals(new DelegationToken({ tokenInformation, hmac: Buffer.from([1, 2]) })));
  assert.ok(!t.equals(new DelegationToken({ tokenInformation, hmac: Buffer.from([1, 3]) })));
  assert.equal(t.hashCode(), new DelegationToken({ tokenInformation, hmac: Buffer.from([1, 2]) }).hashCode());
  assert.ok(Buffer.isBuffer(t.hmac()));
  assert.equal(t.hmacAsBase64String(), 'AQI=');
  assert.equal(t.toString(), `DelegationToken{tokenInformation=${tokenInformation.toString()}, hmac=[*******]}`);
  assert.equal(JSON.stringify(t).includes('AQI'), false);
  // @ts-expect-error a null the types forbid: Java stores it
  assert.throws(() => new DelegationToken({ tokenInformation, hmac: null }).hmacAsBase64String(), {
    name: 'TypeError',
    message: 'hmac must not be null',
  });
});
