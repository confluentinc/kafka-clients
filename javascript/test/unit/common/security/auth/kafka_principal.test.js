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

// Java's KafkaPrincipalTest, and the class's own contract: its null checks,
// toString, ANONYMOUS and the tokenAuthenticated pair.

const assert = require('node:assert/strict');

const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');

test('testEqualsAndHashCode', () => {
  const name = 'KafkaUser';
  const principal1 = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name });
  const principal2 = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name });
  assert.equal(principal1.hashCode(), principal2.hashCode());
  assert.ok(principal1.equals(principal2));
});

test("Java's hashCode, toString and getters; tokenAuthenticated is not compared", () => {
  const p = new KafkaPrincipal({ principalType: 'User', name: 'alice', tokenAuthenticated: true });
  // 31 * "User".hashCode() + "alice".hashCode(), in Java's int arithmetic.
  assert.equal(p.hashCode(), (Math.imul(31, 2645995) + 92903040) | 0);
  assert.equal(p.toString(), 'User:alice');
  assert.equal(p.getName(), 'alice');
  assert.equal(p.getPrincipalType(), 'User');
  assert.equal(p.tokenAuthenticated(), true);
  p.tokenAuthenticated(false);
  assert.equal(p.tokenAuthenticated(), false);
  p.tokenAuthenticated({ tokenAuthenticated: true });
  assert.equal(p.tokenAuthenticated(), true);
  assert.ok(p.equals(new KafkaPrincipal({ principalType: 'User', name: 'alice' })));
  assert.ok(!p.equals(new KafkaPrincipal({ principalType: 'Group', name: 'alice' })));
  assert.ok(!p.equals(null));
  assert.equal(KafkaPrincipal.ANONYMOUS.toString(), 'User:ANONYMOUS');
  assert.deepEqual(JSON.parse(JSON.stringify(p)), { principalType: 'User', name: 'alice', tokenAuthenticated: true });
});

test("Java's null checks: requireNonNull's messages", () => {
  // @ts-expect-error a null type
  assert.throws(() => new KafkaPrincipal({ principalType: null, name: 'a' }), {
    name: 'TypeError',
    message: 'Principal type cannot be null',
  });
  // @ts-expect-error a null name
  assert.throws(() => new KafkaPrincipal({ principalType: 'User', name: null }), {
    name: 'TypeError',
    message: 'Principal name cannot be null',
  });
  assert.throws(
    // @ts-expect-error a parameter Java has not
    () => new KafkaPrincipal({ principalType: 'User', name: 'a', host: 'h' }),
    {
      name: 'IllegalArgumentError',
      message:
        'KafkaPrincipal() takes one of (principalType, name), (principalType, name, tokenAuthenticated); got (principalType, name, host)',
    },
  );
  // @ts-expect-error a reader takes no argument
  assert.throws(() => KafkaPrincipal.ANONYMOUS.getName(1), { name: 'TypeError', message: 'getName() only reads: it takes no argument' });
});
