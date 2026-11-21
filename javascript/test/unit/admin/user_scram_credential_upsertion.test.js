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

// UserScramCredentialUpsertion: Java ships no test of it; this checks what
// Java's three constructors do: the password's UTF-8 bytes, the random salt
// (ScramFormatter.secureRandomBytes: a random 130-bit number in base 36) and
// the null checks in Java's order.

const assert = require('node:assert/strict');

const { ScramCredentialInfo, ScramMechanism, UserScramCredentialUpsertion } = require('@confluentinc/kafka-javascript/admin');

const INFO = new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 });

test("the string password's UTF-8 bytes, a lone surrogate as Java's '?', and a random salt", () => {
  const u = new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password: 'pé\ud800' });
  assert.deepEqual([...u.password()], [...Buffer.from('pé?', 'utf8')]);
  assert.ok(Buffer.isBuffer(u.salt()));
  assert.match(u.salt().toString(), /^[0-9a-z]{1,26}$/);
  const salts = new Set(
    Array.from({ length: 20 }, () =>
      new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password: 'p' }).salt().toString(),
    ),
  );
  assert.equal(salts.size, 20);
  assert.equal(u.credentialInfo(), INFO);
  assert.equal(u.user(), 'u');
});

test('bytes are held as given, a Buffer over them handed out, with or without a salt', () => {
  const password = new Uint8Array([1, 2, 3]);
  const salt = new Uint8Array([9]);
  const u = new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password, salt });
  assert.ok(Buffer.isBuffer(u.password()));
  password[0] = 7;
  assert.equal(u.password()[0], 7);
  assert.deepEqual([...u.salt()], [9]);
  const generated = new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password });
  assert.ok(generated.salt().length > 0);
  // The JSON leaves the password and the salt out.
  assert.deepEqual(JSON.parse(JSON.stringify(u)), { user: 'u', credentialInfo: { mechanism: 'SCRAM_SHA_256', iterations: 4096 } });
});

test("Java's null checks, in Java's order; a null password without a salt fits neither form", () => {
  assert.throws(
    // @ts-expect-error a null password
    () => new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password: null }),
    {
      name: 'IllegalArgumentError',
      message: 'UserScramCredentialUpsertion() takes a string or a Uint8Array password; got null, which fits neither form',
    },
  );
  // @ts-expect-error a null user
  assert.throws(() => new UserScramCredentialUpsertion({ user: null, credentialInfo: null, password: 'p' }), {
    name: 'TypeError',
    message: 'user must not be null',
  });
  // @ts-expect-error a null info
  assert.throws(() => new UserScramCredentialUpsertion({ user: 'u', credentialInfo: null, password: 'p' }), {
    name: 'TypeError',
    message: 'credentialInfo must not be null',
  });
  // @ts-expect-error a null password and salt
  assert.throws(() => new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password: null, salt: null }), {
    name: 'TypeError',
    message: 'password must not be null',
  });
  // @ts-expect-error a null salt
  assert.throws(() => new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password: new Uint8Array(1), salt: null }), {
    name: 'TypeError',
    message: 'salt must not be null',
  });
  // Java has no (String, ScramCredentialInfo, String, byte[]) form.
  // @ts-expect-error a string password with a salt
  assert.throws(() => new UserScramCredentialUpsertion({ user: 'u', credentialInfo: INFO, password: 'p', salt: new Uint8Array(1) }), {
    name: 'TypeError',
    message: 'password must be a Uint8Array, not a string',
  });
});
