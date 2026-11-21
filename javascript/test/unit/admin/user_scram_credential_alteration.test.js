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

// UserScramCredentialAlteration, an abstract class: Java ships no test of it.

const assert = require('node:assert/strict');

const { UserScramCredentialAlteration, UserScramCredentialDeletion, ScramMechanism } = require('@confluentinc/kafka-javascript/admin');

test('abstract: only a subclass constructs it, and the user is required', () => {
  const Ctor = /** @type {new (o: { user: string }) => object} */ (/** @type {unknown} */ (UserScramCredentialAlteration));
  assert.throws(() => new Ctor({ user: 'u' }), {
    name: 'TypeError',
    message: 'UserScramCredentialAlteration cannot be constructed directly; use one of its subclasses',
  });
  // @ts-expect-error a null user
  assert.throws(() => new UserScramCredentialDeletion({ user: null, mechanism: ScramMechanism.SCRAM_SHA_256 }), {
    name: 'TypeError',
    message: 'user must not be null',
  });
  assert.ok(
    new UserScramCredentialDeletion({ user: 'u', mechanism: ScramMechanism.SCRAM_SHA_256 }) instanceof UserScramCredentialAlteration,
  );
});
