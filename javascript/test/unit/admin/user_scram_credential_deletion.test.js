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

// UserScramCredentialDeletion: Java ships no test of it.

const assert = require('node:assert/strict');

const { ScramMechanism, UserScramCredentialDeletion } = require('@confluentinc/kafka-javascript/admin');

test("the user and the mechanism, both required, as Java's requireNonNull", () => {
  const d = new UserScramCredentialDeletion({ user: 'u', mechanism: ScramMechanism.SCRAM_SHA_512 });
  assert.equal(d.user(), 'u');
  assert.equal(d.mechanism(), ScramMechanism.SCRAM_SHA_512);
  // @ts-expect-error a null mechanism
  assert.throws(() => new UserScramCredentialDeletion({ user: 'u', mechanism: null }), {
    name: 'TypeError',
    message: 'mechanism must not be null',
  });
  assert.deepEqual(JSON.parse(JSON.stringify(d)), { user: 'u', mechanism: 'SCRAM_SHA_512' });
});
