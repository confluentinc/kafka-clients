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

// ScramCredentialInfo, UserScramCredentialsDescription and the two
// alterations: Java ships no tests of them; these check what Java's sources
// do (equals, hashCode, toString, the null checks and the random salt).

const assert = require('node:assert/strict');

const {
  ScramCredentialInfo,
  ScramMechanism,
  UserScramCredentialAlteration,
  UserScramCredentialDeletion,
  UserScramCredentialUpsertion,
  UserScramCredentialsDescription,
} = require('@confluentinc/kafka-javascript/admin');

const INFO = new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 });

test("ScramCredentialInfo: Java's equals, toString and null check", () => {
  assert.ok(INFO.equals(new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 })));
  assert.ok(!INFO.equals(new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_512, iterations: 4096 })));
  assert.equal(INFO.hashCode(), new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 }).hashCode());
  assert.equal(INFO.toString(), 'ScramCredentialInfo{mechanism=SCRAM_SHA_256, iterations=4096}');
  // @ts-expect-error a null mechanism
  assert.throws(() => new ScramCredentialInfo({ mechanism: null, iterations: 1 }), {
    name: 'TypeError',
    message: 'mechanism must not be null',
  });
});
