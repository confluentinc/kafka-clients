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

// UserScramCredentialsDescription, a data class: Java ships no test of it;
// this checks what Java's source does (List.copyOf, equals, toString).

const assert = require('node:assert/strict');

const { ScramCredentialInfo, ScramMechanism, UserScramCredentialsDescription } = require('@confluentinc/kafka-javascript/admin');

const INFO = new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 });

test("a data class: frozen fields, Java's equals and toString", () => {
  const infos = [INFO];
  const d = new UserScramCredentialsDescription({ name: 'alice', credentialInfos: infos });
  infos.push(INFO);
  assert.equal(d.credentialInfos.length, 1);
  assert.ok(Object.isFrozen(d) && Object.isFrozen(d.credentialInfos));
  assert.equal(d.name, 'alice');
  assert.equal(
    d.toString(),
    "UserScramCredentialsDescription{name='alice', credentialInfos=[ScramCredentialInfo{mechanism=SCRAM_SHA_256, iterations=4096}]}",
  );
  assert.ok(d.equals(new UserScramCredentialsDescription({ name: 'alice', credentialInfos: [INFO] })));
  assert.equal(d.hashCode(), new UserScramCredentialsDescription({ name: 'alice', credentialInfos: [INFO] }).hashCode());
  assert.deepEqual(JSON.parse(JSON.stringify(d)), { name: 'alice', credentialInfos: [{ mechanism: 'SCRAM_SHA_256', iterations: 4096 }] });
});

test("Java's null checks: the name, the list and an element (List.copyOf)", () => {
  // @ts-expect-error a null name
  assert.throws(() => new UserScramCredentialsDescription({ name: null, credentialInfos: [] }), {
    name: 'TypeError',
    message: 'name must not be null',
  });
  // @ts-expect-error a null list
  assert.throws(() => new UserScramCredentialsDescription({ name: 'a', credentialInfos: null }), {
    name: 'TypeError',
    message: 'credentialInfos must not be null',
  });
  // @ts-expect-error a null element
  assert.throws(() => new UserScramCredentialsDescription({ name: 'a', credentialInfos: [null] }), {
    name: 'TypeError',
    message: 'credentialInfos must not contain null',
  });
});
