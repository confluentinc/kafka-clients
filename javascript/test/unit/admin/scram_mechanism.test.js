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

// Java's ScramMechanismTest, and the enum's own contract.

const assert = require('node:assert/strict');

const { ScramMechanism } = require('@confluentinc/kafka-javascript/admin');

test('testFromMechanismName', () => {
  assert.equal(ScramMechanism.fromMechanismName('UNKNOWN'), ScramMechanism.UNKNOWN);
  assert.equal(ScramMechanism.fromMechanismName('SCRAM-SHA-256'), ScramMechanism.SCRAM_SHA_256);
  assert.equal(ScramMechanism.fromMechanismName('SCRAM-SHA-512'), ScramMechanism.SCRAM_SHA_512);
  assert.equal(ScramMechanism.fromMechanismName('some string'), ScramMechanism.UNKNOWN);
  assert.equal(ScramMechanism.fromMechanismName('scram-sha-256'), ScramMechanism.UNKNOWN);
  // Java's equals against a null name is false.
  assert.equal(ScramMechanism.fromMechanismName(null), ScramMechanism.UNKNOWN);
});

test("the constants: Java's types, mechanism names and order", () => {
  assert.deepEqual(
    ScramMechanism.values().map((m) => [m.name(), m.ordinal(), m.type(), m.mechanismName(), String(m), JSON.stringify(m)]),
    [
      ['UNKNOWN', 0, 0, 'UNKNOWN', 'UNKNOWN', '"UNKNOWN"'],
      ['SCRAM_SHA_256', 1, 1, 'SCRAM-SHA-256', 'SCRAM_SHA_256', '"SCRAM_SHA_256"'],
      ['SCRAM_SHA_512', 2, 2, 'SCRAM-SHA-512', 'SCRAM_SHA_512', '"SCRAM_SHA_512"'],
    ],
  );
  assert.equal(ScramMechanism.fromType(2), ScramMechanism.SCRAM_SHA_512);
  assert.equal(ScramMechanism.fromType({ type: 7 }), ScramMechanism.UNKNOWN);
  assert.equal(ScramMechanism.valueOf('SCRAM_SHA_256'), ScramMechanism.SCRAM_SHA_256);
  assert.throws(() => ScramMechanism.valueOf('SCRAM-SHA-256'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.clients.admin.ScramMechanism.SCRAM-SHA-256',
  });
  assert.throws(() => ScramMechanism.fromType(128), { name: 'RangeError', message: 'type must be from -128 to 127' });
  assert.ok(Object.isFrozen(ScramMechanism.UNKNOWN));
});
