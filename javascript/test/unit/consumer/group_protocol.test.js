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

// Java has no GroupProtocolTest: the core's group_protocol.rs tests, 4 of its
// 5, and the enum's basics. test_of_invalid asserts Java's message, which
// names the upper-cased name (`GroupProtocol.INVALID`), not the core's, which
// keeps the name's case; test_from_str_roundtrip, of Rust's FromStr, is the
// test of of()'s bare form. Not translated: test_display_lowercase, as the core
// prints the name in lower case where Java and this client print `CLASSIC`.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { GroupProtocol } = require('@confluentinc/kafka-javascript/consumer');

test('test_name', () => {
  assert.equal(GroupProtocol.CLASSIC.name(), 'CLASSIC');
  assert.equal(GroupProtocol.CONSUMER.name(), 'CONSUMER');
});

test('test_of_case_insensitive', () => {
  assert.equal(GroupProtocol.of({ name: 'classic' }), GroupProtocol.CLASSIC);
  assert.equal(GroupProtocol.of({ name: 'CLASSIC' }), GroupProtocol.CLASSIC);
  assert.equal(GroupProtocol.of({ name: 'Classic' }), GroupProtocol.CLASSIC);
  assert.equal(GroupProtocol.of({ name: 'consumer' }), GroupProtocol.CONSUMER);
  assert.equal(GroupProtocol.of({ name: 'CONSUMER' }), GroupProtocol.CONSUMER);
  assert.equal(GroupProtocol.of({ name: 'Consumer' }), GroupProtocol.CONSUMER);
});

test("test_of_invalid, with Java's message", () => {
  assert.throws(() => GroupProtocol.of({ name: 'invalid' }), {
    constructor: IllegalArgumentError,
    message: 'No enum constant org.apache.kafka.clients.consumer.GroupProtocol.INVALID',
  });
  // @ts-expect-error a null the types forbid: Java's of dereferences the name
  assert.throws(() => GroupProtocol.of({ name: null }), { constructor: TypeError, message: 'name must not be null' });
});

test('test_from_str_roundtrip, as the bare form of of()', () => {
  assert.equal(GroupProtocol.of('classic'), GroupProtocol.CLASSIC);
  assert.equal(GroupProtocol.of('consumer'), GroupProtocol.CONSUMER);
});

test('the enum basics', () => {
  assert.deepEqual(GroupProtocol.values(), [GroupProtocol.CLASSIC, GroupProtocol.CONSUMER]);
  assert.notEqual(GroupProtocol.values(), GroupProtocol.values());
  assert.deepEqual(
    GroupProtocol.values().map((p) => [p.ordinal(), String(p), JSON.stringify(p)]),
    [
      [0, 'CLASSIC', '"CLASSIC"'],
      [1, 'CONSUMER', '"CONSUMER"'],
    ],
  );
  assert.equal(GroupProtocol.valueOf('CONSUMER'), GroupProtocol.CONSUMER);
  assert.equal(GroupProtocol.valueOf({ name: 'CLASSIC' }), GroupProtocol.CLASSIC);
  assert.throws(() => GroupProtocol.valueOf('consumer'), {
    constructor: IllegalArgumentError,
    message: 'No enum constant org.apache.kafka.clients.consumer.GroupProtocol.consumer',
  });
  // @ts-expect-error a null the types forbid: java.lang.Enum.valueOf's requireNonNull throws
  assert.throws(() => GroupProtocol.valueOf(null), { constructor: TypeError, message: 'Name is null' });
  assert.ok(Object.isFrozen(GroupProtocol.CLASSIC));
  // @ts-expect-error a wrong argument: name() only reads
  assert.throws(() => GroupProtocol.CLASSIC.name(1), { constructor: TypeError, message: 'name() only reads: it takes no argument' });
});
