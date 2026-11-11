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

// SecurityProtocol, as python/test/unit/test_config_classes.py checks the
// Python one.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { SecurityProtocol } = require('@confluentinc/kafka-javascript/common/security/auth');

test('SecurityProtocol', () => {
  assert.deepEqual(
    SecurityProtocol.values().map((p) => [p.name(), p.ordinal(), p.id]),
    [['PLAINTEXT', 0, 0], ['SSL', 1, 1], ['SASL_PLAINTEXT', 2, 2], ['SASL_SSL', 3, 3]],
  );
  assert.deepEqual(SecurityProtocol.names(), ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']);
  assert.equal(SecurityProtocol.forId(3), SecurityProtocol.SASL_SSL);
  assert.equal(SecurityProtocol.forId(9), null);
  // Java's id is a short: neither null nor another type. A bare value that is
  // no number fits no form.
  assert.throws(() => SecurityProtocol.forId(null), { name: 'IllegalArgumentError', message: 'forId() takes one of (id); got null' });
  assert.throws(() => SecurityProtocol.forId({ id: null }), { name: 'TypeError', message: 'id must be a number, not null' });
  assert.throws(() => SecurityProtocol.forId({ id: '1' }), { name: 'TypeError', message: 'id must be a number, not string' });
  assert.equal(SecurityProtocol.forName('sasl_plaintext'), SecurityProtocol.SASL_PLAINTEXT);
  assert.equal(String(SecurityProtocol.SSL), 'SSL');
  assert.ok(Object.isFrozen(SecurityProtocol.SSL));
  assert.throws(() => SecurityProtocol.forName('tls'), (e) => {
    assert.ok(e instanceof IllegalArgumentError);
    assert.equal(e.message, 'No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.TLS');
    return true;
  });
  assert.throws(() => SecurityProtocol.forName(null), { name: 'TypeError', message: 'name must not be null' });
  // Java's one-parameter forId(Short id) and forName(String name): the value
  // on its own or named.
  assert.equal(SecurityProtocol.forId({ id: 1 }), SecurityProtocol.SSL);
  assert.equal(SecurityProtocol.forId({ id: 9 }), null);
  assert.equal(SecurityProtocol.forName({ name: 'ssl' }), SecurityProtocol.SSL);
  assert.throws(() => SecurityProtocol.forName({ name: null }), { name: 'TypeError', message: 'name must not be null' });
  assert.throws(() => SecurityProtocol.forId({ id: 1, name: 'SSL' }), {
    name: 'IllegalArgumentError',
    message: 'forId() takes one of (id); got (id, name)',
  });
  assert.throws(() => SecurityProtocol.forName(), { name: 'IllegalArgumentError', message: 'forName() takes one of (name); got ()' });
  // undefined is not Java's null: a TypeError naming the parameter.
  assert.throws(() => SecurityProtocol.forName(undefined), {
    name: 'TypeError',
    message: "name must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => SecurityProtocol.forId(undefined), { name: 'TypeError', message: 'id must be a number, not undefined' });
  assert.throws(() => SecurityProtocol.forId({ id: undefined }), { name: 'TypeError', message: 'id must be a number, not undefined' });
  // A second argument is refused, not dropped.
  // values() and names() take no argument.
  assert.throws(() => SecurityProtocol.values(1), { name: 'TypeError', message: 'values() only reads: it takes no argument' });
  assert.throws(() => SecurityProtocol.names({ x: 1 }), { name: 'TypeError', message: 'names() only reads: it takes no argument' });
  assert.throws(() => SecurityProtocol.forId(1, 2), {
    name: 'IllegalArgumentError',
    message: 'forId() takes one of (id); got 2 arguments, not one object of named parameters',
  });
  assert.throws(() => SecurityProtocol.forName('ssl', 'x'), {
    name: 'IllegalArgumentError',
    message: 'forName() takes one of (name); got 2 arguments, not one object of named parameters',
  });
});

// java.lang.Enum's members: name(), ordinal(), values(), valueOf(), and JSON.
test("SecurityProtocol has java.lang.Enum's members", () => {
  assert.equal(SecurityProtocol.SASL_SSL.name(), 'SASL_SSL');
  assert.equal(SecurityProtocol.SASL_SSL.ordinal(), 3);
  // values() is a new array each call, as Java's clone.
  assert.notEqual(SecurityProtocol.values(), SecurityProtocol.values());
  assert.equal(SecurityProtocol.valueOf('SSL'), SecurityProtocol.SSL);
  assert.equal(SecurityProtocol.valueOf({ name: 'SASL_PLAINTEXT' }), SecurityProtocol.SASL_PLAINTEXT);
  // Java's Enum.valueOf is exact: no case folding (that is forName's).
  assert.throws(() => SecurityProtocol.valueOf('ssl'), (e) => {
    assert.ok(e instanceof IllegalArgumentError);
    assert.equal(e.message, 'No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.ssl');
    return true;
  });
  assert.throws(() => SecurityProtocol.valueOf(null), { name: 'TypeError', message: 'Name is null' });
  assert.throws(() => SecurityProtocol.valueOf({ name: 1 }), { name: 'TypeError', message: 'name must be a string, not number' });
  assert.throws(() => SecurityProtocol.valueOf(1), { name: 'IllegalArgumentError', message: 'valueOf() takes one of (name); got a number' });
  assert.throws(() => SecurityProtocol.valueOf(undefined), {
    name: 'TypeError',
    message: "name must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => SecurityProtocol.valueOf(), { name: 'IllegalArgumentError', message: 'valueOf() takes one of (name); got ()' });
  assert.throws(() => SecurityProtocol.valueOf('SSL', 'x'), {
    name: 'IllegalArgumentError',
    message: 'valueOf() takes one of (name); got 2 arguments, not one object of named parameters',
  });
  assert.throws(() => SecurityProtocol.valueOf({ id: 1 }), { name: 'IllegalArgumentError', message: 'valueOf() takes one of (name); got (id)' });
  // name() and ordinal() take no argument.
  assert.throws(() => SecurityProtocol.SSL.name(1), { name: 'TypeError', message: 'name() only reads: it takes no argument' });
  assert.throws(() => SecurityProtocol.SSL.ordinal({ x: 1 }), { name: 'TypeError', message: 'ordinal() only reads: it takes no argument' });
  // JSON.stringify writes the name (toJSON is passed the key, which is ignored).
  assert.equal(JSON.stringify(SecurityProtocol.SSL), '"SSL"');
  assert.equal(JSON.stringify({ protocol: SecurityProtocol.SASL_SSL }), '{"protocol":"SASL_SSL"}');
  assert.deepEqual(Object.keys(SecurityProtocol.SSL), ['id']);
  // The constructor is TypeScript-private: no run-time guard.
  assert.equal(typeof SecurityProtocol, 'function');
});
