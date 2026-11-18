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

// SslClientAuth, as python/test/unit/test_config_classes.py checks the Python one.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { SslClientAuth } = require('@confluentinc/kafka-javascript/common/config');

test('SslClientAuth', () => {
  assert.deepEqual(SslClientAuth.VALUES.map((a) => a.name()), ['REQUIRED', 'REQUESTED', 'NONE']);
  assert.deepEqual(SslClientAuth.VALUES, [SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE]);
  assert.equal(SslClientAuth.VALUES[0], SslClientAuth.REQUIRED);
  assert.ok(Object.isFrozen(SslClientAuth.VALUES));
  assert.ok(Object.isFrozen(SslClientAuth.NONE));
  // Java's toString() override, the name in lower case; name() and toJSON()
  // keep the name.
  assert.deepEqual(SslClientAuth.VALUES.map((a) => a.toString()), ['required', 'requested', 'none']);
  assert.equal(String(SslClientAuth.REQUESTED), 'requested');
  assert.equal(`${SslClientAuth.NONE}`, 'none');
  assert.equal(SslClientAuth.REQUESTED.name(), 'REQUESTED');
  assert.equal(SslClientAuth.forConfig(null), SslClientAuth.NONE);
  // Java's one-parameter forConfig(String key): the key on its own or named.
  assert.equal(SslClientAuth.forConfig({ key: null }), SslClientAuth.NONE);
  assert.equal(SslClientAuth.forConfig({ key: 'required' }), SslClientAuth.REQUIRED);
  // undefined is not Java's null: a TypeError naming the parameter.
  assert.throws(() => SslClientAuth.forConfig(undefined), {
    name: 'TypeError',
    message: "key must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => SslClientAuth.forConfig(), {
    name: 'IllegalArgumentError',
    message: 'forConfig() takes one of (key); got ()',
  });
  // A second argument is refused, not dropped.
  assert.throws(() => SslClientAuth.forConfig('required', 'x'), {
    name: 'IllegalArgumentError',
    message: 'forConfig() takes one of (key); got 2 arguments, not one object of named parameters',
  });
  assert.throws(() => SslClientAuth.forConfig({ key: undefined }), {
    name: 'TypeError',
    message: "key must not be undefined. Leave the key out, or pass null for Java's null",
  });
  assert.throws(() => SslClientAuth.forConfig({ name: 'required' }), {
    name: 'IllegalArgumentError',
    message: 'forConfig() takes one of (key); got (name)',
  });
  assert.equal(SslClientAuth.forConfig('required'), SslClientAuth.REQUIRED);
  assert.equal(SslClientAuth.forConfig('ReQuEsTeD'), SslClientAuth.REQUESTED);
  assert.equal(SslClientAuth.forConfig('other'), null);
  assert.throws(() => SslClientAuth.forConfig({ key: 5 }), { name: 'TypeError', message: 'key must be a string, not number' });
  assert.throws(() => SslClientAuth.forConfig(5), { name: 'IllegalArgumentError', message: 'forConfig() takes one of (key); got a number' });
});

// java.lang.Enum's members: name(), ordinal(), values(), valueOf(), and JSON.
test("SslClientAuth has java.lang.Enum's members", () => {
  assert.deepEqual(SslClientAuth.values().map((a) => [a.name(), a.ordinal()]), [['REQUIRED', 0], ['REQUESTED', 1], ['NONE', 2]]);
  assert.deepEqual(SslClientAuth.values(), [...SslClientAuth.VALUES]);
  assert.notEqual(SslClientAuth.values(), SslClientAuth.VALUES);
  assert.equal(SslClientAuth.valueOf('NONE'), SslClientAuth.NONE);
  assert.equal(SslClientAuth.valueOf({ name: 'REQUESTED' }), SslClientAuth.REQUESTED);
  assert.throws(() => SslClientAuth.valueOf('none'), (e) => {
    assert.ok(e instanceof IllegalArgumentError);
    assert.equal(e.message, 'No enum constant org.apache.kafka.common.config.SslClientAuth.none');
    return true;
  });
  assert.throws(() => SslClientAuth.valueOf(null), { name: 'TypeError', message: 'Name is null' });
  assert.throws(() => SslClientAuth.valueOf(undefined), {
    name: 'TypeError',
    message: "name must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => SslClientAuth.valueOf(), { name: 'IllegalArgumentError', message: 'valueOf() takes one of (name); got ()' });
  assert.throws(() => SslClientAuth.values(1), { name: 'TypeError', message: 'values() only reads: it takes no argument' });
  assert.throws(() => SslClientAuth.NONE.name('x'), { name: 'TypeError', message: 'name() only reads: it takes no argument' });
  assert.throws(() => SslClientAuth.NONE.ordinal(1, 2), { name: 'TypeError', message: 'ordinal() only reads: it takes no argument' });

  // JSON writes the name, not toString()'s lower case.
  assert.equal(JSON.stringify([SslClientAuth.REQUIRED]), '["REQUIRED"]');
  assert.deepEqual(Object.keys(SslClientAuth.NONE), []);
});
