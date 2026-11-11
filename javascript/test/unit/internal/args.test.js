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

// src/internal/args.ts, the Java form matcher, as python/test/unit/test_args.py checks
// python/confluent_kafka/_args.py's java_forms: exact matching of the given
// names, `null` as given and `undefined` refused, the bare value of a
// one-parameter form, and the message naming every form.

const test = require('node:test');
const assert = require('node:assert/strict');

const { JavaForms } = require('../../../dist/internal/args');
const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');

const CLOSE = new JavaForms('close', [[], ['timeout']]);
const RECORD = new JavaForms('ProducerRecord', [
  ['topic', 'partition', 'timestamp', 'key', 'value', 'headers'],
  ['topic', 'partition', 'timestamp', 'key', 'value'],
  ['topic', 'partition', 'key', 'value', 'headers'],
  ['topic', 'partition', 'key', 'value'],
  ['topic', 'key', 'value'],
  ['topic', 'value'],
]);
const RECORD_FORMS =
  'ProducerRecord() takes one of (topic, partition, timestamp, key, value, headers), ' +
  '(topic, partition, timestamp, key, value), (topic, partition, key, value, headers), ' +
  '(topic, partition, key, value), (topic, key, value), (topic, value)';

/** An IllegalArgumentError with exactly `message`, Java's built-in (not a KafkaError). */
function illegal(message) {
  return (e) => {
    assert.ok(e instanceof IllegalArgumentError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalArgumentError');
    assert.equal(e.message, message);
    return true;
  };
}

test('each form matches exactly its own names, giving its index', () => {
  assert.equal(CLOSE.match(undefined, 0), 0);
  // close(undefined) is no close(): undefined is never a value.
  assert.throws(() => CLOSE.match(undefined, 1), {
    name: 'TypeError',
    message: "timeout must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.equal(CLOSE.match({}, 1), 0);
  assert.equal(CLOSE.match({ timeout: 5 }, 1), 1);
  assert.equal(RECORD.match({ topic: 't', partition: 0, timestamp: 1, key: 'k', value: 'v', headers: [] }, 1), 0);
  assert.equal(RECORD.match({ topic: 't', partition: 0, timestamp: 1, key: 'k', value: 'v' }, 1), 1);
  assert.equal(RECORD.match({ topic: 't', partition: 0, key: 'k', value: 'v', headers: [] }, 1), 2);
  assert.equal(RECORD.match({ topic: 't', partition: 0, key: 'k', value: 'v' }, 1), 3);
  assert.equal(RECORD.match({ topic: 't', key: 'k', value: 'v' }, 1), 4);
  assert.equal(RECORD.match({ topic: 't', value: 'v' }, 1), 5);
  // The order the keys are written in does not matter.
  assert.equal(RECORD.match({ value: 'v', key: 'k', topic: 't' }, 1), 4);
});

test("null is Java's null, which is given; a parameter given as undefined is a TypeError", () => {
  assert.equal(CLOSE.match({ timeout: null }, 1), 1);
  assert.equal(RECORD.match({ topic: null, value: null }, 1), 5);
  assert.equal(RECORD.match({ topic: 't', partition: null, key: null, value: 'v' }, 1), 3);
  // undefined is not "left out" (leave the key out): a TypeError naming the parameter.
  for (const [forms, args, name] of [
    [CLOSE, { timeout: undefined }, 'timeout'],
    [RECORD, { topic: 't', partition: undefined, value: 'v' }, 'partition'],
    [RECORD, { topic: undefined, value: 'v' }, 'topic'],
  ]) {
    assert.throws(() => forms.match(args, 1), {
      name: 'TypeError',
      message: `${name} must not be undefined. Leave the key out, or pass null for Java's null`,
    });
  }
  // A method may word it for one parameter.
  const record = new JavaForms('ProducerRecord', [['topic', 'value']], { undefinedMessages: { value: 'value: tombstone' } });
  assert.throws(() => record.match({ topic: 't', value: undefined }, 1), { name: 'TypeError', message: 'value: tombstone' });
});

test('a set of names no form has is rejected, naming every form and the given names in parameter order', () => {
  assert.throws(() => RECORD.match({}, 1), illegal(`${RECORD_FORMS}; got ()`));
  assert.throws(() => RECORD.match(undefined, 0), illegal(`${RECORD_FORMS}; got ()`));
  assert.throws(() => RECORD.match({ topic: 't' }, 1), illegal(`${RECORD_FORMS}; got (topic)`));
  // A timestamp or headers need a partition, as Java has no form without one.
  assert.throws(() => RECORD.match({ value: 'v', timestamp: 5, topic: 't' }, 1), illegal(`${RECORD_FORMS}; got (topic, timestamp, value)`));
  assert.throws(() => RECORD.match({ topic: 't', value: 'v', headers: [] }, 1), illegal(`${RECORD_FORMS}; got (topic, value, headers)`));
  assert.throws(() => RECORD.match({ topic: 't', partition: 0, value: 'v' }, 1), illegal(`${RECORD_FORMS}; got (topic, partition, value)`));
  assert.throws(() => RECORD.match({ topic: 't', key: 'k' }, 1), illegal(`${RECORD_FORMS}; got (topic, key)`));
});

test('a name no form has is rejected the same way, listed after the others, before an undefined is', () => {
  assert.throws(() => CLOSE.match({ timeoutMs: 5 }, 1), illegal('close() takes one of (), (timeout); got (timeoutMs)'));
  assert.throws(
    () => CLOSE.match({ other: 1, timeout: 5, timeoutMs: 5 }, 1),
    illegal('close() takes one of (), (timeout); got (timeout, other, timeoutMs)'),
  );
  // An unknown name is present whatever its value.
  assert.throws(() => CLOSE.match({ timeoutMs: undefined }, 1), illegal('close() takes one of (), (timeout); got (timeoutMs)'));
  assert.throws(() => RECORD.match({ topic: 't', value: undefined, extra: null }, 1), illegal(`${RECORD_FORMS}; got (topic, value, extra)`));
});

test('only own enumerable keys are names, and value() reads only those', () => {
  const inherited = Object.assign(Object.create({ partition: 3, key: 'k', timestamp: 7 }), { topic: 't', value: 'v' });
  // Not a plain object: its prototype is another object.
  assert.equal(Object.getPrototypeOf(Object.getPrototypeOf(inherited)), Object.prototype);
  const plain = Object.create(null);
  plain.topic = 't';
  plain.value = 'v';
  Object.defineProperty(plain, 'partition', { value: 5, enumerable: false });
  assert.equal(RECORD.match(plain, 1), 5);
  assert.equal(RECORD.value(plain, 'topic'), 't');
  assert.equal(RECORD.value(plain, 'partition'), undefined);
  assert.equal(CLOSE.match({ [Symbol('timeout')]: 5 }, 1), 0);
  // A polluted Object.prototype is no parameter either.
  Object.prototype.partition = 0;
  try {
    const args = { topic: 't', value: 'v' };
    assert.equal(RECORD.match(args, 1), 5);
    assert.equal(RECORD.value(args, 'partition'), undefined);
  } finally {
    delete Object.prototype.partition;
  }
  // An object that is not a plain one is no options object: without a
  // one-parameter form, the form error.
  assert.throws(() => RECORD.match(inherited, 1), illegal(`${RECORD_FORMS}; got an object`));
});

test('a one-parameter form takes the bare value: anything but a plain object', () => {
  const SEND = new JavaForms('send', [['record'], ['record', 'callback']]);
  class Record {}
  const record = new Record();
  assert.equal(SEND.match(record, 1), 0);
  assert.equal(SEND.value(record, 'record'), record);
  assert.equal(SEND.value(record, 'callback'), undefined);
  assert.equal(SEND.match({ record }, 1), 0);
  assert.equal(SEND.value({ record }, 'record'), record);
  // A plain object is the options object.
  assert.throws(() => SEND.match({}, 1), illegal('send() takes one of (record), (record, callback); got ()'));
  for (const bare of [5, 'x', null, [1], () => 5, Infinity]) {
    assert.equal(CLOSE.match(bare, 1), 1);
    assert.equal(CLOSE.value(bare, 'timeout'), bare);
  }
  assert.equal(CLOSE.value(undefined, 'timeout'), undefined);
});

test('one-parameter forms of different names take a bare value told apart by its type, with bareTypes', () => {
  const isError = (v) => v instanceof Error;
  const ERROR = new JavaForms('E', [['message', 'cause'], ['message'], ['cause'], []], {
    bareTypes: { message: (v) => v === null || typeof v === 'string', cause: isError },
  });
  const cause = new Error('c');
  assert.equal(ERROR.match('m', 1), 1);
  assert.equal(ERROR.value('m', 'message'), 'm');
  assert.equal(ERROR.value('m', 'cause'), undefined);
  // The first test that accepts it wins: null is the message.
  assert.equal(ERROR.match(null, 1), 1);
  assert.equal(ERROR.value(null, 'message'), null);
  assert.equal(ERROR.match(cause, 1), 2);
  assert.equal(ERROR.value(cause, 'cause'), cause);
  assert.equal(ERROR.value(cause, 'message'), undefined);
  // A plain object is still the options object.
  assert.equal(ERROR.match({ message: 'm', cause }, 1), 0);
  // A value no test accepts: the form error, in Python's form.
  assert.throws(() => ERROR.match(5, 1), illegal('E() takes one of (message, cause), (message), (cause), (); got a number'));
  // Without bareTypes, one-parameter forms of different names take no bare value.
  const PLAIN = new JavaForms('P', [['message'], ['cause']]);
  assert.throws(() => PLAIN.match('m', 1), illegal('P() takes one of (message), (cause); got a string'));
  // bareTypes must test every one-parameter form.
  assert.throws(() => new JavaForms('Q', [['message'], ['cause']], { bareTypes: { message: () => true } }), {
    message: 'JavaForms(Q): bareTypes has no test for every one-parameter form',
  });
});

test('when the bare value is a plain object, a plain object is the options object only with a parameter key', () => {
  const PRODUCER = new JavaForms('KafkaProducer', [['configs']], { bareIsPlainObject: true });
  const map = { 'bootstrap.servers': 'x' };
  assert.equal(PRODUCER.match(map, 1), 0);
  assert.equal(PRODUCER.value(map, 'configs'), map);
  assert.equal(PRODUCER.match({}, 1), 0);
  assert.equal(PRODUCER.match({ configs: map }, 1), 0);
  assert.equal(PRODUCER.value({ configs: map }, 'configs'), map);
  // A configs key makes it the options object: a stray key is then a form error.
  assert.throws(
    () => PRODUCER.match({ 'bootstrap.servers': 'x', configs: map }, 1),
    illegal('KafkaProducer() takes one of (configs); got (configs, bootstrap.servers)'),
  );
  assert.throws(() => PRODUCER.match({ configs: undefined }, 1), {
    name: 'TypeError',
    message: "configs must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // A Map is not a plain object: the bare value, which the method refuses.
  const m = new Map();
  assert.equal(PRODUCER.match(m, 1), 0);
  assert.equal(PRODUCER.value(m, 'configs'), m);
});

test('without a one-parameter form, an argument that is not a plain object is the form error, naming its type', () => {
  for (const [args, kind] of [
    [null, 'null'],
    [5, 'a number'],
    ['topic', 'a string'],
    [[5], 'an array'],
    [() => 5, 'a function'],
    [5n, 'a bigint'],
    [new Map(), 'a Map'],
  ]) {
    assert.throws(() => RECORD.match(args, 1), illegal(`${RECORD_FORMS}; got ${kind}`));
  }
});

test('a lone one-parameter form with a test takes only the values it accepts', () => {
  const SEND = new JavaForms('send', [['record'], ['record', 'callback']], {
    bareTypes: { record: (v) => v === null || v instanceof Map },
  });
  assert.equal(SEND.match(new Map(), 1), 0);
  assert.equal(SEND.match(null, 1), 0);
  for (const [bare, kind] of [
    [5, 'a number'],
    ['x', 'a string'],
    [new Set(), 'a Set'],
  ]) {
    assert.throws(() => SEND.match(bare, 1), illegal(`send() takes one of (record), (record, callback); got ${kind}`));
  }
});

test('a reader takes no argument: any argument, undefined included, is a TypeError', () => {
  const IS_DONE = new JavaForms('isDone', [[]], { reader: true });
  assert.equal(IS_DONE.match(undefined, 0), 0);
  for (const arg of [undefined, 5, {}, null]) {
    assert.throws(() => IS_DONE.match(arg, 1), { name: 'TypeError', message: 'isDone() only reads: it takes no argument' });
  }
  assert.throws(() => IS_DONE.match(5, 2), { name: 'TypeError', message: 'isDone() only reads: it takes no argument' });
  assert.throws(() => new JavaForms('m', [['a']], { reader: true }), { message: 'JavaForms(m): a reader takes no parameter' });
});

test('more than one argument is refused with the form error, never dropped', () => {
  const SEND = new JavaForms('send', [['record'], ['record', 'callback']]);
  assert.throws(
    () => SEND.match({}, 2),
    illegal('send() takes one of (record), (record, callback); got 2 arguments, not one object of named parameters'),
  );
  assert.throws(() => CLOSE.match(5, 3), illegal('close() takes one of (), (timeout); got 3 arguments, not one object of named parameters'));
  // A method may word it.
  const ERROR = new JavaForms('E', [['message']], { manyArguments: (count) => `E: ${count}` });
  assert.throws(() => ERROR.match('m', 2), illegal('E: 2'));
});

test('a bare undefined is a TypeError naming the parameter', () => {
  const SEND = new JavaForms('send', [['record'], ['record', 'callback']]);
  assert.throws(() => SEND.match(undefined, 1), {
    name: 'TypeError',
    message: "record must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // Called with nothing, it is the form error: no form takes no parameter.
  assert.throws(() => SEND.match(undefined, 0), illegal('send() takes one of (record), (record, callback); got ()'));
  assert.throws(() => RECORD.match(undefined, 1), {
    name: 'TypeError',
    message: "ProducerRecord() takes one object of named parameters, not undefined: leave the argument out, or pass null for Java's null",
  });
});

test('a method of no parameter refuses any argument: undefined with a TypeError, a value with the form error', () => {
  const FLUSH = new JavaForms('flush', [[]]);
  assert.equal(FLUSH.match(undefined, 0), 0);
  assert.throws(() => FLUSH.match(undefined, 1), {
    name: 'TypeError',
    message: 'flush() takes no argument: leave it out rather than pass undefined',
  });
  assert.equal(FLUSH.match({}, 1), 0);
  assert.throws(() => FLUSH.match(5, 1), illegal('flush() takes one of (); got a number'));
  assert.throws(() => FLUSH.match(null, 1), illegal('flush() takes one of (); got null'));
  assert.throws(() => FLUSH.match({ timeout: 5 }, 1), illegal('flush() takes one of (); got (timeout)'));
});

test('the message order may be given, and must hold every name', () => {
  const subscribe = new JavaForms('subscribe', [['topics'], ['topics', 'callback'], ['pattern', 'callback'], ['pattern']], {
    order: ['topics', 'pattern', 'callback'],
  });
  assert.throws(
    () => subscribe.match({ callback: null, pattern: 'p', topics: [] }, 1),
    illegal('subscribe() takes one of (topics), (topics, callback), (pattern, callback), (pattern); got (topics, pattern, callback)'),
  );
  assert.throws(() => new JavaForms('m', [['a', 'b']], { order: ['a'] }), { message: "JavaForms(m): the order a is not the forms' names a, b" });
});
