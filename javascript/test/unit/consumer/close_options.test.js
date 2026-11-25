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

// Java's CloseOptionsTest (its four tests) and the core's close_options.rs
// tests (three), and the binding's own checks of the option class: its keyed
// constructor, which calls the setters in Java's field order, and the
// one-value calls, written by name, with each bare form checked once.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { CloseOptions } = require('@confluentinc/kafka-javascript/consumer');

const { DEFAULT, LEAVE_GROUP, REMAIN_IN_GROUP } = CloseOptions.GroupMembershipOperation;

test('operationShouldNotBeNull', () => {
  // @ts-expect-error a null the types forbid: Java's requireNonNull throws
  assert.throws(() => CloseOptions.groupMembershipOperation({ operation: null }), {
    constructor: TypeError,
    message: 'operation should not be null',
  });
  // @ts-expect-error a null the types forbid: Java's requireNonNull throws
  assert.throws(() => CloseOptions.timeout({ timeout: 0 }).withGroupMembershipOperation({ operation: null }), {
    constructor: TypeError,
    message: 'operation should not be null',
  });
});

test('operationShouldHaveDefaultValue', () => {
  assert.equal(CloseOptions.timeout({ timeout: 0 }).groupMembershipOperation(), DEFAULT);
});

test('timeoutCouldBeNull', () => {
  const closeOptions = CloseOptions.timeout({ timeout: null });
  assert.equal(closeOptions.timeout(), null);
});

test('timeoutShouldBeDefaultEmpty', () => {
  assert.equal(CloseOptions.groupMembershipOperation({ operation: DEFAULT }).timeout(), null);
});

test('test_default_values', () => {
  const opts = new CloseOptions();
  assert.equal(opts.groupMembershipOperation(), DEFAULT);
  assert.equal(opts.timeout(), null);
});

test('test_timeout_constructor', () => {
  const opts = CloseOptions.timeout({ timeout: 1000 });
  assert.equal(opts.timeout(), 1000);
  assert.equal(opts.groupMembershipOperation(), DEFAULT);
});

test('test_group_membership_operation_constructor', () => {
  const opts = CloseOptions.groupMembershipOperation({ operation: LEAVE_GROUP });
  assert.equal(opts.groupMembershipOperation(), LEAVE_GROUP);
  assert.equal(opts.timeout(), null);
});

test('the keyed constructor: each key calls its setter; a key left out keeps the starting value', () => {
  const both = new CloseOptions({ operation: REMAIN_IN_GROUP, timeout: 5000 });
  assert.equal(both.groupMembershipOperation(), REMAIN_IN_GROUP);
  assert.equal(both.timeout(), 5000);
  assert.equal(new CloseOptions({ timeout: 5000 }).groupMembershipOperation(), DEFAULT);
  assert.equal(new CloseOptions({ operation: LEAVE_GROUP }).timeout(), null);
  assert.equal(new CloseOptions({ timeout: null }).timeout(), null);
  assert.equal(new CloseOptions({}).timeout(), null);
  // `undefined` leaves a key out, in an option class's constructor.
  assert.equal(new CloseOptions({ operation: undefined, timeout: undefined }).groupMembershipOperation(), DEFAULT);
  assert.equal(new CloseOptions(undefined).timeout(), null);
  // @ts-expect-error a null the types forbid: the setter's requireNonNull throws
  assert.throws(() => new CloseOptions({ operation: null }), { constructor: TypeError, message: 'operation should not be null' });
  // @ts-expect-error a wrong argument: an unknown key throws IllegalArgumentError
  assert.throws(() => new CloseOptions({ timeoutMs: 5 }), {
    constructor: IllegalArgumentError,
    message: 'CloseOptions() takes an object of the keys (operation, timeout); got (timeoutMs)',
  });
  // @ts-expect-error a wrong argument: the constructor takes one object
  assert.throws(() => new CloseOptions({ timeout: 1 }, {}), {
    constructor: IllegalArgumentError,
    message: 'CloseOptions() takes an object of the keys (operation, timeout); got 2 arguments',
  });
  // @ts-expect-error a wrong argument: the constructor takes one object of keys
  assert.throws(() => new CloseOptions(5000), {
    constructor: TypeError,
    message: 'CloseOptions() takes an object of the keys (operation, timeout), not a number',
  });
});

test("the setters run in Java's field order, operation then timeout", () => {
  // The operation's error comes first, whichever order the keys are written in.
  // @ts-expect-error a null the types forbid: the setter's requireNonNull throws
  assert.throws(() => new CloseOptions({ timeout: NaN, operation: null }), {
    constructor: TypeError,
    message: 'operation should not be null',
  });
  assert.throws(() => new CloseOptions({ timeout: NaN, operation: LEAVE_GROUP }), {
    constructor: RangeError,
    message: 'timeout must be a number of milliseconds, not NaN',
  });
});

test('the statics against the getters; the setters return this', () => {
  const options = CloseOptions.timeout({ timeout: 250.7 });
  assert.equal(options.timeout(), 250);
  assert.equal(options.withTimeout({ timeout: Infinity }), options);
  assert.equal(options.timeout(), Infinity);
  assert.equal(options.withTimeout({ timeout: -1 }).timeout(), -1);
  assert.equal(options.withTimeout({ timeout: null }).timeout(), null);
  assert.equal(options.withGroupMembershipOperation({ operation: LEAVE_GROUP }), options);
  assert.equal(options.groupMembershipOperation(), LEAVE_GROUP);
  assert.ok(CloseOptions.timeout({ timeout: 1 }) !== CloseOptions.timeout({ timeout: 1 }));
  // @ts-expect-error a wrong argument: a getter only reads
  assert.throws(() => options.timeout(5), { constructor: TypeError, message: 'timeout() only reads: it takes no argument' });
  // @ts-expect-error a wrong argument: a getter only reads
  assert.throws(() => options.groupMembershipOperation(LEAVE_GROUP), {
    constructor: TypeError,
    message: 'groupMembershipOperation() only reads: it takes no argument',
  });
  // @ts-expect-error a wrong argument: a timeout is a Duration
  assert.throws(() => options.withTimeout({ timeout: '5' }), {
    constructor: TypeError,
    message: 'timeout must be a number of milliseconds or a Temporal.Duration',
  });
  // @ts-expect-error a wrong argument: an operation is a GroupMembershipOperation
  assert.throws(() => options.withGroupMembershipOperation({ operation: 'LEAVE_GROUP' }), {
    constructor: TypeError,
    message: 'operation must be a CloseOptions.GroupMembershipOperation',
  });
});

test('the bare forms, once per method', () => {
  assert.equal(CloseOptions.timeout(5000).timeout(), 5000);
  assert.equal(CloseOptions.groupMembershipOperation(REMAIN_IN_GROUP).groupMembershipOperation(), REMAIN_IN_GROUP);
  assert.equal(new CloseOptions().withTimeout(10).timeout(), 10);
  assert.equal(new CloseOptions().withGroupMembershipOperation(LEAVE_GROUP).groupMembershipOperation(), LEAVE_GROUP);
  assert.equal(CloseOptions.timeout(null).timeout(), null);
  // @ts-expect-error a wrong argument: a bare string fits no form
  assert.throws(() => CloseOptions.timeout('5'), {
    constructor: IllegalArgumentError,
    message: 'timeout() takes one of (timeout); got a string',
  });
});

test("GroupMembershipOperation is Java's nested enum", () => {
  assert.deepEqual(CloseOptions.GroupMembershipOperation.values(), [LEAVE_GROUP, REMAIN_IN_GROUP, DEFAULT]);
  assert.deepEqual(
    [LEAVE_GROUP, REMAIN_IN_GROUP, DEFAULT].map((c) => [c.name(), c.ordinal(), String(c), JSON.stringify(c)]),
    [
      ['LEAVE_GROUP', 0, 'LEAVE_GROUP', '"LEAVE_GROUP"'],
      ['REMAIN_IN_GROUP', 1, 'REMAIN_IN_GROUP', '"REMAIN_IN_GROUP"'],
      ['DEFAULT', 2, 'DEFAULT', '"DEFAULT"'],
    ],
  );
  assert.equal(CloseOptions.GroupMembershipOperation.valueOf('DEFAULT'), DEFAULT);
  assert.equal(CloseOptions.GroupMembershipOperation.valueOf({ name: 'LEAVE_GROUP' }), LEAVE_GROUP);
  assert.throws(() => CloseOptions.GroupMembershipOperation.valueOf('default'), {
    constructor: IllegalArgumentError,
    message: 'No enum constant org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation.default',
  });
  // @ts-expect-error a null the types forbid: java.lang.Enum.valueOf's requireNonNull throws
  assert.throws(() => CloseOptions.GroupMembershipOperation.valueOf(null), { constructor: TypeError, message: 'Name is null' });
  assert.ok(Object.isFrozen(DEFAULT));
});
