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

// The option classes (javascript/CLAUDE.md, JavaScript Binding Conventions, Class
// family): AbstractOptions, abstract; the keyed constructors; a
// getter and setter of one name as one method, and a pair of two names kept;
// the setters recorded in the order they last ran, which the client runs on
// the native options; Java's timeoutMs as an Integer. Java ships no
// test of these classes.

const assert = require('node:assert/strict');

const { AbstractOptions, DescribeClusterOptions, ListTopicsOptions } = require('@confluentinc/kafka-javascript/admin');

/**
 * The setters an options object ran, the library's own view (src/admin/abstract_options.ts).
 *
 * @param {AbstractOptions} options
 * @returns {Array<[string, unknown]>}
 */
const setters = (options) =>
  /** @type {{ internalSetters(): Array<[string, unknown]> }} */ (/** @type {unknown} */ (options)).internalSetters();

test('AbstractOptions is abstract', () => {
  // @ts-expect-error an abstract class: tsc refuses new, and the run time throws TypeError
  assert.throws(() => new AbstractOptions(), {
    name: 'TypeError',
    message: 'AbstractOptions cannot be constructed directly; use one of its subclasses',
  });
  assert.ok(new DescribeClusterOptions() instanceof AbstractOptions);
});

test("timeoutMs: no argument reads, one sets and returns the options; Java's Integer", () => {
  const o = new DescribeClusterOptions();
  assert.equal(o.timeoutMs(), null);
  assert.equal(o.timeoutMs(1000), o);
  assert.equal(o.timeoutMs(), 1000);
  assert.equal(o.timeoutMs({ timeoutMs: null }), o);
  assert.equal(o.timeoutMs(), null);
  o.timeoutMs(-5);
  assert.equal(o.timeoutMs(), -5);
  assert.throws(() => o.timeoutMs(2 ** 31), { name: 'RangeError', message: 'timeoutMs must be from -2147483648 to 2147483647' });
  assert.throws(() => o.timeoutMs(Infinity), { name: 'RangeError', message: 'timeoutMs must be finite, not Infinity' });
  assert.throws(() => o.timeoutMs(1.5), { name: 'TypeError', message: 'timeoutMs must be an integer' });
  // @ts-expect-error an empty options object neither reads nor sets
  assert.throws(() => o.timeoutMs({}), {
    name: 'TypeError',
    message: 'timeoutMs() takes the value to set: leave the argument out to read it',
  });
  // @ts-expect-error undefined is never a value
  assert.throws(() => o.timeoutMs(undefined), TypeError);
});

test('DescribeClusterOptions: the keyed constructor and the getter / setter pairs', () => {
  const o = new DescribeClusterOptions({ includeFencedBrokers: true, timeoutMs: 10, includeAuthorizedOperations: undefined });
  assert.equal(o.includeFencedBrokers(), true);
  assert.equal(o.includeAuthorizedOperations(), false);
  assert.equal(o.timeoutMs(), 10);
  // In the fixed order (the parent's field first), whatever the key order.
  assert.deepEqual(setters(o), [
    ['timeoutMs', 10],
    ['includeFencedBrokers', true],
  ]);
  assert.equal(o.includeAuthorizedOperations(true), o);
  assert.equal(o.includeAuthorizedOperations(), true);
  o.timeoutMs(20);
  // The order the setters last ran.
  assert.deepEqual(setters(o), [
    ['includeFencedBrokers', true],
    ['includeAuthorizedOperations', true],
    ['timeoutMs', 20],
  ]);
  // @ts-expect-error a null boolean: Java's primitive cannot hold it
  assert.throws(() => o.includeFencedBrokers({ includeFencedBrokers: null }), {
    name: 'TypeError',
    message: 'includeFencedBrokers must be a boolean, not null',
  });
  // @ts-expect-error an unknown key
  assert.throws(() => new DescribeClusterOptions({ validateOnly: true }), {
    name: 'IllegalArgumentError',
    message:
      'DescribeClusterOptions() takes an object of the keys (timeoutMs, includeAuthorizedOperations, includeFencedBrokers); got (validateOnly)',
  });
  assert.deepEqual(setters(new DescribeClusterOptions()), []);
});

test("ListTopicsOptions: Java's listInternal / shouldListInternal, toString, equals and hashCode", () => {
  const o = new ListTopicsOptions();
  assert.equal(o.shouldListInternal(), false);
  assert.equal(o.listInternal(true), o);
  assert.equal(o.shouldListInternal(), true);
  // @ts-expect-error the setter with no value: it does not read
  assert.throws(() => o.listInternal(), {
    name: 'TypeError',
    message: 'listInternal() takes the value to set: read it with shouldListInternal()',
  });
  // @ts-expect-error the reader takes no argument
  assert.throws(() => o.shouldListInternal(true), { name: 'TypeError', message: 'shouldListInternal() only reads: it takes no argument' });
  assert.equal(o.toString(), 'ListTopicsOptions(listInternal=true)');
  assert.ok(o.equals(new ListTopicsOptions({ listInternal: true })));
  assert.ok(!o.equals(new ListTopicsOptions()));
  assert.ok(!o.equals(null));
  assert.equal(o.hashCode(), new ListTopicsOptions({ listInternal: true }).hashCode());
  // Objects.hash(listInternal): 31 + Boolean.hashCode.
  assert.equal(new ListTopicsOptions().hashCode(), 31 + 1237);
});
