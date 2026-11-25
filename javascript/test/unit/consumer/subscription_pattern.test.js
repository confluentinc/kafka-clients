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

// Java has no SubscriptionPatternTest: the core's subscription_pattern.rs
// tests (three), and the binding's own checks of the class.

const assert = require('node:assert/strict');

const { SubscriptionPattern } = require('@confluentinc/kafka-javascript/consumer');

test('test_pattern_accessor', () => {
  const p = new SubscriptionPattern({ pattern: 'foo.*' });
  assert.equal(p.pattern(), 'foo.*');
});

test('test_display_is_pattern', () => {
  const p = new SubscriptionPattern({ pattern: 'foo.*' });
  assert.equal(String(p), 'foo.*');
});

test('test_equality_and_hash', () => {
  const p1 = new SubscriptionPattern({ pattern: 'a.*' });
  const p2 = new SubscriptionPattern({ pattern: 'a.*' });
  const p3 = new SubscriptionPattern({ pattern: 'b.*' });
  assert.ok(p1.equals(p2));
  assert.equal(p1.equals(p3), false);
  assert.equal(p1.hashCode(), p2.hashCode());
  // Java's "a.*".hashCode().
  assert.equal(p1.hashCode(), 94685);
  assert.notEqual(p1.hashCode(), p3.hashCode());
});

test('the bare form, a null pattern, toJSON()', () => {
  const bare = new SubscriptionPattern('t.*');
  assert.equal(bare.pattern(), 't.*');
  assert.equal(JSON.stringify(bare), '{"pattern":"t.*"}');
  // @ts-expect-error a null the types forbid: Java keeps a null pattern
  const none = new SubscriptionPattern(null);
  assert.equal(none.pattern(), null);
  assert.equal(String(none), 'null');
  assert.throws(() => none.hashCode(), { constructor: TypeError, message: 'pattern must not be null' });
  // @ts-expect-error a null the types forbid: Java keeps a null pattern
  assert.ok(none.equals(new SubscriptionPattern({ pattern: null })));
  assert.equal(none.equals(bare), false);
  // @ts-expect-error a wrong argument: a pattern is RE2/J text, not a RegExp
  assert.throws(() => new SubscriptionPattern(/t.*/), {
    constructor: TypeError,
    message: "pattern must be a string: a SubscriptionPattern takes an RE2/J pattern as text, such as a RegExp's source",
  });
});
