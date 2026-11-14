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

// src/internal/java_string.ts: Java's String.valueOf and Double.toString, for
// the messages Java builds by concatenation. The expected texts are Java's,
// from JDK 21's String.valueOf and Double.toString on the same values.

const test = require('node:test');
const assert = require('node:assert/strict');

const { javaString, javaDoubleString } = require('../../../dist/internal/java_string');

test("javaString is Java's String.valueOf", () => {
  assert.equal(javaString(null), 'null');
  assert.equal(javaString('x'), 'x');
  assert.equal(javaString(5), '5');
  assert.equal(javaString(5n), '5');
  assert.equal(javaString(true), 'true');
  assert.equal(javaString(['a', 'b']), '[a, b]');
  assert.equal(javaString(new Set(['a'])), '[a]');
  assert.equal(javaString(new Set()), '[]');
  assert.equal(javaString(new Map([['k', 1n]])), '{k=1}');
  assert.equal(javaString({ toString: () => 'own' }), 'own');
});

test("javaDoubleString is Java's Double.toString", () => {
  const cases = [
    [1, '1.0'],
    [0.5, '0.5'],
    [-2.25, '-2.25'],
    [0, '0.0'],
    [-0, '-0.0'],
    [1e7, '1.0E7'],
    [1.5e-7, '1.5E-7'],
    [0.001, '0.001'],
    [9999999.5, '9999999.5'],
    [1e21, '1.0E21'],
    [1.0e23, '1.0E23'],
    [Number.MAX_VALUE, '1.7976931348623157E308'],
    [NaN, 'NaN'],
    [Infinity, 'Infinity'],
    [-Infinity, '-Infinity'],
  ];
  for (const [d, text] of cases) assert.equal(javaDoubleString(d), text, String(d));
});
