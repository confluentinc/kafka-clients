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

// N9 check-string-encoding (scripts/lint/rules/string_encoding.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { runN } = require('./_n_harness');

const RULE = 'check-string-encoding';

// Node's Buffer and TextEncoder, as @types/node declares the calls used here.
const DECLS = [
  'declare class Buffer extends Uint8Array {',
  '  static from(data: string | Uint8Array, encoding?: string): Buffer;',
  '}',
  'declare class TextEncoder {',
  '  encode(s: string): Uint8Array;',
  '}',
];

test('the fixture encodes no string outside src/internal/encode.ts', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
});

test('Buffer.from of a string and TextEncoder are findings outside encode.ts, with their exact text', () => {
  const ENCODES = [
    ...DECLS,
    'export function encode(s: string, maybe: string | Uint8Array, bytes: Uint8Array): Uint8Array[] {',
    "  return [Buffer.from(s, 'utf8'), Buffer.from(maybe), Buffer.from(bytes), new TextEncoder().encode(s)];",
    '}',
    '',
  ].join('\n');
  const r = runN(RULE, { 'src/producer/encodes.ts': ENCODES });
  const why = 'encodes a string: Node writes U+FFFD for a lone surrogate, Java ? (use encodeLikeJava of src/internal/encode.ts)';
  // The declaration of the class is a reference too.
  assert.deepEqual(findings(r), [
    `src/producer/encodes.ts:4: TextEncoder ${why}`,
    `src/producer/encodes.ts:8: Buffer.from(s, ...) ${why}`,
    `src/producer/encodes.ts:8: Buffer.from(maybe, ...) ${why}`,
    `src/producer/encodes.ts:8: TextEncoder ${why}`,
  ]);
});

test('Buffer.alloc with a string fill, write and fill of a string, and new Buffer are findings too', () => {
  const ENCODES = [
    'declare class Buffer extends Uint8Array {',
    '  constructor(s: string);',
    '  static alloc(size: number, fill?: string | number): Buffer;',
    '  write(s: string, offset?: number): number;',
    '  fill(value: string | number): this;',
    '}',
    'export function encode(s: string, b: Buffer, n: number): unknown[] {',
    '  return [Buffer.alloc(4, s), Buffer.alloc(4, 0), b.write(s), b.write(s, 1), b.fill(s), b.fill(0), new Buffer(s)];',
    '}',
    '',
  ].join('\n');
  const r = runN(RULE, { 'src/producer/encodes.ts': ENCODES });
  const why = 'encodes a string: Node writes U+FFFD for a lone surrogate, Java ? (use encodeLikeJava of src/internal/encode.ts)';
  assert.deepEqual(findings(r), [
    `src/producer/encodes.ts:8: Buffer.alloc(4, s, ...) ${why}`,
    `src/producer/encodes.ts:8: b.write(s, ...) ${why}`,
    `src/producer/encodes.ts:8: b.write(s, ...) ${why}`,
    `src/producer/encodes.ts:8: b.fill(s, ...) ${why}`,
    `src/producer/encodes.ts:8: new Buffer(...) ${why}`,
  ]);
});

test('encodeLikeJava of encode.ts may encode; without it, or without encode.ts, the rule reports it missing', () => {
  // Node's Buffer only: a TextEncoder declaration in encode.ts would be a reference outside encodeLikeJava.
  const BUFFER = DECLS.slice(0, 3);
  const ENCODER = [...BUFFER, "export function encodeLikeJava(s: string): Uint8Array { return Buffer.from(s, 'utf8'); }", ''].join('\n');
  assert.equal(runN(RULE, { 'src/internal/encode.ts': ENCODER }).status, 0);
  let r = runN(RULE, { 'src/internal/encode.ts': undefined });
  assert.deepEqual(findings(r), ['src/internal/encode.ts: is missing: it is the one place a string becomes bytes']);
  // Another function of encode.ts encoding, and no encodeLikeJava exported.
  const OTHER = [...BUFFER, "export function encodeUtf8(s: string): Uint8Array { return Buffer.from(s, 'utf8'); }", ''].join('\n');
  r = runN(RULE, { 'src/internal/encode.ts': OTHER });
  assert.deepEqual(findings(r), [
    'src/internal/encode.ts:4: Buffer.from(s, ...) encodes a string: Node writes U+FFFD for a lone surrogate, Java ? (use encodeLikeJava of src/internal/encode.ts)',
    'src/internal/encode.ts:encodeLikeJava: exports no function encodeLikeJava(s): it is the one place a string becomes bytes',
  ]);
});
