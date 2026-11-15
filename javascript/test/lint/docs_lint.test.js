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

// The docs lint of scripts/lint/eslint/config.js (root CLAUDE.md, JavaScript
// Binding Conventions, Documentation, Q64): eslint-plugin-jsdoc's
// require-param, require-returns and require-hyphen-before-param-description
// read every doc comment on src/, the body-less declarations included
// (overload signatures, abstract methods, interface methods), where the
// public docs live.

const assert = require('node:assert/strict');
const os = require('node:os');
const path = require('node:path');

const { Linter } = require('eslint');

const { config, KAFKA_RULES } = require('../../scripts/lint/eslint/config');

const ROOT = path.join(os.tmpdir(), 'docs-lint-fixture');

/** The docs lint's findings on `source`, as `<line> <rule>: <message>`. */
function findings(source) {
  const linter = new Linter({ cwd: ROOT });
  const off = Object.fromEntries(Object.keys(KAFKA_RULES).map((id) => [id, 'off']));
  const cfg = [...config(ROOT, { typed: false }), { rules: off }];
  return linter
    .verify(source, cfg, path.join(ROOT, 'src', 'fixture.ts'))
    .map((m) => {
      assert.ok(m.ruleId !== null, m.message);
      return `${m.line} ${m.ruleId}: ${m.message}`;
    })
    .sort();
}

test('overload signatures, constructor overloads, abstract and interface methods are checked', () => {
  const source = [
    'export abstract class A {', // 1
    '  /** Sends. */', // 2
    '  send(record: string): number;', // 3
    '  /**', // 4
    '   * Sends.', // 5
    '   *', // 6
    '   * @param options - the record', // 7
    '   * @returns the offset', // 8
    '   */', // 9
    '  send(options: { record: string }): number;', // 10
    '  send(options: unknown): number {', // 11
    '    return typeof options === "string" ? 1 : 0;', // 12
    '  }', // 13
    '  /** Makes one. */', // 14
    '  constructor(value: number);', // 15
    '  /** Makes one. */', // 16
    '  constructor();', // 17
    '  constructor(value?: number) {', // 18
    '    void value;', // 19
    '  }', // 20
    '  /** Completes. */', // 21
    '  abstract complete(value: number): boolean;', // 22
    '}', // 23
    'export interface B {', // 24
    '  /** Reads. */', // 25
    '  read(key: string): string;', // 26
    '}', // 27
  ].join('\n');
  // Each finding is reported on its doc comment.
  assert.deepEqual(findings(source), [
    '14 jsdoc/require-param: Missing JSDoc @param "value" declaration.',
    '2 jsdoc/require-param: Missing JSDoc @param "record" declaration.',
    '2 jsdoc/require-returns: Missing JSDoc @returns declaration.',
    '21 jsdoc/require-param: Missing JSDoc @param "value" declaration.',
    '21 jsdoc/require-returns: Missing JSDoc @returns declaration.',
    '25 jsdoc/require-param: Missing JSDoc @param "key" declaration.',
    '25 jsdoc/require-returns: Missing JSDoc @returns declaration.',
  ]);
});

test('a void or Promise<void> declaration needs no @returns, an options object one @param, a @param its hyphen, and a member no doc comment', () => {
  const source = [
    'export abstract class A {', // 1
    '  /** Flushes. */', // 2
    '  abstract flush(): void;', // 3
    '  /** Closes. */', // 4
    '  abstract close(): Promise<void>;', // 5
    '  /**', // 6
    '   * Sets.', // 7
    '   *', // 8
    '   * @param options - the key and value', // 9
    '   */', // 10
    '  set({ key, value }: { key: string; value: number }): void {', // 11
    '    void key;', // 12
    '    void value;', // 13
    '  }', // 14
    '  /**', // 15
    '   * Gets.', // 16
    '   *', // 17
    '   * @param key the key', // 18
    '   * @returns the value', // 19
    '   */', // 20
    '  abstract get(key: string): Promise<number>;', // 21
    '}', // 22
    "// Which members need a doc comment is not the lint's: this one has none.", // 23
    'export function f(x: number): number {', // 24
    '  return x;', // 25
    '}', // 26
  ].join('\n');
  assert.deepEqual(findings(source), [
    '18 jsdoc/require-hyphen-before-param-description: There must be a hyphen before @param description.',
  ]);
});
