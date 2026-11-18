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

// H3 check-internal-members (scripts/lint/rules/internal_members.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-internal-members';

const FUTURE = `export class Future {
  #done = false;
  private constructor() {}
  /**
   * Made by the library.
   *
   * @internal
   */
  static internalCreate(): Future { return new Future(); }
  /** @internal */
  internalComplete(): void { this.#done = true; }
  isDone(): boolean { return this.#done; }
}
`;

test('@internal members named internal..., called with dot access, pass', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'src/future.ts': FUTURE,
    'src/user.ts': `import { Future } from './future';
export class User {
  constructor(private readonly name: string) {}
  run(): void {
    const f = Future.internalCreate();
    f.internalComplete();
    void this.name;
  }
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 0, r.text);
  // Future: the constructor, internalCreate, internalComplete, isDone (the
  // untagged # field is not a name to check); User: the constructor and its
  // parameter property, run.
  assert.ok(r.out.includes('✅ check-internal-members: 7 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'src/future.ts': `export class Future {
  _open(): void {}
  static _make(): Future { return new Future(); }
  constructor(public _id = 1) {}
  internalSend(): void {}
  /** @internal */
  complete(): void {}
  /** @internal */
  private internalHidden(): void {}
  /** @internal */
  #internalOwn(): void {}
  own(): void { this.#internalOwn(); this.internalHidden(); }
}
export class Made {
  /** @internal */
  constructor() {}
}
`,
    'src/user.ts': `import { Future } from './future';
export function use(f: Future): void {
  f['internalSend']();
  (f as unknown as Record<string, () => void>)['_any']();
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  const tag = 'a member only the library calls is `@internal` and named `internal...`';
  const hidden = 'is `@internal` and also hidden: an `@internal` member is public, called by the library with dot access';
  assert.deepEqual(findings(r), [
    `src/future.ts:Future._open: \`_open\` is \`_\`-prefixed: ${tag} (line 2)`,
    `src/future.ts:Future._make: \`_make\` is \`_\`-prefixed: ${tag} (line 3)`,
    `src/future.ts:Future._id: \`_id\` is \`_\`-prefixed: ${tag} (line 4)`,
    'src/future.ts:Future.internalSend: `internalSend` is named `internal...` but its TSDoc has no `@internal` (line 5)',
    'src/future.ts:Future.complete: `complete` is `@internal` but not named `internal...` (line 7)',
    `src/future.ts:Future.internalHidden: \`internalHidden\` ${hidden} (line 9)`,
    `src/future.ts:Future.internalOwn: \`internalOwn\` ${hidden} (line 11)`,
    'src/future.ts:Made.constructor: `constructor` is `@internal`: stripped, the class would declare none, which TypeScript reads as public; make it `private` (line 16)',
    "src/user.ts:3: `f['internalSend']` names a member by a string: call it as `.internalSend`",
    "src/user.ts:4: `(f as unknown as Record<string, () => void>)['_any']` names a member by a string: call it as `._any`",
  ]);
  assert.ok(r.err.includes('❌ check-internal-members: 10 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      'Make a member only the library calls a public `@internal` member named `internal...`, called with dot access; make a constructor users must not call `private`; keep an error\'s FFI id in `_ffiId` (javascript/CLAUDE.md "Files and layout"; root CLAUDE.md, JavaScript Binding Conventions, Errors).',
    ),
    r.text,
  );
});

test("an error class's FFI id is `_ffiId`: allowed on the registry's classes, read by dot or bracket access; KafkaError declares it", () => {
  const registry = "export const ERRORS = [\n  ['../common/kafka_error', 'KafkaError', 'org.apache.kafka.common.KafkaException'],\n];\n";
  const files = {
    'tsconfig.json': TSCONFIG,
    'src/internal/error_registry.ts': registry,
    'src/common/kafka_error.ts': 'export class KafkaError extends Error {\n  private static readonly _ffiId: number = -1;\n}\n',
    'src/internal/errors.ts': `import { KafkaError } from '../common/kafka_error';
export function ffiIdOf(): number {
  return (KafkaError as unknown as Record<string, number>)['_ffiId'];
}
`,
  };
  let r = lint(project(files), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Not on another class, and KafkaError must declare it.
  r = lint(
    project({
      ...files,
      'src/common/kafka_error.ts': 'export class KafkaError extends Error {}\n',
      'src/other.ts': 'export class Other {\n  private readonly _ffiId = 1;\n}\n',
    }),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    'src/other.ts:Other._ffiId: `_ffiId` is `_`-prefixed: a member only the library calls is `@internal` and named `internal...` (line 2)',
    'src/common/kafka_error.ts:KafkaError._ffiId: KafkaError declares no `_ffiId`: the FFI id is kept in the private `_ffiId`, for the binding and the test harness',
  ]);
});

test('other string keys and non-literal keys are not member accesses to check', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'src/a.ts': `export class A {
  value = 1;
  read(k: string): unknown { return (this as unknown as Record<string, unknown>)[k]; }
  other(): unknown { return (this as unknown as Record<string, unknown>)['value']; }
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 0, r.text);
  // value, read, other, and the one literal key.
  assert.ok(r.out.includes('✅ check-internal-members: 4 item(s) checked'), r.text);
});

test('a baseline line waives a finding', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'src/a.ts': 'export class A { _x = 1; }\n',
    'eslint-baseline.txt': 'underscore src/a.ts:A._x\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
});

test('cannot run without tsconfig.json', () => {
  const r = lint(project({ 'src/a.ts': 'export class A {}\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(r.err.slice(0, 2), ['', '❌ check-internal-members: cannot run, no tsconfig.json']);
});

test('a src/ without classes or string keys fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'src/a.ts': 'export const a = 1;\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-internal-members: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});
