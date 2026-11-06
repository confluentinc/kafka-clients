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

// H3 check-private-members (scripts/lint/rules/private_members.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-private-members';

const FUTURE = `export class Future {
  private static _create(): Future { return new Future(); }
  private _complete(): void {}
  private _done = false;
  get done(): boolean { return this._done; }
  static of(): Future { return Future._create(); }
}
`;

test('private _ members reached from another class with bracket access pass', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'lib/future.ts': FUTURE,
    'lib/user.ts': `import { Future } from './future';
export class User {
  constructor(private readonly _name: string) {}
  run(): void {
    const f = Future['_create']();
    f['_complete']();
  }
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 0, r.text);
  // Future: 5 named members; User: the constructor's parameter property and run; 2 accesses.
  assert.ok(r.out.includes('✅ check-private-members: 9 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'lib/future.ts': `export class Future {
  _open(): void {}
  static _make(): Future { return new Future(); }
  protected _state = 0;
  constructor(public _id = 1) {}
  private _hidden(): void {}
  self(): void { this['_hidden'](); }
}
`,
    'lib/user.ts': `import { Future } from './future';
export function use(f: Future): void {
  f['_open']();
  Future['_make']();
  f['_missing' as never];
  (f as unknown as Record<string, () => void>)['_any']();
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lib/future.ts:Future._open: `_open` is `_`-prefixed but not `private` (line 2)',
    'lib/future.ts:Future._make: `_make` is `_`-prefixed but not `private` (line 3)',
    'lib/future.ts:Future._state: `_state` is `_`-prefixed but not `private` (line 4)',
    'lib/future.ts:Future._id: `_id` is `_`-prefixed but not `private` (line 5)',
    "lib/future.ts:7: \`this['_hidden']\` is inside \`Future\` itself: use \`._hidden\`",
    "lib/user.ts:3: \`f['_open']\` reaches \`_open\`, which is not \`private\`",
    "lib/user.ts:4: \`Future['_make']\` reaches \`_make\`, which is not \`private\`",
    "lib/user.ts:6: \`(f as unknown as Record<string, () => void>)['_any']\` resolves to no member",
  ]);
  assert.ok(r.err.includes('❌ check-private-members: 8 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      "Declare a \`_\` member \`private\`, and reach another class's \`_\` member with \`x['_m']\` only when it is that class's \`private\` member (javascript/CLAUDE.md \"Files and layout\").",
    ),
    r.text,
  );
});

test('a member not prefixed with _ and a non-literal key are not checked as _ members', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'lib/a.ts': `export class A {
  value = 1;
  read(k: string): unknown { return (this as unknown as Record<string, unknown>)[k]; }
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-private-members: 2 item(s) checked'), r.text);
});

test('a baseline line waives a finding', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'lib/a.ts': 'export class A { _x = 1; }\n',
    'eslint-baseline.txt': 'not-private lib/a.ts:A._x\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
});

test('cannot run without tsconfig.json', () => {
  const r = lint(project({ 'lib/a.ts': 'export class A {}\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(r.err.slice(0, 2), ['', '❌ check-private-members: cannot run, no tsconfig.json']);
});

test('a lib/ without classes or bracket accesses fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'lib/a.ts': 'export const a = 1;\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-private-members: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

test('an optional chain resolves through the non-null type', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'lib/future.ts': FUTURE,
    'lib/user.ts': `import { Future } from './future';
export class Pub { _pub(): void {} }
export class User {
  f: Future | undefined;
  run(g: Pub | undefined): void {
    const f: Future | undefined = this.f;
    f?.['_complete']();
    this.f?.['_complete']();
    g?.['_pub']();
  }
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lib/user.ts:Pub._pub: `_pub` is `_`-prefixed but not `private` (line 2)',
    "lib/user.ts:9: `g?.['_pub']` reaches `_pub`, which is not `private`",
  ]);
});
