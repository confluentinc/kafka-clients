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

// The rule linter's baseline (scripts/lint/baseline.js), which follows
// rust/xtask/src/lint_custom/baseline.rs, alone, through the runner
// (lint-baseline.txt) and through ESLint (eslint-baseline.txt).

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const { Baseline, key } = require('../../scripts/lint/baseline');
const { TODO_MARK, CLAUDE_MD, project, lint, lintRunner, findings } = require('./_project');
const { part } = require('../../scripts/lint/checks');

const f = (kind, subject) => ({ kind, subject, message: 'm' });
const subjects = (list) => list.map((x) => `${x.kind} ${x.subject}: ${x.message}`);

test('a key line waives its finding; comments and blank lines are ignored', () => {
  const b = Baseline.parse('b.txt', '# header\n\n  todo src/a.ts:1  \n');
  assert.deepEqual(b.apply([f('todo', 'src/a.ts:1'), f('todo', 'src/a.ts:2')]), [f('todo', 'src/a.ts:2')]);
  assert.equal(key(f('todo', 'x')), 'todo x');
});

test('a prefix line waives every finding whose subject starts with it', () => {
  const b = Baseline.parse('b.txt', 'prefix:src/producer/\n');
  assert.deepEqual(
    b.apply([f('todo', 'src/producer/a.ts:1'), f('java-line', 'src/producer/b.ts'), f('todo', 'src/c.ts:1')]),
    [f('todo', 'src/c.ts:1')],
  );
});

test('a stale line, a stale prefix and a duplicate are findings', () => {
  const b = Baseline.parse('b.txt', 'todo gone\nprefix:old/\ntodo kept\ntodo kept\nprefix:old/\n');
  assert.deepEqual(subjects(b.apply([f('todo', 'kept')])), [
    'baseline b.txt: todo gone: stale entry: no finding matches it any more; delete the line',
    'baseline b.txt: prefix:old/: stale entry: no finding starts with it any more; delete the line',
    'baseline b.txt: todo kept: listed twice; delete one',
    'baseline b.txt: prefix:old/: listed twice; delete one',
  ]);
});

test('a missing file waives nothing', () => {
  const b = Baseline.load(path.join(project({}), 'lint-baseline.txt'), 'lint-baseline.txt');
  assert.deepEqual(b.apply([f('todo', 'a')]), [f('todo', 'a')]);
});

test('per rule: a line belongs to the rule of its kind, prefixes and unknown kinds to the whole run', () => {
  const b = Baseline.parse('b.txt', 'todo a\njava-line b\nprefix:p\nnope c\n');
  assert.deepEqual(b.waive([f('todo', 'x')]), [f('todo', 'x')]);
  assert.deepEqual(subjects(b.leftovers(Baseline.forKinds(['todo']))), [
    'baseline b.txt: todo a: stale entry: no finding matches it any more; delete the line',
  ]);
  assert.deepEqual(subjects(b.leftovers(Baseline.outsideKinds(['todo', 'java-line']))), [
    'baseline b.txt: nope c: stale entry: no finding matches it any more; delete the line',
    'baseline b.txt: prefix:p: stale entry: no finding starts with it any more; delete the line',
  ]);
});

test('through the runner: a stale line fails its rule; an unknown kind and a stale prefix fail the whole run', () => {
  const noTodo = part(require('../../scripts/lint/rules/no_todo'), 'other');
  const root = project({
    ...CLAUDE_MD,
    'a.md': 'text\n',
    'lint-baseline.txt': 'todo a.md:9\nprefix:nowhere/\nno-such-kind x\n',
  });
  let r = lintRunner(root, ['check-no-todo'], { rules: [noTodo] });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lint-baseline.txt: todo a.md:9: stale entry: no finding matches it any more; delete the line',
  ]);
  r = lintRunner(root, [], { rules: [noTodo] });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lint-baseline.txt: todo a.md:9: stale entry: no finding matches it any more; delete the line',
    'lint-baseline.txt: no-such-kind x: stale entry: no finding matches it any more; delete the line',
    'lint-baseline.txt: prefix:nowhere/: stale entry: no finding starts with it any more; delete the line',
  ]);
  assert.ok(r.err.includes('❌ lint-baseline.txt: 2 finding(s).'), r.text);
  assert.ok(r.err.includes('Delete the line, or name a finding a rule reports (`<kind> <subject>`).'), r.text);
  // --no-baseline reads no line at all.
  assert.equal(lintRunner(root, ['check-no-todo', '--no-baseline'], { rules: [noTodo] }).status, 0);
});

test('through the runner: a prefix line waives findings', () => {
  const noTodo = part(require('../../scripts/lint/rules/no_todo'), 'other');
  const root = project({ ...CLAUDE_MD, 'docs/a.md': `${TODO_MARK}\n`, 'lint-baseline.txt': 'prefix:docs/\n' });
  const r = lintRunner(root, [], { rules: [noTodo] });
  assert.equal(r.status, 0, r.text);
});

test('through ESLint: a stale line fails its rule; an unknown kind and a stale prefix fail kafka/baseline', () => {
  const noTodo = require('../../scripts/lint/rules/no_todo');
  const root = project({
    ...CLAUDE_MD,
    'src/a.ts': 'export {};\n',
    'eslint-baseline.txt': 'todo src/a.ts:9\nprefix:nowhere/\nno-such-kind x\n',
  });
  let r = lint(root, ['check-no-todo'], { set: [noTodo] });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'eslint-baseline.txt: todo src/a.ts:9: stale entry: no finding matches it any more; delete the line',
  ]);
  r = lint(root, [], { set: [noTodo] });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'eslint-baseline.txt: todo src/a.ts:9: stale entry: no finding matches it any more; delete the line',
    'eslint-baseline.txt: no-such-kind x: stale entry: no finding matches it any more; delete the line',
    'eslint-baseline.txt: prefix:nowhere/: stale entry: no finding starts with it any more; delete the line',
  ]);
  assert.ok(r.err.includes('❌ eslint-baseline.txt: 2 finding(s).'), r.text);
  assert.ok(r.err.includes('Delete the line, or name a finding a rule reports (`<kind> <subject>`).'), r.text);
  assert.equal(lint(root, ['check-no-todo', '--no-baseline'], { set: [noTodo] }).status, 0);
});

test('through ESLint: a prefix line waives findings', () => {
  const noTodo = require('../../scripts/lint/rules/no_todo');
  const root = project({ ...CLAUDE_MD, 'src/a.ts': `// ${TODO_MARK}\n`, 'eslint-baseline.txt': 'prefix:src/\n' });
  const r = lint(root, [], { set: [noTodo] });
  assert.equal(r.status, 0, r.text);
});
