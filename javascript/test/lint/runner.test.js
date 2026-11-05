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

// The rule linter's runner (scripts/lint_rules.js) and its context
// (scripts/lint/context.js): arguments, the report, the inputs a rule needs,
// and the real tree.

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { RULES } = require('../../scripts/lint_rules');
const { Context } = require('../../scripts/lint/context');
const { project, lint } = require('./_project');

const ROOT = path.join(__dirname, '..', '..');

test('every rule has a name, kinds, needs, a hint and a check', () => {
  const names = new Set();
  for (const rule of RULES) {
    assert.match(rule.name, /^check-[a-z-]+$/);
    assert.ok(!names.has(rule.name), rule.name);
    names.add(rule.name);
    assert.ok(Array.isArray(rule.kinds) && rule.kinds.length > 0, rule.name);
    assert.ok(Array.isArray(rule.needs), rule.name);
    assert.equal(typeof rule.hint, 'string');
    assert.equal(typeof rule.check, 'function');
  }
  assert.deepEqual([...names], ['check-no-todo', 'check-exception-word', 'check-private-members', 'check-java-line']);
  // No two rules share a finding kind, so a baseline line belongs to one rule.
  const kinds = RULES.flatMap((r) => r.kinds);
  assert.equal(new Set(kinds).size, kinds.length);
});

test('a usage error exits 2', () => {
  let r = lint(project({}), ['--bogus']);
  assert.equal(r.status, 2);
  assert.deepEqual(r.err, [
    'unknown lint_rules argument `--bogus`; usage: node scripts/lint_rules.js [check-<name>] [--no-baseline]',
  ]);
  r = lint(project({}), ['check-nothing']);
  assert.equal(r.status, 2);
  assert.deepEqual(r.err, ['no rule named `check-nothing`']);
});

test('a rule that throws fails with the reason, and the others still run', () => {
  const boom = { name: 'check-boom', kinds: ['boom'], needs: [], hint: '', check: () => { throw new Error('bad input'); } };
  const ok = { name: 'check-ok', kinds: ['ok'], needs: [], hint: '', check: () => ({ findings: [], checked: 3 }) };
  const r = lint(project({}), [], { rules: [boom, ok] });
  assert.equal(r.status, 1);
  assert.deepEqual(r.out, ["🔍 Running the Node.js client's rule lint...", '✅ check-ok: 3 item(s) checked']);
  assert.deepEqual(r.err, ['', '❌ check-boom: cannot run, bad input', '', '❌ 1 rule-lint finding(s)']);
});

test('a rule that reports no positive item count fails', () => {
  const silent = { name: 'check-silent', kinds: ['s'], needs: [], hint: '', check: () => ({ findings: [] }) };
  const broken = { name: 'check-broken', kinds: ['b'], needs: [], hint: '', check: () => ({ checked: 1 }) };
  const r = lint(project({}), [], { rules: [silent, broken] });
  assert.equal(r.status, 1);
  assert.deepEqual(r.err, [
    '',
    '❌ check-silent: no item count (checked: undefined); a rule that checks nothing cannot pass',
    '',
    '❌ check-broken: cannot run, its check returned no findings list',
    '',
    '❌ 2 rule-lint finding(s)',
  ]);
});

test('a rule that cannot run leaves the prefix and unknown-kind lines unjudged', () => {
  const root = project({ 'lib/a.ts': 'export class A { _x = 1; }\n', 'lint-baseline.txt': 'prefix:lib/a.ts:A.\nother x\n' });
  const r = lint(root, [], { rules: [require('../../scripts/lint/rules/private_members')] });
  assert.equal(r.status, 1);
  assert.deepEqual(r.err, ['', '❌ check-private-members: cannot run, no tsconfig.json', '', '❌ 1 rule-lint finding(s)']);
});

test('the inputs a later rule needs: a missing Java index or header is a reason, never a skip', () => {
  const root = project({});
  const ctx = new Context({ root, headerPath: path.join(root, 'none.h') });
  assert.equal(ctx.missing('java'), 'no Java index at build/java-index.json (run `cargo xtask java-index --json` from rust/)');
  assert.equal(
    ctx.missing('header'),
    `no bindings header at ${path.join(root, 'none.h')} (build the Rust library first: \`make build-javascript\` from the repository root)`,
  );
  assert.equal(ctx.missing('lib'), 'no TypeScript sources under lib/');
  assert.equal(ctx.missing('src'), 'no C sources under src/');
  assert.throws(() => ctx.missing('nothing'), /unknown lint input `nothing`/);
  const needy = { name: 'check-needy', kinds: ['n'], needs: ['java', 'header'], hint: '', check: () => ({ findings: [], checked: 1 }) };
  const r = lint(root, [], { rules: [needy], headerPath: path.join(root, 'none.h') });
  assert.equal(r.status, 1);
  assert.equal(
    r.err[1],
    `❌ check-needy: cannot run, no Java index at build/java-index.json (run \`cargo xtask java-index --json\` from rust/); ` +
      `no bindings header at ${path.join(root, 'none.h')} (build the Rust library first: \`make build-javascript\` from the repository root)`,
  );
});

test('a present Java index and header load once', () => {
  const root = project({ 'build/java-index.json': '{"schema":1,"classes":[]}', 'h.h': 'void kafka_x(void);\n' });
  const ctx = new Context({ root, headerPath: path.join(root, 'h.h') });
  assert.equal(ctx.missing('java'), null);
  assert.equal(ctx.missing('header'), null);
  assert.equal(ctx.javaIndex(), ctx.javaIndex());
  assert.equal(ctx.javaIndex().schema, 1);
  assert.equal(ctx.header(), 'void kafka_x(void);\n');
  const bad = project({ 'build/java-index.json': '{' });
  assert.match(new Context({ root: bad }).missing('java'), /^cannot read the Java index build\/java-index\.json: /);
});

test('the TypeScript program is built only for a rule that needs it', () => {
  const ctx = new Context({ root: ROOT });
  ctx.files(['lib'], ['.ts']);
  ctx.typescript();
  assert.ok(!ctx.cache.has('program'));
  assert.ok(ctx.program().sourceFiles.length > 0);
  assert.ok(ctx.cache.has('program'));
});

test('the real tree passes with the checked-in baseline', () => {
  const r = spawnSync(process.execPath, [path.join(ROOT, 'scripts', 'lint_rules.js')], { cwd: ROOT, encoding: 'utf8' });
  assert.equal(r.status, 0, `${r.stdout}${r.stderr}`);
  for (const rule of RULES) assert.match(r.stdout, new RegExp(`✅ ${rule.name}: \\d+ item\\(s\\) checked`));
});
