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

// The rule linter's runner (scripts/lint_rules.js), its ESLint plugin
// (scripts/lint/eslint/) and their context (scripts/lint/context.js):
// arguments, the report, the inputs a rule needs, the split between the two
// tools, and the real tree.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { RULES } = require('../../scripts/lint_rules');
const { ESLINT_CHECKS } = require('../../scripts/lint/checks');
const plugin = require('../../scripts/lint/eslint/plugin');
const { Context } = require('../../scripts/lint/context');
const { project, lint, lintRunner } = require('./_project');

const ROOT = path.join(__dirname, '..', '..');

/** Checks the shape of `rules` and returns their names. */
function shape(rules) {
  const names = [];
  for (const rule of rules) {
    assert.match(rule.name, /^check-[a-z-]+$/);
    assert.ok(!names.includes(rule.name), rule.name);
    names.push(rule.name);
    assert.ok(Array.isArray(rule.kinds) && rule.kinds.length > 0, rule.name);
    assert.ok(Array.isArray(rule.needs), rule.name);
    assert.equal(typeof rule.hint, 'string');
    assert.equal(typeof rule.check, 'function');
  }
  // No two rules of one tool share a finding kind, so a baseline line belongs to one rule.
  const kinds = rules.flatMap((r) => r.kinds);
  assert.equal(new Set(kinds).size, kinds.length);
  return names;
}

test('every rule has a name, kinds, needs, a hint and a check; each tool runs its half', () => {
  assert.deepEqual(shape(RULES), [
    'check-no-todo',
    'check-exception-word',
    'check-java-line',
    'check-out-err',
    'check-cb-twin-only',
    'check-notify-hook',
    'check-single-drainer',
    'check-completion-order',
    'check-ownership',
    'check-zero-copy',
    'check-string-encoding',
    'check-no-binding-queue',
    'check-no-closed-flag',
    'check-no-repeated-precondition',
    'check-tsfn-lifetime',
    'check-default-helpers',
    'check-mock-native',
    'check-encode-callers',
    'check-commonjs-pin',
  ]);
  assert.deepEqual(shape(ESLINT_CHECKS), [
    'check-no-todo',
    'check-exception-word',
    'check-internal-members',
    'check-java-line',
    'check-java-name',
    'check-java-members',
    'check-java-completeness',
    'check-public-audience',
    'check-no-deprecated-translation',
    'check-data-objects',
    'check-invented-names',
    'check-module-exports',
    'check-overload-shape',
    'check-type-mapping',
    'check-waiting-call-promise',
    'check-native-registry',
    'check-directives',
    'check-config',
  ]);
  // The three rules split by file type: each half reads its own files.
  const half = (list, name) => list.find((r) => r.name === name).PART;
  assert.deepEqual(
    ['check-no-todo', 'check-exception-word', 'check-java-line'].map((n) => [half(ESLINT_CHECKS, n), half(RULES, n)]),
    [['code', 'other'], ['ts', 'c'], ['ts', 'c']],
  );
  // Only the rules that read TypeScript are ESLint's; the runner keeps the addon's (and no TypeScript-only rule).
  const tsOnly = ESLINT_CHECKS.filter((c) => !Object.prototype.hasOwnProperty.call(c, 'PART')).map((c) => c.name);
  assert.ok(tsOnly.every((n) => !RULES.some((r) => r.name === n)));
});

test('the ESLint plugin: one `kafka/` rule per ESLint check, named after it, plus kafka/baseline', () => {
  assert.deepEqual(Object.keys(plugin.rules), [...ESLINT_CHECKS.map((c) => c.name.replace(/^check-/, '')), 'baseline']);
  for (const check of ESLINT_CHECKS) {
    const rule = plugin.rules[check.name.replace(/^check-/, '')];
    assert.equal(rule.meta.docs.description, check.hint);
    for (const kind of [...check.kinds, 'cannot-run', 'nothing-checked', 'baseline']) assert.equal(rule.meta.messages[kind], '{{text}}');
  }
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
  const r = lintRunner(project({}), [], { rules: [boom, ok] });
  assert.equal(r.status, 1);
  assert.deepEqual(r.out, ["🔍 Running the Node.js client's rule lint...", '✅ check-ok: 3 item(s) checked']);
  assert.deepEqual(r.err, ['', '❌ check-boom: cannot run, bad input', '', '❌ 1 rule-lint finding(s)']);
});

test('a rule that reports no positive item count fails', () => {
  const silent = { name: 'check-silent', kinds: ['s'], needs: [], hint: '', check: () => ({ findings: [] }) };
  const broken = { name: 'check-broken', kinds: ['b'], needs: [], hint: '', check: () => ({ checked: 1 }) };
  const r = lintRunner(project({}), [], { rules: [silent, broken] });
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
  const root = project({ 'src/a.ts': 'export class A { _x = 1; }\n', 'eslint-baseline.txt': 'prefix:src/a.ts:A.\nother x\n' });
  const r = lint(root, [], { set: [require('../../scripts/lint/rules/internal_members')] });
  assert.equal(r.status, 1);
  assert.deepEqual(r.err, ['', '❌ check-internal-members: cannot run, no tsconfig.json', '', '❌ 1 rule-lint finding(s)']);
});

test('the inputs a later rule needs: a missing Java index or header is a reason, never a skip', () => {
  const root = project({});
  const ctx = new Context({ root, headerPath: path.join(root, 'none.h') });
  assert.equal(ctx.missing('java'), 'no Java index at java-index.json (run `cargo xtask java-api` from rust/)');
  assert.equal(
    ctx.missing('header'),
    `no bindings header at ${path.join(root, 'none.h')} (build the Rust library first: \`make build-javascript\` from the repository root)`,
  );
  assert.equal(ctx.missing('src'), 'no TypeScript sources under src/');
  assert.equal(ctx.missing('native'), 'no C sources under native/');
  assert.throws(() => ctx.missing('nothing'), /unknown lint input `nothing`/);
  const needy = { name: 'check-needy', kinds: ['n'], needs: ['java', 'header'], hint: '', check: () => ({ findings: [], checked: 1 }) };
  const r = lintRunner(root, [], { rules: [needy], headerPath: path.join(root, 'none.h') });
  assert.equal(r.status, 1);
  assert.equal(
    r.err[1],
    `❌ check-needy: cannot run, no Java index at java-index.json (run \`cargo xtask java-api\` from rust/); ` +
      `no bindings header at ${path.join(root, 'none.h')} (build the Rust library first: \`make build-javascript\` from the repository root)`,
  );
});

test('a present Java index and header load once', () => {
  const root = project({ 'java-index.json': '{"schema":2,"classes":[{"package":"x","path":["X"]}]}', 'h.h': 'void kafka_x(void);\n' });
  const ctx = new Context({ root, headerPath: path.join(root, 'h.h') });
  assert.equal(ctx.missing('java'), null);
  assert.equal(ctx.missing('header'), null);
  assert.equal(ctx.javaIndex(), ctx.javaIndex());
  assert.equal(ctx.javaIndex().schema, 2);
  // The loader puts back what the file leaves out at its default.
  assert.deepEqual(ctx.javaIndex().classes[0], {
    package: 'x',
    path: ['X'],
    kind: 'class',
    visibility: 'public',
    abstract: false,
    deprecated: false,
    public_audience: false,
    unsupported_package: false,
    supertypes: [],
    supertype_fqns: [],
    overloads: [],
    fields: [],
    enum_constants: [],
    fqn: 'org.apache.kafka.x.X',
  });
  assert.equal(ctx.header(), 'void kafka_x(void);\n');
  const bad = project({ 'java-index.json': '{' });
  assert.match(new Context({ root: bad }).missing('java'), /^cannot read the Java index java-index\.json: /);
  const empty = project({ 'java-index.json': '{"schema":2,"classes":[]}' });
  assert.equal(new Context({ root: empty }).missing('java'), 'the Java index java-index.json lists no class');
});

test('the TypeScript program is built only for a rule that needs it', () => {
  const ctx = new Context({ root: ROOT });
  ctx.files(['src'], ['.ts']);
  ctx.typescript();
  assert.ok(!ctx.cache.has('program'));
  assert.ok(ctx.program().sourceFiles.length > 0);
  assert.ok(ctx.cache.has('program'));
});

test('the real tree passes with the checked-in baselines: the runner, and ESLint with its typed configuration', () => {
  let r = spawnSync(process.execPath, [path.join(ROOT, 'scripts', 'lint_rules.js')], { cwd: ROOT, encoding: 'utf8' });
  assert.equal(r.status, 0, `${r.stdout}${r.stderr}`);
  for (const rule of RULES) assert.match(r.stdout, new RegExp(`✅ ${rule.name}: \\d+ item\\(s\\) checked`));
  r = spawnSync(process.execPath, [path.join(ROOT, 'node_modules', '.bin', 'eslint'), '.'], { cwd: ROOT, encoding: 'utf8' });
  assert.equal(r.status, 0, `${r.stdout}${r.stderr}`);
});

test('the real tree without baselines: exactly the findings the baselines list, all ESLint\'s', () => {
  const r = spawnSync(process.execPath, [path.join(ROOT, 'node_modules', '.bin', 'eslint'), '.', '-f', 'json'], {
    cwd: ROOT,
    encoding: 'utf8',
    env: { ...process.env, KAFKA_LINT_NO_BASELINE: '1' },
    maxBuffer: 64 * 1024 * 1024,
  });
  const keys = JSON.parse(r.stdout)
    .flatMap((f) => f.messages)
    .filter((m) => m.ruleId && m.ruleId.startsWith('kafka/'))
    .map((m) => `${m.messageId} ${m.message.slice(0, m.message.indexOf(': '))}`)
    .sort();
  const listed = (file) =>
    fs
      .readFileSync(path.join(ROOT, file), 'utf8')
      .split('\n')
      .map((l) => l.trim())
      .filter((l) => l !== '' && !l.startsWith('#'));
  assert.deepEqual(keys, listed('eslint-baseline.txt').sort());
  assert.deepEqual(listed('lint-baseline.txt'), []);
  const runner = spawnSync(process.execPath, [path.join(ROOT, 'scripts', 'lint_rules.js'), '--no-baseline'], { cwd: ROOT, encoding: 'utf8' });
  assert.equal(runner.status, 0, `${runner.stdout}${runner.stderr}`);
});
