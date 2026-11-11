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

// Where the ESLint rules report (scripts/lint/eslint/analysis.js): a finding
// on the file and line it concerns, and a finding about the project (a rule
// that cannot run or checks nothing, a stale eslint-baseline.txt line, an
// unused allowlist entry) on every file the rule visits; and kafka/directives.

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const { Project } = require('../../scripts/lint/eslint/analysis');

const { TSCONFIG, project, eslintMessages } = require('./_project');
const { CLASSES, LIB, javaIndex } = require('./_java_fixture');
const { FILES, ADDON } = require('./_native_fixture');

const INDEX = javaIndex(CLASSES);

/** The messages of rule `id`: `<file>:<line> <messageId> <message>`. */
const of = (messages, id) => messages.filter((m) => m.ruleId === id).map((m) => `${m.rel}:${m.line} ${m.messageId} ${m.message}`);

test('a missing Java member is reported on the declaration of the class that should hold it', () => {
  const rec = LIB['src/producer/rec.ts'].replace('  get size(): number { return 0; }\n', '');
  const root = project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, ...LIB, 'src/producer/rec.ts': rec });
  assert.deepEqual(of(eslintMessages(root), 'kafka/java-completeness'), [
    'src/producer/rec.ts:1 missing clients.producer.Rec#getSize: Rec does not translate it',
  ]);
});

test('a member finding is reported on the member', () => {
  const rec = LIB['src/producer/rec.ts'].replace('  flush(): void {}\n', '  flush(): void {}\n  bogus(): void {}\n');
  const root = project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, ...LIB, 'src/producer/rec.ts': rec });
  const lines = rec.split('\n');
  const line = lines.findIndex((l) => l.includes('bogus()')) + 1;
  const found = of(eslintMessages(root), 'kafka/java-members');
  assert.equal(found.length, 1, found.join('\n'));
  assert.ok(found[0].startsWith(`src/producer/rec.ts:${line} no-java-member Rec.bogus: `), found[0]);
});

test('a registration src/ never uses is reported on src/internal/native.ts, naming it', () => {
  const addon = ADDON.replace('{"setFactories"', '{"extra", NULL, js_extra, NULL, NULL, NULL, napi_default, NULL},\n        {"setFactories"');
  const root = project({ 'tsconfig.json': TSCONFIG, ...FILES, 'native/addon.c': addon });
  const found = of(eslintMessages(root), 'kafka/native-registry');
  assert.deepEqual(
    found.map((f) => f.replace(/:\d+ /, ' ')),
    ['src/internal/native.ts registered-unused native/addon.c:extra: registers extra (js_extra), which src/ never uses'],
  );
});

/** Every file ESLint lints in `files`' project, sorted: where a project finding is reported. */
const linted = (files) => Object.keys(files).filter((f) => /\.(ts|js|cjs|mjs)$/.test(f)).sort();

test('a rule that cannot run is reported on every file it visits, line 1', () => {
  const files = { 'tsconfig.json': TSCONFIG, ...LIB };
  const text = 'cannot-run check-java-name: cannot run, no Java index at build/java-index.json (run `cargo xtask java-index --json` from rust/)';
  assert.deepEqual(of(eslintMessages(project(files)), 'kafka/java-name'), linted(files).map((f) => `${f}:1 ${text}`));
});

test('a rule that checks nothing is reported on every file it visits', () => {
  const files = { 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, 'src/index.ts': 'export {};\n', 'src/b.ts': 'const b = 1;\nvoid b;\n' };
  assert.deepEqual(of(eslintMessages(project(files)), 'kafka/java-name'), [
    'src/b.ts:1 nothing-checked check-java-name: 0 item(s) checked; a rule that checks nothing cannot pass',
    'src/index.ts:1 nothing-checked check-java-name: 0 item(s) checked; a rule that checks nothing cannot pass',
  ]);
});

test('a stale baseline line is reported on every file by the rule owning its kind', () => {
  const files = {
    'tsconfig.json': TSCONFIG,
    'build/java-index.json': INDEX,
    ...LIB,
    'eslint-baseline.txt': 'missing clients.producer.Rec#gone\nno-such-kind x\n',
  };
  const messages = eslintMessages(project(files));
  const text = 'baseline eslint-baseline.txt: missing clients.producer.Rec#gone: stale entry: no finding matches it any more; delete the line';
  assert.deepEqual(of(messages, 'kafka/java-completeness'), linted(files).map((f) => `${f}:1 ${text}`));
  // A rule cannot run here (no C sources, no header): the lines no rule owns are not judged.
  assert.deepEqual(of(messages, 'kafka/baseline'), []);
});

test('an unknown kind is reported on every file by kafka/baseline, once every rule ran', () => {
  const files = {
    'tsconfig.json': TSCONFIG,
    'build/java-index.json': INDEX,
    ...LIB,
    ...FILES,
    'eslint-baseline.txt': 'no-such-kind x\nprefix:nowhere/\n',
  };
  const texts = [
    'baseline eslint-baseline.txt: no-such-kind x: stale entry: no finding matches it any more; delete the line',
    'baseline eslint-baseline.txt: prefix:nowhere/: stale entry: no finding starts with it any more; delete the line',
  ];
  assert.deepEqual(of(eslintMessages(project(files)), 'kafka/baseline'), linted(files).flatMap((f) => texts.map((t) => `${f}:1 ${t}`)));
});

test('an unused allowlist entry is located on the entry, in the rule module that lists it, and reported everywhere', () => {
  const root = project({ 'src/index.ts': 'export {};\n', 'scripts/rule.js': "module.exports = {\n  ALLOWED: [\n    'kept',\n    'gone',\n  ],\n};\n" });
  const check = require(path.join(root, 'scripts', 'rule.js'));
  const p = new Project(root, { checks: [check] });
  assert.deepEqual(p.locate(check, { subject: 'gone', unusedEntry: 'gone' }), { file: 'scripts/rule.js', line: 4, column: 0, global: true });
  // A module outside the directory (a test's copy of a rule) has no line of its own.
  assert.deepEqual(p.locate({ name: 'check-x' }, { subject: 'gone', unusedEntry: 'gone' }), { file: null, line: 1, column: 0, global: true });
});

test('kafka/directives: a disable names one rule, not a kafka/ one, with a reason; no inline configuration', () => {
  const files = { 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, ...LIB };
  const cases = [
    ['// eslint-disable-next-line no-console -- the reason', []],
    ['/* eslint-disable no-console -- a block, one rule, a reason */', []],
    ['// eslint-disable-next-line kafka/java-name -- later', ['disable-kafka eslint-disable-next-line names kafka/java-name: a kafka/ finding is waived only in eslint-baseline.txt']],
    ['/* eslint-disable */', ['disable-unnamed eslint-disable names no rule: name the one rule it disables', 'disable-no-reason eslint-disable gives no reason: add `-- <reason>`']],
    ['// eslint-disable-line no-console, no-debugger -- two', ['disable-several eslint-disable-line names 2 rules: disable one rule per directive']],
    ['// eslint-disable-next-line no-console', ['disable-no-reason eslint-disable-next-line gives no reason: add `-- <reason>`']],
    ['/* eslint kafka/java-name: "off" */', ['config-inline an inline configuration (eslint): rules are configured in eslint.config.cjs only']],
    ['/* eslint "no-undef": "off" */', ['config-inline an inline configuration (eslint): rules are configured in eslint.config.cjs only']],
    ['/* eslint-env node */', ['config-inline an inline configuration (eslint-env): rules are configured in eslint.config.cjs only']],
  ];
  for (const [comment, want] of cases) {
    const root = project({ ...files, 'src/x.ts': `${comment}\nconst x = 1;\nvoid x;\n` });
    const messages = eslintMessages(root).filter((m) => m.ruleId === 'kafka/directives');
    // Each finding is reported on every file: deduplicated here.
    const got = [...new Set(messages.map((m) => `${m.messageId} ${m.message}`))];
    assert.deepEqual(got, want.map((w) => w.replace(' ', ' src/x.ts:1: ')), comment);
  }
  // A `/* eslint-disable */` silences its own file, but not the other files, where the finding stands too.
  const root = project({ ...files, 'src/x.ts': '/* eslint-disable */\nconst x = 1;\nvoid x;\n' });
  const where = eslintMessages(root).filter((m) => m.ruleId === 'kafka/directives').map((m) => m.rel);
  assert.ok(!where.includes('src/x.ts'));
  assert.deepEqual([...new Set(where)], linted({ ...files, 'src/x.ts': '' }).filter((f) => f !== 'src/x.ts'));
});

test('kafka/directives reads every inline form ESLint accepts, as ESLint itself reads it', () => {
  const { Linter } = require('eslint');
  const { config } = require('../../scripts/lint/eslint/config');
  const { eslintReading } = require('../../scripts/lint/rules/directives');
  const files = { 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, ...LIB };
  const R = 'kafka/java-name';
  // [comment, the check's kind, whether ESLint itself reads a configuration or a kafka/ rule in it]
  const forms = [
    [`/* eslint ${R}: "off" */`, 'config-inline', true],
    [`/* eslint "${R}": "off" */`, 'config-inline', true],
    [`/* eslint '${R}': 0 */`, 'config-inline', true],
    [`/* eslint { "${R}": "off" } */`, 'config-inline', true],
    [`/*eslint ${R}:0*/`, 'config-inline', true],
    [`/* eslint   no-console : 0 ,  ${R}  :  "off"  */`, 'config-inline', true],
    // The Critic's escaped forms: ESLint decodes the JSON keys.
    ['/* eslint "kafka/directives": "off", "kafka/java-members": "off" */', 'config-inline', true],
    ['/* eslint "kafka\\/directives": "off", "kafka\\/java-members": "off" */', 'config-inline', true],
    ['/* eslint "no-undef": "off" */', 'config-inline', true],
    [`/* eslint-disable ${R} -- why */`, 'disable-kafka', true],
    [`/* eslint-disable "${R}" -- why */`, 'disable-kafka', true],
    [`/* eslint-enable ${R} */`, 'disable-kafka', true],
    [`// eslint-disable-line ${R} -- why`, 'disable-kafka', true],
    [`// eslint-disable-next-line ${R} -- why`, 'disable-kafka', true],
    [`/*\n * eslint-disable-next-line ${R} -- why\n */`, null, false],
    [`// eslint-disable ${R} -- a line comment is no directive for this label`, null, false],
    [`// eslint ${R}: 0`, null, false],
  ];
  for (const [comment, kind, read] of forms) {
    const root = project({ ...files, 'src/x.ts': `// header\n${comment}\nconst x = 1;\nvoid x;\n` });
    const got = [
      ...new Set(
        eslintMessages(root)
          .filter((m) => m.ruleId === 'kafka/directives')
          .map((m) => m.messageId),
      ),
    ];
    // Reported (from the other files at least: the comment may switch the rule off in its own), and no directive-unread.
    assert.deepEqual(got, kind === null ? [] : [kind], comment);
    // ESLint's own reading agrees.
    const linter = new Linter({ cwd: root });
    const file = path.join(root, 'src', 'x.ts');
    linter.verify(require('node:fs').readFileSync(file, 'utf8'), config(root, { typed: false }), file);
    assert.equal(eslintReading(linter.getSourceCode()).length > 0, read, comment);
  }
});

test('what ESLint reads but the check does not is directive-unread', () => {
  const directives = require('../../scripts/lint/rules/directives');
  const { Context } = require('../../scripts/lint/context');
  const root = project({
    'src/x.ts': '// header\n/* eslint "kafka\\/java-name": "off" */\n// eslint-disable-next-line kafka/java-name -- later\nconst x = 1;\n',
  });
  // With its own reading on, the check reports both comments, and ESLint's reading adds nothing.
  let found = directives.check(new Context({ root })).findings;
  assert.deepEqual(found.map((f) => `${f.subject} ${f.kind}`), ['src/x.ts:2 config-inline', 'src/x.ts:3 disable-kafka']);
  // With it off, ESLint's reading alone finds both, as directive-unread, reported everywhere and at 1:0.
  const blind = Object.assign(Object.create(directives), { OWN_READING: false });
  found = blind.check(new Context({ root })).findings;
  assert.deepEqual(
    found.map((f) => `${f.subject} ${f.kind} ${f.everywhere} ${f.alsoAtStart}: ${f.message}`),
    [
      'src/x.ts:2 directive-unread true true: ESLint reads an inline configuration in this comment, which the check did not: configure and waive in eslint.config.cjs and eslint-baseline.txt only',
      'src/x.ts:3 directive-unread true true: ESLint reads kafka/java-name in this comment, which the check did not: configure and waive in eslint.config.cjs and eslint-baseline.txt only',
    ],
  );
});

test('a directive finding is also reported at 1:0 of its own file, which a file-wide disable does not reach', () => {
  const files = { 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, ...LIB };
  const root = project({ ...files, 'src/x.ts': '// header\n/* eslint-disable */\nconst x = 1;\nvoid x;\n' });
  const own = eslintMessages(root).filter((m) => m.ruleId === 'kafka/directives' && m.rel === 'src/x.ts');
  // Its own line (2) is covered by the disable; 1:0, before it, is not.
  assert.deepEqual(own.map((m) => `${m.line} ${m.messageId}`).sort(), ['1 disable-no-reason', '1 disable-unnamed']);
});

test('kafka/config: an ESLint configuration besides eslint.config.cjs is reported on every file', () => {
  const files = {
    'tsconfig.json': TSCONFIG,
    'build/java-index.json': INDEX,
    ...LIB,
    'eslint.config.cjs': "'use strict';\n",
    'src/producer/eslint.config.cjs': 'module.exports = [];\n',
    'test/.eslintrc.json': '{}\n',
  };
  const texts = [
    'other-config src/producer/eslint.config.cjs: is an ESLint configuration besides eslint.config.cjs: the files below it would lose the kafka/ rules',
    'other-config test/.eslintrc.json: is an ESLint configuration besides eslint.config.cjs: the files below it would lose the kafka/ rules',
  ];
  const got = of(eslintMessages(project(files)), 'kafka/config').sort();
  assert.deepEqual(got, linted(files).flatMap((f) => texts.map((t) => `${f}:1 ${t}`)).sort());
});

test('the formatter groups a message the same rule reports on several files, and counts it once', () => {
  const format = require('../../scripts/lint/eslint/formatter');
  const m = (ruleId, message, line = 1) => ({ ruleId, message, line, column: 0, severity: 2 });
  const out = format(
    [
      { filePath: '/c/src/a.ts', messages: [m('kafka/x', 'check-x: cannot run, why'), m('kafka/y', 'src/a.ts:A: own', 3)] },
      { filePath: '/c/src/b.ts', messages: [m('kafka/x', 'check-x: cannot run, why')] },
    ],
    { cwd: '/c' },
  );
  assert.equal(
    out,
    [
      '',
      'src/a.ts',
      '  3:0  error  src/a.ts:A: own  kafka/y',
      '',
      'About the project (reported on every file the rule visits):',
      '  error  check-x: cannot run, why  kafka/x  (on 2 files)',
      '',
      '✖ 2 problems (2 errors, 0 warnings)',
      '',
    ].join('\n'),
  );
  assert.equal(format([{ filePath: '/c/a.ts', messages: [] }], { cwd: '/c' }), '');
});

test('the formatter groups only kafka/ messages: another rule\'s identical messages stay under their files', () => {
  const format = require('../../scripts/lint/eslint/formatter');
  const any = (line, column) => ({ ruleId: '@typescript-eslint/no-explicit-any', message: 'Unexpected any. Specify a different type.', line, column, severity: 2 });
  const out = format(
    [
      { filePath: '/c/src/internal/config.ts', messages: [any(182, 22)] },
      { filePath: '/c/src/internal/errors.ts', messages: [any(134, 22)] },
    ],
    { cwd: '/c' },
  );
  assert.equal(
    out,
    [
      '',
      'src/internal/config.ts',
      '  182:22  error  Unexpected any. Specify a different type.  @typescript-eslint/no-explicit-any',
      '',
      'src/internal/errors.ts',
      '  134:22  error  Unexpected any. Specify a different type.  @typescript-eslint/no-explicit-any',
      '',
      '✖ 2 problems (2 errors, 0 warnings)',
      '',
    ].join('\n'),
  );
});
