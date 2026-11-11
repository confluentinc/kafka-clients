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

// The JavaScript side reads Java only from the committed java-index.json,
// which `cargo xtask java-api` writes from the kafka submodule and the Rust
// jobs check (`cargo xtask check-generated`): no file of the client names the
// submodule as a path, or runs an xtask (`xtask java-api` reads the
// submodule). Comments are not read; every string a program holds is.
//
// - In a JavaScript or TypeScript file, every string literal and template
//   part is read through the TypeScript parser. One holding a path segment
//   `kafka` (`'kafka'`, `'../kafka'`, `'kafka/clients'`, `'a/kafka/b'`) is a
//   finding: a mention of an ESLint rule (`kafka/<rule>`, a rule of the
//   plugin) is no segment. One naming `xtask` is a finding, except in the
//   instruction a linter message gives (run `cargo xtask java-api` from
//   rust/). The few strings left, ALLOWED lists, each with its file.
// - In a Makefile, a shell, Python or gyp file or a Dockerfile, and in
//   package.json's scripts, every line outside a `#` comment is read whole:
//   a path segment `kafka`, a quoted `kafka`, or the word `xtask` is a
//   finding.
//
// `kafka-javascript`, `@confluentinc/kafka-javascript` and the like are no
// path segment, so need no entry. This file is not read: its fixtures are
// the dodges it tests.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const ROOT = path.join(__dirname, '..', '..');
const SELF = path.relative(ROOT, __filename).split(path.sep).join('/');

/** The directories not read: dependencies and outputs. */
const SKIPPED = new Set(['node_modules', 'build', 'dist', '.git', '.claude']);

/** The JavaScript and TypeScript files, read through the parser. */
const SCRIPT = /\.(js|cjs|mjs|ts|cts|mts)$/;

/** The other files a program runs from, read line by line. */
const OTHER = /(\.(sh|py|gyp|gypi)|^Makefile|^Dockerfile[^/]*)$/;

/** The rules of the ESLint plugin, whose ids are `kafka/<rule>`. */
const RULES = Object.keys(require('../../scripts/lint/eslint/plugin').rules);

/**
 * A mention of an ESLint rule, which is no path: `kafka/` and a rule of the
 * plugin or a placeholder the linter's texts and tests use (`kafka/<rule>`,
 * `kafka/x`), JSON-escaped or not, or the prefix `kafka/` in prose, followed
 * by a space or a backtick (`a kafka/ rule`).
 */
const RULE_MENTION = new RegExp(
  `kafka\\\\?/(?:(?:${[...RULES, '<rule>', '<name>', 'x', 'y'].join('|')})(?![\\w-])|(?=[\\s\`]))`,
  'g',
);

/**
 * A path segment `kafka`: next to a `/` or `\\` (`../kafka`, `kafka/clients`,
 * `a/kafka/b`), or a whole string (`'kafka'`, as `path.join(REPO, 'kafka')`
 * takes it). `kafka-javascript`, `kafka producer`, a host (`//kafka.apache.org`)
 * and an image (`apache/kafka:4.3.1`) are no segment.
 */
const KAFKA_SEGMENT = /[/\\]kafka(?![\w.:-])|(?<![\w.-])kafka[/\\]|^kafka$/;

/** In a line of a Makefile or a script, also a quoted word `kafka`. */
const KAFKA_LINE = new RegExp(`${KAFKA_SEGMENT.source.replace('^kafka$', '[\'"]kafka[\'"]')}`);

/** Whether `text` names kafka as a path, its rule mentions aside. */
const namesKafka = (text, pattern = KAFKA_SEGMENT) => pattern.test(text.replace(RULE_MENTION, ''));

/** The word `xtask`. */
const XTASK = /\bxtask\b/;

/**
 * The instruction a linter message gives, which names an xtask without
 * running it: `` (re)run `cargo xtask java-api` from rust/ ``.
 */
const XTASK_INSTRUCTION = /`cargo xtask java-api` from rust\//g;

/** Whether `text` runs an xtask, the instruction aside. */
const runsXtask = (text) => XTASK.test(text.replace(XTASK_INSTRUCTION, ''));

/**
 * The strings that name `kafka` otherwise and are not a path, each with the
 * file it is in: the ESLint plugin's name and the prefix of its rule ids,
 * whole or as the head of a template building one (`kafka/${name}`).
 */
const ALLOWED = [
  ['scripts/lint/eslint/plugin.js', 'kafka', "the plugin's name"],
  ['scripts/lint/eslint/formatter.js', 'kafka/', 'the prefix of a rule id'],
  ['scripts/lint/rules/directives.js', 'kafka/', 'the prefix of a rule id'],
  ['test/lint/runner.test.js', 'kafka/', 'the prefix of a rule id'],
  ['scripts/lint/eslint/config.js', 'kafka/', 'a rule id built from the rule name'],
  ['test/lint/_project.js', 'kafka/', 'a rule id built from the rule name'],
  ['scripts/lint/java_index.js', 'cargo xtask java-api', 'the command a message tells the user to run'],
];

/** Every file under `dir`, relative to the client's directory. */
function files(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!SKIPPED.has(entry.name)) out.push(...files(full));
    } else {
      out.push(path.relative(ROOT, full).split(path.sep).join('/'));
    }
  }
  return out;
}

/** Every string literal and template part of a script, outside comments. */
function scriptStrings(rel, text) {
  const kind = /\.[cm]?ts$/.test(rel) ? ts.ScriptKind.TS : ts.ScriptKind.JS;
  const source = ts.createSourceFile(rel, text, ts.ScriptTarget.Latest, true, kind);
  const out = [];
  const visit = (node) => {
    if (
      ts.isStringLiteral(node) ||
      ts.isNoSubstitutionTemplateLiteral(node) ||
      node.kind === ts.SyntaxKind.TemplateHead ||
      node.kind === ts.SyntaxKind.TemplateMiddle ||
      node.kind === ts.SyntaxKind.TemplateTail
    ) {
      out.push({ text: node.text, line: source.getLineAndCharacterOfPosition(node.getStart()).line + 1 });
    }
    ts.forEachChild(node, visit);
  };
  // A template's head, middles and tail are not children forEachChild walks
  // as nodes of their own kind in every version: read the spans too.
  const visitTemplate = (node) => {
    if (ts.isTemplateExpression(node)) {
      out.push({ text: node.head.text, line: source.getLineAndCharacterOfPosition(node.head.getStart()).line + 1 });
      for (const span of node.templateSpans) {
        out.push({ text: span.literal.text, line: source.getLineAndCharacterOfPosition(span.literal.getStart()).line + 1 });
      }
    }
    ts.forEachChild(node, visitTemplate);
  };
  visit(source);
  visitTemplate(source);
  // A part read twice (by both walks) is one string.
  const seen = new Set();
  return out.filter((s) => {
    const key = `${s.line}\0${s.text}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** The lines of a Makefile, script or Dockerfile, without their `#` comments. */
function codeLines(text) {
  return text.split('\n').map((l, i) => ({ text: l.replace(/(^|\s)#.*$/, ''), line: i + 1 }));
}

/** The findings of one file: `<file>:<line>: <what>`. */
function findings(rel, text) {
  const base = rel.split('/').pop();
  const out = [];
  const allowed = (s) => ALLOWED.some(([file, literal]) => file === rel && literal === s);
  if (SCRIPT.test(base)) {
    for (const s of scriptStrings(rel, text)) {
      if (allowed(s.text)) continue;
      if (namesKafka(s.text)) out.push(`${rel}:${s.line}: names kafka as a path: ${JSON.stringify(s.text)}`);
      if (runsXtask(s.text)) out.push(`${rel}:${s.line}: runs an xtask: ${JSON.stringify(s.text)}`);
    }
  } else if (OTHER.test(base) || base === 'package.json') {
    const lines = base === 'package.json'
      ? Object.values(JSON.parse(text).scripts || {}).map((s) => ({ text: s, line: 0 }))
      : codeLines(text);
    for (const l of lines) {
      if (namesKafka(l.text, KAFKA_LINE)) out.push(`${rel}:${l.line}: names kafka as a path: ${l.text.trim()}`);
      if (XTASK.test(l.text)) out.push(`${rel}:${l.line}: runs an xtask: ${l.text.trim()}`);
    }
  }
  return out;
}

test('no file of the client names the kafka submodule or runs an xtask: Java comes from java-index.json', () => {
  const all = files(ROOT).filter((rel) => rel !== SELF);
  const read = all.filter((rel) => SCRIPT.test(rel.split('/').pop()) || OTHER.test(rel.split('/').pop()));
  assert.ok(read.length > 100, `${read.length} files read`);
  const found = [];
  for (const rel of all) found.push(...findings(rel, fs.readFileSync(path.join(ROOT, rel), 'utf8')));
  assert.deepEqual(found, []);
});

test('every ALLOWED entry waives a string its file holds', () => {
  for (const [file, literal, reason] of ALLOWED) {
    assert.ok(reason, `${file}: ${literal} has no reason`);
    const strings = scriptStrings(file, fs.readFileSync(path.join(ROOT, file), 'utf8')).map((s) => s.text);
    assert.ok(strings.includes(literal), `${file} holds no string ${JSON.stringify(literal)}: drop the entry`);
  }
});

test('the dodges are found: a kafka path built from parts, a relative one, an xtask in a Makefile', () => {
  const script = [
    "const path = require('node:path');",
    "const fs = require('node:fs');",
    "// A comment may say kafka/clients/src and cargo xtask java-api.",
    "const KAFKA = path.join(REPO, 'kafka');",
    "fs.readFileSync(path.join(KAFKA, 'gradle.properties'), 'utf8');",
    "const up = path.resolve(REPO, '../kafka');",
    "const src = `${REPO}/kafka/clients/src`;",
    "spawnSync('cargo', ['xtask', 'java-api']);",
    "const ok = ['kafka/java-name', 'kafka/baseline: stale', '@confluentinc/kafka-javascript', 'confluent-kafka-javascript.node'];",
  ].join('\n');
  assert.deepEqual(findings('scripts/new.js', script), [
    'scripts/new.js:4: names kafka as a path: "kafka"',
    'scripts/new.js:6: names kafka as a path: "../kafka"',
    'scripts/new.js:7: names kafka as a path: "/kafka/clients/src"',
    'scripts/new.js:8: runs an xtask: "xtask"',
  ]);
  // The plugin's name is allowed in its own file only, and a rule id only
  // for a rule the plugin has.
  assert.deepEqual(findings('scripts/lint/eslint/plugin.js', "module.exports = { name: 'kafka' };"), []);
  assert.deepEqual(findings('scripts/other.js', "const p = 'kafka/clients';"), [
    'scripts/other.js:1: names kafka as a path: "kafka/clients"',
  ]);
  assert.deepEqual(findings('test/x.test.ts', "const t: string = 'kafka/clients/src/main/java';"), [
    'test/x.test.ts:1: names kafka as a path: "kafka/clients/src/main/java"',
  ]);
  const makefile = [
    '# cargo xtask java-api writes java-index.json from ../kafka',
    'java-index:',
    '\tcd ../rust && cargo xtask java-api',
    'sources:',
    '\tls ../kafka/clients/src # list them',
    'package:',
    '\tnpm pack @confluentinc/kafka-javascript',
  ].join('\n');
  assert.deepEqual(findings('Makefile', makefile), [
    'Makefile:3: runs an xtask: cd ../rust && cargo xtask java-api',
    'Makefile:5: names kafka as a path: ls ../kafka/clients/src',
  ]);
  assert.deepEqual(findings('package.json', JSON.stringify({ scripts: { index: 'cd ../rust && cargo xtask java-api' } })), [
    'package.json:0: runs an xtask: cd ../rust && cargo xtask java-api',
  ]);
});

test('the committed java-index.json is the schema the readers load', () => {
  const { SCHEMA, INDEX_PATH, loadJavaIndex } = require('../../scripts/lint/java_index');
  assert.equal(INDEX_PATH, path.join(ROOT, 'java-index.json'));
  const index = loadJavaIndex();
  assert.equal(index.schema, SCHEMA);
  assert.ok(index.classes.length > 1000, `${index.classes.length} classes`);
  // One class per line, so a regeneration diffs readably.
  const lines = fs.readFileSync(INDEX_PATH, 'utf8').trimEnd().split('\n');
  assert.equal(lines.length, index.classes.length + 2);
});
