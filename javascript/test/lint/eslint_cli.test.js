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

// The ESLint half as users run it, with the client's typed configuration, on
// a copy of the real tree: the client directory is the configuration's
// whatever ESLint's working directory, a run of part of the client fails
// while a rule cannot run, and an edit is always seen.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { ESLint } = require('eslint');

const { project, copy, baselineCurrent, TODO_MARK } = require('./_project');

const ROOT = path.join(__dirname, '..', '..');
const BIN = path.join(ROOT, 'node_modules', '.bin', 'eslint');
const CONFIG = path.join(ROOT, 'scripts', 'lint', 'eslint', 'config.js');

/**
 * A directory holding `client/`, a copy of the client with its own
 * eslint.config.cjs, and `stray.js`, a file outside the client carrying a
 * marker the analysis must never read.
 */
function layout() {
  const parent = project({ 'stray.js': `// ${TODO_MARK} outside the client\n` });
  const root = path.join(parent, 'client');
  fs.mkdirSync(root);
  for (const rel of ['src', 'native', 'tsconfig.json', 'CLAUDE.md', 'eslint-baseline.txt', 'lint-baseline.txt', 'lint-mapping.json']) copy(ROOT, root, rel);
  fs.mkdirSync(path.join(root, 'test', 'unit'), { recursive: true });
  copy(ROOT, root, 'test/unit/internal/native.test.js');
  copy(ROOT, root, 'java-index.json');
  // The type declarations the typed configuration reads (@types/node); ignored by the linter itself.
  fs.symlinkSync(path.join(ROOT, 'node_modules'), path.join(root, 'node_modules'), 'dir');
  fs.writeFileSync(
    path.join(root, 'eslint.config.cjs'),
    `'use strict';\n\nmodule.exports = require(${JSON.stringify(CONFIG)}).config(__dirname);\n`,
  );
  // The real tree's findings now, baselined in the copy only: a test sees what it changes.
  baselineCurrent(root);
  return { parent: fs.realpathSync(parent), root: fs.realpathSync(root) };
}

/** `eslint <args>` from `cwd`: `{status, messages}`, each message `<file relative to root>:<line> <rule> <text>`. */
function eslint(cwd, root, args) {
  const r = spawnSync(process.execPath, [BIN, '-f', 'json', ...args], { cwd, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  assert.ok(r.stdout.startsWith('['), r.stderr);
  // ESLint reports real paths (a temporary directory's /var is /private/var).
  const real = fs.realpathSync(root);
  const messages = JSON.parse(r.stdout).flatMap((f) =>
    f.messages.map((m) => `${path.relative(real, f.filePath).split(path.sep).join('/')}:${m.line} ${m.ruleId} ${m.message}`),
  );
  return { status: r.status, messages: messages.sort() };
}

/** Adds a member no Java class has to RecordMetadata (a kafka/java-members finding); returns its line. */
function addMember(root) {
  const file = path.join(root, 'src', 'producer', 'record_metadata.ts');
  const text = fs.readFileSync(file, 'utf8');
  const open = 'export class RecordMetadata {\n';
  assert.ok(text.includes(open));
  fs.writeFileSync(file, text.replace(open, `${open}  bogus(): number {\n    return 1;\n  }\n`));
  return text.slice(0, text.indexOf(open)).split('\n').length + 1;
}

/** The finding `addMember` makes, as `eslint` reports it. */
const BOGUS = 'kafka/java-members RecordMetadata.bogus: no Java member of clients.producer.RecordMetadata';

test('`eslint .` from the client and `eslint client` from its parent report the same, and read nothing outside it', () => {
  const { parent, root } = layout();
  const line = addMember(root);
  const inside = eslint(root, root, ['.']);
  const outside = eslint(parent, root, ['client']);
  assert.equal(inside.status, 1);
  // stray.js, beside the client, is not read: its marker is no finding.
  assert.deepEqual(inside.messages, [`src/producer/record_metadata.ts:${line} ${BOGUS}`]);
  assert.equal(outside.status, 1);
  assert.deepEqual(outside.messages, inside.messages);
});

test('a run of part of the client fails while a rule cannot run, or the baseline has a stale line', () => {
  const { root } = layout();
  fs.rmSync(path.join(root, 'java-index.json'));
  for (const target of ['src', 'src/producer', 'src/producer/record_metadata.ts']) {
    const r = eslint(root, root, [target]);
    assert.equal(r.status, 1, target);
    assert.ok(r.messages.some((m) => / kafka\/java-name check-java-name: cannot run, no Java index at java-index\.json/.test(m)), target);
  }
  copy(ROOT, root, 'java-index.json');
  fs.appendFileSync(path.join(root, 'eslint-baseline.txt'), 'missing clients.producer.KafkaProducer#gone\n');
  const r = eslint(root, root, ['src/producer/record_metadata.ts']);
  assert.equal(r.status, 1);
  assert.deepEqual(r.messages, [
    'src/producer/record_metadata.ts:1 kafka/java-completeness eslint-baseline.txt: missing clients.producer.KafkaProducer#gone: stale entry: no finding matches it any more; delete the line',
  ]);
});

test('in one process, an edit, a removed Java index and a removed baseline line are seen at once', async () => {
  const { root } = layout();
  const file = path.join(root, 'src', 'producer', 'record_metadata.ts');
  const lint = async () => {
    const results = await new ESLint({ cwd: root }).lintFiles([file]);
    return results.flatMap((r) => r.messages.map((m) => `${m.ruleId} ${m.messageId}`)).sort();
  };
  assert.deepEqual(await lint(), []);
  addMember(root);
  assert.deepEqual(await lint(), ['kafka/java-members no-java-member']);
  fs.rmSync(path.join(root, 'java-index.json'));
  const missing = await lint();
  assert.ok(missing.includes('kafka/java-name cannot-run'), missing.join('\n'));
});

test('a nested ESLint configuration is a finding, even though ESLint uses it below it', () => {
  const { root } = layout();
  addMember(root);
  fs.writeFileSync(path.join(root, 'src', 'producer', 'eslint.config.cjs'), "'use strict';\n\nmodule.exports = [];\n");
  const r = eslint(root, root, ['.']);
  assert.equal(r.status, 1);
  // src/producer/ runs under the empty configuration (its java-members finding is gone); every other file reports it.
  assert.ok(!r.messages.some((m) => m.includes('kafka/java-members')), r.messages.join('\n'));
  assert.ok(
    r.messages.some((m) =>
      m.endsWith('kafka/config src/producer/eslint.config.cjs: is an ESLint configuration besides eslint.config.cjs: the files below it would lose the kafka/ rules'),
    ),
    r.messages.join('\n'),
  );
});

test('a one-file run of a file carrying a file-wide disable fails: the finding is at 1:0 too', () => {
  const { root } = layout();
  const file = path.join(root, 'src', 'producer', 'record_metadata.ts');
  const text = fs.readFileSync(file, 'utf8');
  fs.writeFileSync(file, text.replace('export class RecordMetadata', '/* eslint-disable */\nexport class RecordMetadata'));
  const r = eslint(root, root, ['src/producer/record_metadata.ts']);
  assert.equal(r.status, 1);
  assert.ok(r.messages.some((m) => m.startsWith('src/producer/record_metadata.ts:1 kafka/directives ')), r.messages.join('\n'));
});

test('the formatter of make lint-rules prints a project finding once, with its file count; the exit code is ESLint\'s', () => {
  const { root } = layout();
  fs.rmSync(path.join(root, 'java-index.json'));
  // A finding of a rule that needs no Java index: an underscore member.
  fs.writeFileSync(path.join(root, 'src', 'producer', 'stray.ts'), 'export class Stray {\n  _x = 1;\n}\n');
  const r = spawnSync(process.execPath, [BIN, '.', '--format', path.join(ROOT, 'scripts', 'lint', 'eslint', 'formatter.js')], {
    cwd: root,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  assert.equal(r.status, 1, r.stderr);
  const once = r.stdout.split('\n').filter((l) => l.includes('check-java-name: cannot run'));
  assert.equal(once.length, 1, r.stdout);
  assert.match(once[0], /^ {2}error {2}check-java-name: cannot run, no Java index at java-index\.json .* kafka\/java-name {2}\(on \d+ files\)$/);
  // An ordinary finding stays under its file.
  assert.match(r.stdout, /\nsrc\/producer\/stray\.ts\n {2}\d+:\d+ {2}error {2}src\/producer\/stray\.ts:Stray\._x: .* {2}kafka\/internal-members\n/);
  assert.match(r.stdout, /\n✖ \d+ problems \(\d+ errors, 0 warnings\)\n/);
});

test('an inline configuration, even JSON-escaped and switching kafka/directives off in its file, fails the run', () => {
  for (const comment of [
    '/* eslint "kafka/directives": "off", "kafka/java-members": "off" */',
    '/* eslint "kafka\\/directives": "off", "kafka\\/java-members": "off" */',
  ]) {
    const { root } = layout();
    addMember(root);
    const file = path.join(root, 'src', 'producer', 'record_metadata.ts');
    fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace('export class RecordMetadata', `${comment}\nexport class RecordMetadata`));
    const r = eslint(root, root, ['.']);
    assert.equal(r.status, 1, comment);
    assert.ok(r.messages.some((m) => / kafka\/directives src\/producer\/record_metadata\.ts:\d+: an inline configuration \(eslint\)/.test(m)), comment);
  }
});
