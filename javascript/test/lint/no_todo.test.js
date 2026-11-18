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

// H1 check-no-todo (scripts/lint/rules/no_todo.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const path = require('node:path');

const { TODO_MARK, FIXME_MARK, CLAUDE_MD, project, lint, findings } = require('./_project');

const RULE = 'check-no-todo';

test('a clean project passes, every file counted', () => {
  const root = project({
    ...CLAUDE_MD,
    'src/a.ts': 'export const a = 1;\n',
    'native/a.c': 'int a;\n',
    'Makefile': 'all:\n',
    'release/notes.txt': 'done\n',
    'node_modules/x/y.js': `// ${TODO_MARK} in a dependency\n`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.deepEqual(r.out, ["🔍 Running the Node.js client's rule lint...", '✅ check-no-todo: 5 item(s) checked']);
});

test('each marker is a finding with its file and line, in code, comments and strings', () => {
  const root = project({
    ...CLAUDE_MD,
    'src/a.ts': `export const a = 1;\n// ${TODO_MARK}: later\nconst s = '${FIXME_MARK}';\n`,
    'tools/t.js': `x(); /* ${FIXME_MARK} */\n`,
    'examples/e.js': `// ${TODO_MARK}S and ${TODO_MARK}x are not the marker; a${TODO_MARK} neither\n`,
    'Makefile': `# ${TODO_MARK}\n`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  // ESLint's half (TypeScript and JavaScript) first, then the runner's.
  assert.deepEqual(findings(r), [
    `src/a.ts:2: \`${TODO_MARK}\` marker: finish the work or delete the note`,
    `src/a.ts:3: \`${FIXME_MARK}\` marker: finish the work or delete the note`,
    `tools/t.js:1: \`${FIXME_MARK}\` marker: finish the work or delete the note`,
    `Makefile:1: \`${TODO_MARK}\` marker: finish the work or delete the note`,
  ]);
  assert.ok(r.err.includes('❌ check-no-todo: 4 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      `Finish the work the ${TODO_MARK} / ${FIXME_MARK} marks, or delete the note: nothing is left for later (root CLAUDE.md §7).`,
    ),
    r.text,
  );
  assert.equal(r.err[r.err.length - 1], '❌ 4 rule-lint finding(s)');
});

test('a baseline line waives a marker', () => {
  const root = project({
    ...CLAUDE_MD,
    'src/a.ts': `// ${TODO_MARK}\n`,
    'eslint-baseline.txt': '# waived\ntodo src/a.ts:1\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
  assert.equal(lint(root, [RULE, '--no-baseline']).status, 1);
});

test('a binary file is not read', () => {
  const root = project({ ...CLAUDE_MD, 'src/a.ts': 'export {};\n', 'native/blob.bin': `\u0000${TODO_MARK}` });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-no-todo: 2 item(s) checked'));
});

test("git's environment and index change nothing: every file is read, an unstaged new one too", () => {
  const files = { ...CLAUDE_MD, 'src/a.ts': 'export {};\n', 'src/new.ts': `// ${TODO_MARK}\n`, '.claude/notes.md': `${TODO_MARK}\n` };
  const expected = [`src/new.ts:1: \`${TODO_MARK}\` marker: finish the work or delete the note`];
  const plain = lint(project(files), [RULE]);
  assert.deepEqual(findings(plain), expected);
  assert.ok(plain.err.includes('❌ check-no-todo: 1 finding(s).'), plain.text);
  const saved = { GIT_DIR: process.env.GIT_DIR, GIT_INDEX_FILE: process.env.GIT_INDEX_FILE };
  const bogus = project({});
  process.env.GIT_DIR = path.join(bogus, 'no-such-repository.git');
  process.env.GIT_INDEX_FILE = path.join(bogus, 'no-such-index');
  try {
    const r = lint(project(files), [RULE]);
    assert.deepEqual(findings(r), expected);
    assert.deepEqual(r.err, plain.err);
  } finally {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
});

test('a project with no file to read checks 0 items, which the runner fails', () => {
  // The rule alone: with CLAUDE.md absent it also reports its exemption unused,
  // so through the runner that finding would hide the 0-items failure.
  const { Context } = require('../../scripts/lint/context');
  const rule = require('../../scripts/lint/rules/no_todo');
  const result = rule.check(new Context({ root: project({ 'package-lock.json': '{}\n', 'blob.bin': '\u0000' }) }));
  assert.equal(result.checked, 0);
  assert.deepEqual(
    result.findings.map((f) => f.kind),
    ['todo-unused-exemption'],
  );
});

test('the rule needs no input, so it always runs', () => {
  assert.deepEqual(require('../../scripts/lint/rules/no_todo').needs, []);
});

test("CLAUDE.md's rule sentence is exempt, wherever it wraps; any other marker of the file is a finding", () => {
  const claude = `# Rules\n\n- Every file carries the header. No ${TODO_MARK}\n  or ${FIXME_MARK}.\n`;
  let r = lint(project({ 'CLAUDE.md': claude, 'src/a.ts': 'export {};\n' }), [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-no-todo: 2 item(s) checked'), r.text);
  r = lint(project({ 'CLAUDE.md': `${claude}\n${TODO_MARK}: write this section.\n`, 'src/CLAUDE.md': `No ${TODO_MARK} or ${FIXME_MARK}.\n` }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    `CLAUDE.md:6: \`${TODO_MARK}\` marker: finish the work or delete the note`,
    `src/CLAUDE.md:1: \`${TODO_MARK}\` marker: finish the work or delete the note`,
    `src/CLAUDE.md:1: \`${FIXME_MARK}\` marker: finish the work or delete the note`,
  ]);
});

test('an exemption whose sentence is gone is a finding: reworded, or the file missing', () => {
  const unused = `CLAUDE.md: exempts the sentence "No ${TODO_MARK} or ${FIXME_MARK}." as it states the rule itself, but the file does not carry it: delete the exemption`;
  let r = lint(project({ 'CLAUDE.md': 'Nothing is left unfinished.\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [unused]);
  r = lint(project({ 'src/a.ts': 'export {};\n' }), [RULE]);
  assert.deepEqual(findings(r), [unused]);
});

test('every file is read: the gRPC harness, build files and docs too; package-lock.json and outputs are not', () => {
  const root = project({
    ...CLAUDE_MD,
    'grpc-server/grpc_server.js': `// ${TODO_MARK}\n`,
    'native/binding.gyp': `# ${FIXME_MARK}\n`,
    'Dockerfile.grpc': `# ${TODO_MARK}\n`,
    'README.md': `${TODO_MARK}\n`,
    'package-lock.json': `"${TODO_MARK}"\n`,
    'dist/index.js': `// ${TODO_MARK}\n`,
    'build/x.js': `// ${TODO_MARK}\n`,
    'native/build/Makefile': `# ${TODO_MARK}\n`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  // ESLint's half (the gRPC harness's JavaScript) first, then the runner's.
  assert.deepEqual(findings(r), [
    `grpc-server/grpc_server.js:1: \`${TODO_MARK}\` marker: finish the work or delete the note`,
    `Dockerfile.grpc:1: \`${TODO_MARK}\` marker: finish the work or delete the note`,
    `README.md:1: \`${TODO_MARK}\` marker: finish the work or delete the note`,
    `native/binding.gyp:1: \`${FIXME_MARK}\` marker: finish the work or delete the note`,
  ]);
});
