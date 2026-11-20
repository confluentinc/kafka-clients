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

// The code of the user docs type-checks (root CLAUDE.md, JavaScript Binding
// Conventions, Documentation, Q64: the docs carry the contract). Every `js`
// and `ts` block of README.md and MIGRATION.md is written to a file of its
// own, its body inside an async function (its `import` declarations, which
// cannot go there, before it), and tsc checks them all under --strict, the
// JavaScript ones through checkJs, against the compiled declarations in
// dist/ (`make build-ts` first), as a user's code types against them. A block
// marked as a fragment (test/_doc_snippets.js) is left out.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { runTsc, tscAvailable, tscCommand, typeRoots, writeConfig } = require('../../scripts/typescript');
const { DOCS, FRAGMENT, ROOT, docBlocks } = require('../_doc_snippets');

const DIR = path.join(ROOT, 'build', 'doc-snippets');

/** @typedef {import('../_doc_snippets').Block} Block */

const BLOCKS = docBlocks();
const SNIPPETS = BLOCKS.filter((b) => ['js', 'ts', 'javascript', 'typescript'].includes(b.lang));
const CHECKED = SNIPPETS.filter((b) => !b.fragment);

/**
 * The file a block is checked as, with its import declarations first and the
 * rest in an async function, each import line left blank there.
 *
 * @param {Block} block the block
 * @returns {{ file: string, text: string }} the file's path and text
 */
function fileOf(block) {
  const ts = block.lang === 'ts' || block.lang === 'typescript';
  const isImport = (/** @type {string} */ l) => /^import\s/.test(l);
  const esm = block.code.some(isImport);
  const name = `${block.doc.replace(/\W/g, '_')}_${block.line}.${ts ? (esm ? 'mts' : 'cts') : esm ? 'mjs' : 'cjs'}`;
  const head = block.code.filter(isImport);
  const body = block.code.map((l) => (isImport(l) ? '' : l));
  const text = [...head, 'async function snippet() {', ...body, '}', 'void snippet;', ''].join('\n');
  return { file: path.join(DIR, name), text };
}

/** @type {Map<string, string[]>} the diagnostics of each file */
const diagnostics = new Map();
const SKIP = tscAvailable() ? false : `${tscCommand()} cannot be run: install the devDependencies or set TSC`;

beforeAll(() => {
  if (SKIP) return;
  assert.ok(fs.existsSync(path.join(ROOT, 'dist', 'index.d.ts')), 'dist/ has no declarations: run `make build-ts` first');
  fs.rmSync(DIR, { recursive: true, force: true });
  fs.mkdirSync(DIR, { recursive: true });
  const files = CHECKED.map((block) => {
    const { file, text } = fileOf(block);
    fs.writeFileSync(file, text);
    return file;
  });
  const config = writeConfig(DIR, {
    compilerOptions: {
      strict: true,
      allowJs: true,
      checkJs: true,
      noEmit: true,
      target: 'es2022',
      lib: ['es2022', 'esnext.disposable'],
      module: 'nodenext',
      moduleResolution: 'nodenext',
      moduleDetection: 'force',
      types: ['node'],
      typeRoots: [typeRoots()],
    },
    files,
  });
  const r = runTsc(['-p', config, '--pretty', 'false']);
  for (const line of r.output.split('\n')) {
    const match = /^(.*?)\((\d+),(\d+)\): (.*)$/.exec(line);
    if (match === null) continue;
    const file = path.resolve(ROOT, match[1]);
    diagnostics.set(file, [...(diagnostics.get(file) ?? []), `${match[2]}:${match[3]} ${match[4]}`]);
  }
  assert.ok(r.status === 0 || diagnostics.size > 0, `tsc failed without a diagnostic in a snippet:\n${r.output}`);
}, 120_000);

test('README.md and MIGRATION.md have code blocks to check, each fragment marker before a block', () => {
  assert.ok(CHECKED.length >= 4, `only ${CHECKED.length} blocks are checked`);
  for (const doc of DOCS) {
    const lines = fs.readFileSync(path.join(ROOT, doc), 'utf8').split('\n');
    lines.forEach((line, n) => {
      if (line.trim() !== FRAGMENT) return;
      assert.ok(
        SNIPPETS.some((b) => b.doc === doc && b.fragment && b.line > n + 1 && lines.slice(n + 1, b.line - 1).every((l) => l.trim() === '')),
        `${doc}:${n + 1}: the fragment marker is not followed by a js or ts block`,
      );
    });
  }
});

describe.each(CHECKED.map((b) => [`${b.doc} line ${b.line}`, b]))('the block at %s', (_, block) => {
  (SKIP ? test.skip : test)('type-checks under --strict against dist/', () => {
    const { file } = fileOf(/** @type {Block} */ (block));
    const found = diagnostics.get(file) ?? [];
    // Each diagnostic with the doc's line: the body starts after the imports
    // and the function's line, the import lines left blank in it.
    const { doc, line, code } = /** @type {Block} */ (block);
    const head = code.filter((l) => /^import\s/.test(l)).length;
    const located = found.map((d) => {
      const [at, ...rest] = d.split(' ');
      const fileLine = Number(at.split(':')[0]);
      const docLine = fileLine <= head ? `an import` : `${doc}:${line + fileLine - head - 1}`;
      return `${docLine}: ${rest.join(' ')}`;
    });
    assert.deepEqual(located, [], `${file}`);
  });
});
