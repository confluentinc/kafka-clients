// Copyright 2025 Confluent Inc.
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

// The package layout (javascript/CLAUDE.md, "Files and layout"): what the
// rest of the client relies on to stay one public Java type per file, with
// typed module namespaces and no install-time build.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { stripComments } = require('../../scripts/header');

const ROOT = path.join(__dirname, '..', '..');
const LIB = path.join(ROOT, 'lib');
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));

/** Every path under `dir`, relative to it, recursively; directories end with `/`. */
function walk(dir, base = dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    const rel = path.relative(base, full).split(path.sep).join('/');
    if (entry.isDirectory()) out.push(`${rel}/`, ...walk(full, base));
    else out.push(rel);
  }
  return out.sort();
}

const libPaths = walk(LIB);
const libDirs = ['', ...libPaths.filter((p) => p.endsWith('/'))];
const libFiles = libPaths.filter((p) => !p.endsWith('/'));
const isPrivate = (rel) => rel.split('/').some((segment) => segment.startsWith('_'));

test('lib/ file and folder names are snake_case, private ones prefixed with _', () => {
  for (const rel of libPaths) {
    for (const segment of rel.split('/').filter((s) => s !== '')) {
      assert.match(segment, /^_?[a-z][a-z0-9_]*(\.d\.ts|\.js)?$/, `lib/${rel}`);
    }
  }
});

test('every lib/ folder is a module: index.js and index.d.ts', () => {
  for (const dir of libDirs) {
    if (isPrivate(dir)) continue;
    assert.ok(libFiles.includes(`${dir}index.js`), `lib/${dir}index.js is missing`);
    assert.ok(libFiles.includes(`${dir}index.d.ts`), `lib/${dir}index.d.ts is missing`);
  }
});

test('an index.js only requires its public modules and has one module.exports literal', () => {
  for (const dir of libDirs) {
    if (isPrivate(dir)) continue;
    const code = stripComments(fs.readFileSync(path.join(LIB, dir, 'index.js'), 'utf8'));
    const statements = code
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s !== '');
    let exportsLiterals = 0;
    for (const statement of statements) {
      if (statement === "'use strict'") continue;
      const required = /^const\s+(?:\{[\w\s,]+\}|\w+)\s*=\s*require\('(\.\/[\w/]+)'\)$/.exec(statement);
      if (required !== null) {
        assert.ok(!isPrivate(required[1]), `lib/${dir}index.js requires a private module: ${required[1]}`);
        continue;
      }
      if (/^module\.exports\s*=\s*\{[\w\s,]*\}$/.test(statement)) {
        exportsLiterals++;
        continue;
      }
      assert.fail(`lib/${dir}index.js: not a require or the module.exports literal: ${statement}`);
    }
    assert.equal(exportsLiterals, 1, `lib/${dir}index.js has ${exportsLiterals} module.exports literals`);
  }
});

test('every public .js has a .d.ts beside it, and no private one has', () => {
  for (const rel of libFiles.filter((f) => f.endsWith('.js'))) {
    const dts = rel.replace(/\.js$/, '.d.ts');
    if (isPrivate(rel)) assert.ok(!libFiles.includes(dts), `lib/${dts} declares a private module`);
    else assert.ok(libFiles.includes(dts), `lib/${dts} is missing`);
  }
  for (const rel of libFiles.filter((f) => f.endsWith('.d.ts'))) {
    assert.ok(libFiles.includes(rel.replace(/\.d\.ts$/, '.js')), `lib/${rel} has no .js`);
  }
});

test('no declaration refers to a private module', () => {
  for (const rel of libFiles.filter((f) => f.endsWith('.d.ts'))) {
    const text = fs.readFileSync(path.join(LIB, rel), 'utf8');
    for (const m of text.matchAll(/(?:from\s+|import\(|require\()\s*["']([^"']+)["']/g)) {
      assert.ok(!isPrivate(m[1]), `lib/${rel} refers to ${m[1]}`);
    }
  }
});

test('package.json exports every module folder, with its types, and nothing else', () => {
  const expected = {};
  for (const dir of libDirs) {
    if (isPrivate(dir)) continue;
    const key = dir === '' ? '.' : `./${dir.replace(/\/$/, '')}`;
    expected[key] = { types: `./lib/${dir}index.d.ts`, default: `./lib/${dir}index.js` };
  }
  assert.deepEqual(pkg.exports, expected);
  assert.equal(pkg.main, 'lib/index.js');
  assert.equal(pkg.types, 'lib/index.d.ts');
});

test('package.json: no install-time build or scripts, private, Node >= 18', () => {
  assert.equal(pkg.gypfile, false);
  for (const script of ['preinstall', 'install', 'postinstall', 'prepare']) {
    assert.ok(!(script in (pkg.scripts || {})), `package.json has an ${script} script`);
  }
  assert.equal(pkg.private, true);
  assert.equal(pkg.engines.node, '>=18');
});

test('package.json files leave out the build inputs, the tests and the gRPC harness', () => {
  for (const entry of pkg.files) {
    const top = entry.replace(/^\.\//, '').split('/')[0];
    assert.ok(!['binding.gyp', 'src', 'scripts', 'test', 'build', 'examples'].includes(top), `files includes ${entry}`);
    assert.ok(!/grpc/i.test(entry), `files includes ${entry}`);
  }
  assert.ok(pkg.files.includes('lib/'));
});

test('only src/util.h includes the bindings header', () => {
  for (const name of fs.readdirSync(path.join(ROOT, 'src'))) {
    const text = stripComments(fs.readFileSync(path.join(ROOT, 'src', name), 'utf8'));
    const includes = /#\s*include\s*[<"]confluent_kafka_bindings\.h[>"]/.test(text);
    assert.equal(includes, name === 'util.h', `src/${name}`);
  }
});

test('every source file carries the Apache 2.0 header of Confluent Inc.', () => {
  const dirs = ['lib', 'src', 'scripts', 'test', 'examples'];
  const files = [
    ...dirs.flatMap((d) => walk(path.join(ROOT, d)).map((f) => `${d}/${f}`)),
    'binding.gyp',
    'Makefile',
  ].filter((f) => /\.(js|ts|c|h)$|^binding\.gyp$|^Makefile$/.test(f));
  for (const file of files) {
    const head = fs.readFileSync(path.join(ROOT, file), 'utf8').split('\n').slice(0, 6).join('\n');
    assert.match(head, /Copyright \d{4} Confluent Inc\./, file);
    assert.match(head, /Licensed under the Apache License, Version 2\.0/, file);
  }
});
