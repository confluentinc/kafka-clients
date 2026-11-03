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

/** The public module folders, without their trailing `/` (`producer`, `common`). */
const moduleDirs = libDirs.filter((d) => d !== '' && !isPrivate(d)).map((d) => d.replace(/\/$/, ''));

/**
 * The statements of `lib/<dir>index.js`: its requires, as
 * `{bindings, path}` (bindings `null` for `const x = require(...)`, else the
 * names destructured), and the names of its one `module.exports` literal.
 */
function parseIndex(dir) {
  const code = stripComments(fs.readFileSync(path.join(LIB, dir, 'index.js'), 'utf8'));
  const statements = code
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s !== '');
  const requires = [];
  const exportsLiterals = [];
  for (const statement of statements) {
    if (statement === "'use strict'") continue;
    const required = /^const\s+(?:\{([\w\s,]+)\}|(\w+))\s*=\s*require\('(\.\/[\w/]+)'\)$/.exec(statement);
    if (required !== null) {
      const names = required[1] === undefined ? [required[2]] : required[1].split(',').map((n) => n.trim()).filter(Boolean);
      requires.push({ destructured: required[1] !== undefined, names, path: required[3] });
      continue;
    }
    const literal = /^module\.exports\s*=\s*\{([\w\s,]*)\}$/.exec(statement);
    if (literal !== null) {
      exportsLiterals.push(literal[1].split(',').map((n) => n.trim()).filter(Boolean));
      continue;
    }
    assert.fail(`lib/${dir}index.js: not a require or the module.exports literal: ${statement}`);
  }
  assert.equal(exportsLiterals.length, 1, `lib/${dir}index.js has ${exportsLiterals.length} module.exports literals`);
  return { requires, exports: exportsLiterals[0] };
}

test('the root index.js requires each module folder and exports exactly those namespaces', () => {
  const { requires, exports } = parseIndex('');
  for (const r of requires) {
    assert.ok(!r.destructured, `lib/index.js destructures ${r.path}: the root exports namespaces only`);
    assert.match(r.path, /^\.\/[a-z][a-z0-9_]*$/, `lib/index.js requires ${r.path}, not a module folder`);
    assert.ok(moduleDirs.includes(r.path.slice(2)), `lib/index.js requires ${r.path}, not a public lib/ folder`);
    assert.equal(r.names[0], r.path.slice(2), `lib/index.js binds ${r.path} as ${r.names[0]}`);
  }
  assert.deepEqual(requires.map((r) => r.path.slice(2)).sort(), [...moduleDirs].sort(), 'lib/index.js requires');
  assert.deepEqual([...exports].sort(), [...moduleDirs].sort(), 'lib/index.js module.exports');
});

test('a module folder index.js requires only public files of its own folder, and exports what it requires', () => {
  for (const dir of libDirs) {
    if (dir === '' || isPrivate(dir)) continue;
    const { requires, exports } = parseIndex(dir);
    const bound = [];
    for (const r of requires) {
      assert.match(r.path, /^\.\/[a-z][a-z0-9_]*$/, `lib/${dir}index.js requires ${r.path}, not a file of its folder`);
      assert.ok(libFiles.includes(`${dir}${r.path.slice(2)}.js`), `lib/${dir}index.js requires ${r.path}, not a public .js of lib/${dir}`);
      bound.push(...r.names);
    }
    assert.deepEqual([...exports].sort(), bound.sort(), `lib/${dir}index.js module.exports`);
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

test('package.json typesVersions maps every module subpath to its index.d.ts', () => {
  // `exports` is read only by TypeScript's node16, nodenext and bundler
  // resolution; typesVersions gives the subpaths their types under node10, the
  // default for `"module": "commonjs"`.
  const expected = {};
  for (const dir of moduleDirs) expected[dir] = [`lib/${dir}/index.d.ts`];
  assert.deepEqual(pkg.typesVersions, { '*': expected });
});

test('a unit test file is named after the lib/ file it tests, in the same folder', () => {
  // test/unit/<module>/<file>.test.js for lib/<module>/<file>.js: the Java test
  // class without its Test suffix, snake_cased (KafkaProducerTest ->
  // producer/kafka_producer.test.js).
  const unit = path.join(ROOT, 'test', 'unit');
  for (const rel of walk(unit).filter((p) => !p.endsWith('/'))) {
    assert.match(rel, /\.test\.js$/, `test/unit/${rel} is not a .test.js`);
    assert.doesNotMatch(rel, /_test\.test\.js$/, `test/unit/${rel}: drop the Test suffix`);
    const tested = rel.replace(/\.test\.js$/, '.js');
    assert.ok(libFiles.includes(tested), `test/unit/${rel} names no lib/${tested}`);
  }
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
    'grpc_server.js',
    'grpc_translate.js',
    'Dockerfile.grpc',
  ].filter((f) => /\.(js|ts|c|h)$|^binding\.gyp$|^Makefile$|^Dockerfile\.grpc$/.test(f));
  for (const file of files) {
    const head = fs.readFileSync(path.join(ROOT, file), 'utf8').split('\n').slice(0, 6).join('\n');
    assert.match(head, /Copyright \d{4} Confluent Inc\./, file);
    assert.match(head, /Licensed under the Apache License, Version 2\.0/, file);
  }
});
