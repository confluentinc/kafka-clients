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
// typed module namespaces and no install-time build. It reads the TypeScript
// sources (lib/**/*.ts) and package.json only: no build needed.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

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

test('lib/ holds only .ts sources, snake_case, private ones prefixed with _', () => {
  // No .js and no .d.ts: tsc compiles lib/ into dist/, which is not committed.
  for (const rel of libPaths) {
    for (const segment of rel.split('/').filter((s) => s !== '')) {
      assert.match(segment, /^_?[a-z][a-z0-9_]*(\.ts)?$/, `lib/${rel}`);
    }
  }
  for (const rel of libFiles) assert.ok(rel.endsWith('.ts') && !rel.endsWith('.d.ts'), `lib/${rel} is not a .ts source`);
});

test('every lib/ folder is a module: index.ts', () => {
  for (const dir of libDirs) {
    if (isPrivate(dir)) continue;
    assert.ok(libFiles.includes(`${dir}index.ts`), `lib/${dir}index.ts is missing`);
  }
});

/** The public module folders, without their trailing `/` (`producer`, `common`). */
const moduleDirs = libDirs.filter((d) => d !== '' && !isPrivate(d)).map((d) => d.replace(/\/$/, ''));

/**
 * The statements of `lib/<dir>index.ts`, which only re-exports: each as
 * `{kind, names, path}`, `kind` `value` for `export { A, B } from './x'`,
 * `type` for `export type { A, B } from './x'` and `namespace` for
 * `export * as x from './x'`; besides, it declares the `__esModule` marker
 * (`export declare const __esModule: true`, no JS emitted), exactly once.
 */
function parseIndex(dir) {
  const code = stripComments(fs.readFileSync(path.join(LIB, dir, 'index.ts'), 'utf8'));
  const statements = code
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s !== '');
  const reexports = [];
  let markers = 0;
  for (const statement of statements) {
    // tsc's CommonJS output sets `exports.__esModule`; declared, a default
    // import under esModuleInterop fails to compile instead of reading
    // `undefined` (test/typing checks it).
    if (/^export\s+declare\s+const\s+__esModule\s*:\s*true$/.test(statement)) {
      markers += 1;
      continue;
    }
    const named = /^export\s+(type\s+)?\{([\w\s,]+)\}\s*from\s*'(\.\/[\w/]+)'$/.exec(statement);
    if (named !== null) {
      const names = named[2].split(',').map((n) => n.trim()).filter(Boolean);
      reexports.push({ kind: named[1] === undefined ? 'value' : 'type', names, path: named[3] });
      continue;
    }
    const namespace = /^export\s+\*\s+as\s+(\w+)\s+from\s*'(\.\/[\w/]+)'$/.exec(statement);
    if (namespace !== null) {
      reexports.push({ kind: 'namespace', names: [namespace[1]], path: namespace[2] });
      continue;
    }
    assert.fail(`lib/${dir}index.ts: not a re-export: ${statement}`);
  }
  assert.equal(markers, 1, `lib/${dir}index.ts declares the __esModule marker ${markers} times, not once`);
  return reexports;
}

/**
 * Checks `lib/<dir>index.ts`: a named re-export names a public .ts of the
 * folder, a namespace one a child module folder, under the folder's name;
 * every child module folder is re-exported, and no name twice.
 */
function checkIndex(dir) {
  const reexports = parseIndex(dir);
  const children = moduleDirs.filter((d) => `${d}/`.startsWith(dir) && d.slice(dir.length) !== '' && !d.slice(dir.length).includes('/'));
  const bound = [];
  const exportedDirs = [];
  for (const r of reexports) {
    assert.match(r.path, /^\.\/[a-z][a-z0-9_]*$/, `lib/${dir}index.ts re-exports ${r.path}, not a file or folder of its own`);
    const target = `${dir}${r.path.slice(2)}`;
    if (r.kind === 'namespace') {
      assert.ok(moduleDirs.includes(target), `lib/${dir}index.ts re-exports ${r.path}, not a public lib/ folder`);
      assert.equal(r.names[0], r.path.slice(2), `lib/${dir}index.ts exports ${r.path} as ${r.names[0]}`);
      exportedDirs.push(target);
    } else {
      assert.ok(libFiles.includes(`${target}.ts`), `lib/${dir}index.ts re-exports ${r.path}, not a public .ts of lib/${dir}`);
      assert.ok(!isPrivate(target), `lib/${dir}index.ts re-exports the private ${r.path}`);
    }
    bound.push(...r.names);
  }
  assert.deepEqual(exportedDirs.sort(), [...children].sort(), `lib/${dir}index.ts re-exports its module folders`);
  assert.equal(new Set(bound).size, bound.length, `lib/${dir}index.ts exports a name twice`);
}

test('the root index.ts exports the module namespaces and its own classes, the Java built-in errors', () => {
  checkIndex('');
  for (const r of parseIndex('').filter((x) => x.kind !== 'namespace')) {
    assert.match(r.path, /_error$/, `lib/index.ts re-exports ${r.path}: the only modules of the root are Java built-in errors`);
  }
});

test('a module folder index.ts re-exports public files of its own folder and its module folders, and nothing else', () => {
  for (const dir of libDirs) {
    if (dir === '' || isPrivate(dir)) continue;
    checkIndex(dir);
  }
});

test('the build outputs are not committed: dist/ and build/ are git-ignored and untracked', (t) => {
  const git = (args) => spawnSync('git', args, { cwd: ROOT, encoding: 'utf8' });
  if (git(['rev-parse', '--is-inside-work-tree']).status !== 0) {
    t.skip('not a git checkout');
    return;
  }
  const tracked = git(['ls-files', '--', 'dist', 'build']);
  assert.equal(tracked.status, 0, tracked.stderr);
  assert.equal(tracked.stdout, '', 'dist/ or build/ has tracked files');
  for (const output of ['dist/index.js', 'dist/index.d.ts', 'build/Release/confluent-kafka-javascript.node']) {
    assert.equal(git(['check-ignore', '-q', output]).status, 0, `${output} is not git-ignored`);
  }
  const libTracked = git(['ls-files', '--', 'lib']).stdout.split('\n').filter(Boolean);
  for (const file of libTracked) {
    assert.ok(file.endsWith('.ts') && !file.endsWith('.d.ts'), `${file} is tracked under lib/ but is not a .ts source`);
  }
});

test('the generated modules carry the license and the GENERATED, DO NOT EDIT header', () => {
  // Every error class the registry lists, the registry itself, the config
  // classes and the key-type table (`cargo xtask generate-error-codes`).
  const registry = fs.readFileSync(path.join(LIB, '_error_registry.ts'), 'utf8');
  const modules = [...registry.matchAll(/^ {2}\['\.\/([\w/]+)', '\w+', '[\w.]+'\],$/gm)].map((m) => `${m[1]}.ts`);
  assert.ok(modules.length > 100, `the registry lists ${modules.length} error modules`);
  const generated = [
    ...modules,
    '_error_registry.ts',
    '_config_types.ts',
    'producer/producer_config.ts',
    'common/config/sasl_configs.ts',
    'common/config/ssl_configs.ts',
    'common/config/security_config.ts',
  ];
  for (const rel of generated) {
    assert.ok(libFiles.includes(rel), `lib/${rel} is missing`);
    const head = fs.readFileSync(path.join(LIB, rel), 'utf8').split('\n').slice(0, 17).join('\n');
    assert.match(head, /^\/\/ Copyright \d{4} Confluent Inc\.\n/, `lib/${rel}`);
    assert.match(head, /Licensed under the Apache License, Version 2\.0/, `lib/${rel}`);
    assert.match(head, /\/\/ GENERATED, DO NOT EDIT\. Produced by `cargo xtask generate-error-codes`/, `lib/${rel}`);
  }
});

test('package.json exports every module folder, with its compiled types, and nothing else', () => {
  // dist/ is lib/ compiled (tsconfig.json: rootDir lib, outDir dist).
  const expected = {};
  for (const dir of libDirs) {
    if (isPrivate(dir)) continue;
    const key = dir === '' ? '.' : `./${dir.replace(/\/$/, '')}`;
    expected[key] = { types: `./dist/${dir}index.d.ts`, default: `./dist/${dir}index.js` };
  }
  assert.deepEqual(pkg.exports, expected);
  assert.equal(pkg.main, 'dist/index.js');
  assert.equal(pkg.types, 'dist/index.d.ts');
});

test('package.json typesVersions maps every module subpath to its index.d.ts', () => {
  // `exports` is read only by TypeScript's node16, nodenext and bundler
  // resolution; typesVersions gives the subpaths their types under node10, the
  // default for `"module": "commonjs"`.
  const expected = {};
  for (const dir of moduleDirs) expected[dir] = [`dist/${dir}/index.d.ts`];
  assert.deepEqual(pkg.typesVersions, { '*': expected });
});

test('a unit test file is named after the lib/ file it tests, in the same folder', () => {
  // test/unit/<module>/<file>.test.js for lib/<module>/<file>.ts: the Java test
  // class without its Test suffix, snake_cased (KafkaProducerTest ->
  // producer/kafka_producer.test.js).
  const unit = path.join(ROOT, 'test', 'unit');
  for (const rel of walk(unit).filter((p) => !p.endsWith('/'))) {
    assert.match(rel, /\.test\.js$/, `test/unit/${rel} is not a .test.js`);
    assert.doesNotMatch(rel, /_test\.test\.js$/, `test/unit/${rel}: drop the Test suffix`);
    const tested = rel.replace(/\.test\.js$/, '.ts');
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

test('package.json has no dependencies: the published copy alone names the platform packages', () => {
  // .semaphore/check-npm.py manifest writes the published package.json: this
  // one without private, devDependencies and scripts, with the platform
  // packages as optionalDependencies at its exact version.
  for (const field of ['dependencies', 'optionalDependencies', 'peerDependencies', 'bundleDependencies']) {
    assert.ok(!(field in pkg), `package.json has ${field}`);
  }
  assert.deepEqual(pkg.repository, {
    type: 'git',
    url: 'git+https://github.com/confluentinc/kafka-clients.git',
    directory: 'javascript',
  });
  assert.equal(pkg.license, 'Apache-2.0');
});

test('package.json files hold the compiled library, not the build inputs, the tests or the gRPC harness', () => {
  for (const entry of pkg.files) {
    const top = entry.replace(/^\.\//, '').split('/')[0];
    assert.ok(
      !['binding.gyp', 'addon.exports.map', 'src', 'scripts', 'test', 'tools', 'build', 'examples', 'lib', 'tsconfig.json'].includes(top),
      `files includes ${entry}`,
    );
    assert.ok(!/grpc/i.test(entry), `files includes ${entry}`);
  }
  assert.ok(pkg.files.includes('dist/'));
});

test('only src/util.h includes the bindings header', () => {
  for (const name of fs.readdirSync(path.join(ROOT, 'src'))) {
    const text = stripComments(fs.readFileSync(path.join(ROOT, 'src', name), 'utf8'));
    const includes = /#\s*include\s*[<"]confluent_kafka_bindings\.h[>"]/.test(text);
    assert.equal(includes, name === 'util.h', `src/${name}`);
  }
});

test('every source file carries the Apache 2.0 header of Confluent Inc.', () => {
  const dirs = ['lib', 'src', 'scripts', 'test', 'tools', 'examples'];
  const files = [
    ...dirs.flatMap((d) => walk(path.join(ROOT, d)).map((f) => `${d}/${f}`)),
    'binding.gyp',
    'addon.exports.map',
    'Makefile',
    'tsconfig.json',
    'grpc_server.js',
    'grpc_translate.js',
    'Dockerfile.grpc',
  ].filter((f) => /\.(js|ts|c|h)$|^binding\.gyp$|^addon\.exports\.map$|^Makefile$|^tsconfig\.json$|^Dockerfile\.grpc$/.test(f));
  for (const file of files) {
    const head = fs.readFileSync(path.join(ROOT, file), 'utf8').split('\n').slice(0, 6).join('\n');
    assert.match(head, /Copyright \d{4} Confluent Inc\./, file);
    assert.match(head, /Licensed under the Apache License, Version 2\.0/, file);
  }
});
