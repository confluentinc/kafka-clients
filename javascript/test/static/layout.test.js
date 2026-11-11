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
// rest of the client relies on to stay one public Java type per file, one
// import path per Java package, and no install-time build. It reads the TypeScript
// sources (src/**/*.ts) and package.json only: no build needed.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { stripComments } = require('../../scripts/header');

const ROOT = path.join(__dirname, '..', '..');
const LIB = path.join(ROOT, 'src');
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
/** Whether `rel` (relative to src/) is private: under src/internal/, the package's own helpers. */
const isPrivate = (rel) => rel.split('/')[0] === 'internal';

test('src/ holds only .ts sources, snake_case, private ones under src/internal/', () => {
  // No .js and no .d.ts: tsc compiles src/ into dist/, which is not committed.
  for (const rel of libPaths) {
    for (const segment of rel.split('/').filter((s) => s !== '')) {
      assert.match(segment, /^[a-z][a-z0-9_]*(\.ts)?$/, `src/${rel}`);
    }
  }
  for (const rel of libFiles) assert.ok(rel.endsWith('.ts') && !rel.endsWith('.d.ts'), `src/${rel} is not a .ts source`);
});

test('every src/ folder is a module: index.ts', () => {
  for (const dir of libDirs) {
    if (isPrivate(dir)) continue;
    assert.ok(libFiles.includes(`${dir}index.ts`), `src/${dir}index.ts is missing`);
  }
});

/** The public module folders, without their trailing `/` (`producer`, `common`). */
const moduleDirs = libDirs.filter((d) => d !== '' && !isPrivate(d)).map((d) => d.replace(/\/$/, ''));

/**
 * The statements of `src/<dir>index.ts`, which only re-exports: each as
 * `{kind, names, path}`, `kind` `value` for `export { A, B } from './x'` and
 * `type` for `export type { A, B } from './x'`; besides, it declares the
 * `__esModule` marker (`export declare const __esModule: true`, no JS
 * emitted), exactly once. A namespace re-export (`export * as x from './x'`)
 * fails: each package is reached by its own import path only (root
 * conventions, Layout and modules).
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
    assert.doesNotMatch(statement, /^export\s+\*/, `src/${dir}index.ts exports a namespace or a whole module: ${statement}`);
    assert.fail(`src/${dir}index.ts: not a re-export of one of its own files: ${statement}`);
  }
  assert.equal(markers, 1, `src/${dir}index.ts declares the __esModule marker ${markers} times, not once`);
  return reexports;
}

/**
 * Checks `src/<dir>index.ts`: each re-export names a public .ts of the folder
 * itself (never a child folder, never a file of another folder), and no name
 * twice.
 */
function checkIndex(dir) {
  const reexports = parseIndex(dir);
  const bound = [];
  for (const r of reexports) {
    assert.match(r.path, /^\.\/[a-z][a-z0-9_]*$/, `src/${dir}index.ts re-exports ${r.path}, not a file of its own folder`);
    const target = `${dir}${r.path.slice(2)}`;
    assert.ok(libFiles.includes(`${target}.ts`), `src/${dir}index.ts re-exports ${r.path}, not a public .ts of src/${dir}`);
    assert.ok(!isPrivate(target), `src/${dir}index.ts re-exports the private ${r.path}`);
    bound.push(...r.names);
  }
  assert.equal(new Set(bound).size, bound.length, `src/${dir}index.ts exports a name twice`);
}

test('the root index.ts exports only the Java built-in errors and Duration', () => {
  // Java's java.* packages map to the root, as in the Python client: the
  // java.lang and java.util.concurrent errors and java.time.Duration's type.
  // No namespace and no other package (root conventions, Layout and modules).
  checkIndex('');
  const reexports = parseIndex('');
  assert.ok(reexports.length > 0, 'src/index.ts exports nothing');
  for (const r of reexports) {
    assert.match(r.path, /_error$|^\.\/duration$/, `src/index.ts re-exports ${r.path}: the root exports only Java built-in errors and Duration`);
    if (r.path === './duration') assert.deepEqual(r, { kind: 'type', names: ['Duration'], path: './duration' }, 'the root exports the type Duration only');
    else assert.deepEqual(r.names, [r.path.slice(2).replace(/_([a-z])/g, (_m, c) => c.toUpperCase()).replace(/^[a-z]/, (c) => c.toUpperCase())], `src/index.ts re-exports ${r.path} under its class name only`);
  }
});

test('a module folder index.ts re-exports public files of its own folder, and nothing else', () => {
  for (const dir of libDirs) {
    if (dir === '' || isPrivate(dir)) continue;
    checkIndex(dir);
  }
});

test('the build outputs are not committed: dist/, build/ and native/build/ are git-ignored and untracked', (t) => {
  const git = (args) => spawnSync('git', args, { cwd: ROOT, encoding: 'utf8' });
  if (git(['rev-parse', '--is-inside-work-tree']).status !== 0) {
    t.skip('not a git checkout');
    return;
  }
  const tracked = git(['ls-files', '--', 'dist', 'build', 'native/build']);
  assert.equal(tracked.status, 0, tracked.stderr);
  assert.equal(tracked.stdout, '', 'dist/, build/ or native/build/ has tracked files');
  for (const output of ['dist/index.js', 'dist/index.d.ts', 'native/build/Release/confluent-kafka-javascript.node']) {
    assert.equal(git(['check-ignore', '-q', output]).status, 0, `${output} is not git-ignored`);
  }
  const libTracked = git(['ls-files', '--', 'src']).stdout.split('\n').filter(Boolean);
  for (const file of libTracked) {
    assert.ok(file.endsWith('.ts') && !file.endsWith('.d.ts'), `${file} is tracked under src/ but is not a .ts source`);
  }
});

test('the generated modules carry the license and the GENERATED, DO NOT EDIT header', () => {
  // Every error class the registry lists, the registry itself, the config
  // classes and the key-type table (`cargo xtask generate-error-codes`).
  // The registry's modules are relative to src/internal/.
  const registry = fs.readFileSync(path.join(LIB, 'internal', 'error_registry.ts'), 'utf8');
  const modules = [...registry.matchAll(/^ {2}\['\.\.\/([\w/]+)', '\w+', '[\w.]+'\],$/gm)].map((m) => `${m[1]}.ts`);
  assert.ok(modules.length > 100, `the registry lists ${modules.length} error modules`);
  const generated = [
    ...modules,
    'internal/error_registry.ts',
    'internal/config_types.ts',
    'producer/producer_config.ts',
    'common/config/sasl_configs.ts',
    'common/config/ssl_configs.ts',
    'common/config/security_config.ts',
  ];
  for (const rel of generated) {
    assert.ok(libFiles.includes(rel), `src/${rel} is missing`);
    const head = fs.readFileSync(path.join(LIB, rel), 'utf8').split('\n').slice(0, 17).join('\n');
    assert.match(head, /^\/\/ Copyright \d{4} Confluent Inc\.\n/, `src/${rel}`);
    assert.match(head, /Licensed under the Apache License, Version 2\.0/, `src/${rel}`);
    assert.match(head, /\/\/ GENERATED, DO NOT EDIT\. Produced by `cargo xtask generate-error-codes`/, `src/${rel}`);
  }
});

test('package.json exports every module folder, with its compiled types, and nothing else', () => {
  // dist/ is src/ compiled (tsconfig.json: rootDir src, outDir dist).
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

test('a unit test file is named after the src/ file it tests, in the same folder', () => {
  // test/unit/<module>/<file>.test.js for src/<module>/<file>.ts: the Java test
  // class without its Test suffix, snake_cased (KafkaProducerTest ->
  // producer/kafka_producer.test.js).
  const unit = path.join(ROOT, 'test', 'unit');
  for (const rel of walk(unit).filter((p) => !p.endsWith('/'))) {
    assert.match(rel, /\.test\.js$/, `test/unit/${rel} is not a .test.js`);
    assert.doesNotMatch(rel, /_test\.test\.js$/, `test/unit/${rel}: drop the Test suffix`);
    const tested = rel.replace(/\.test\.js$/, '.ts');
    assert.ok(libFiles.includes(tested), `test/unit/${rel} names no src/${tested}`);
  }
});

test('package.json: no install-time build or scripts, private, Node >= 22.14.0', () => {
  assert.equal(pkg.gypfile, false);
  for (const script of ['preinstall', 'install', 'postinstall', 'prepare']) {
    assert.ok(!(script in (pkg.scripts || {})), `package.json has an ${script} script`);
  }
  assert.equal(pkg.private, true);
  assert.equal(pkg.engines.node, '>=22.14.0');
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
      !['binding.gyp', 'addon.exports.map', 'native', 'scripts', 'test', 'tools', 'build', 'examples', 'src', 'tsconfig.json', 'npm', 'api', 'verifiable_clients', 'grpc-server'].includes(top),
      `files includes ${entry}`,
    );
    assert.ok(!/grpc/i.test(entry), `files includes ${entry}`);
  }
  assert.ok(pkg.files.includes('dist/'));
  // The folders behind the import paths, which pack-npm.sh writes
  // (check-npm.py path-folders: <path>/package.json pointing into dist/), for
  // TypeScript setups that do not read `exports`.
  const tops = [...new Set(moduleDirs.map((d) => `${d.split('/')[0]}/`))].sort();
  assert.deepEqual(pkg.files.filter((f) => tops.includes(f)).sort(), tops, 'files names the folders behind the import paths');
  // Apache Kafka's credit line, which the package carries as Kafka's NOTICE asks.
  assert.ok(pkg.files.includes('NOTICE'));
  assert.match(fs.readFileSync(path.join(ROOT, 'NOTICE'), 'utf8'), /This product includes software developed at\nThe Apache Software Foundation \(https:\/\/www\.apache\.org\/\)\./);
});

test('only native/util.h includes the bindings header', () => {
  const sources = fs.readdirSync(path.join(ROOT, 'native')).filter((name) => /\.(c|h)$/.test(name));
  assert.ok(sources.includes('util.h'), 'native/util.h is missing');
  for (const name of sources) {
    const text = stripComments(fs.readFileSync(path.join(ROOT, 'native', name), 'utf8'));
    const includes = /#\s*include\s*[<"]confluent_kafka_bindings\.h[>"]/.test(text);
    assert.equal(includes, name === 'util.h', `native/${name}`);
  }
});

/**
 * The files under `dirs` that git does not ignore (tracked, or new and not
 * ignored), relative to the package root: an installed or built tree
 * (node_modules/, native/build/) is not a source.
 * Outside a git checkout, every file under them but node_modules/ and build/.
 */
function sourceFiles(dirs) {
  const listed = spawnSync('git', ['ls-files', '--cached', '--others', '--exclude-standard', '-z', '--', ...dirs], {
    cwd: ROOT,
    encoding: 'utf8',
  });
  if (listed.status === 0) {
    return [...new Set(listed.stdout.split('\0').filter(Boolean))].filter((f) => fs.existsSync(path.join(ROOT, f))).sort();
  }
  const skipped = (rel) => rel.split('/').some((s) => s === 'node_modules' || s === 'build');
  return dirs.flatMap((d) => walk(path.join(ROOT, d)).map((f) => `${d}/${f}`)).filter((f) => !f.endsWith('/') && !skipped(f));
}

test('every source file carries the Apache 2.0 header of Confluent Inc.', () => {
  const files = [
    ...sourceFiles(['src', 'native', 'scripts', 'test', 'tools', 'examples', 'grpc-server', 'verifiable_clients']),
    'Makefile',
    'tsconfig.json',
    'Dockerfile.grpc',
  ].filter((f) => /\.(js|ts|c|h)$|(^|\/)binding\.gyp$|(^|\/)addon\.exports\.map$|^Makefile$|^tsconfig\.json$|^Dockerfile\.grpc$/.test(f));
  assert.ok(files.includes('native/binding.gyp') && files.includes('native/addon.exports.map'), 'native/ has no binding.gyp or addon.exports.map');
  for (const expected of ['grpc-server/grpc_server.js', 'grpc-server/grpc_translate.js', 'verifiable_clients/verifiable_producer.js']) {
    assert.ok(files.includes(expected), `${expected} is not among the files checked`);
  }
  for (const file of files) {
    const head = fs.readFileSync(path.join(ROOT, file), 'utf8').split('\n').slice(0, 6).join('\n');
    assert.match(head, /Copyright \d{4} Confluent Inc\./, file);
    assert.match(head, /Licensed under the Apache License, Version 2\.0/, file);
  }
});

test('a relative CONFLUENT_KAFKA_LIB_DIR or CONFLUENT_KAFKA_INCLUDE_DIR is relative to the package root, not native/', () => {
  // gyp runs each `<!(...)` command in the .gyp file's directory, native/;
  // scripts/header.js reads the same variable from the package root under make.
  const gyp = fs.readFileSync(path.join(ROOT, 'native', 'binding.gyp'), 'utf8');
  const cases = [
    ['ck_lib_dir', 'CONFLUENT_KAFKA_LIB_DIR', path.join('..', 'rust', 'target', 'release')],
    ['ck_include_dir', 'CONFLUENT_KAFKA_INCLUDE_DIR', path.join('..', 'rust', 'target', 'include')],
  ];
  for (const [variable, env, fallback] of cases) {
    const m = new RegExp(`"${variable}": "<!\\(node -p \\\\"(.*)\\\\"\\)"`).exec(gyp);
    assert.ok(m, `native/binding.gyp has no ${variable} command`);
    const expression = m[1].replace(/\\"/g, '"');
    const run = (value) => {
      const childEnv = { ...process.env };
      delete childEnv[env];
      if (value !== undefined) childEnv[env] = value;
      const r = spawnSync(process.execPath, ['-p', expression], { cwd: path.join(ROOT, 'native'), env: childEnv, encoding: 'utf8' });
      assert.equal(r.status, 0, r.stderr);
      return r.stdout.trim();
    };
    assert.equal(run('../rust/target/debug'), path.resolve(ROOT, '..', 'rust', 'target', 'debug'), variable);
    assert.equal(run(undefined), path.resolve(ROOT, fallback), `${variable} default`);
    assert.equal(run('/abs/dir'), path.resolve('/abs/dir'), `${variable} absolute`);
  }
});

test('the addon builds in native/: the package root has no binding.gyp, so npm never compiles on install', () => {
  assert.ok(fs.existsSync(path.join(ROOT, 'native', 'binding.gyp')), 'native/binding.gyp is missing');
  for (const name of ['binding.gyp', 'addon.exports.map']) {
    assert.ok(!fs.existsSync(path.join(ROOT, name)), `the package root has ${name}`);
  }
  for (const name of fs.readdirSync(ROOT)) {
    assert.doesNotMatch(name, /\.(c|h)$/, `the package root has the C source ${name}: the addon's sources are in native/`);
  }
});

test('npm/ holds one folder per platform package of the preview, each published as committed', () => {
  // npm/README.md: linux-x64 (glibc) and darwin-arm64. Each folder holds its
  // package.json and README.md only; pack-npm.sh adds the addon, the LICENSE,
  // the NOTICE and the third-party licence file, which its `files` name.
  const npmDir = path.join(ROOT, 'npm');
  const selectors = {
    'linux-x64-gnu': { os: ['linux'], cpu: ['x64'], libc: ['glibc'] },
    'darwin-arm64': { os: ['darwin'], cpu: ['arm64'] },
  };
  const folders = fs.readdirSync(npmDir, { withFileTypes: true }).filter((e) => e.isDirectory()).map((e) => e.name);
  assert.deepEqual(folders.sort(), Object.keys(selectors).sort());
  for (const [platform, selector] of Object.entries(selectors)) {
    const listed = sourceFiles([`npm/${platform}`]).map((f) => f.slice(`npm/${platform}/`.length));
    assert.deepEqual(listed.sort(), ['README.md', 'package.json'], `npm/${platform}`);
    const manifest = JSON.parse(fs.readFileSync(path.join(npmDir, platform, 'package.json'), 'utf8'));
    assert.deepEqual(manifest, {
      name: `@confluentinc/kafka-javascript-${platform}`,
      version: pkg.version,
      description: `The ${platform} native binary of @confluentinc/kafka-javascript`,
      license: pkg.license,
      repository: { ...pkg.repository, directory: `javascript/npm/${platform}` },
      ...selector,
      main: 'confluent-kafka-javascript.node',
      // The root conventions' dist/ and NOTICE, which every package's files
      // name (a platform package has no dist/: the entry matches nothing).
      files: ['dist/', 'NOTICE', 'confluent-kafka-javascript.node', 'THIRD_PARTY_LICENSES.txt'],
      engines: pkg.engines,
    }, `npm/${platform}/package.json`);
  }
  // The cargo-about configuration and template of THIRD_PARTY_LICENSES.txt.
  for (const name of ['about.toml', 'about.hbs', 'README.md']) {
    assert.ok(fs.existsSync(path.join(npmDir, name)), `npm/${name} is missing`);
  }
  assert.match(fs.readFileSync(path.join(npmDir, 'about.hbs'), 'utf8'), /^Third-party licences of the native addon of @confluentinc\/kafka-javascript$/m);
});
