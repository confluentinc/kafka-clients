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

// The type declarations tsc compiles from src/'s TypeScript into dist/
// (`make -C javascript build-ts`, which test-typing runs first): no public
// declaration refers to a private module, and they type-check under --strict
// with test/typing/usage.ts (and test/typing/temporal.ts, against the
// Temporal polyfill's types, except in the ES2020 project, whose lib lacks the
// polyfill's Intl types): resolved through package.json's `exports`
// (node16 and nodenext), through its `typesVersions` (node10, the default for
// `"module": "commonjs"`), and in a project with no ambient types and an
// ES2020 lib; and a default import is rejected under esModuleInterop. The
// tsc runs are skipped, with a visible message, when tsc (a devDependency,
// `TSC`) cannot be run.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { ROOT, runTsc, tscAvailable, tscCommand, typeRoots, writeConfig } = require('../../scripts/typescript');

const SKIP = tscAvailable() ? false : `${tscCommand()} cannot be run: install the devDependencies or set TSC`;
if (SKIP) console.log(`skipping the typing tests: ${SKIP}`);

const LIB = path.join(ROOT, 'src');
const DIST = path.join(ROOT, 'dist');

/** Every file under `dir`, relative to it with `/` separators, recursively. */
function walk(dir, base = dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walk(full, base));
    else out.push(path.relative(base, full).split(path.sep).join('/'));
  }
  return out.sort();
}

/** Whether `rel` (relative to src/ or dist/) is a private module: under internal/. */
const isPrivate = (rel) => rel.split('/')[0] === 'internal';

test('every public module is declared, no private one is, and no declaration refers to a private module', () => {
  assert.ok(fs.existsSync(path.join(DIST, 'index.d.ts')), 'dist/ is not built: run `make -C javascript build-ts`');
  const sources = walk(LIB).filter((f) => f.endsWith('.ts'));
  const dist = new Set(walk(DIST));
  for (const rel of sources) {
    const js = rel.replace(/\.ts$/, '.js');
    const dts = rel.replace(/\.ts$/, '.d.ts');
    assert.ok(dist.has(js), `dist/${js} is missing`);
    // build-ts removes the private modules' declarations: nothing can type
    // against them.
    if (isPrivate(rel)) assert.ok(!dist.has(dts), `dist/${dts} declares a private module`);
    else assert.ok(dist.has(dts), `dist/${dts} is missing`);
  }
  for (const rel of [...dist].filter((f) => f.endsWith('.d.ts'))) {
    assert.ok(!isPrivate(rel), `dist/${rel} declares a private module`);
    const text = fs.readFileSync(path.join(DIST, rel), 'utf8');
    for (const m of text.matchAll(/(?:from\s+|import\(|require\()\s*["']([^"']+)["']/g)) {
      const target = m[1].startsWith('.') ? path.posix.join(path.posix.dirname(rel), m[1]) : m[1];
      assert.ok(!isPrivate(target), `dist/${rel} refers to ${m[1]}`);
    }
  }
});

// The library's own members (TSDoc `@internal`, named `internal...`) are left
// out of the declarations by stripInternal, and no `_` member is declared. A
// class users never construct keeps its constructor as `private
// constructor();`: a declaration with no constructor would let TypeScript
// assume a public one.
test('the declarations keep private constructors and leave out the library\'s own members', () => {
  for (const [rel, cls] of [
    ['common/security/auth/security_protocol.d.ts', 'SecurityProtocol'],
    ['common/config/ssl_client_auth.d.ts', 'SslClientAuth'],
  ]) {
    const text = fs.readFileSync(path.join(DIST, rel), 'utf8');
    assert.match(text, new RegExp(`export declare class ${cls}\\b[^]*\\n    private constructor\\(\\);\\n`), `dist/${rel}: ${cls} has no private constructor`);
  }
  // RecordMetadata has Java's public constructor only: the library's own
  // construction is no declared signature.
  const metadata = fs.readFileSync(path.join(DIST, 'producer/record_metadata.d.ts'), 'utf8');
  assert.match(metadata, /\n    constructor\(options: \{\n[^]*?\n        topicPartition: TopicPartition;/);
  assert.doesNotMatch(metadata, /private constructor/);
  assert.doesNotMatch(metadata, /internal\?/);
  // KafkaFuture is abstract, as Java's, its constructor public for a
  // subclass; the client's own subclass is not declared.
  const future = fs.readFileSync(path.join(DIST, 'common/kafka_future.d.ts'), 'utf8');
  assert.match(future, /export declare abstract class KafkaFuture<T> \{\n[^]*?\n    constructor\(\);\n/);
  assert.doesNotMatch(future, /class KafkaFutureImpl/);
  for (const rel of [...walk(DIST)].filter((f) => f.endsWith('.d.ts'))) {
    const text = fs.readFileSync(path.join(DIST, rel), 'utf8');
    for (const line of text.split('\n')) {
      if (/^\s*(?:\/\/|\*|\/\*)/.test(line)) continue;
      assert.doesNotMatch(line, /^\s*(?:(?:static|readonly|private|protected)\s+)*(?:internal[A-Z]\w*|_\w+)\s*[(<:?]/, `dist/${rel} declares ${line.trim()}`);
    }
  }
  // The internal members the sources do have, so the check above is not vacuous.
  const sources = ['producer/producer_record.ts', 'producer/record_metadata.ts', 'common/kafka_future.ts'].map((rel) => fs.readFileSync(path.join(LIB, rel), 'utf8'));
  for (const name of ['internalToNative', 'internalCreate', 'internalComplete']) {
    assert.ok(sources.some((text) => ['', 'static ', 'abstract '].some((m) => text.includes(`@internal\n   */\n  ${m}${name}`))), `src/ has no @internal ${name}`);
  }
});

test('the declarations type-check under --strict', { skip: SKIP }, () => {
  const config = writeConfig(path.join(ROOT, 'build', 'typing'), {
    compilerOptions: {
      strict: true,
      noEmit: true,
      target: 'es2022',
      lib: ['es2022'],
      module: 'node16',
      moduleResolution: 'node16',
      types: ['node'],
      typeRoots: [typeRoots()],
    },
    files: [path.join(ROOT, 'test', 'typing', 'usage.ts'), path.join(ROOT, 'test', 'typing', 'temporal.ts')],
  });
  const r = runTsc(['-p', config]);
  assert.equal(r.status, 0, r.output);
});

// The same declarations, resolved as a project on `nodenext` resolves them,
// the setting the library itself builds with (tsconfig.json).
test('the declarations type-check under --strict with "module": "nodenext"', { skip: SKIP }, () => {
  const config = writeConfig(path.join(ROOT, 'build', 'typing-nodenext'), {
    compilerOptions: {
      strict: true,
      noEmit: true,
      target: 'es2022',
      lib: ['es2022'],
      module: 'nodenext',
      moduleResolution: 'nodenext',
      types: ['node'],
      typeRoots: [typeRoots()],
    },
    files: [path.join(ROOT, 'test', 'typing', 'usage.ts'), path.join(ROOT, 'test', 'typing', 'temporal.ts')],
  });
  const r = runTsc(['-p', config]);
  assert.equal(r.status, 0, r.output);
});

test('the declarations type-check under "module": "commonjs" (node10 resolution, typesVersions)', { skip: SKIP }, () => {
  // node10 ignores `exports` and has no self-reference: a consumer project
  // finds the package under its node_modules/, so the test builds one, with
  // the package linked in and a copy of usage.ts.
  const dir = path.join(ROOT, 'build', 'typing-commonjs');
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(path.join(dir, 'node_modules', '@confluentinc'), { recursive: true });
  fs.symlinkSync(ROOT, path.join(dir, 'node_modules', '@confluentinc', 'kafka-javascript'), 'junction');
  fs.copyFileSync(path.join(ROOT, 'test', 'typing', 'usage.ts'), path.join(dir, 'usage.ts'));
  fs.copyFileSync(path.join(ROOT, 'test', 'typing', 'temporal.ts'), path.join(dir, 'temporal.ts'));
  const config = writeConfig(dir, {
    compilerOptions: {
      strict: true,
      noEmit: true,
      target: 'es2022',
      lib: ['es2022'],
      module: 'commonjs',
      types: ['node'],
      typeRoots: [typeRoots()],
    },
    files: [path.join(dir, 'usage.ts'), path.join(dir, 'temporal.ts')],
  });
  const r = runTsc(['-p', config]);
  assert.equal(r.status, 0, r.output);
});

test('the declarations type-check with no ambient types and an ES2020 lib', { skip: SKIP }, () => {
  // `"types": []`: Buffer comes from the declarations' own reference to
  // @types/node; ES2020 predates Error.cause, which KafkaError declares.
  const config = writeConfig(path.join(ROOT, 'build', 'typing-es2020'), {
    compilerOptions: {
      strict: true,
      noEmit: true,
      target: 'es2020',
      lib: ['es2020'],
      module: 'node16',
      moduleResolution: 'node16',
      types: [],
      typeRoots: [typeRoots()],
    },
    files: [path.join(ROOT, 'test', 'typing', 'usage.ts')],
  });
  const r = runTsc(['-p', config]);
  assert.equal(r.status, 0, r.output);
});

test('a default import fails to compile under esModuleInterop; named, namespace and Node ESM default imports compile', { skip: SKIP }, () => {
  // tsc's CommonJS output sets `exports.__esModule`, so the interop helpers
  // (TypeScript's esModuleInterop, Babel) read `module.exports.default`, which
  // does not exist. Every index.ts declares `__esModule`, so TypeScript rejects
  // that import (TS1192) instead of letting it crash at run time. Node's own
  // ES module loader (an .mts) gives `module.exports` as the default.
  const dir = path.join(ROOT, 'build', 'typing-interop');
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(path.join(dir, 'node_modules', '@confluentinc'), { recursive: true });
  fs.symlinkSync(ROOT, path.join(dir, 'node_modules', '@confluentinc', 'kafka-javascript'), 'junction');
  fs.writeFileSync(
    path.join(dir, 'default.ts'),
    "import producer from '@confluentinc/kafka-javascript/producer';\nvoid producer.KafkaProducer;\n",
  );
  fs.writeFileSync(
    path.join(dir, 'forms.ts'),
    [
      "import * as kafka from '@confluentinc/kafka-javascript';",
      "import { IllegalStateError } from '@confluentinc/kafka-javascript';",
      "import * as producer from '@confluentinc/kafka-javascript/producer';",
      "import { KafkaProducer } from '@confluentinc/kafka-javascript/producer';",
      "import * as errors from '@confluentinc/kafka-javascript/common/errors';",
      'const same: typeof KafkaProducer = producer.KafkaProducer;',
      'const state: typeof IllegalStateError = kafka.IllegalStateError;',
      'void [same, state, errors.SaslAuthenticationError];',
      '',
    ].join('\n'),
  );
  fs.writeFileSync(
    path.join(dir, 'node.mts'),
    "import producer from '@confluentinc/kafka-javascript/producer';\nconst p: typeof producer.KafkaProducer = producer.KafkaProducer;\nvoid p;\n",
  );
  const interop = (name, files) =>
    writeConfig(path.join(dir, name), {
      compilerOptions: {
        strict: true,
        noEmit: true,
        target: 'es2022',
        lib: ['es2022'],
        module: 'commonjs',
        esModuleInterop: true,
        types: ['node'],
        typeRoots: [typeRoots()],
      },
      files: files.map((f) => path.join(dir, f)),
    });
  const failed = runTsc(['-p', interop('default', ['default.ts'])]);
  assert.notEqual(failed.status, 0, 'a default import compiled under esModuleInterop');
  assert.match(failed.output, /default\.ts\(1,8\): error TS1192: Module '.*' has no default export\./);
  const forms = runTsc(['-p', interop('forms', ['forms.ts'])]);
  assert.equal(forms.status, 0, forms.output);
  const node = runTsc([
    '-p',
    writeConfig(path.join(dir, 'node'), {
      compilerOptions: {
        strict: true,
        noEmit: true,
        target: 'es2022',
        lib: ['es2022'],
        module: 'node16',
        moduleResolution: 'node16',
        types: ['node'],
        typeRoots: [typeRoots()],
      },
      files: [path.join(dir, 'node.mts')],
    }),
  ]);
  assert.equal(node.status, 0, node.output);
});
