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

// The type declarations: the committed .d.ts are what `make types` generates
// from lib/'s JSDoc, and they type-check under --strict with
// test/typing/usage.ts: resolved through package.json's `exports` (node16),
// through its `typesVersions` (node10, the default for `"module": "commonjs"`),
// and in a project with no ambient types and an ES2020 lib. Skipped, with a
// visible message, when tsc (a devDependency, `TSC`) cannot be run.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { ROOT, runTsc, tscAvailable, tscCommand, typeRoots, writeConfig } = require('../../scripts/typescript');

const SKIP = tscAvailable() ? false : `${tscCommand()} cannot be run: install the devDependencies or set TSC`;
if (SKIP) console.log(`skipping the typing tests: ${SKIP}`);

test('the committed declarations are what make types generates', { skip: SKIP }, () => {
  const r = spawnSync(process.execPath, [path.join(ROOT, 'scripts', 'gen_types.js'), '--check'], {
    cwd: ROOT,
    encoding: 'utf8',
  });
  assert.equal(r.status, 0, `${r.stdout}${r.stderr}`);
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
    files: [path.join(ROOT, 'test', 'typing', 'usage.ts')],
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
    files: [path.join(dir, 'usage.ts')],
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
