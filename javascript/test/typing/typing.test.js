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
// test/typing/usage.ts, resolved through package.json's `exports`. Skipped,
// with a visible message, when tsc (a devDependency, `TSC`) cannot be run.

const test = require('node:test');
const assert = require('node:assert/strict');
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
