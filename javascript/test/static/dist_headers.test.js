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

// scripts/dist_headers.js, which `make build-ts` runs after tsc: each compiled
// .js and .d.ts starts with its source's licence header, the source maps
// shifted to match, and a source without the header fails the build.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'dist_headers.js');
const HEADER = fs.readFileSync(SCRIPT, 'utf8').split('\n').slice(0, 13);

function fixture(sourceHeader) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dist-headers-'));
  fs.mkdirSync(path.join(dir, 'src'));
  fs.mkdirSync(path.join(dir, 'dist'));
  fs.writeFileSync(path.join(dir, 'src', 'a.ts'), `${sourceHeader.join('\n')}\n\nexport const a = 1;\n`);
  fs.writeFileSync(path.join(dir, 'dist', 'a.js'), ['"use strict";', ...sourceHeader, 'exports.a = 1;', ''].join('\n'));
  fs.writeFileSync(path.join(dir, 'dist', 'a.js.map'), JSON.stringify({ version: 3, mappings: `A;${'B;'.repeat(13)}C` }));
  fs.writeFileSync(path.join(dir, 'dist', 'a.d.ts'), 'export declare const a = 1;\n');
  fs.writeFileSync(path.join(dir, 'dist', 'a.d.ts.map'), JSON.stringify({ version: 3, mappings: 'D' }));
  return dir;
}

const run = (dir) => spawnSync(process.execPath, [SCRIPT, path.join(dir, 'src'), path.join(dir, 'dist')], { encoding: 'utf8' });

test('the header goes first in the .js and the .d.ts, and the maps follow it', () => {
  const dir = fixture(HEADER);
  try {
    const r = run(dir);
    assert.equal(r.status, 0, r.stderr);
    const read = (f) => fs.readFileSync(path.join(dir, 'dist', f), 'utf8');
    assert.equal(read('a.js'), [...HEADER, '"use strict";', 'exports.a = 1;', ''].join('\n'));
    assert.equal(read('a.d.ts'), [...HEADER, 'export declare const a = 1;', ''].join('\n'));
    assert.equal(JSON.parse(read('a.js.map')).mappings, `${'B;'.repeat(13)}A;C`);
    assert.equal(JSON.parse(read('a.d.ts.map')).mappings, `${';'.repeat(13)}D`);
    // A second run changes nothing.
    assert.equal(run(dir).status, 0);
    assert.equal(read('a.d.ts'), [...HEADER, 'export declare const a = 1;', ''].join('\n'));
    assert.equal(JSON.parse(read('a.js.map')).mappings, `${'B;'.repeat(13)}A;C`);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a source without the licence header fails the build', () => {
  const dir = fixture(['// Some other comment']);
  try {
    const r = run(dir);
    assert.equal(r.status, 1);
    assert.match(r.stderr, /^dist_headers: .*a\.ts: does not start with the licence header\n$/);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
