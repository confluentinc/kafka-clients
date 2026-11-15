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

// The HTML reference builds (`make docs`, typedoc.config.cjs; root
// conventions, Documentation, Q64): TypeDoc over src/, with a warning failing
// the build, into a scratch directory. A module per import path, each
// package's `@packageDocumentation` on its page, and no `@internal` member.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..');
const TYPEDOC = path.join(ROOT, 'node_modules', '.bin', 'typedoc');

/** @type {string} */
let out;
afterEach(() => fs.rmSync(out, { recursive: true, force: true }));

test('the reference builds, a page per import path, without the @internal members', () => {
  out = fs.mkdtempSync(path.join(os.tmpdir(), 'kafka-js-docs-'));
  const run = spawnSync(process.execPath, [TYPEDOC, '--options', 'typedoc.config.cjs', '--out', out], {
    cwd: ROOT,
    encoding: 'utf8',
  });
  assert.equal(run.status, 0, `typedoc failed:\n${run.stdout}${run.stderr}`);
  const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
  for (const subpath of Object.keys(pkg.exports)) {
    if (subpath === '.') continue;
    const page = path.join(out, 'modules', `${subpath.slice(2).replaceAll('/', '_')}.html`);
    assert.ok(fs.existsSync(page), `no page for ${subpath}`);
  }
  // The producer package's @packageDocumentation lists what it leaves out.
  const producer = fs.readFileSync(path.join(out, 'modules', 'producer.html'), 'utf8');
  assert.match(producer, /left out/);
  const pages = fs.readdirSync(out, { recursive: true, encoding: 'utf8' }).filter((f) => f.endsWith('.html'));
  for (const f of pages) {
    const html = fs.readFileSync(path.join(out, f), 'utf8');
    assert.doesNotMatch(html, /\binternal(Create|ToNative|Complete|SetDelivery|SetNative)\b/, `${f} shows an @internal member`);
  }
}, 120_000);
