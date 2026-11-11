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

// The interface check's rules (scripts/api_report.js), without a build: the
// committed baseline covers every import path and the exports map, and a
// change warns in a pre-release or a major release and fails in any other
// release (root conventions, Stability). `make check-api` compares the build.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { entryPoints, severity, differences } = require('../../scripts/api_report');

const ROOT = path.join(__dirname, '..', '..');
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));

test('api/ holds a report per import path, the exports map and the baseline version', () => {
  const api = path.join(ROOT, 'api');
  const expected = [...entryPoints(pkg).map((e) => e.report), 'exports.json', 'baseline.json'].sort();
  assert.deepEqual(fs.readdirSync(api).sort(), expected);
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(api, 'exports.json'), 'utf8')), pkg.exports);
  const baseline = JSON.parse(fs.readFileSync(path.join(api, 'baseline.json'), 'utf8'));
  assert.match(baseline.version, /^\d+\.\d+\.\d+(-[a-z]+\.\d+)?$/);
  for (const entry of entryPoints(pkg)) {
    const report = fs.readFileSync(path.join(api, entry.report), 'utf8');
    assert.match(report, /^## API Report File for "@confluentinc\/kafka-javascript"\n/, entry.report);
  }
});

test('the report names follow the import paths', () => {
  const names = Object.fromEntries(entryPoints(pkg).map((e) => [e.subpath, e.report]));
  assert.equal(names['.'], 'index.api.md');
  assert.equal(names['./producer'], 'producer.api.md');
  assert.equal(names['./common/security/auth'], 'common_security_auth.api.md');
});

test('a change warns before GA and in a major release, and fails in a minor or patch release after GA', () => {
  assert.equal(severity('4.4.0-preview.2', '4.4.0-preview.1'), 'warn');
  assert.equal(severity('4.5.0-rc.1', '4.4.0-preview.3'), 'warn');
  assert.equal(severity('5.0.0', '4.9.0'), 'warn');
  assert.equal(severity('4.6.0', '4.5.0'), 'fail');
  assert.equal(severity('4.5.1', '4.5.0'), 'fail');
  // GA, from a preview baseline: the interface freezes only from GA on.
  assert.equal(severity('4.5.0', '4.4.0-preview.3'), 'warn');
  assert.equal(severity('4.5.1', '4.5.0'), 'fail');
});

test('differences lists the changed, the new and the gone reports', () => {
  const a = new Map([['x.api.md', '1'], ['y.api.md', '2'], ['exports.json', '{}']]);
  const b = new Map([['x.api.md', '1'], ['y.api.md', '3'], ['z.api.md', '4']]);
  assert.deepEqual(differences(a, b), ['exports.json: gone', 'y.api.md: changed', 'z.api.md: new']);
  assert.deepEqual(differences(a, a), []);
});
