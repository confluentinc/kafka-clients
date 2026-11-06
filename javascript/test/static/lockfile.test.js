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

// package-lock.json resolves every package from the public npm registry. A
// lockfile regenerated behind a private mirror (CodeArtifact) records the
// mirror's URL, which outside users and CI cannot reach; npm maps the public
// host to whatever registry is configured, so public URLs work everywhere.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..');
const lock = JSON.parse(fs.readFileSync(path.join(ROOT, 'package-lock.json'), 'utf8'));
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));

test('every locked package resolves from registry.npmjs.org, with an integrity hash', () => {
  const entries = Object.entries(lock.packages).filter(([name]) => name !== '');
  assert.ok(entries.length > 0, 'the lockfile lists no packages');
  for (const [name, entry] of entries) {
    if (entry.link) continue;
    assert.match(entry.resolved ?? '', /^https:\/\/registry\.npmjs\.org\//, `${name} resolves from ${entry.resolved}`);
    assert.match(entry.integrity ?? '', /^sha512-/, `${name} has no sha512 integrity`);
  }
});

test('the lockfile matches package.json', () => {
  assert.equal(lock.name, pkg.name);
  assert.equal(lock.version, pkg.version);
  assert.deepEqual(lock.packages[''].devDependencies ?? {}, pkg.devDependencies ?? {});
  assert.deepEqual(lock.packages[''].dependencies ?? {}, pkg.dependencies ?? {});
});
