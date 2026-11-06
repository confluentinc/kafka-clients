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

// The package's entry points (lib/index.ts and every module folder's
// index.ts, compiled to CommonJS in dist/): an ES module `import`s each with
// the same named exports `require` returns, through Node's CommonJS interop.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'package.json'), 'utf8'));

test('every entry point imports from an ES module with the names require returns', async () => {
  for (const subpath of Object.keys(pkg.exports)) {
    const specifier = subpath === '.' ? pkg.name : `${pkg.name}/${subpath.slice(2)}`;
    const required = require(specifier);
    const imported = await import(specifier);
    const names = Object.keys(required).sort();
    assert.ok(names.length > 0, `${specifier} exports nothing`);
    for (const name of names) assert.equal(imported[name], required[name], `${specifier}: ${name}`);
    assert.equal(imported.default, required, `${specifier}: default`);
  }
});

test('the namespaces are the module folders themselves', () => {
  const root = require(pkg.name);
  assert.equal(root.producer, require(`${pkg.name}/producer`));
  assert.equal(root.common, require(`${pkg.name}/common`));
  assert.equal(root.common.errors, require(`${pkg.name}/common/errors`));
  assert.equal(root.common.security.auth, require(`${pkg.name}/common/security/auth`));
  assert.equal(root.producer.KafkaProducer, require(`${pkg.name}/producer`).KafkaProducer);
});
