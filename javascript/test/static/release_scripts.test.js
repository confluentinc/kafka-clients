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

// The release scripts' registry checks, without the registry:
// .semaphore/check-published-npm.py's own self-test (its version ordering and
// dist-tag rules).

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const REPO = path.join(__dirname, '..', '..', '..');
const PYTHON = process.env.PYTHON || 'python3';

test("check-published-npm.py's version ordering and dist-tag rules", () => {
  const r = spawnSync(PYTHON, [path.join(REPO, '.semaphore', 'check-published-npm.py'), 'self-test'], {
    encoding: 'utf8',
  });
  assert.equal(r.error, undefined, `cannot run ${PYTHON}: ${r.error}`);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'check-published-npm: self-test passed\n');
});
