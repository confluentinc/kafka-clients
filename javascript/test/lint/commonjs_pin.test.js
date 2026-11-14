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

// check-commonjs-pin (scripts/lint/rules/commonjs_pin.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { project, lint, findings } = require('./_project');

const RULE = 'check-commonjs-pin';

const TSCONFIG = '// A comment, as the real one has.\n{ "compilerOptions": { "module": "nodenext", "moduleResolution": "NodeNext" } }\n';
const PACKAGE = (extra = {}, exports = { '.': { types: './dist/index.d.ts', default: './dist/index.js' } }) =>
  JSON.stringify({ name: 'p', ...extra, exports }, null, 2);

test('a CommonJS package built with nodenext, each export types then default, passes', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'package.json': PACKAGE() }), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('a "type", another module setting and other export conditions are findings', () => {
  const r = lint(
    project({
      'tsconfig.json': '{ "compilerOptions": { "module": "node16" } }\n',
      'package.json': PACKAGE({ type: 'commonjs' }, {
        '.': { default: './dist/index.js', types: './dist/index.d.ts' },
        './producer': { types: './dist/producer/index.d.ts', import: './x.mjs', default: './dist/producer/index.js' },
        './common': './dist/common/index.js',
      }),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'package.json:type: is "commonjs": the package is CommonJS, with no "type"',
    'package.json:exports["."]: has the conditions default, types, not exactly types then default',
    'package.json:exports["./producer"]: has the conditions types, import, default, not exactly types then default',
    'package.json:exports["./common"]: has "./dist/common/index.js", not exactly types then default',
    'tsconfig.json:module: is "node16", not "nodenext": tsc emits the package\'s CommonJS with nodenext',
    'tsconfig.json:moduleResolution: is undefined, not "nodenext": tsc emits the package\'s CommonJS with nodenext',
  ]);
});
