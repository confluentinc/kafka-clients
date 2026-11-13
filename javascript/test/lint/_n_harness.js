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

// Runs one N rule over the C fixture (test/lint/_c_fixture.js), with the
// fixture's allowlists unless a test gives its own.

const { RULES } = require('../../scripts/lint_rules');
const { TSCONFIG, project, lint } = require('./_project');
const { FILES, LISTS, withLists } = require('./_c_fixture');

/** The report of rule `name` over the fixture with `files` replacing its files, and `lists` its allowlists. */
function runN(name, files = {}, lists = LISTS[name] || {}) {
  const rule = RULES.find((r) => r.name === name);
  return lint(project({ 'tsconfig.json': TSCONFIG, ...FILES, ...files }), [name], { rules: [withLists(rule, lists)] });
}

module.exports = { runN };
