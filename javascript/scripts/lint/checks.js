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

// The rule linter's checks, split between its two tools
// (design/current/javascript-rule-linter/PLAN.md §3): the rules that read
// TypeScript run in ESLint (scripts/lint/eslint/, `kafka/<name>`); the
// addon's C rules and the checks of the other files stay in the runner
// (scripts/lint_rules.js). Three rules read both kinds of file and are split
// by file type under one name: their TypeScript and JavaScript half runs in
// ESLint, their other half in the runner.

/** `check` restricted to part `part` of its files (its `PART`). */
function part(check, PART) {
  return Object.assign(Object.create(check), { PART });
}

const noTodo = require('./rules/no_todo');
const exceptionWord = require('./rules/exception_word');
const javaLine = require('./rules/java_line');

/** The checks ESLint runs, in report order. */
const ESLINT_CHECKS = [
  part(noTodo, 'code'),
  part(exceptionWord, 'ts'),
  require('./rules/internal_members'),
  part(javaLine, 'ts'),
  require('./rules/java_name'),
  require('./rules/java_members'),
  require('./rules/java_completeness'),
  require('./rules/public_audience'),
  require('./rules/no_deprecated_translation'),
  require('./rules/data_objects'),
  require('./rules/invented_names'),
  require('./rules/module_exports'),
  require('./rules/overload_shape'),
  require('./rules/type_mapping'),
  require('./rules/waiting_call_promise'),
  require('./rules/native_registry'),
  require('./rules/directives'),
  require('./rules/eslint_config'),
];

/** The checks the runner runs, in report order. */
const RUNNER_CHECKS = [
  part(noTodo, 'other'),
  part(exceptionWord, 'c'),
  part(javaLine, 'c'),
  require('./rules/out_err'),
  require('./rules/cb_twin_only'),
  require('./rules/notify_hook'),
  require('./rules/single_drainer'),
  require('./rules/completion_order'),
  require('./rules/ownership'),
  require('./rules/zero_copy'),
  require('./rules/string_encoding'),
  require('./rules/no_binding_queue'),
  require('./rules/no_closed_flag'),
];

/** The ESLint rule name of check `check`: `check-java-name` -> `java-name`. */
function eslintName(check) {
  return check.name.replace(/^check-/, '');
}

module.exports = { ESLINT_CHECKS, RUNNER_CHECKS, part, eslintName };
