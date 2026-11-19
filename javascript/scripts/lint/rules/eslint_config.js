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

// check-config, ESLint's `kafka/config`
// (design/current/javascript-rule-linter/PLAN.md §3.5): the client has one
// ESLint configuration, eslint.config.cjs at its root. ESLint takes each
// file's nearest configuration, so another one below it (a flat
// `eslint.config.{js,cjs,mjs,ts,cts,mts}`, or a legacy `.eslintrc*`) could
// drop the `kafka/` rules from a subtree; each is a finding, reported on every
// file the rule visits.

/** The client's one configuration. */
const ROOT_CONFIG = 'eslint.config.cjs';

/** An ESLint configuration file, by its name. */
const CONFIG = /^(eslint\.config\.(js|cjs|mjs|ts|cts|mts)|\.eslintrc(\..*)?)$/;

module.exports = {
  name: 'check-config',
  kinds: ['other-config'],
  needs: [],
  hint: 'Delete the configuration: the client is linted by eslint.config.cjs alone (PLAN.md §3.5).',
  check(ctx) {
    const findings = [];
    const files = ctx.allFiles();
    for (const rel of files) {
      if (rel === ROOT_CONFIG || !CONFIG.test(rel.split('/').pop())) continue;
      findings.push({
        kind: 'other-config',
        subject: rel,
        everywhere: true,
        at: { file: rel, line: 1, column: 0 },
        message: `is an ESLint configuration besides ${ROOT_CONFIG}: the files below it would lose the kafka/ rules`,
      });
    }
    return { findings, checked: files.length };
  },
};
