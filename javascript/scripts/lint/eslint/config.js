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

// The flat ESLint configuration of the Node.js client
// (design/current/javascript-rule-linter/PLAN.md §3), for the client
// directory `root` (eslint.config.cjs passes its own): the `kafka` rules on
// every TypeScript and JavaScript file, typescript-eslint's
// recommended type-checked rules on src/, typed through tsconfig.json.

const tseslint = require('typescript-eslint');

const plugin = require('./plugin');

/** Every `kafka` rule, as an error. */
const KAFKA_RULES = Object.fromEntries(Object.keys(plugin.rules).map((name) => [`kafka/${name}`, 'error']));

/**
 * The configuration of client directory `root`. `typed: false` leaves out
 * the type-checked parsing and typescript-eslint's rules, for the linter's own
 * tests of the `kafka` rules, whose analysis builds its own program.
 */
function config(root, { typed = true } = {}) {
  const typedConfigs = !typed
    ? []
    : [
        ...tseslint.configs.recommendedTypeChecked.map((c) => ({ ...c, files: ['src/**/*.ts'] })),
        {
          files: ['src/**/*.ts'],
          languageOptions: { parserOptions: { projectService: true, tsconfigRootDir: root } },
        },
      ];
  return [
    { ignores: ['node_modules/', 'build/', 'dist/', '.claude/', 'package-lock.json'] },
    { linterOptions: { reportUnusedDisableDirectives: 'error' } },
    { files: ['**/*.js', '**/*.cjs'], languageOptions: { ecmaVersion: 'latest', sourceType: 'commonjs' } },
    { files: ['**/*.mjs'], languageOptions: { ecmaVersion: 'latest', sourceType: 'module' } },
    { files: ['**/*.ts'], languageOptions: { parser: tseslint.parser } },
    ...typedConfigs,
    {
      files: ['**/*.ts', '**/*.js', '**/*.cjs', '**/*.mjs'],
      plugins: { kafka: plugin },
      // The client directory the `kafka/` analysis reads: this configuration's,
      // whatever ESLint's working directory.
      settings: { kafka: { root } },
      rules: KAFKA_RULES,
    },
  ];
}

module.exports = { config, KAFKA_RULES };
