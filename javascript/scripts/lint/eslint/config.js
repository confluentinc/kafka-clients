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
// (javascript/design/current/rule-linter/PLAN.md §3), for the client
// directory `root` (eslint.config.cjs passes its own): the `kafka` rules on
// every TypeScript and JavaScript file, typescript-eslint's
// recommended type-checked rules on src/, typed through tsconfig.json, and
// the docs lint on src/: eslint-plugin-jsdoc's `require-param` and
// `require-returns` (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Documentation, Q64, and Build, packaging and release, Q74).

const path = require('node:path');

const jsdoc = require('eslint-plugin-jsdoc');
const tseslint = require('typescript-eslint');

const plugin = require('./plugin');

// The declarations the docs lint reads: eslint-plugin-jsdoc's defaults
// (functions, and `declare function`), plus the body-less ones, where the
// public docs live: the overload signatures of a function, method or
// constructor and an abstract method (`TSEmptyBodyFunctionExpression`, under
// `TSDeclareFunction` for a function), and an interface's methods
// (`TSMethodSignature`).
const FUNCTION_CONTEXTS = ['ArrowFunctionExpression', 'FunctionDeclaration', 'FunctionExpression', 'TSDeclareFunction'];
const BODYLESS_CONTEXTS = ['TSEmptyBodyFunctionExpression', 'TSMethodSignature'];
const PARAM_CONTEXTS = [...FUNCTION_CONTEXTS, ...BODYLESS_CONTEXTS];
// A body-less declaration has no body to read a return from, so its
// `@returns` is required by its return type: any but one that returns no
// value, `void`, `undefined` and `never`, alone or in a `Promise`
// (`forceRequireReturn`, which would flag a `void` one too), and not on an
// interface's setter, which returns nothing. Not exempted yet: a union of
// them (`void | Promise<void>`), which no declaration of src/ has; the
// consumer's listener methods will.
const NO_VALUE = ['TSVoidKeyword', 'TSUndefinedKeyword', 'TSNeverKeyword'];
const RETURNS_VALUE = [
  ...NO_VALUE.map((type) => `:not([returnType.typeAnnotation.type='${type}'])`),
  ...NO_VALUE.map(
    (type) => `:not([returnType.typeAnnotation.typeName.name='Promise'][returnType.typeAnnotation.typeArguments.params.0.type='${type}'])`,
  ),
  ":not([kind='set'])",
].join('');
const RETURNS_CONTEXTS = [
  ...FUNCTION_CONTEXTS.filter((c) => c !== 'TSDeclareFunction'),
  ...['TSDeclareFunction', ...BODYLESS_CONTEXTS].map((c) => ({ context: `${c}${RETURNS_VALUE}`, forceRequireReturn: true })),
];

/** The examples' files, which the typed configuration lints as an app's code. */
const EXAMPLE_FILES = ['examples/**/*.js'];

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
  // The examples (examples/) are user code: typescript-eslint's recommended
  // type-checked rules (`no-floating-promises`, `no-unused-vars`, ...), typed
  // through examples/tsconfig.eslint.json, which maps the package's import
  // paths to src/ so that no build is needed; `typed: false` leaves them out. The
  // examples load the client with `require`, as a CommonJS app does.
  const exampleConfigs = !typed
    ? []
    : [
        ...tseslint.configs.recommendedTypeChecked.map((c) => ({ ...c, files: EXAMPLE_FILES })),
        {
          files: EXAMPLE_FILES,
          languageOptions: {
            sourceType: 'commonjs',
            parserOptions: { project: path.join(root, 'examples', 'tsconfig.eslint.json'), tsconfigRootDir: root },
          },
          // Off: `require` is how a CommonJS app loads the client; and the
          // examples print the client's values in template literals on
          // purpose, through Java's `toString()` (a `TopicPartition` prints
          // `orders-0`, a `Uuid` its base64 form), which
          // `restrict-template-expressions` refuses for any non-primitive.
          // `no-base-to-string` stays on: it skips a type with a `toString`
          // of its own, and catches an object printed as `[object Object]`.
          rules: {
            '@typescript-eslint/no-require-imports': 'off',
            '@typescript-eslint/restrict-template-expressions': 'off',
          },
        },
      ];
  return [
    { ignores: ['node_modules/', 'build/', 'dist/', '.claude/', 'package-lock.json'] },
    { linterOptions: { reportUnusedDisableDirectives: 'error' } },
    { files: ['**/*.js', '**/*.cjs'], languageOptions: { ecmaVersion: 'latest', sourceType: 'commonjs' } },
    { files: ['**/*.mjs'], languageOptions: { ecmaVersion: 'latest', sourceType: 'module' } },
    { files: ['**/*.ts'], languageOptions: { parser: tseslint.parser } },
    ...typedConfigs,
    ...exampleConfigs,
    {
      // A doc comment names each parameter with `@param` and what the function
      // returns with `@returns`. An options object is one parameter, described
      // as a whole (Documentation, Q64): its destructured keys are documented
      // on its options type, so `checkDestructured` is off. Only an existing
      // doc comment is checked: which members need one is not this lint's.
      files: ['src/**/*.ts'],
      plugins: { jsdoc },
      settings: { jsdoc: { mode: 'typescript' } },
      rules: {
        'jsdoc/require-param': ['error', { checkDestructured: false, contexts: PARAM_CONTEXTS }],
        'jsdoc/require-returns': ['error', { contexts: RETURNS_CONTEXTS }],
        // TSDoc's form, `@param name - text`.
        'jsdoc/require-hyphen-before-param-description': ['error', 'always'],
      },
    },
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
