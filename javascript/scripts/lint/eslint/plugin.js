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

// The rule linter's ESLint plugin (`kafka`): one rule per check of
// scripts/lint/checks.js's ESLINT_CHECKS, named after it (`check-java-name`
// -> `kafka/java-name`, `check-directives` -> `kafka/directives`), plus
// `kafka/baseline`. Each rule reports, on the
// file ESLint lints, the findings scripts/lint/eslint/analysis.js locates
// there; its message is the finding's `<subject>: <message>`, its message id
// the finding's kind (the baseline key is `<kind> <subject>`).

const path = require('node:path');

const { ESLINT_CHECKS, eslintName } = require('../checks');
const { projectOf } = require('./analysis');

/** The kinds every rule may report besides its check's. */
const COMMON_KINDS = ['cannot-run', 'nothing-checked', 'baseline'];

/**
 * The client directory of `context`: the configuration's
 * `settings.kafka.root` (scripts/lint/eslint/config.js), never ESLint's
 * working directory, so `eslint javascript` from the repository and
 * `eslint .` from javascript/ lint the same client. `null` when the
 * configuration gives none.
 */
function rootOf(context) {
  const kafka = context.settings && context.settings.kafka;
  return kafka && typeof kafka.root === 'string' ? kafka.root : null;
}

/** The root-relative path of the file `context` lints, with `/` separators. */
function relativeFile(root, context) {
  return path.relative(root, context.physicalFilename).split(path.sep).join('/');
}

/** The report of a configuration without `settings.kafka.root`. */
const NO_ROOT = {
  line: 1,
  column: 0,
  kind: 'cannot-run',
  text: 'cannot run: the configuration gives no settings.kafka.root (scripts/lint/eslint/config.js)',
};

/** Reports `reports` (`{line, column, kind, text}`) through `context`. */
function reportAll(context, reports) {
  for (const r of reports) {
    context.report({ loc: { line: r.line, column: r.column }, messageId: r.kind, data: { text: r.text } });
  }
}

/** The ESLint rule of check `check`, by its name, looked up in the analysis's checks. */
function ruleOf(check) {
  return {
    meta: {
      type: 'problem',
      docs: { description: check.hint },
      schema: [],
      messages: Object.fromEntries([...check.kinds, ...COMMON_KINDS].map((k) => [k, '{{text}}'])),
    },
    create(context) {
      return {
        Program() {
          const root = rootOf(context);
          if (root === null) return reportAll(context, [NO_ROOT]);
          const project = projectOf(root, context.sourceCode);
          // The analysis may hold the tests' copy of the check, with their allowlists.
          const own = project.checks.find((c) => c.name === check.name) || check;
          return reportAll(context, project.reports(own, relativeFile(root, context)));
        },
      };
    },
  };
}

const rules = Object.fromEntries(ESLINT_CHECKS.map((c) => [eslintName(c), ruleOf(c)]));

rules.baseline = {
  meta: {
    type: 'problem',
    docs: { description: 'Delete the line, or name a finding a rule reports (`<kind> <subject>`).' },
    schema: [],
    messages: { baseline: '{{text}}', 'cannot-run': '{{text}}' },
  },
  create(context) {
    return {
      Program() {
        const root = rootOf(context);
        if (root === null) return reportAll(context, [NO_ROOT]);
        return reportAll(context, projectOf(root, context.sourceCode).baselineReports());
      },
    };
  },
};


module.exports = { meta: { name: 'kafka' }, rules };
