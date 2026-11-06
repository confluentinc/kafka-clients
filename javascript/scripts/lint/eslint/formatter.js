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

// The ESLint formatter of `make -C javascript lint-rules`
// (`eslint . --format ./scripts/lint/eslint/formatter.js`,
// design/current/javascript-rule-linter/PLAN.md §3.5). The `kafka/` rules
// report a finding about the project (a rule that cannot run, a stale
// baseline line, a forbidden directive, ...) on every file they visit, so a
// run of any part of the client fails; this prints each such message once,
// with the number of files it was reported on, and every other message under
// its file, as ESLint's own formatter does. A `kafka/` message is about the
// project when the same rule reports the same text on more than one file (a
// `kafka/` finding names its subject in its text); any other message
// (typescript-eslint's, ESLint's own, a parsing error) stays under its file
// with its line and column, counted on its own. The exit code is ESLint's.

const path = require('node:path');

/** Whether `m` is a `kafka/` rule's message. */
function isKafka(m) {
  return typeof m.ruleId === 'string' && m.ruleId.startsWith('kafka/');
}

/** The severity word of an ESLint message. */
function severity(m) {
  return m.severity === 2 || m.fatal ? 'error' : 'warning';
}

/**
 * The report of ESLint results `results`.
 *
 * @param results - ESLint's lint results
 * @param context - ESLint's formatter context (its `cwd`)
 * @returns the text to print
 */
function format(results, context = {}) {
  const cwd = context.cwd || process.cwd();
  const key = (m) => `${m.ruleId}\u0000${severity(m)}\u0000${m.message}`;
  const files = new Map();
  for (const r of results) {
    for (const m of r.messages) {
      if (!files.has(key(m))) files.set(key(m), new Set());
      files.get(key(m)).add(r.filePath);
    }
  }
  const project = new Map();
  const lines = [];
  let errors = 0;
  let warnings = 0;
  const count = (m) => (severity(m) === 'error' ? (errors += 1) : (warnings += 1));
  for (const r of results) {
    const own = [];
    for (const m of r.messages) {
      const k = key(m);
      // Only a kafka/ message carries its subject, so only there does one text on several files mean one finding.
      if (isKafka(m) && files.get(k).size > 1) {
        if (!project.has(k)) {
          project.set(k, m);
          count(m);
        }
        continue;
      }
      own.push(m);
      count(m);
    }
    if (own.length === 0) continue;
    lines.push('', path.relative(cwd, r.filePath));
    for (const m of own) lines.push(`  ${m.line}:${m.column}  ${severity(m)}  ${m.message}  ${m.ruleId || ''}`.trimEnd());
  }
  if (project.size > 0) {
    lines.push('', 'About the project (reported on every file the rule visits):');
    for (const [k, m] of project) {
      const n = files.get(k).size;
      lines.push(`  ${severity(m)}  ${m.message}  ${m.ruleId || ''}  (on ${n} files)`);
    }
  }
  const total = errors + warnings;
  if (total === 0) return '';
  lines.push('', `✖ ${total} problem${total === 1 ? '' : 's'} (${errors} error${errors === 1 ? '' : 's'}, ${warnings} warning${warnings === 1 ? '' : 's'})`, '');
  return lines.join('\n');
}

module.exports = format;
