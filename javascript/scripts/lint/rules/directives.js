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

// check-directives, ESLint's `kafka/directives`
// (design/current/javascript-rule-linter/PLAN.md §3.5): an inline comment
// never changes what the `kafka/` rules report, which only the reviewed
// eslint-baseline.txt and the rules' allowlists may, and a disabled standard
// rule is targeted and explained. In every TypeScript and JavaScript file of
// the client:
//   - no inline configuration comment (`/* eslint … */`, `/* eslint-env … */`),
//     whatever it configures: the configuration is eslint.config.cjs's alone;
//   - each `eslint-disable`, `eslint-disable-line`, `eslint-disable-next-line`
//     and `eslint-enable` comment names no `kafka/` rule, its names read as
//     ESLint reads them (split on commas, trimmed, unquoted); each disable
//     names exactly one rule and gives its reason after ` -- `.
//
// The comments are read twice: by this check, from the TypeScript parser's
// comments, and by ESLint itself (its Linter, parsing each file with no rule,
// then the SourceCode's `getDisableDirectives()` and `applyInlineConfig()`).
// What ESLint reads that this reading did not report is `directive-unread`.
// Both run in the analysis, so no comment can switch them off, and each
// finding is reported on every file the rule visits (`everywhere`) and at 1:0
// of its own file, which no disable comment reaches (the license header comes
// first).

const path = require('node:path');

const { sourceComments } = require('../comments');

/** The files read: ESLint's. */
const CODE = /\.(ts|js|cjs|mjs)$/;

/**
 * A directive comment's text, as ESLint reads it: its label, the first word,
 * then what follows. A line comment is a directive only for the `-line` and
 * `-next-line` labels.
 */
const DIRECTIVE = /^\s*(eslint-disable-next-line|eslint-disable-line|eslint-disable|eslint-enable|eslint-env|eslint)(?:\s+([^]*))?$/;

/** The text of comment `text`, without its delimiters. */
function body(text) {
  return text.startsWith('//') ? text.slice(2) : text.replace(/^\/\*/, '').replace(/\*\/$/, '');
}

/** The rule names of a directive's value, as ESLint's `parseListConfig` reads them. */
function ruleNames(value) {
  return value
    .split(',')
    .map((n) => n.trim().replace(/^(['"]?)(.*)\1$/s, '$2'))
    .filter((n) => n !== '');
}

/**
 * ESLint's own reading of the file `sourceCode` holds: `{line, what}` for
 * each inline configuration (`config`) and each `kafka/` rule a disable or
 * enable directive names (`kafka/<rule>`).
 */
function eslintReading(sourceCode) {
  const out = [];
  for (const d of sourceCode.getDisableDirectives().directives) {
    for (const r of ruleNames(String(d.value || '')).filter((n) => n.startsWith('kafka/'))) {
      out.push({ line: d.node.loc.start.line, what: r });
    }
  }
  for (const c of sourceCode.applyInlineConfig().configs) out.push({ line: c.loc.start.line, what: 'config' });
  return out.sort((a, b) => a.line - b.line);
}

/** ESLint's reading of every file `files` of the client at `root`: `rel -> [{line, what}]`. */
function readWithEslint(ctx, files) {
  const { Linter } = require('eslint');
  const tseslint = require('typescript-eslint');
  const linter = new Linter({ cwd: ctx.root });
  // Parsing only: no rule, so the reading is the comments'.
  const config = [
    { files: ['**/*.js', '**/*.cjs'], languageOptions: { ecmaVersion: 'latest', sourceType: 'commonjs' } },
    { files: ['**/*.mjs'], languageOptions: { ecmaVersion: 'latest', sourceType: 'module' } },
    { files: ['**/*.ts'], languageOptions: { parser: tseslint.parser } },
  ];
  const out = new Map();
  for (const rel of files) {
    const file = path.join(ctx.root, rel);
    linter.verify(ctx.read(rel), config, file);
    const sc = linter.getSourceCode();
    out.set(rel, sc ? eslintReading(sc) : []);
  }
  return out;
}

module.exports = {
  eslintReading,
  name: 'check-directives',
  /** Whether the check reads the comments itself (the tests turn it off to see ESLint's reading alone). */
  OWN_READING: true,
  kinds: ['disable-kafka', 'disable-unnamed', 'disable-several', 'disable-no-reason', 'config-inline', 'directive-unread'],
  needs: ['typescript'],
  hint:
    'Configure rules in eslint.config.cjs and waive a kafka/ finding in eslint-baseline.txt, never inline; ' +
    'disable a standard rule by name, with `-- <reason>` (design/current/javascript-rule-linter/PLAN.md §3.5).',
  check(ctx) {
    const ts = ctx.typescript();
    const findings = [];
    let checked = 0;
    const files = ctx.allFiles().filter((f) => CODE.test(f));
    const reported = new Set();
    for (const rel of files) {
      for (const c of sourceComments(ts, ctx.sourceFile(rel))) {
        checked += 1;
        const m = this.OWN_READING ? DIRECTIVE.exec(body(c.text)) : null;
        if (m === null) continue;
        const [, directive, rest = ''] = m;
        if (c.text.startsWith('//') && !directive.endsWith('-line')) continue;
        const at = { file: rel, line: c.line, column: 0 };
        const push = (kind, message, what) => {
          if (what !== undefined) reported.add(`${rel}:${c.line}:${what}`);
          findings.push({ kind, subject: `${rel}:${c.line}`, at, everywhere: true, alsoAtStart: true, message });
        };
        if (directive === 'eslint' || directive === 'eslint-env') {
          push('config-inline', `an inline configuration (${directive}): rules are configured in eslint.config.cjs only`, 'config');
          continue;
        }
        // ESLint's own split: the reason follows whitespace, two or more dashes, whitespace.
        const cut = /\s-{2,}\s/.exec(` ${rest}`);
        const named = (cut === null ? rest : ` ${rest}`.slice(0, cut.index)).trim();
        const reason = cut === null ? '' : ` ${rest}`.slice(cut.index + cut[0].length).trim();
        const names = ruleNames(named);
        for (const r of names.filter((n) => n.startsWith('kafka/'))) {
          const why = directive === 'eslint-enable' ? 'a kafka/ rule is never disabled or enabled inline' : 'a kafka/ finding is waived only in eslint-baseline.txt';
          push('disable-kafka', `${directive} names ${r}: ${why}`, r);
        }
        if (directive === 'eslint-enable') continue;
        if (names.length === 0) push('disable-unnamed', `${directive} names no rule: name the one rule it disables`);
        else if (names.length > 1) push('disable-several', `${directive} names ${names.length} rules: disable one rule per directive`);
        if (reason === '') push('disable-no-reason', `${directive} gives no reason: add \`-- <reason>\``);
      }
    }
    // ESLint's own reading: anything it reads that the reading above did not report.
    for (const [rel, refs] of readWithEslint(ctx, files)) {
      for (const { line, what } of refs) {
        if (reported.has(`${rel}:${line}:${what}`)) continue;
        findings.push({
          kind: 'directive-unread',
          subject: `${rel}:${line}`,
          at: { file: rel, line, column: 0 },
          everywhere: true,
          alsoAtStart: true,
          message: `ESLint reads ${what === 'config' ? 'an inline configuration' : what} in this comment, which the check did not: configure and waive in eslint.config.cjs and eslint-baseline.txt only`,
        });
      }
    }
    return { findings, checked };
  },
};
