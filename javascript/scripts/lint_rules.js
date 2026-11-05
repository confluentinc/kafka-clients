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

// The rule linter of the Node.js client (design/current/javascript-rule-linter/PLAN.md):
// the rules of javascript/CLAUDE.md, and the root CLAUDE.md rules it inherits,
// checked from the sources, as `cargo xtask lint-custom` checks the Rust
// crate's. Every rule runs and reports every finding; a rule that cannot run,
// or checks nothing, fails; the run exits 1 if anything failed.
//
// Usage: node scripts/lint_rules.js [check-<name>] [--no-baseline]
//
// `make -C javascript lint-rules` writes the Java index first
// (`cargo xtask java-index --json`). `--no-baseline` ignores
// lint-baseline.txt, so every finding is shown.

const path = require('node:path');

const { Baseline, key } = require('./lint/baseline');
const { Context } = require('./lint/context');

/** The rules, in the order they run. */
const RULES = [
  require('./lint/rules/no_todo'),
  require('./lint/rules/exception_word'),
  require('./lint/rules/private_members'),
  require('./lint/rules/java_line'),
  require('./lint/rules/java_name'),
  require('./lint/rules/java_members'),
  require('./lint/rules/java_completeness'),
  require('./lint/rules/public_audience'),
  require('./lint/rules/no_deprecated_translation'),
  require('./lint/rules/getters_readonly'),
  require('./lint/rules/java_tags'),
];

/** The baseline file, relative to the client's directory. */
const BASELINE = 'lint-baseline.txt';

/** The printed line of a finding. */
function render(finding) {
  return `${finding.subject}: ${finding.message}`;
}

/**
 * Runs `rules` (default every rule, or the one `args` names) over the client
 * at `options.root` and writes the report to `out` / `err`.
 *
 * @param args - the command-line arguments: `[check-<name>] [--no-baseline]`
 * @param options - the {@link Context} options, plus `rules` and `out` / `err` (functions taking a line)
 * @returns the exit status: 0 when every rule passed, 1 otherwise, 2 for a usage error
 */
function run(args, options = {}) {
  const out = options.out || ((line) => process.stdout.write(`${line}\n`));
  const err = options.err || ((line) => process.stderr.write(`${line}\n`));
  const all = options.rules || RULES;
  let only = null;
  let useBaseline = true;
  for (const arg of args) {
    if (arg === '--no-baseline') useBaseline = false;
    else if (arg.startsWith('check-') && only === null) only = arg;
    else {
      err(`unknown lint_rules argument \`${arg}\`; usage: node scripts/lint_rules.js [check-<name>] [--no-baseline]`);
      return 2;
    }
  }
  const rules = all.filter((r) => only === null || r.name === only);
  if (rules.length === 0) {
    err(`no rule named \`${only}\``);
    return 2;
  }

  const ctx = new Context(options);
  const baseline = useBaseline ? Baseline.load(path.join(ctx.root, BASELINE), BASELINE) : null;
  out("🔍 Running the Node.js client's rule lint...");
  let failed = 0;

  const report = (name, findings, hint) => {
    err('');
    for (const f of findings) err(`  ${render(f)}`);
    err('');
    err(`❌ ${name}: ${findings.length} finding(s).`);
    if (hint) err(hint);
    failed += findings.length;
  };

  // Whether every rule selected produced a result: the baseline lines no
  // single rule owns are judged only then.
  let allRan = true;
  for (const rule of rules) {
    const reasons = (rule.needs || []).map((n) => ctx.missing(n)).filter((r) => r !== null);
    if (reasons.length > 0) {
      err('');
      err(`❌ ${rule.name}: cannot run, ${reasons.join('; ')}`);
      failed += 1;
      allRan = false;
      continue;
    }
    let result;
    try {
      result = rule.check(ctx);
    } catch (e) {
      err('');
      err(`❌ ${rule.name}: cannot run, ${e.message}`);
      failed += 1;
      allRan = false;
      continue;
    }
    if (!result || !Array.isArray(result.findings)) {
      err('');
      err(`❌ ${rule.name}: cannot run, its check returned no findings list`);
      failed += 1;
      allRan = false;
      continue;
    }
    let findings = result.findings;
    if (baseline) {
      findings = [...baseline.waive(findings), ...baseline.leftovers(Baseline.forKinds(rule.kinds))];
    }
    if (findings.length > 0) {
      report(rule.name, findings, rule.hint);
    } else if (!Number.isInteger(result.checked) || result.checked <= 0) {
      err('');
      const count = result.checked === 0 ? '0 item(s) checked' : `no item count (checked: ${String(result.checked)})`;
      err(`❌ ${rule.name}: ${count}; a rule that checks nothing cannot pass`);
      failed += 1;
    } else {
      out(`✅ ${rule.name}: ${result.checked} item(s) checked`);
    }
  }

  // The lines no single rule owns: judged only when every rule was selected
  // and ran.
  if (baseline && only === null && allRan) {
    const kinds = all.flatMap((r) => r.kinds);
    const left = baseline.leftovers(Baseline.outsideKinds(kinds));
    if (left.length > 0) {
      report(BASELINE, left, `Delete the line, or name a finding a rule reports (\`<kind> <subject>\`).`);
    }
  }

  if (failed > 0) {
    err('');
    err(`❌ ${failed} rule-lint finding(s)`);
    return 1;
  }
  return 0;
}

module.exports = { run, render, key, RULES, BASELINE };

if (require.main === module) {
  process.exitCode = run(process.argv.slice(2));
}
