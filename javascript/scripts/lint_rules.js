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
// crate's. This runner holds the half ESLint does not (PLAN.md §3): the
// addon's C rules (N1 to N8) and the C and other non-TypeScript files of the
// three rules split by file type; the rules that read TypeScript are ESLint's
// `kafka/` rules (eslint.config.cjs). Every rule runs and reports every
// finding; a rule that cannot run, or checks nothing, fails; the run exits 1
// if anything failed.
//
// Usage: node scripts/lint_rules.js [check-<name>] [--no-baseline]
//
// `make -C javascript lint-rules` writes the Java index first
// (`cargo xtask java-index --json`). `--no-baseline` ignores
// lint-baseline.txt, so every finding is shown.

const path = require('node:path');

const { Baseline, key } = require('./lint/baseline');
const { Context } = require('./lint/context');
const { RUNNER_CHECKS } = require('./lint/checks');

/** The rules, in the order they run: the runner's half (scripts/lint/checks.js). */
const RULES = RUNNER_CHECKS;

/** The baseline file, relative to the client's directory. */
const BASELINE = 'lint-baseline.txt';

/** The printed line of a finding. */
function render(finding) {
  return `${finding.subject}: ${finding.message}`;
}

/** The hint of the baseline's own section. */
const BASELINE_HINT = 'Delete the line, or name a finding a rule reports (`<kind> <subject>`).';

/**
 * Evaluates `rules` (default every rule, or the one `args` names) over the
 * client at `options.root`: `{usage}` for a usage error, else `{sections}`,
 * one per rule in order, `{name, hint, findings}` / `{name, cannot}` /
 * `{name, nothing}` / `{name, checked}`, plus a last one for the baseline's
 * own lines when it has any.
 *
 * @param args - the command-line arguments: `[check-<name>] [--no-baseline]`
 * @param options - the {@link Context} options, plus `rules`
 * @returns the sections, or the usage error's message
 */
function evaluate(args, options = {}) {
  const all = options.rules || RULES;
  let only = null;
  let useBaseline = true;
  for (const arg of args) {
    if (arg === '--no-baseline') useBaseline = false;
    else if (arg.startsWith('check-') && only === null) only = arg;
    else return { usage: `unknown lint_rules argument \`${arg}\`; usage: node scripts/lint_rules.js [check-<name>] [--no-baseline]` };
  }
  const rules = all.filter((r) => only === null || r.name === only);
  if (rules.length === 0) return { usage: `no rule named \`${only}\`` };

  const ctx = new Context(options);
  const baseline = useBaseline ? Baseline.load(path.join(ctx.root, BASELINE), BASELINE) : null;
  const sections = [];
  // Whether every rule selected produced a result: the baseline lines no
  // single rule owns are judged only then.
  let allRan = true;
  for (const rule of rules) {
    const reasons = (rule.needs || []).map((n) => ctx.missing(n)).filter((r) => r !== null);
    if (reasons.length > 0) {
      sections.push({ name: rule.name, cannot: reasons.join('; ') });
      allRan = false;
      continue;
    }
    let result;
    try {
      result = rule.check(ctx);
    } catch (e) {
      sections.push({ name: rule.name, cannot: e.message });
      allRan = false;
      continue;
    }
    if (!result || !Array.isArray(result.findings)) {
      sections.push({ name: rule.name, cannot: 'its check returned no findings list' });
      allRan = false;
      continue;
    }
    let findings = result.findings;
    if (baseline) {
      findings = [...baseline.waive(findings), ...baseline.leftovers(Baseline.forKinds(rule.kinds))];
    }
    if (findings.length > 0) {
      sections.push({ name: rule.name, hint: rule.hint, findings, checked: result.checked });
    } else if (!Number.isInteger(result.checked) || result.checked <= 0) {
      const count = result.checked === 0 ? '0 item(s) checked' : `no item count (checked: ${String(result.checked)})`;
      sections.push({ name: rule.name, nothing: `${count}; a rule that checks nothing cannot pass` });
    } else {
      sections.push({ name: rule.name, checked: result.checked });
    }
  }

  // The lines no single rule owns: judged only when every rule was selected
  // and ran.
  if (baseline && only === null && allRan) {
    const kinds = all.flatMap((r) => r.kinds);
    const left = baseline.leftovers(Baseline.outsideKinds(kinds));
    if (left.length > 0) sections.push({ name: BASELINE, hint: BASELINE_HINT, findings: left });
  }
  return { sections };
}

/**
 * Writes the report of `sections` (`evaluate`'s) to `out` / `err`.
 *
 * @param sections - the sections to report
 * @param out - writes a line to the standard output
 * @param err - writes a line to the standard error
 * @returns the exit status: 0 when every rule passed, 1 otherwise
 */
function print(sections, out, err) {
  out("🔍 Running the Node.js client's rule lint...");
  let failed = 0;
  for (const s of sections) {
    if (s.findings !== undefined) {
      err('');
      for (const f of s.findings) err(`  ${render(f)}`);
      err('');
      err(`❌ ${s.name}: ${s.findings.length} finding(s).`);
      if (s.hint) err(s.hint);
      failed += s.findings.length;
    } else if (s.cannot !== undefined) {
      err('');
      err(`❌ ${s.name}: cannot run, ${s.cannot}`);
      failed += 1;
    } else if (s.nothing !== undefined) {
      err('');
      err(`❌ ${s.name}: ${s.nothing}`);
      failed += 1;
    } else {
      out(`✅ ${s.name}: ${s.checked} item(s) checked`);
    }
  }
  if (failed > 0) {
    err('');
    err(`❌ ${failed} rule-lint finding(s)`);
    return 1;
  }
  return 0;
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
  const result = evaluate(args, options);
  if (result.usage !== undefined) {
    err(result.usage);
    return 2;
  }
  return print(result.sections, out, err);
}

module.exports = { run, evaluate, print, render, key, RULES, BASELINE, BASELINE_HINT };

if (require.main === module) {
  process.exitCode = run(process.argv.slice(2));
}
