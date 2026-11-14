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

// Synthetic projects for the rule linter's tests: a temporary directory laid
// out as javascript/ is, holding only the files a test gives, and the runner
// run over it with its report captured.

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const assert = require('node:assert/strict');

const { Linter } = require('eslint');

const { run, evaluate, print, BASELINE: RUNNER_BASELINE } = require('../../scripts/lint_rules');
const { Context } = require('../../scripts/lint/context');
const { ESLINT_CHECKS, RUNNER_CHECKS, part, eslintName } = require('../../scripts/lint/checks');
const { BASELINE, configure, projectOf } = require('../../scripts/lint/eslint/analysis');
const { config, KAFKA_RULES } = require('../../scripts/lint/eslint/config');
const plugin = require('../../scripts/lint/eslint/plugin');

/** A tsconfig.json like the client's, for the rules that build the TypeScript program. */
const TSCONFIG = JSON.stringify({
  compilerOptions: { strict: true, target: 'es2022', lib: ['es2022'], module: 'node16', rootDir: 'src', noEmit: true },
  include: ['src/**/*.ts'],
});

/** The unfinished-work markers, spelt in pieces so the test sources do not carry them. */
const TODO_MARK = 'TO' + 'DO';
const FIXME_MARK = 'FIX' + 'ME';

/** javascript/CLAUDE.md as check-no-todo needs it: the file carrying the rule's own sentence, which it exempts. */
const CLAUDE_MD = { 'CLAUDE.md': `No ${TODO_MARK} or ${FIXME_MARK}.\n` };

/** The temporary directories made, removed when the test process exits. */
const made = [];
process.on('exit', () => {
  for (const dir of made) fs.rmSync(dir, { recursive: true, force: true });
});

/** A new temporary directory holding `files` (relative path to text; `undefined` leaves one out); returns its path. */
function project(files) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'js-lint-'));
  made.push(root);
  for (const [rel, text] of Object.entries(files)) {
    if (text === undefined) continue;
    fs.mkdirSync(path.dirname(path.join(root, rel)), { recursive: true });
    fs.writeFileSync(path.join(root, rel), text);
  }
  return root;
}

/** Copies directory or file `rel` of `from` into `to`. */
function copy(from, to, rel) {
  fs.cpSync(path.join(from, rel), path.join(to, rel), { recursive: true });
}

/** Both tools' rules, in the order the report lists them. */
const ORDER = [
  'check-no-todo',
  'check-exception-word',
  'check-internal-members',
  'check-java-line',
  'check-java-name',
  'check-java-members',
  'check-java-completeness',
  'check-public-audience',
  'check-no-deprecated-translation',
  'check-data-objects',
  'check-invented-names',
  'check-module-exports',
  'check-overload-shape',
  'check-type-mapping',
  'check-waiting-call-promise',
  'check-native-registry',
  'check-out-err',
  'check-cb-twin-only',
  'check-notify-hook',
  'check-single-drainer',
  'check-completion-order',
  'check-ownership',
  'check-zero-copy',
  'check-string-encoding',
  'check-no-binding-queue',
  'check-no-closed-flag',
  'check-directives',
  'check-config',
];

/** `checks` with a test's copy (`rules`, by name) in place of each it replaces, keeping a part's file split. */
function withOverrides(checks, rules = []) {
  return checks.map((c) => {
    const o = rules.find((r) => r.name === c.name);
    if (!o) return c;
    return Object.prototype.hasOwnProperty.call(c, 'PART') ? part(o, c.PART) : o;
  });
}

/**
 * ESLint's half over `root`: lints every file it reads with the `kafka`
 * rules of `checks` enabled (and `kafka/baseline` when all are), through
 * ESLint's own Linter and the client's configuration, untyped (the analysis
 * builds its own program); returns the sections in the runner's shape, each
 * checked against the analysis: ESLint must report a finding with a place
 * once, and a finding about the project once on every file it lints.
 */
function eslintSections(root, selected, all, options, everything) {
  configure(root, {
    checks: all,
    headerPath: options.headerPath,
    javaIndexPath: options.javaIndexPath,
    loadTypescript: options.loadTypescript,
    baseline: !options.noBaseline,
  });
  const project = projectOf(root);
  const on = new Set(selected.map((c) => `kafka/${eslintName(c)}`));
  if (everything) on.add('kafka/baseline');
  const byRule = new Map();
  const fatal = [];
  for (const m of verifyAll(root, project, on)) {
    if (m.fatal || !m.ruleId) {
      fatal.push(`${m.rel}:${m.line}: ${m.message}`);
      continue;
    }
    if (!byRule.has(m.ruleId)) byRule.set(m.ruleId, []);
    byRule.get(m.ruleId).push(m);
  }
  const sections = [];
  const files = project.linted().size;
  const expect = (id, want) => {
    const got = (byRule.get(id) || []).map((m) => `${m.messageId}\u0000${m.message}`).sort();
    const exp = want.flatMap((w) => Array((w.global ? files : 1) + (w.atStart ? 1 : 0)).fill(`${w.kind}\u0000${w.text}`)).sort();
    assert.deepEqual(got, exp, `ESLint's ${id} reports what the analysis found`);
  };
  for (const check of selected) {
    const id = `kafka/${eslintName(check)}`;
    const r = project.result(check);
    if (r.cannot !== undefined) {
      expect(id, [{ kind: 'cannot-run', text: `${check.name}: cannot run, ${r.cannot}`, global: true }]);
      sections.push({ name: check.name, cannot: r.cannot });
    } else if (r.nothing !== undefined) {
      expect(id, [{ kind: 'nothing-checked', text: `${check.name}: ${r.nothing}`, global: true }]);
      sections.push({ name: check.name, nothing: r.nothing });
    } else {
      expect(id, r.findings.map((f) => ({ kind: f.kind, text: `${f.subject}: ${f.message}`, global: f.where.global, atStart: f.where.atStart })));
      // The findings ESLint shows: not those an inline directive suppressed everywhere.
      const shown = new Set((byRule.get(id) || []).filter((m) => !m.suppressed).map((m) => m.message));
      const findings = r.findings.filter((f) => shown.has(`${f.subject}: ${f.message}`));
      sections.push(findings.length > 0 ? { name: check.name, hint: check.hint, findings, checked: r.checked } : { name: check.name, checked: r.checked });
    }
  }
  if (everything) {
    const left = project.baselineReports();
    expect('kafka/baseline', left);
    if (left.length > 0) sections.push({ name: BASELINE, hint: plugin.rules.baseline.meta.docs.description, findings: left });
  }
  for (const f of fatal) sections.push({ name: 'eslint', cannot: f });
  return sections;
}

/**
 * Lints every file ESLint reads in `root` with the `kafka` rules `on` (ids)
 * enabled and the others off, through ESLint's Linter and the client's
 * configuration, untyped: each message with its root-relative file (`rel`).
 */
function verifyAll(root, project, on) {
  const rules = Object.fromEntries(Object.keys(KAFKA_RULES).map((id) => [id, on.has(id) ? 'error' : 'off']));
  const linter = new Linter({ cwd: root });
  const cfg = [...config(root, { typed: false }), { rules }];
  const out = [];
  for (const rel of [...project.linted()].sort()) {
    const file = path.join(root, rel);
    for (const m of linter.verify(fs.readFileSync(file, 'utf8'), cfg, file)) out.push({ rel, ...m });
    // What an inline directive suppressed: ESLint found it, and does not report it.
    for (const m of linter.getSuppressedMessages()) out.push({ rel, ...m, suppressed: true });
  }
  return out;
}

/**
 * ESLint's messages over `root` with every `kafka` rule on (`options` as
 * `lint`'s: `headerPath`, `rules`, `noBaseline`): `{rel, line, ruleId,
 * messageId, message}`, for the tests of where a finding is reported.
 */
function eslintMessages(root, options = {}) {
  configure(root, {
    checks: withOverrides(ESLINT_CHECKS, options.rules),
    headerPath: options.headerPath || path.join(root, 'confluent_kafka_bindings.h'),
    javaIndexPath: options.javaIndexPath,
    baseline: !options.noBaseline,
  });
  return verifyAll(root, projectOf(root), new Set(Object.keys(KAFKA_RULES))).filter((m) => !m.suppressed);
}

/** Merges the two halves of a rule split by file type into one section. */
function merge(a, b) {
  if (a === undefined) return b;
  if (b === undefined) return a;
  const cannot = [a, b].filter((s) => s.cannot !== undefined);
  if (cannot.length > 0) return cannot;
  // Each half's file kind may be absent from a fixture: the rule checks
  // nothing only when neither half checks anything.
  if (a.nothing !== undefined && b.nothing !== undefined) return a;
  if (a.nothing !== undefined) return b;
  if (b.nothing !== undefined) return a;
  const findings = [...(a.findings || []), ...(b.findings || [])];
  const checked = (a.checked || 0) + (b.checked || 0);
  return findings.length > 0 ? { name: a.name, hint: a.hint, findings, checked } : { name: a.name, checked };
}

/**
 * Runs both halves of the linter with `args` over `root` (ESLint's for the
 * rules that read TypeScript, the runner's for the rest; a rule split by file
 * type runs in both and is reported as one): `{status, out, err, text}`,
 * `out` and `err` the lines of the runner's report format, and `text` both,
 * in order. `options.disable` names rules to turn off.
 */
function lint(root, args = [], options = {}) {
  const out = [];
  const err = [];
  const all = [];
  const w = (to) => (line) => {
    to.push(line);
    all.push(line);
  };
  const opts = { headerPath: path.join(root, 'confluent_kafka_bindings.h'), ...options };
  const names = args.filter((a) => a.startsWith('check-'));
  const bad = args.find((a) => !a.startsWith('check-') && a !== '--no-baseline');
  let runnerChecks = withOverrides(RUNNER_CHECKS, opts.rules);
  let eslintChecks = withOverrides(ESLINT_CHECKS, opts.rules);
  if (opts.set !== undefined) {
    // `set`: the whole rule set, each rule on its tool's side; a rule of
    // neither (a test's own) is the runner's.
    const names = (list) => new Set(list.map((c) => c.name));
    const [r, e] = [names(RUNNER_CHECKS), names(ESLINT_CHECKS)];
    eslintChecks = withOverrides(ESLINT_CHECKS.filter((c) => opts.set.some((x) => x.name === c.name)), opts.set);
    runnerChecks = [
      ...withOverrides(RUNNER_CHECKS.filter((c) => opts.set.some((x) => x.name === c.name)), opts.set),
      ...opts.set.filter((x) => !r.has(x.name) && !e.has(x.name)),
    ];
  }
  const only = names[0];
  const inRunner = runnerChecks.some((c) => c.name === only);
  const inEslint = eslintChecks.some((c) => c.name === only);
  if (bad !== undefined || (only !== undefined && !inRunner && !inEslint)) {
    const status = run(args, { ...opts, rules: runnerChecks, out: w(out), err: w(err) });
    return { status, out, err, text: all.join('\n') };
  }
  const noBaseline = args.includes('--no-baseline');
  // The rules turned off, as ESLint's config turns a rule off: the others run as usual.
  const disabled = opts.disable || [];
  // ESLint's half first: a rule split by file type lists its TypeScript and
  // JavaScript findings before the runner's (src/ before native/).
  const bySide = [];
  if ((only === undefined && eslintChecks.length > 0) || inEslint) {
    const selected = eslintChecks.filter((c) => (only === undefined || c.name === only) && !disabled.includes(c.name));
    bySide.push(eslintSections(root, selected, eslintChecks, { ...opts, noBaseline }, only === undefined));
  }
  if ((only === undefined && runnerChecks.length > 0) || inRunner) {
    const result = evaluate(only === undefined ? args : [only, ...(noBaseline ? ['--no-baseline'] : [])], { ...opts, root, rules: runnerChecks });
    bySide.push(result.sections.filter((x) => !disabled.includes(x.name)));
  }
  const sections = [];
  const custom = runnerChecks.map((c) => c.name).filter((n) => !ORDER.includes(n));
  // Every rule's section is printed: a rule missing from ORDER would vanish from the report.
  const known = new Set([...ORDER, ...custom, BASELINE, RUNNER_BASELINE, 'eslint']);
  for (const list of bySide) for (const x of list) assert.ok(known.has(x.name), `the test report has no place for ${x.name}`);
  for (const name of [...ORDER, ...custom, BASELINE, RUNNER_BASELINE, 'eslint']) {
    const found = bySide.map((list) => list.filter((s) => s.name === name)).flat();
    if (found.length === 0) continue;
    const merged = found.length === 1 ? found[0] : merge(found[0], found[1]);
    sections.push(...(Array.isArray(merged) ? merged : [merged]));
  }
  const status = print(sections, w(out), w(err));
  return { status, out, err, text: all.join('\n') };
}

/** The findings lines of a report (those indented by two spaces), without the indent. */
function findings(report) {
  return report.err.filter((l) => l.startsWith('  ')).map((l) => l.slice(2));
}

/** The names of the rules a report failed. */
function failedRules(report) {
  return report.err
    .map((l) => /^❌ (check-[\w-]+|kafka\/[\w-]+):/.exec(l))
    .filter((m) => m !== null)
    .map((m) => m[1]);
}

/** Runs the runner alone, with `options.rules` as its whole rule set: the runner's own report. */
function lintRunner(root, args = [], options = {}) {
  const out = [];
  const err = [];
  const all = [];
  const status = run(args, {
    root,
    headerPath: path.join(root, 'confluent_kafka_bindings.h'),
    out: (line) => {
      out.push(line);
      all.push(line);
    },
    err: (line) => {
      err.push(line);
      all.push(line);
    },
    ...options,
  });
  return { status, out, err, text: all.join('\n') };
}

/**
 * Adds every finding both tools report on `root` now, and their baselines
 * do not list, to its copy of those baselines (eslint-baseline.txt, lint-baseline.txt), so that a copy of the
 * real tree starts clean while the real tree still has findings the rules
 * report on code that does not follow them yet: a test on the copy then sees
 * only what it changes. Never used on the real tree. Returns the lines added.
 */
function baselineCurrent(root) {
  const header = path.join(root, 'confluent_kafka_bindings.h');
  const ctx = new Context({ root, headerPath: fs.existsSync(header) ? header : undefined });
  const keys = (checks) => {
    const out = new Set();
    for (const c of checks) {
      if ((c.needs || []).some((n) => ctx.missing(n) !== null)) continue;
      for (const f of c.check(ctx).findings) out.add(`${f.kind} ${f.subject}`);
    }
    return [...out];
  };
  const added = {};
  for (const [file, checks] of [
    ['eslint-baseline.txt', ESLINT_CHECKS],
    ['lint-baseline.txt', RUNNER_CHECKS],
  ]) {
    const listed = fs.existsSync(path.join(root, file))
      ? new Set(fs.readFileSync(path.join(root, file), 'utf8').split('\n').map((l) => l.trim()))
      : new Set();
    const lines = keys(checks).filter((l) => !listed.has(l));
    added[file] = lines;
    if (lines.length > 0) {
      fs.appendFileSync(path.join(root, file), `\n# The real tree's findings when the copy was made.\n${lines.join('\n')}\n`);
    }
  }
  return added;
}

module.exports = {
  TSCONFIG,
  TODO_MARK,
  FIXME_MARK,
  CLAUDE_MD,
  project,
  copy,
  lint,
  lintRunner,
  eslintMessages,
  findings,
  failedRules,
  baselineCurrent,
};
