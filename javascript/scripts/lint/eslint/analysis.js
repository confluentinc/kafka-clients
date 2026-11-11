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

// The project-wide analysis behind the ESLint rules (`kafka/<name>`,
// design/current/javascript-rule-linter/PLAN.md §3.5). The checks read the
// whole client at once (the Java index, the TypeScript program over src/,
// the addon's C sources and the bindings header), so each check runs once per
// state of those inputs, and each ESLint rule reports, while ESLint lints a
// file, what concerns that file:
//
//   - a finding with a place: at the place the check gives (`at`), or the
//     file and line its subject names (`src/x.ts:12`, `src/x.ts:Name`), only
//     there;
//   - a finding about the project, on every file the rule visits, so any run,
//     of the whole directory or of one file, fails while it stands: a check
//     that cannot run or checks nothing, a stale, duplicate or unknown
//     baseline line, an unused allowlist or exemption entry (on its entry's
//     line in the check's module, line 1 elsewhere), and any finding whose
//     place ESLint does not lint.
//
// The client directory is the configuration's (`settings.kafka.root`), never
// ESLint's working directory. The baseline (eslint-baseline.txt) is applied
// here, as the runner applies lint-baseline.txt: a check's findings it lists
// are not reported, and a line that waives nothing, or is listed twice, is a
// finding of the check owning its kind; `prefix:` lines and unknown kinds are
// `kafka/baseline`'s.

const fs = require('node:fs');
const path = require('node:path');

const { Baseline } = require('../baseline');
const { Context } = require('../context');
const { ESLINT_CHECKS } = require('../checks');
const { atText } = require('../location');

/** The baseline of the ESLint rules, relative to the client's directory. */
const BASELINE = 'eslint-baseline.txt';

/** The files ESLint lints (scripts/lint/eslint/config.js). */
const CODE = /\.(ts|js|cjs|mjs)$/;

/** Files ESLint does not read, besides the ignored directories. */
const SKIPPED_FILES = new Set(['package-lock.json']);

/** Per client directory: what the tests put in place of the defaults (`configure`). */
const overrides = new Map();

/** Per client directory: `{project, sig}`, the analysis of the inputs whose signature is `sig`. */
const cache = new Map();

/** Per file ESLint lints (its SourceCode): the analysis its rules share. */
const bySource = new WeakMap();

/**
 * Replaces, for the client directory `root`, the checks (`checks`, the tests'
 * allowlists in place), the Java index (`javaIndexPath`), the header
 * (`headerPath`), the loading of `typescript` (`loadTypescript`) or the use
 * of the baseline (`baseline: false`); `null` restores the defaults.
 */
function configure(root, options) {
  const key = path.resolve(root);
  if (options === null) overrides.delete(key);
  else overrides.set(key, options);
  cache.delete(key);
}

/** The module file a check object comes from (a part or a test's copy inherits from it). */
function moduleFile(check) {
  for (let o = check; o !== null; o = Object.getPrototypeOf(o)) {
    for (const m of Object.values(require.cache)) {
      if (m.exports === o) return m.filename;
    }
  }
  return null;
}

class Project {
  constructor(root, options = {}) {
    this.root = root;
    this.ctx = new Context({
      root,
      javaIndexPath: options.javaIndexPath,
      headerPath: options.headerPath,
      loadTypescript: options.loadTypescript,
    });
    this.checks = options.checks || ESLINT_CHECKS;
    // `KAFKA_LINT_NO_BASELINE=1` shows every finding, as the runner's `--no-baseline`.
    const off = options.baseline === false || process.env.KAFKA_LINT_NO_BASELINE === '1';
    this.baseline = off ? null : Baseline.load(path.join(root, BASELINE), BASELINE);
    this.results = new Map();
  }

  /** The files ESLint lints, relative to the root. */
  linted() {
    return this.ctx.once('eslint:linted', () => {
      const out = new Set();
      for (const rel of this.ctx.allFiles()) {
        if (CODE.test(rel) && !SKIPPED_FILES.has(path.posix.basename(rel))) out.add(rel);
      }
      return out;
    });
  }

  /**
   * `{file, line, column, global}` where finding `f` of check `check` is
   * reported: `global` (on every file) for a finding about the project, whose
   * `file` (null when none) is where it has a line of its own.
   */
  locate(check, f) {
    const linted = this.linted();
    if (f.kind === 'baseline') return { file: null, line: 1, column: 0, global: true };
    // `everywhere`: a finding with a place that is also reported on every file (check-directives').
    if (f.at && linted.has(f.at.file)) {
      return {
        line: Math.max(1, f.at.line),
        column: f.at.column || 0,
        file: f.at.file,
        global: f.everywhere === true,
        // Also at 1:0 of its own file, where no disable comment reaches (check-directives').
        atStart: f.alsoAtStart === true && Math.max(1, f.at.line) !== 1,
      };
    }
    if (f.unusedEntry !== undefined) {
      const file = moduleFile(check);
      // `require` resolves symbolic links (a temporary directory's /var -> /private/var).
      const rel = file === null ? null : path.relative(fs.realpathSync(this.root), file).split(path.sep).join('/');
      if (rel !== null && linted.has(rel)) {
        const text = this.ctx.read(rel);
        const needle = [`'${f.unusedEntry}'`, `"${f.unusedEntry}"`, f.unusedEntry].find((n) => text.includes(n));
        const at = needle === undefined ? { file: rel, line: 1, column: 0 } : atText(this.ctx, rel, needle);
        return { ...at, global: true };
      }
      return { file: null, line: 1, column: 0, global: true };
    }
    const m = /^([^:\s]+):(\d+)$/.exec(f.subject);
    if (m && linted.has(m[1])) return { file: m[1], line: Number(m[2]), column: 0, global: false };
    const file = /^([^:\s]+):/.exec(f.subject);
    if (file && linted.has(file[1])) return { file: file[1], line: 1, column: 0, global: false };
    return { file: null, line: 1, column: 0, global: true };
  }

  /**
   * Check `check`'s outcome, computed once: `{findings, checked}` (each
   * finding located), `{cannot: reason}` or `{nothing: text}`.
   */
  result(check) {
    if (this.results.has(check)) return this.results.get(check);
    const out = this.compute(check);
    this.results.set(check, out);
    return out;
  }

  compute(check) {
    const reasons = (check.needs || []).map((n) => this.ctx.missing(n)).filter((r) => r !== null);
    if (reasons.length > 0) return { cannot: reasons.join('; ') };
    let result;
    try {
      result = check.check(this.ctx);
    } catch (e) {
      return { cannot: e.message };
    }
    if (!result || !Array.isArray(result.findings)) return { cannot: 'its check returned no findings list' };
    let findings = result.findings;
    if (this.baseline) {
      findings = [...this.baseline.waive(findings), ...this.baseline.leftovers(Baseline.forKinds(check.kinds))];
    }
    if (findings.length === 0 && !(Number.isInteger(result.checked) && result.checked > 0)) {
      const count = result.checked === 0 ? '0 item(s) checked' : `no item count (checked: ${String(result.checked)})`;
      return { nothing: `${count}; a rule that checks nothing cannot pass`, checked: result.checked };
    }
    return {
      checked: result.checked,
      findings: findings.map((f) => ({ ...f, where: this.locate(check, f) })),
    };
  }

  /**
   * The reports of check `check` on file `rel`: `{line, column, kind, text,
   * global}`, a project finding on every file (at its own line in its own
   * file, line 1 elsewhere), another only on its file.
   */
  reports(check, rel) {
    const r = this.result(check);
    if (r.cannot !== undefined) {
      return [{ line: 1, column: 0, kind: 'cannot-run', text: `${check.name}: cannot run, ${r.cannot}`, global: true }];
    }
    if (r.nothing !== undefined) {
      return [{ line: 1, column: 0, kind: 'nothing-checked', text: `${check.name}: ${r.nothing}`, global: true }];
    }
    const out = [];
    for (const f of r.findings) {
      const own = f.where.file === rel;
      if (!own && !f.where.global) continue;
      const text = `${f.subject}: ${f.message}`;
      out.push({ line: own ? f.where.line : 1, column: own ? f.where.column : 0, kind: f.kind, text, global: f.where.global });
      if (own && f.where.atStart) out.push({ line: 1, column: 0, kind: f.kind, text, global: f.where.global });
    }
    return out;
  }

  /**
   * `kafka/baseline`'s reports, on every file (`{kind, subject, message,
   * line, column, text, global}`): the baseline's `prefix:` lines and lines of
   * no check's kind that waive nothing, and duplicate `prefix:` lines, judged
   * once every check ran.
   */
  baselineReports() {
    if (this.baseline === null) return [];
    const results = this.checks.map((c) => this.result(c));
    if (results.some((r) => r.cannot !== undefined)) return [];
    const kinds = this.checks.flatMap((c) => c.kinds);
    return this.baseline
      .leftovers(Baseline.outsideKinds(kinds))
      .map((f) => ({ ...f, line: 1, column: 0, text: `${f.subject}: ${f.message}`, global: true }));
  }
}

/**
 * The signature of the analysis's inputs: every file of the client
 * directory, the Java index and the bindings header, each by size and
 * modification time, so any edit is seen.
 */
function signature(root, options = {}) {
  const ctx = new Context({ root, javaIndexPath: options.javaIndexPath, headerPath: options.headerPath });
  const stat = (file) => {
    try {
      const st = fs.statSync(file);
      return `${st.size}:${st.mtimeMs}`;
    } catch (e) {
      if (e.code === 'ENOENT') return 'none';
      throw e;
    }
  };
  const files = ctx.allFiles().map((rel) => `${rel}:${stat(path.join(root, rel))}`);
  return [...files, `index:${stat(ctx.javaIndexPath)}`, `header:${stat(ctx.headerPath)}`].join('\n');
}

/**
 * The analysis of client directory `root` (the configuration's
 * `settings.kafka.root`), for the file whose SourceCode is `sourceCode`: the
 * rules of one file share one; a new file compares the inputs' signature,
 * and a change makes a new analysis.
 */
function projectOf(root, sourceCode) {
  if (sourceCode !== undefined && bySource.has(sourceCode)) return bySource.get(sourceCode);
  const key = path.resolve(root);
  const options = overrides.get(key);
  const sig = signature(key, options);
  let hit = cache.get(key);
  if (!hit || hit.sig !== sig) {
    // One client directory at a time: a lint run reads one, and a program is large.
    cache.clear();
    hit = { project: new Project(key, options), sig };
    cache.set(key, hit);
  }
  if (sourceCode !== undefined) bySource.set(sourceCode, hit.project);
  return hit.project;
}

module.exports = { BASELINE, configure, projectOf, Project };
