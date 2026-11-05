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

const { run } = require('../../scripts/lint_rules');

/** A tsconfig.json like the client's, for the rules that build the TypeScript program. */
const TSCONFIG = JSON.stringify({
  compilerOptions: { strict: true, target: 'es2022', lib: ['es2022'], module: 'node16', rootDir: 'lib', noEmit: true },
  include: ['lib/**/*.ts'],
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

/** A new temporary directory holding `files` (relative path to text); returns its path. */
function project(files) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'js-lint-'));
  made.push(root);
  for (const [rel, text] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(root, rel)), { recursive: true });
    fs.writeFileSync(path.join(root, rel), text);
  }
  return root;
}

/** Copies directory or file `rel` of `from` into `to`. */
function copy(from, to, rel) {
  fs.cpSync(path.join(from, rel), path.join(to, rel), { recursive: true });
}

/**
 * Runs the linter with `args` over `root`: `{status, out, err, text}`, `out`
 * and `err` the lines written to each stream and `text` both, in order.
 */
function lint(root, args = [], options = {}) {
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

/** The findings lines of a report (those indented by two spaces), without the indent. */
function findings(report) {
  return report.err.filter((l) => l.startsWith('  ')).map((l) => l.slice(2));
}

/** The names of the rules a report failed. */
function failedRules(report) {
  return report.err
    .map((l) => /^❌ (check-[\w-]+):/.exec(l))
    .filter((m) => m !== null)
    .map((m) => m[1]);
}

module.exports = { TSCONFIG, TODO_MARK, FIXME_MARK, CLAUDE_MD, project, copy, lint, findings, failedRules };
