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

// H1 check-no-todo (javascript/CLAUDE.md "Files and process": every file,
// root CLAUDE.md §7): no unfinished-work marker in any file of the client
// (every file under javascript/: the sources, tests, scripts, tools, the gRPC
// harness, the build files, the docs), in code, comments or strings alike.
// Dependencies, outputs and notes (`node_modules/`, `build/`, `dist/`,
// `.claude/`), the generated `package-lock.json` and binary files are not
// read. The markers are spelt in pieces here so this file does not carry
// them.
//
// One sentence is exempt, with its reason: javascript/CLAUDE.md states the
// rule itself, naming both markers. Only the markers inside that sentence are
// waived, wherever it sits; any other marker of the file is a finding. An
// exemption whose sentence is not found (the file gone, renamed or reworded)
// is a finding, so the list cannot grow stale.

const MARKERS = ['TO' + 'DO', 'FIX' + 'ME'];

const PATTERN = new RegExp(`\\b(${MARKERS.join('|')})\\b`, 'g');

/** Files never read, besides those under the skipped directories. */
const SKIPPED_FILES = new Set(['package-lock.json']);

/** The sentences whose markers are not findings: file, sentence (whitespace runs matching any whitespace), reason. */
const EXEMPT = [['CLAUDE.md', `No ${MARKERS[0]} or ${MARKERS[1]}.`, 'it states the rule itself']];

/** The TypeScript and JavaScript files: ESLint's side of the rule (`kafka/no-todo`). */
const CODE = /\.(ts|js|cjs|mjs)$/;

/** `sentence` as a regular expression, each run of spaces matching any whitespace (a line break included). */
function sentencePattern(sentence) {
  const escaped = sentence.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(escaped.replace(/ +/g, '\\s+'), 'g');
}

module.exports = {
  name: 'check-no-todo',
  /**
   * The files this instance reads (PLAN.md §3): `all`, `code` (TypeScript and
   * JavaScript, ESLint's `kafka/no-todo`) or `other` (the rest, the runner's).
   */
  PART: 'all',
  /** Whether this instance reads root-relative file `rel`. */
  covers(rel) {
    return this.PART === 'all' || (this.PART === 'code') === CODE.test(rel);
  },
  kinds: ['todo', 'todo-unused-exemption'],
  needs: [],
  hint:
    `Finish the work the ${MARKERS.join(' / ')} marks, or delete the note: ` +
    'nothing is left for later (root CLAUDE.md §7).',
  check(ctx) {
    const findings = [];
    const used = new Set();
    let checked = 0;
    for (const rel of ctx.allFiles()) {
      if (SKIPPED_FILES.has(rel.split('/').pop()) || !this.covers(rel)) continue;
      const text = ctx.read(rel);
      if (text.includes('\u0000')) continue;
      checked += 1;
      // The [start, end) offsets of this file's exempt sentences.
      const waived = [];
      for (const [file, sentence] of EXEMPT) {
        if (file !== rel) continue;
        for (const m of text.matchAll(sentencePattern(sentence))) {
          waived.push([m.index, m.index + m[0].length]);
          used.add(`${file}\u0000${sentence}`);
        }
      }
      let line = 1;
      let lineStart = 0;
      for (const m of text.matchAll(PATTERN)) {
        for (; lineStart < m.index; lineStart++) if (text[lineStart] === '\n') line++;
        if (waived.some(([from, to]) => m.index >= from && m.index < to)) continue;
        findings.push({
          kind: 'todo',
          subject: `${rel}:${line}`,
          message: `\`${m[1]}\` marker: finish the work or delete the note`,
        });
      }
    }
    for (const [file, sentence, reason] of EXEMPT) {
      if (!this.covers(file)) continue;
      if (!used.has(`${file}\u0000${sentence}`)) {
        findings.push({
          kind: 'todo-unused-exemption',
          subject: file,
          unusedEntry: sentence,
          message: `exempts the sentence "${sentence}" as ${reason}, but the file does not carry it: delete the exemption`,
        });
      }
    }
    return { findings, checked };
  },
};
