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

// check-source-line (javascript/CLAUDE.md, Comments, point 4): no comment of
// the files the Comments rule covers (scripts/lint/comment_sources.js) cites a
// line of a Python, C#, Rust, Scala or C++ source (scripts/lint/source_line.js);
// it names the class, member or file instead. check-java-line reads the Java
// references. One finding per file, as check-java-line reports, keyed by the
// file.

const { scopeFiles, commentsOf } = require('../comment_sources');
const { sourceLineRefsOfComments } = require('../source_line');

module.exports = {
  name: 'check-source-line',
  kinds: ['source-line'],
  /**
   * The files this instance reads: `all`, `ts` (TypeScript and JavaScript,
   * ESLint's `kafka/source-line`) or `other` (the rest, the runner's).
   */
  PART: 'all',
  get needs() {
    return this.PART === 'other' ? [] : ['typescript'];
  },
  hint:
    'Name the class, member or file, not a line of it: lines move with every change and cause needless edits ' +
    '(javascript/CLAUDE.md, Comments, point 4).',
  check(ctx) {
    const findings = [];
    const files = scopeFiles(ctx, this.PART);
    for (const rel of files) {
      const refs = sourceLineRefsOfComments(commentsOf(ctx, rel));
      if (refs.length === 0) continue;
      const [line, first] = refs[0];
      findings.push({
        kind: 'source-line',
        subject: rel,
        at: { file: rel, line, column: 0 },
        message:
          `${refs.length} Python, C#, Rust, Scala or C++ file:line reference(s) in comments, the first \`${first}\` ` +
          `at line ${line}: name the class, member or file instead`,
      });
    }
    return { findings, checked: files.length };
  },
};
