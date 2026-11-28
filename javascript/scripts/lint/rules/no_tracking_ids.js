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

// check-no-tracking-ids (javascript/CLAUDE.md, Comments, point 5): no comment
// of the files the Comments rule covers (scripts/lint/comment_sources.js)
// holds a phase, review or question ID (scripts/lint/tracking_ids.js): the
// design notes and the rules hold them. A comment's lines are read as one
// text, so an ID wrapped onto the next line counts. One finding per ID, on
// its line.

const { scopeFiles, commentsOf, lineBody } = require('../comment_sources');
const { commentRuns } = require('../java_line');
const { trackingIds } = require('../tracking_ids');

/** The IDs of `comments` (`{line, text}`): `{line, column, id, what}`. */
function idsOfComments(comments) {
  const out = [];
  for (const run of commentRuns(comments)) {
    let joined = '';
    const starts = [];
    for (const [, physical] of run) {
      if (starts.length > 0) joined += ' ';
      starts.push(joined.length);
      joined += lineBody(physical);
    }
    for (const { at, id, what } of trackingIds(joined)) {
      let i = 0;
      while (i + 1 < starts.length && starts[i + 1] <= at) i++;
      const [line, physical] = run[i];
      const column = Math.max(0, physical.indexOf(id.split(' ')[0]));
      out.push({ line, column, id, what });
    }
  }
  return out;
}

module.exports = {
  name: 'check-no-tracking-ids',
  kinds: ['tracking-id'],
  /**
   * The files this instance reads: `all`, `ts` (TypeScript and JavaScript,
   * ESLint's `kafka/no-tracking-ids`) or `other` (the rest, the runner's).
   */
  PART: 'all',
  get needs() {
    return this.PART === 'other' ? [] : ['typescript'];
  },
  hint:
    "Drop the ID, keeping the rule group's or the decision's name: the design notes and the rules hold the IDs " +
    '(javascript/CLAUDE.md, Comments, point 5).',
  check(ctx) {
    const findings = [];
    const files = scopeFiles(ctx, this.PART);
    for (const rel of files) {
      for (const { line, column, id, what } of idsOfComments(commentsOf(ctx, rel))) {
        findings.push({
          kind: 'tracking-id',
          subject: `${rel}:${line}`,
          at: { file: rel, line, column },
          message: `\`${id}\`, ${what}, in a comment: drop it`,
        });
      }
    }
    return { findings, checked: files.length };
  },
};
