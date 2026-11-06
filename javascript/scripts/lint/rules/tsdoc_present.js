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

// D1 check-tsdoc-present (javascript/CLAUDE.md "Files and layout": the prose
// docs are TSDoc comments; root CLAUDE.md §6): every export of a public
// module, and every public member of an exported class, interface or object
// type, has a TSDoc (`/** ... */`) comment. A get / set pair, and an
// overload set, needs one. A comment documents the item only when its
// description (the text before its first block tag) is not blank: an empty
// `/** */`, or one holding only tags, is `doc-empty`.

const { atNode } = require('../location');
const { documentedItems } = require('../tsdoc');

/** The description of JSDoc comment `doc`, trimmed: its text before the first block tag. */
function description(ts, doc) {
  const c = doc.comment;
  if (c === undefined) return '';
  return (typeof c === 'string' ? c : ts.getTextOfJSDocComment(c) || '').trim();
}

module.exports = {
  name: 'check-tsdoc-present',
  kinds: ['doc-missing', 'doc-empty'],
  needs: ['program'],
  hint: 'Document it with a TSDoc comment, translated from its Javadoc where Java has one (root CLAUDE.md §6).',
  check(ctx) {
    const { ts } = ctx.program();
    const findings = [];
    let checked = 0;
    for (const d of documentedItems(ctx)) {
      checked += 1;
      if (d.docs.length === 0) findings.push({ at: atNode(ctx, d.node), kind: 'doc-missing', subject: d.subject, message: 'has no TSDoc comment' });
      else if (!d.docs.some((doc) => description(ts, doc) !== '')) {
        findings.push({ at: atNode(ctx, d.node), kind: 'doc-empty', subject: d.subject, message: 'has a TSDoc comment with no description (an empty comment, or tags only)' });
      }
    }
    return { findings, checked };
  },
};
