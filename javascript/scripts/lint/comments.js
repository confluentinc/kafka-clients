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

// The comments of a TypeScript or JavaScript source, read through the
// TypeScript parser, so a string, a template or a regular expression literal
// is never taken for one: every comment is trivia before some token, so the
// comments are the comment ranges at the start of every token of the tree,
// trailing ones (on the line of the token before) and leading ones.

/** The comments of parsed source `sf`, in order, as `{line, text}` (1-based line of the comment's start). */
function sourceComments(ts, sf) {
  const text = sf.getFullText();
  const seen = new Map();
  const visit = (node) => {
    if (node.kind >= ts.SyntaxKind.FirstJSDocNode && node.kind <= ts.SyntaxKind.LastJSDocNode) return;
    const at = node.getFullStart();
    const ranges = [...(ts.getTrailingCommentRanges(text, at) || []), ...(ts.getLeadingCommentRanges(text, at) || [])];
    for (const range of ranges) {
      if (!seen.has(range.pos)) seen.set(range.pos, range);
    }
    for (const child of node.getChildren(sf)) visit(child);
  };
  visit(sf);
  return [...seen.values()]
    .sort((a, b) => a.pos - b.pos)
    .map((r) => ({ line: sf.getLineAndCharacterOfPosition(r.pos).line + 1, text: text.slice(r.pos, r.end) }));
}

module.exports = { sourceComments };
