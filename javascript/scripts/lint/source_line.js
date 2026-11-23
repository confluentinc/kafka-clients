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

// The references to a line of a Python, C#, Rust, Scala or C++ source that
// javascript/CLAUDE.md (Comments, point 4) bans from comments, beside the
// Java ones of scripts/lint/java_line.js, whose helpers it shares. The forms
// are listed on `sourceLineRefsRun`, in words, since this file's own
// comments are linted too.

const { lineBody } = require('./comment_sources');
const { commentRuns, countWhile, wordStarts, skipLineWord, lineNumber } = require('./java_line');

/** The extensions of the languages read here: Python, C#, Rust, Scala (Kafka's tests) and C++ (Node's sources). */
const EXTENSIONS = ['py', 'pyi', 'cs', 'rs', 'scala', 'cc'];

/** A file name with one of the extensions. */
const FILE = new RegExp(`[\\p{L}\\p{N}_-]+\\.(?:${EXTENSIONS.join('|')})(?![\\p{L}\\p{N}_])`, 'gu');

/** The words naming one of the languages. */
const LANGUAGE_WORDS = ['Python', 'Rust', 'Scala', 'C#', 'C++', '.NET'];

const DIGIT = /[0-9]/;
const SPACE = /\s/u;

/** Whether `text` names a file with one of the extensions, or one of the languages. */
function namesSource(text) {
  FILE.lastIndex = 0;
  return FILE.test(text) || LANGUAGE_WORDS.some((w) => wordStarts(text, w).length > 0);
}

/** The line or range a colon is followed by in `s` (`s` past the colon), or null. */
function colonLine(s) {
  let end = countWhile(s, DIGIT);
  if (end === 0) return null;
  for (const sep of ['-', '–']) {
    if (s.slice(end).startsWith(sep)) {
      const more = countWhile(s.slice(end + sep.length), DIGIT);
      if (more > 0) end += sep.length + more;
      break;
    }
  }
  return s.slice(0, end);
}

/** The candidate references in `text`: `[start, end, needs-context, reference]`. */
function sourceLineHits(text) {
  const out = [];
  FILE.lastIndex = 0;
  for (const m of text.matchAll(FILE)) {
    const start = m.index;
    const name = m[0];
    let after = text.slice(start + name.length);
    let skipped = name.length;
    if (after.startsWith('`')) {
      after = after.slice(1);
      skipped += 1;
    }
    if (after.startsWith(':')) {
      const number = colonLine(after.slice(1));
      if (number !== null) out.push([start, start + skipped + 1 + number.length, false, `${name}:${number}`]);
      continue;
    }
    // Blanks, or a comma or an opening parenthesis with blanks around it.
    const sep = /^(?:\s*[,(]\s*|\s+)/u.exec(after);
    if (sep === null) continue;
    const rest = after.slice(sep[0].length);
    const lineWord = skipLineWord(rest);
    if (lineWord !== null) {
      const number = lineNumber(lineWord);
      if (number !== null) out.push([start, text.length - lineWord.length + number.length, false, `${name} line ${number}`]);
    } else if (SPACE.test(sep[0][0]) && sep[0].trim() === '') {
      const number = lineNumber(rest);
      if (number !== null && /[-–/]/.test(number)) out.push([start, text.length - rest.length + number.length, false, `${name} ${number}`]);
    }
  }
  for (const at of [...wordStarts(text, 'line'), ...wordStarts(text, 'lines')]) {
    const rest = skipLineWord(text.slice(at));
    if (rest === null) continue;
    const number = lineNumber(rest);
    if (number !== null) out.push([at, text.length - rest.length + number.length, true, `line ${number}`]);
  }
  for (const at of wordStarts(text, 'at')) {
    let rest = text.slice(at + 'at'.length);
    if (!SPACE.test(rest[0] || '')) continue;
    rest = rest.slice(1).trimStart();
    const number = lineNumber(rest);
    if (number !== null && /[-–/]/.test(number)) out.push([at, text.length - rest.length + number.length, true, `at ${number}`]);
  }
  out.sort((a, b) => a[0] - b[0]);
  // A hit inside another's span (the line word of a file's own form) is the same reference.
  const kept = [];
  for (const hit of out) {
    if (kept.some(([s, e]) => hit[0] >= s && hit[0] < e)) continue;
    kept.push(hit);
  }
  return kept;
}

/**
 * The references in `lines`, one run of consecutive comment lines, as
 * `[index of the line the reference starts on, reference]`. The lines are
 * joined, their comment markers dropped, so a reference that wraps onto the
 * next line counts. The forms:
 *   - a file name with one of the extensions, possibly in a code span,
 *     followed by a colon and a line or range;
 *   - such a file name followed by a space and a range (a single number
 *     after a space is too often a count to read as a line);
 *   - such a file name followed by the word line or lines and a line or
 *     range, a comma or an opening parenthesis allowed between them;
 *   - the word line or lines and a number, or the word at and a range, when
 *     the line or the one above names such a file or one of the languages.
 */
function sourceLineRefsRun(lines) {
  let joined = '';
  const starts = [];
  for (const line of lines) {
    if (starts.length > 0) joined += ' ';
    starts.push(joined.length);
    joined += lineBody(line);
  }
  const lineOf = (at) => {
    let i = 0;
    while (i + 1 < starts.length && starts[i + 1] <= at) i++;
    return i;
  };
  const span = (first, last) => joined.slice(starts[first], last + 1 < starts.length ? starts[last + 1] - 1 : joined.length);
  return sourceLineHits(joined)
    .filter(([start, end, needsContext]) => {
      if (!needsContext) return true;
      const first = lineOf(start);
      const last = lineOf(Math.max(end - 1, start));
      return namesSource(span(Math.max(first - 1, 0), last));
    })
    .map(([start, , , reference]) => [lineOf(start), reference]);
}

/** The references in `text`, a comment with no comment line before it. */
function sourceLineRefs(text) {
  return sourceLineRefsRun([text]).map(([, r]) => r);
}

/** The references of a file's comments (`{line, text}`): `[line, reference]`. */
function sourceLineRefsOfComments(comments) {
  const out = [];
  for (const run of commentRuns(comments)) {
    for (const [index, reference] of sourceLineRefsRun(run.map(([, t]) => t))) out.push([run[index][0], reference]);
  }
  return out;
}

module.exports = { EXTENSIONS, sourceLineRefs, sourceLineRefsRun, sourceLineRefsOfComments };
