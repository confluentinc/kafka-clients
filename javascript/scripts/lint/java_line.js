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

// The references to a Java source line that .claude/rules/documentation.md §7
// bans from comments, found as rust/xtask/src/lint_custom/api_docs.rs finds
// them (`java_line_refs_run` and its helpers, ported one to one), so the Rust
// and the JavaScript lints agree on what counts. The forms are listed on
// `javaLineRefsRun`; they are described in words here, since this file's own
// comments are linted too.

const ALNUM = /[\p{L}\p{N}]/u;
const IDENT = /[\p{L}\p{N}_]/u;
const SPACE = /\s/u;
const DIGIT = /[0-9]/;

/** The number of leading characters of `s` matching `re`. */
function countWhile(s, re) {
  let n = 0;
  while (n < s.length && re.test(s[n])) n++;
  return n;
}

/** The offsets of `word` in `text` as a whole word. */
function wordStarts(text, word) {
  const out = [];
  for (let at = text.indexOf(word); at >= 0; at = text.indexOf(word, at + 1)) {
    const before = at > 0 ? text[at - 1] : '';
    const after = text[at + word.length] || '';
    if (!IDENT.test(before) && !IDENT.test(after)) out.push(at);
  }
  return out;
}

/** `s` after a leading `line` or `lines` and its whitespace, or null. */
function skipLineWord(s) {
  let rest;
  if (s.startsWith('lines')) rest = s.slice(5);
  else if (s.startsWith('line')) rest = s.slice(4);
  else return null;
  const trimmed = rest.trimStart();
  return trimmed.length < rest.length ? trimmed : null;
}

/**
 * The line number or range `s` starts with (a number, two joined by a dash,
 * an en dash or a slash), unless it is a version, part of a word, or zero,
 * which no line is.
 */
function lineNumber(s) {
  const digits = countWhile(s, DIGIT);
  if (digits === 0 || /^0+$/.test(s.slice(0, digits))) return null;
  let end = digits;
  for (const sep of ['-', '–', '/']) {
    if (s.slice(end).startsWith(sep)) {
      const more = countWhile(s.slice(end + sep.length), DIGIT);
      if (more > 0) end += sep.length + more;
      break;
    }
  }
  const rest = s.slice(end);
  const version = rest.startsWith('.') && DIGIT.test(rest[1] || '');
  const word = IDENT.test(rest[0] || '') || rest.startsWith("'s");
  return !version && !word ? s.slice(0, end) : null;
}

/** Whether `text` names a Java file or class: a `.java` file, or a code span holding a Java-style name. */
function namesJavaFileOrClass(text) {
  if (text.includes('.java')) return true;
  return text
    .split('`')
    .filter((_, i) => i % 2 === 1)
    .some((span) => {
      if (span.includes('_') || span.includes('::')) return false;
      const name = span.slice(0, countWhile(span, /[\p{L}\p{N}.#]/u));
      return (
        name.includes('#') ||
        name.endsWith('Test') ||
        name.split('.').some((segment) => /^[a-z]/.test(segment) && /[A-Z]/.test(segment))
      );
    });
}

/** Whether `text` names Java: the word `Java`, a `.java` file, or a Java-style name in a code span. */
function namesJava(text) {
  return wordStarts(text, 'Java').length > 0 || namesJavaFileOrClass(text);
}

/** The candidate references in `text`: `[start, end, what it needs, reference]`. */
function javaLineHits(text) {
  const out = [];
  for (let at = text.indexOf('.java'); at >= 0; at = text.indexOf('.java', at + 1)) {
    const before = text.slice(0, at);
    let nameLen = 0;
    while (nameLen < before.length && /[\p{L}\p{N}_]/u.test(before[before.length - 1 - nameLen])) nameLen++;
    if (nameLen === 0) continue;
    const start = at - nameLen;
    const after = text.slice(at + '.java'.length);
    if (after.startsWith(':')) {
      const digits = countWhile(after.slice(1), DIGIT);
      if (digits > 0) {
        const end = at + '.java:'.length + digits;
        out.push([start, end, 'always', text.slice(start, end)]);
      }
      continue;
    }
    const spaced = after.startsWith('`') ? after.slice(1) : after;
    if (!SPACE.test(spaced[0] || '')) continue;
    const rest = spaced.slice(1).trimStart();
    const number = lineNumber(rest);
    if (number !== null) {
      const end = text.length - rest.length + number.length;
      out.push([start, end, 'always', `${text.slice(start, at)}.java ${number}`]);
    }
  }
  for (let at = text.indexOf('`:'); at >= 0; at = text.indexOf('`:', at + 1)) {
    const after = text.slice(at + 2);
    const digits = countWhile(after, DIGIT);
    if (digits === 0) continue;
    let end = digits;
    if (after.slice(end).startsWith('-')) {
      const more = countWhile(after.slice(end + 1), DIGIT);
      if (more > 0) end += 1 + more;
    }
    if (after.slice(end).startsWith('`')) out.push([at, at + 2 + end + 1, 'always', `\`:${after.slice(0, end)}\``]);
  }
  for (const at of wordStarts(text, 'Java')) {
    let rest = text.slice(at + 'Java'.length);
    if (!SPACE.test(rest[0] || '')) continue;
    rest = rest.slice(1).trimStart();
    rest = skipLineWord(rest) ?? rest;
    const number = lineNumber(rest);
    if (number !== null) out.push([at, text.length - rest.length + number.length, 'always', `Java ${number}`]);
  }
  for (const at of [...wordStarts(text, 'line'), ...wordStarts(text, 'lines')]) {
    if (text.slice(0, at).trimEnd().endsWith('Java')) continue;
    const rest = skipLineWord(text.slice(at));
    if (rest === null) continue;
    const number = lineNumber(rest);
    if (number !== null) out.push([at, text.length - rest.length + number.length, 'line-word', `line ${number}`]);
  }
  for (const at of wordStarts(text, 'at')) {
    let rest = text.slice(at + 'at'.length);
    if (!SPACE.test(rest[0] || '')) continue;
    rest = rest.slice(1).trimStart();
    const number = lineNumber(rest);
    if (number === null) continue;
    // A bare number after `at` is mostly a value, so only a range, or a
    // number right after a Java name in a code span, counts.
    const range = /[-–/]/.test(number);
    const before = text.slice(0, at).trimEnd();
    let afterJavaSpan = false;
    if (before.endsWith('`')) {
      const open = before.slice(0, -1).lastIndexOf('`');
      afterJavaSpan = open >= 0 && namesJavaFileOrClass(before.slice(open));
    }
    if (range || afterJavaSpan) out.push([at, text.length - rest.length + number.length, 'at', `at ${number}`]);
  }
  out.sort((a, b) => a[0] - b[0]);
  return out;
}

/** `line` without its comment marker (a line comment, a block comment's opening or inner `*`) and its blanks. */
function commentBody(line) {
  const trimmed = line.trimStart();
  for (const marker of ['///', '//!', '//', '/**', '/*!', '/*', '*']) {
    if (trimmed.startsWith(marker)) return trimmed.slice(marker.length).trim();
  }
  return trimmed.trim();
}

/**
 * The Java file:line references in `lines`, one run of consecutive comment
 * lines, as `[index of the line the reference starts on, reference]`. The
 * lines are joined, their comment markers dropped, so a reference that wraps
 * onto the next line counts. The forms:
 *   - a Java file name followed by a colon and a line, or by a space and a
 *     line or range, the file name possibly in a code span; a code span
 *     holding only a colon and a line or range;
 *   - a line or range after the word Java (not a version), possibly after
 *     the word line;
 *   - the word line or lines and a number, when the line or the one above
 *     names Java and the lines name no Rust file and do not say Rust;
 *   - the word at and a range, or a number right after a Java name in a
 *     code span, when the run names a Java file or class and its lines name
 *     no Rust file and do not say Rust.
 */
function javaLineRefsRun(lines) {
  let joined = '';
  const starts = [];
  for (const line of lines) {
    if (starts.length > 0) joined += ' ';
    starts.push(joined.length);
    joined += commentBody(line);
  }
  const lineOf = (at) => {
    let i = 0;
    while (i + 1 < starts.length && starts[i + 1] <= at) i++;
    return i;
  };
  const span = (first, last) =>
    joined.slice(starts[first], last + 1 < starts.length ? starts[last + 1] - 1 : joined.length);
  const rust = (text) => text.includes('.rs') || wordStarts(text, 'Rust').length > 0;
  const javaClass = namesJavaFileOrClass(joined);
  return javaLineHits(joined)
    .filter(([start, end, kind]) => {
      const first = lineOf(start);
      const last = lineOf(Math.max(end - 1, start));
      if (kind === 'always') return true;
      if (kind === 'line-word') return !rust(span(first, last)) && namesJava(span(Math.max(first - 1, 0), last));
      return !rust(span(first, last)) && javaClass;
    })
    .map(([start, , , reference]) => [lineOf(start), reference]);
}

/** The references in `text`, a comment with no comment line before it. */
function javaLineRefs(text) {
  return javaLineRefsRun([text]).map(([, r]) => r);
}

/**
 * The references of a file's comments, `comments` being `{line, text}` (a
 * block comment's text spanning several lines): `[line, reference]`, the
 * comment lines grouped into runs of consecutive lines.
 */
function javaLineRefsOfComments(comments) {
  const runs = [];
  for (const { line, text } of comments) {
    text.split('\n').forEach((physical, offset) => {
      const number = line + offset;
      const run = runs[runs.length - 1];
      if (run && run[run.length - 1][0] + 1 === number) run.push([number, physical]);
      else runs.push([[number, physical]]);
    });
  }
  const out = [];
  for (const run of runs) {
    for (const [index, reference] of javaLineRefsRun(run.map(([, t]) => t))) out.push([run[index][0], reference]);
  }
  return out;
}

module.exports = { javaLineRefsRun, javaLineRefs, javaLineRefsOfComments, commentBody };
