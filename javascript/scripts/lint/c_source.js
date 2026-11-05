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

// Reads the addon's C sources (src/*.c, src/*.h) and the bindings header for
// the rule linter (scripts/lint_rules.js) and the header-conformance test. A
// tokenizer, not a C parser: the sources are written in one plain style, and
// what the rules need (comments, identifiers, function bodies, call sites and
// the N-API property table) follows from comments, literals and brackets.

/** Strips C block and line comments (a plain text pass, string literals not excepted). */
function stripComments(text) {
  return text.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
}

/** The kafka_* functions `text` calls: an identifier followed by `(`, comments stripped. */
function calledFunctions(text) {
  const code = stripComments(text);
  const names = new Set();
  const re = /\b(kafka_\w+)\s*\(/g;
  let m;
  while ((m = re.exec(code)) !== null) names.add(m[1]);
  return names;
}

/** The names of every function the bindings header declares. */
function declaredFunctions(text) {
  const names = new Set();
  const code = stripComments(text);
  // A declaration: a return type at column 0, then the name and `(`.
  const re = /^[A-Za-z_][\w \t*]*?\b(kafka_\w+)\s*\(/gm;
  let m;
  while ((m = re.exec(code)) !== null) names.add(m[1]);
  return names;
}

/**
 * `text` scanned once: `noComments` has every comment replaced by spaces, and
 * `code` also the contents of string and character literals (their quotes
 * kept), both with the newlines kept, so an offset means the same place in
 * all three; `comments` lists each comment as `{line, text}` (1-based line),
 * and `tokens` each identifier, number, literal and punctuation mark as
 * `{type, value, line, start, end}`, `type` one of `ident`, `number`,
 * `string`, `char` and `punct`.
 */
function scan(text) {
  const noComments = text.split('');
  const code = text.split('');
  const comments = [];
  const tokens = [];
  const blank = (arr, from, to) => {
    for (let k = from; k < to; k++) if (arr[k] !== '\n') arr[k] = ' ';
  };
  let line = 1;
  let i = 0;
  const lineAt = (from, to) => {
    for (let k = from; k < to; k++) if (text[k] === '\n') line++;
  };
  while (i < text.length) {
    const c = text[i];
    const next = text[i + 1];
    if (c === '/' && next === '/') {
      let j = i;
      while (j < text.length && text[j] !== '\n') j++;
      comments.push({ line, text: text.slice(i, j) });
      blank(noComments, i, j);
      blank(code, i, j);
      i = j;
    } else if (c === '/' && next === '*') {
      const close = text.indexOf('*/', i + 2);
      const j = close < 0 ? text.length : close + 2;
      comments.push({ line, text: text.slice(i, j) });
      blank(noComments, i, j);
      blank(code, i, j);
      lineAt(i, j);
      i = j;
    } else if (c === '"' || c === "'") {
      let j = i + 1;
      while (j < text.length && text[j] !== c && text[j] !== '\n') j += text[j] === '\\' ? 2 : 1;
      j = Math.min(j + 1, text.length);
      tokens.push({ type: c === '"' ? 'string' : 'char', value: text.slice(i, j), line, start: i, end: j });
      blank(code, i + 1, j - 1);
      i = j;
    } else if (/[A-Za-z_]/.test(c)) {
      let j = i + 1;
      while (j < text.length && /\w/.test(text[j])) j++;
      tokens.push({ type: 'ident', value: text.slice(i, j), line, start: i, end: j });
      i = j;
    } else if (/[0-9]/.test(c)) {
      let j = i + 1;
      while (j < text.length && /[\w.]/.test(text[j])) j++;
      tokens.push({ type: 'number', value: text.slice(i, j), line, start: i, end: j });
      i = j;
    } else {
      if (c === '\n') line++;
      else if (!/\s/.test(c)) tokens.push({ type: 'punct', value: c, line, start: i, end: i + 1 });
      i++;
    }
  }
  return { text, noComments: noComments.join(''), code: code.join(''), comments, tokens };
}

/** A function from an offset of `text` to its 1-based line. */
function lineIndex(text) {
  const starts = [0];
  for (let k = 0; k < text.length; k++) if (text[k] === '\n') starts.push(k + 1);
  return (at) => {
    let lo = 0;
    let hi = starts.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (starts[mid] <= at) lo = mid;
      else hi = mid - 1;
    }
    return lo + 1;
  };
}

/** The identifier ending just before offset `k` of `code` (blanks skipped), as `{name, index}`, or null. */
function identBefore(code, k) {
  let end = k;
  while (end > 0 && /\s/.test(code[end - 1])) end--;
  let start = end;
  while (start > 0 && /\w/.test(code[start - 1])) start--;
  if (start === end || !/[A-Za-z_]/.test(code[start])) return null;
  return { name: code.slice(start, end), index: start };
}

/** The offset just past the bracket closing the one at `open` in `code` (`(`, `[` or `{`), or -1. */
function matching(code, open) {
  const pairs = { '(': ')', '[': ']', '{': '}' };
  const want = pairs[code[open]];
  let depth = 0;
  for (let k = open; k < code.length; k++) {
    if (code[k] === code[open]) depth++;
    else if (code[k] === want && --depth === 0) return k + 1;
  }
  return -1;
}

/** `s` with its whitespace runs collapsed to one space, trimmed. */
function collapse(s) {
  return s.replace(/\s+/g, ' ').trim();
}

/** The top-level comma-separated parts of `code[from, to)`, sliced from `source`, each collapsed. */
function splitArgs(code, source, from, to) {
  const out = [];
  let depth = 0;
  let start = from;
  for (let k = from; k < to; k++) {
    const ch = code[k];
    if (ch === '(' || ch === '[' || ch === '{') depth++;
    else if (ch === ')' || ch === ']' || ch === '}') depth--;
    else if (ch === ',' && depth === 0) {
      out.push(collapse(source.slice(start, k)));
      start = k + 1;
    }
  }
  const last = collapse(source.slice(start, to));
  if (last !== '' || out.length > 0) out.push(last);
  return out;
}

// Words followed by `(` that call nothing: keywords, and the type names of a
// cast or a function-pointer declaration.
const NOT_CALLS = new Set([
  'if', 'while', 'for', 'switch', 'return', 'sizeof', 'defined', '_Alignof', 'alignof', 'case', 'else', 'do',
  'void', 'int', 'char', 'long', 'short', 'unsigned', 'signed', 'double', 'float', 'const', 'static', 'struct',
  'enum', 'union', 'bool', 'size_t',
]);

/**
 * The function definitions of a C source, by name: `{name, line, start, end,
 * bodyStart, bodyEnd, signature, body}`, `start` the offset of the line the
 * definition starts on, `body` the text between its braces (comments kept).
 * A definition is a top-level `name(...)` followed by `{`.
 */
function functionBodies(text) {
  const { code } = scan(text);
  const lineAt = lineIndex(text);
  const out = new Map();
  let depth = 0;
  for (let k = 0; k < code.length; k++) {
    const ch = code[k];
    if (ch === '{') depth++;
    else if (ch === '}') depth--;
    else if (ch === '(' && depth === 0) {
      const before = identBefore(code, k);
      const close = matching(code, k);
      if (close < 0) break;
      const after = /^\s*\{/.exec(code.slice(close, close + 64));
      if (!before || !after || NOT_CALLS.has(before.name)) {
        k = close - 1;
        continue;
      }
      const bodyStart = close + after[0].length;
      const end = matching(code, bodyStart - 1);
      if (end < 0) break;
      const start = code.lastIndexOf('\n', before.index) + 1;
      out.set(before.name, {
        name: before.name,
        line: lineAt(before.index),
        start,
        end,
        bodyStart,
        bodyEnd: end - 1,
        signature: collapse(text.slice(start, close)),
        body: text.slice(bodyStart, end - 1),
      });
      k = end - 1;
    }
  }
  return out;
}

/** Whether offset `at` of `code` is on a preprocessor line, or a `\`-continued line of one. */
function inDirective(code, at) {
  let lineStart = code.lastIndexOf('\n', at - 1) + 1;
  for (;;) {
    if (/^\s*#/.test(code.slice(lineStart, lineStart + 64))) return true;
    if (lineStart === 0) return false;
    const prevEnd = lineStart - 1;
    if (!/\\\s*$/.test(code.slice(code.lastIndexOf('\n', prevEnd - 1) + 1, prevEnd))) return false;
    lineStart = code.lastIndexOf('\n', prevEnd - 1) + 1;
  }
}

/**
 * The call sites of a C source, in order: `{name, args, line, start, end,
 * function}`, `args` the argument texts (comments dropped, whitespace
 * collapsed), `function` the name of the definition enclosing it (or
 * `null`), `directive` whether it sits on a preprocessor line (a macro body;
 * the macro's own name in its `#define` is not a call).
 * A call is an identifier followed by `(` that is neither a keyword nor a
 * definition. With `name`, only the calls of that function.
 */
function callSites(text, name) {
  const { code, noComments } = scan(text);
  const lineAt = lineIndex(text);
  const bodies = [...functionBodies(text).values()];
  const out = [];
  const re = /\b([A-Za-z_]\w*)\s*\(/g;
  let m;
  while ((m = re.exec(code)) !== null) {
    const callee = m[1];
    if (NOT_CALLS.has(callee) || (name !== undefined && callee !== name)) continue;
    const directive = inDirective(code, m.index);
    // A macro's own name in its `#define NAME(...)`.
    if (directive && /#\s*define\s+$/.test(code.slice(code.lastIndexOf('\n', m.index) + 1, m.index))) continue;
    const enclosing = bodies.find((b) => m.index >= b.bodyStart && m.index < b.bodyEnd);
    // Outside every body and every macro: a declaration, not a call.
    if (!enclosing && !directive) continue;
    const open = m.index + m[0].length - 1;
    const close = matching(code, open);
    if (close < 0) continue;
    out.push({
      name: callee,
      args: splitArgs(code, noComments, open + 1, close - 1),
      line: lineAt(m.index),
      start: m.index,
      end: close,
      function: enclosing ? enclosing.name : null,
      directive,
    });
  }
  return out;
}

/**
 * The entries of every `napi_property_descriptor <name>[] = { ... };` table
 * of a C source: `{name, method, getter, setter, line}`, `name` the property
 * name and the others the C functions of the descriptor's 3rd, 4th and 5th
 * fields (`null` for `NULL`).
 */
function propertyDescriptors(text) {
  const { code, noComments } = scan(text);
  const lineAt = lineIndex(text);
  const out = [];
  const re = /\bnapi_property_descriptor\s+\w+\s*\[\s*\]\s*=\s*\{/g;
  let m;
  while ((m = re.exec(code)) !== null) {
    const open = m.index + m[0].length - 1;
    const close = matching(code, open);
    if (close < 0) continue;
    for (let k = open + 1; k < close - 1; k++) {
      if (code[k] !== '{') continue;
      const end = matching(code, k);
      const fields = splitArgs(code, noComments, k + 1, end - 1);
      const fn = (field) => (field === undefined || field === 'NULL' || field === '0' ? null : field);
      out.push({
        name: JSON.parse(fields[0]),
        method: fn(fields[2]),
        getter: fn(fields[3]),
        setter: fn(fields[4]),
        line: lineAt(k),
      });
      k = end - 1;
    }
  }
  return out;
}

/**
 * The function prototypes of the bindings header, by name: `{name,
 * returnType, params}`, each parameter `{type, name}` (a function-pointer
 * parameter's whole text as its type), `params` empty for `(void)`.
 */
function prototypes(header) {
  const code = stripComments(header);
  const out = new Map();
  const re = /^([A-Za-z_][\w \t*]*?)\b(kafka_\w+)\s*\(/gm;
  let m;
  while ((m = re.exec(code)) !== null) {
    const returnType = collapse(m[1]);
    if (returnType.startsWith('typedef')) continue;
    const open = m.index + m[0].length - 1;
    const close = matching(code, open);
    if (close < 0) continue;
    const params = splitArgs(code, code, open + 1, close - 1)
      .filter((p) => p !== 'void')
      .map((p) => {
        const pm = /^(.*?)\b([A-Za-z_]\w*)$/.exec(p);
        return pm && !/\(/.test(p) ? { type: collapse(pm[1]), name: pm[2] } : { type: p, name: '' };
      });
    out.set(m[2], { name: m[2], returnType, params });
  }
  return out;
}

module.exports = {
  stripComments,
  calledFunctions,
  declaredFunctions,
  scan,
  functionBodies,
  callSites,
  propertyDescriptors,
  prototypes,
};
