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

// The files and the comments the rules on comments read (javascript/CLAUDE.md,
// Comments): every file of the directories the rule covers whose comments
// the linter can read. A TypeScript or JavaScript file is read through the
// TypeScript parser (ESLint's half of such a rule, the `ts` part); a C source
// or header, a Java source, a linker map or a JSON file (whose `//` and `/* */`
// comments a tsconfig.json may hold) through the C tokenizer, and a gyp file's
// `#` comments by the scanner below (the runner's half, the `other` part).
// Other files (a README) hold no comments.

const { scan } = require('./c_source');
const { sourceComments } = require('./comments');
const { commentBody } = require('./java_line');

/** The directories the rule on comments covers: point 7 of the Comments rule. */
const SCOPE = ['src', 'native', 'test', 'verifiable_clients', 'grpc-server', 'scripts', 'tools'];

/** The extensions read through the TypeScript parser. */
const TS_LIKE = ['.ts', '.js', '.cjs', '.mjs'];

/** The extensions whose comments are C's. */
const C_LIKE = ['.c', '.h', '.java', '.map', '.json'];

/** The extensions whose comments start with `#`. */
const HASH_LIKE = ['.gyp'];

/** The part of a split rule reading root-relative file `rel` (`ts` or `other`), or null for none. */
function partOf(rel) {
  if (TS_LIKE.some((e) => rel.endsWith(e))) return 'ts';
  if (C_LIKE.some((e) => rel.endsWith(e)) || HASH_LIKE.some((e) => rel.endsWith(e))) return 'other';
  return null;
}

/** The files of the scope that part `part` (`ts`, `other` or `all`) reads, root-relative and sorted. */
function scopeFiles(ctx, part) {
  return ctx.files(SCOPE).filter((rel) => {
    const p = partOf(rel);
    return p !== null && (part === 'all' || p === part);
  });
}

/**
 * The `#` comments of `text` (a gyp file: Python's literals, quoted with `'`
 * or `"`), as `{line, text}`, `text` starting at the `#`.
 */
function hashComments(text) {
  const out = [];
  let line = 1;
  let quote = null;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === '\n') {
      line++;
      quote = null;
    } else if (quote !== null) {
      if (c === '\\') i++;
      else if (c === quote) quote = null;
    } else if (c === "'" || c === '"') {
      quote = c;
    } else if (c === '#') {
      let j = i;
      while (j < text.length && text[j] !== '\n') j++;
      out.push({ line, text: text.slice(i, j) });
      i = j - 1;
    }
  }
  return out;
}

/** `line`, a line of a comment, without its comment marker (`//`, `/*`, `*` or `#`) and its blanks. */
function lineBody(line) {
  const trimmed = line.trimStart();
  return trimmed.startsWith('#') ? trimmed.replace(/^#+/, '').trim() : commentBody(line);
}

/** The comments of root-relative file `rel`, as `{line, text}` (1-based line of the comment's start). */
function commentsOf(ctx, rel) {
  if (TS_LIKE.some((e) => rel.endsWith(e))) return sourceComments(ctx.typescript(), ctx.sourceFile(rel));
  if (HASH_LIKE.some((e) => rel.endsWith(e))) return hashComments(ctx.read(rel));
  return scan(ctx.read(rel)).comments;
}

module.exports = { SCOPE, TS_LIKE, C_LIKE, HASH_LIKE, partOf, scopeFiles, commentsOf, hashComments, lineBody };
