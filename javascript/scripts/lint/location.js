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

const path = require('node:path');

// Where a finding is reported: `{file, line, column}`, `file` relative to the
// client's directory, `line` 1-based, `column` 0-based (ESLint's). A check
// puts it in a finding's `at`; the ESLint plugin reports the finding there
// (scripts/lint/eslint-plugin/analysis.js).

/** The start of TypeScript node `node`. */
function atNode(ctx, node) {
  const sf = node.getSourceFile();
  const p = sf.getLineAndCharacterOfPosition(node.getStart(sf));
  const file = path.isAbsolute(sf.fileName) ? ctx.rel(sf.fileName) : sf.fileName;
  return { file, line: p.line + 1, column: p.character };
}

/** Line `line` (1-based) of file `file`. */
function atLine(file, line, column = 0) {
  return { file, line, column };
}

/** The line of byte offset `offset` in file `file`. */
function atOffset(ctx, file, offset) {
  const text = ctx.read(file);
  let line = 1;
  for (let i = 0; i < offset && i < text.length; i++) if (text[i] === '\n') line++;
  return { file, line, column: 0 };
}

/** The first line of file `file` holding `needle`, or its first line. */
function atText(ctx, file, needle) {
  let text;
  try {
    text = ctx.read(file);
  } catch (e) {
    if (e.code === 'ENOENT') return { file, line: 1, column: 0 };
    throw e;
  }
  const i = text.indexOf(needle);
  return i < 0 ? { file, line: 1, column: 0 } : atOffset(ctx, file, i);
}

module.exports = { atNode, atLine, atOffset, atText };
