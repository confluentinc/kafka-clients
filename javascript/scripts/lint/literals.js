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

// The string literals of the client's code, for the rules that look for a
// message the binding must not write: in src/, each string, the pieces of a
// `+` chain of strings folded into one, and a template's pieces; in
// native/*.c, each string literal, adjacent literals joined as C joins them,
// comments left out. Each is `{rel, line, pieces}`: the literal text, cut
// where a value is put in (a template's `${x}`, a non-string operand of a
// `+` chain), so a message split around a value still matches (`holds`).
// A C literal's escapes are kept as written.

const { scan } = require('./c_source');

/** Whether node `n` is a string literal or a template with no substitution. */
function isPlainString(ts, n) {
  return ts.isStringLiteral(n) || ts.isNoSubstitutionTemplateLiteral(n);
}

/** The operands of the `+` chain `n`, left to right. */
function chainOperands(ts, n, out = []) {
  if (ts.isBinaryExpression(n) && n.operatorToken.kind === ts.SyntaxKind.PlusToken) {
    chainOperands(ts, n.left, out);
    chainOperands(ts, n.right, out);
  } else if (ts.isParenthesizedExpression(n)) {
    chainOperands(ts, n.expression, out);
  } else {
    out.push(n);
  }
  return out;
}

/**
 * The pieces of the operands `parts`: the text of each run of strings, joined,
 * cut at each value put in; a template's literal pieces join the runs around
 * them.
 */
function piecesOf(ts, parts) {
  const pieces = [''];
  const hole = () => pieces.push('');
  for (const n of parts) {
    if (isPlainString(ts, n)) {
      pieces[pieces.length - 1] += n.text;
    } else if (ts.isTemplateExpression(n)) {
      pieces[pieces.length - 1] += n.head.text;
      for (const span of n.templateSpans) {
        hole();
        pieces[pieces.length - 1] += span.literal.text;
      }
    } else {
      hole();
    }
  }
  return pieces;
}

/** The string literals of src/'s TypeScript. */
function tsLiterals(ctx) {
  return ctx.once('tsLiterals', () => {
    const { ts, sourceFiles } = ctx.program();
    const out = [];
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const line = (n) => sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1;
      const visit = (n) => {
        if (ts.isBinaryExpression(n) && n.operatorToken.kind === ts.SyntaxKind.PlusToken) {
          const parts = chainOperands(ts, n);
          if (parts.some((p) => isPlainString(ts, p) || ts.isTemplateExpression(p))) {
            out.push({ rel, line: line(n), pieces: piecesOf(ts, parts) });
            // The operands that are no string may hold literals of their own.
            for (const p of parts) if (!isPlainString(ts, p) && !ts.isTemplateExpression(p)) visit(p);
            for (const p of parts) if (ts.isTemplateExpression(p)) for (const span of p.templateSpans) visit(span.expression);
            return;
          }
        }
        if (isPlainString(ts, n) || ts.isTemplateExpression(n)) {
          out.push({ rel, line: line(n), pieces: piecesOf(ts, [n]) });
          if (ts.isTemplateExpression(n)) for (const span of n.templateSpans) visit(span.expression);
          return;
        }
        ts.forEachChild(n, visit);
      };
      visit(sf);
    }
    return out;
  });
}

/** The string literals of native/*.c and *.h, adjacent literals joined. */
function cLiterals(ctx) {
  return ctx.once('cLiterals', () => {
    const out = [];
    for (const rel of ctx.files(['native'], ['.c', '.h'])) {
      const tokens = scan(ctx.read(rel)).tokens;
      for (let i = 0; i < tokens.length; i++) {
        if (tokens[i].type !== 'string') continue;
        let text = tokens[i].value.slice(1, -1);
        const line = tokens[i].line;
        while (i + 1 < tokens.length && tokens[i + 1].type === 'string') text += tokens[++i].value.slice(1, -1);
        out.push({ rel, line, pieces: [text] });
      }
    }
    return out;
  });
}

/**
 * Whether literal `lit` writes `message`: one of its pieces holds it, or a run
 * of its pieces does, the message read across the values put in between them
 * (`` `Cannot perform operation after ${who} has been closed` ``), its first
 * piece's end starting the message and its last piece's start ending it, the
 * pieces' text covering at least half the message.
 */
function holds(lit, message) {
  const { pieces } = lit;
  if (pieces.some((p) => p.includes(message))) return true;
  for (let i = 0; i < pieces.length; i++) {
    for (let j = i + 1; j < pieces.length; j++) {
      if (holdsAcross(pieces.slice(i, j + 1), message)) return true;
    }
  }
  return false;
}

/** Whether `message` reads across `pieces`, each value between them any text. */
function holdsAcross(pieces, message) {
  const first = pieces[0];
  let start = 0;
  for (let k = Math.min(first.length, message.length); k > 0; k--) {
    if (first.endsWith(message.slice(0, k))) {
      start = k;
      break;
    }
  }
  let pos = start;
  let covered = start;
  for (const piece of pieces.slice(1, -1)) {
    const at = message.indexOf(piece, pos);
    if (at < 0) return false;
    pos = at + piece.length;
    covered += piece.length;
  }
  const last = pieces[pieces.length - 1];
  let end = 0;
  for (let k = Math.min(last.length, message.length - pos); k > 0; k--) {
    if (message.endsWith(last.slice(0, k))) {
      end = k;
      break;
    }
  }
  covered += end;
  return covered * 2 >= message.length && covered > 0;
}

module.exports = { tsLiterals, cLiterals, holds };
