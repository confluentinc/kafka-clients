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

// N9 check-string-encoding (root CLAUDE.md, JavaScript Binding Conventions,
// Types, strings; javascript/CLAUDE.md boundary rule 8): one function,
// `encodeLikeJava(s)` in src/internal/encode.ts, is the only place where the
// binding turns a string into bytes, as Java's `String.getBytes(UTF_8)` does
// (a lone surrogate as `?`, where Node writes U+FFFD). encode.ts exports that
// function (`encoder-missing` otherwise). Anywhere else in src/ (every
// hand-written module), encode.ts's other functions included, the rule
// refuses the ways Node encodes a string its own way, a "string" below being
// a value typed as a string or as a union holding one:
//   - `Buffer.from(x, ...)` of a string;
//   - `Buffer.alloc(n, x, ...)` with a string fill;
//   - `b.write(x, ...)` and `b.fill(x, ...)` of a string, `b` typed as bytes
//     (a `Buffer` or a `Uint8Array`);
//   - `new Buffer(...)`, whatever its arguments (deprecated, and it encodes a
//     string as `Buffer.from` does);
//   - `TextEncoder`, by any reference (`new TextEncoder()`, a type, an alias).
// Not checked: that header keys, group IDs and config values go through
// `encodeLikeJava`, and that topic names do not.

const ENCODER = 'src/internal/encode.ts';

/** The one function that turns a string into bytes. */
const ENCODE_FUNCTION = 'encodeLikeJava';

/** The mark of a file `cargo xtask generate-error-codes` writes. */
const GENERATED = /GENERATED, DO NOT EDIT/;

module.exports = {
  name: 'check-string-encoding',
  kinds: ['string-encoded', 'encoder-missing'],
  needs: ['program'],
  hint:
    'Encode a string through encodeLikeJava(s) of src/internal/encode.ts, which replaces a lone surrogate with ? ' +
    'as Java does (root CLAUDE.md, JavaScript Binding Conventions, Types).',
  ENCODER,
  ENCODE_FUNCTION,
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const findings = [];
    let checked = 0;
    const isString = (node) => {
      const type = checker.getTypeAtLocation(node);
      return (type.isUnion() ? type.types : [type]).some((t) => (t.flags & ts.TypeFlags.StringLike) !== 0);
    };
    const BYTES = new Set(['Buffer', 'Uint8Array']);
    const isBytes = (node) => {
      const type = checker.getNonNullableType(checker.getTypeAtLocation(node));
      return (type.isUnion() ? type.types : [type]).some((t) => t.symbol !== undefined && BYTES.has(t.symbol.name));
    };
    const onBuffer = (e, method) =>
      ts.isPropertyAccessExpression(e) && ts.isIdentifier(e.expression) && e.expression.text === 'Buffer' && e.name.text === method;
    const why = 'encodes a string: Node writes U+FFFD for a lone surrogate, Java ?';
    let encoder = false;
    let exported = false;
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      if (!rel.startsWith('src/') || GENERATED.test(sf.text.slice(0, 2000))) continue;
      const isEncoder = rel === ENCODER;
      if (isEncoder) encoder = true;
      const at = (n) => `${rel}:${sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1}`;
      const visit = (n) => {
        // Inside encode.ts, `encodeLikeJava` itself may encode.
        if (isEncoder && ts.isFunctionDeclaration(n) && n.name && n.name.text === ENCODE_FUNCTION) {
          if ((ts.getCombinedModifierFlags(n) & ts.ModifierFlags.Export) !== 0) exported = true;
          return;
        }
        if (ts.isCallExpression(n)) {
          checked += 1;
          const e = n.expression;
          const first = n.arguments[0];
          const second = n.arguments[1];
          let encoded = null;
          if (onBuffer(e, 'from') && first !== undefined && isString(first)) {
            encoded = `${e.getText(sf)}(${first.getText(sf)}, ...)`;
          } else if (onBuffer(e, 'alloc') && second !== undefined && isString(second)) {
            encoded = `${e.getText(sf)}(${first.getText(sf)}, ${second.getText(sf)}, ...)`;
          } else if (
            ts.isPropertyAccessExpression(e) &&
            (e.name.text === 'write' || e.name.text === 'fill') &&
            isBytes(e.expression) &&
            first !== undefined &&
            isString(first)
          ) {
            encoded = `${e.getText(sf)}(${first.getText(sf)}, ...)`;
          }
          if (encoded !== null) {
            findings.push({ kind: 'string-encoded', subject: at(n), message: `${encoded} ${why} (use ${ENCODE_FUNCTION} of ${ENCODER})` });
          }
        } else if (ts.isNewExpression(n) && ts.isIdentifier(n.expression) && n.expression.text === 'Buffer') {
          checked += 1;
          findings.push({ kind: 'string-encoded', subject: at(n), message: `new Buffer(...) ${why} (use ${ENCODE_FUNCTION} of ${ENCODER})` });
        } else if (ts.isIdentifier(n) && n.text === 'TextEncoder') {
          checked += 1;
          findings.push({
            kind: 'string-encoded',
            subject: at(n),
            message: `TextEncoder ${why} (use ${ENCODE_FUNCTION} of ${ENCODER})`,
          });
        }
        ts.forEachChild(n, visit);
      };
      visit(sf);
    }
    if (!encoder) {
      findings.push({ kind: 'encoder-missing', subject: ENCODER, message: 'is missing: it is the one place a string becomes bytes' });
    } else if (!exported) {
      checked += 1;
      findings.push({
        kind: 'encoder-missing',
        subject: `${ENCODER}:${ENCODE_FUNCTION}`,
        message: `exports no function ${ENCODE_FUNCTION}(s): it is the one place a string becomes bytes`,
      });
    }
    return { findings, checked };
  },
};
