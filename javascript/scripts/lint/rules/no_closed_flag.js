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

// check-no-closed-flag (root CLAUDE.md, JavaScript Binding Conventions,
// Implementation over the FFI, Q61: "The binding keeps no closed flag. A call
// after close() goes to the FFI, and the error is the core's, with Java's
// message"; javascript/CLAUDE.md boundary rule 1):
//   - no class of src/, a client's, a mock's or any other, has a field
//     (`#closed`, `closed`, `_closing`, `isClosed`, ...) named for a closed
//     or closing state (`closed-field`);
//   - no member of a C struct of native/ has such a name, and no C code reads
//     or writes one (`p->closed`) (`closed-field`);
//   - no string literal of src/ or native/ holds one of Java's closed-client
//     messages, which only the core may report (`closed-message`).
// The callback queue's `self_strong` and `cbq_is_closed` are not a closed
// flag: they record the core's answer to keep the handle object alive, and
// no call is refused on them (javascript/CLAUDE.md boundary rule 10). Not
// checked: a closed state under another name.

const { tsLiterals, cLiterals, holds } = require('../literals');
const { scan } = require('../c_source');

/** A field name for a closed or closing state, `#` or `_` prefixes and an `is` included. */
const CLOSED_NAME = /^[#_]*(is_?)?(closed|closing)$/i;

/**
 * Java's closed-client messages: the core reports them for a call made after
 * close(), so a binding that writes one checks the closed state itself.
 */
const CLOSED_MESSAGES = [
  'Cannot perform operation after producer has been closed',
  'This consumer has already been closed.',
  'Cannot accept new calls when AdminClient is closing.',
  'MockProducer is already closed.',
];

module.exports = {
  name: 'check-no-closed-flag',
  kinds: ['closed-field', 'closed-message'],
  needs: ['program'],
  CLOSED_MESSAGES,
  hint:
    'Keep no closed state and write no closed message: a call after close() goes to the FFI, whose error is the ' +
    "core's (root CLAUDE.md, JavaScript Binding Conventions, Implementation over the FFI, Q61).",
  check(ctx) {
    const { ts, sourceFiles } = ctx.program();
    const findings = [];
    let checked = 0;
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const visit = (n) => {
        if (ts.isClassLike(n)) {
          checked++;
          for (const m of n.members) {
            if (!ts.isPropertyDeclaration(m) || !m.name) continue;
            const name = ts.isIdentifier(m.name) || ts.isPrivateIdentifier(m.name) || ts.isStringLiteral(m.name) ? m.name.text : null;
            if (name === null || !CLOSED_NAME.test(name)) continue;
            const line = sf.getLineAndCharacterOfPosition(m.getStart(sf)).line + 1;
            findings.push({
              kind: 'closed-field',
              subject: `${rel}:${line}`,
              message: `${n.name ? n.name.text : 'a class'} has the field ${name}: the binding keeps no closed flag`,
            });
          }
        }
        ts.forEachChild(n, visit);
      };
      visit(sf);
    }
    for (const rel of ctx.files(['native'], ['.c', '.h'])) {
      checked++;
      const { code } = scan(ctx.read(rel));
      const lines = code.split('\n');
      lines.forEach((text, i) => {
        // A struct member or variable declared with the name, or a member read through `->` or `.`.
        const decl = /\b[A-Za-z_]\w*[\s*]+((?:is_)?(?:closed|closing))\s*[;=[,]/.exec(text);
        const access = /(?:->|\.)\s*((?:is_)?(?:closed|closing))\b/.exec(text);
        const m = decl || access;
        if (m) {
          findings.push({
            kind: 'closed-field',
            subject: `${rel}:${i + 1}`,
            message: `names ${m[1]}: the binding keeps no closed flag`,
          });
        }
      });
    }
    for (const lit of [...tsLiterals(ctx), ...cLiterals(ctx)]) {
      checked++;
      const message = CLOSED_MESSAGES.find((m) => holds(lit, m));
      if (message) {
        findings.push({
          kind: 'closed-message',
          subject: `${lit.rel}:${lit.line}`,
          message: `writes Java's closed message "${message}": only the core reports a closed client`,
        });
      }
    }
    return { findings, checked };
  },
};
