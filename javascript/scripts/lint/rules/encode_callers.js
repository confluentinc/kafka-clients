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

// check-encode-callers (root CLAUDE.md, JavaScript Binding Conventions,
// Types, Q24: "`stringSerializer()` calls it for keys and values, the
// headers code for header keys, and the config code once per client";
// Class family, Q27: a string key or value given without a serializer
// throws `TypeError`): `encodeLikeJava` of src/internal/encode.ts is used
// only by the modules of `CALLERS` (`encode-caller`), each reference found
// through the type checker, so an import under another name counts. An
// entry of `ALLOWANCES` admits one more module, with its reason, and is a
// finding once that module no longer uses it (`encode-unused-allowance`).
// check-string-encoding keeps every other encoder out of src/.

const { ENCODER, ENCODE_FUNCTION } = require('./string_encoding');

/** The modules, or folders (ending `/`), that turn strings into bytes, with what they encode. */
const CALLERS = [
  ['src/common/serialization/', "the string serializer's keys and values"],
  ['src/common/header/', 'header keys'],
  ['src/internal/config.ts', 'config keys and values, group IDs among them'],
];

/** The modules that may use it too until their item lands: module, reason. */
const ALLOWANCES = [
  [
    'src/producer/producer_record.ts',
    'encodes a string key or value given without a serializer, which the serializers replace with a TypeError ' +
      'pointing to stringSerializer() (root Class family, Q24, Q27)',
  ],
];

module.exports = {
  name: 'check-encode-callers',
  kinds: ['encode-caller', 'encode-unused-allowance'],
  needs: ['program'],
  CALLERS,
  ALLOWANCES,
  hint:
    'Leave strings to the string serializer, the headers code and the config code: only they call encodeLikeJava ' +
    '(root CLAUDE.md, JavaScript Binding Conventions, Types, Q24, and Class family, Q27).',
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const encoder = sourceFiles.find((sf) => ctx.rel(sf.fileName) === ENCODER);
    let target = null;
    if (encoder) {
      for (const st of encoder.statements) {
        if (ts.isFunctionDeclaration(st) && st.name && st.name.text === ENCODE_FUNCTION) target = checker.getSymbolAtLocation(st.name);
      }
    }
    const findings = [];
    if (target === null) return { findings, checked: 0 };
    const used = new Set();
    let checked = 0;
    const allowed = (rel) => CALLERS.some(([p]) => (p.endsWith('/') ? rel.startsWith(p) : rel === p));
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      if (rel === ENCODER) continue;
      checked++;
      const visit = (n) => {
        if (ts.isIdentifier(n) && !ts.isImportSpecifier(n.parent) && !ts.isImportClause(n.parent)) {
          let symbol = checker.getSymbolAtLocation(n);
          if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
          if (symbol === target) {
            const allowance = ALLOWANCES.find(([m]) => m === rel);
            if (allowance) used.add(rel);
            else if (!allowed(rel)) {
              findings.push({
                kind: 'encode-caller',
                subject: `${rel}:${sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1}`,
                message: `uses ${ENCODE_FUNCTION}: only the string serializer, the headers code and the config code turn strings into bytes`,
              });
            }
          }
        }
        ts.forEachChild(n, visit);
      };
      visit(sf);
    }
    for (const [rel, reason] of ALLOWANCES) {
      if (!used.has(rel)) {
        findings.push({
          kind: 'encode-unused-allowance',
          subject: rel,
          message: `is allowed to use ${ENCODE_FUNCTION} (it ${reason}), but no longer does: delete the allowance`,
        });
      }
    }
    return { findings, checked };
  },
};
