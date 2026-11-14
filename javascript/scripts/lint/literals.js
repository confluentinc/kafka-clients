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
// message the binding must not write: in src/, each string literal and each
// piece of a template literal, as the type checker's parser reads them; in
// native/*.c, each string literal, comments left out. Each is
// `{rel, line, text}`, `text` the literal's content (a C literal's escapes
// kept as written).

const { scan } = require('./c_source');

/** The string literals of src/'s TypeScript. */
function tsLiterals(ctx) {
  return ctx.once('tsLiterals', () => {
    const { ts, sourceFiles } = ctx.program();
    const out = [];
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const visit = (n) => {
        if (
          ts.isStringLiteral(n) ||
          ts.isNoSubstitutionTemplateLiteral(n) ||
          ts.isTemplateHead(n) ||
          ts.isTemplateMiddle(n) ||
          ts.isTemplateTail(n)
        ) {
          out.push({ rel, line: sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1, text: n.text });
        }
        ts.forEachChild(n, visit);
      };
      visit(sf);
    }
    return out;
  });
}

/** The string literals of native/*.c. */
function cLiterals(ctx) {
  return ctx.once('cLiterals', () => {
    const out = [];
    for (const rel of ctx.files(['native'], ['.c', '.h'])) {
      for (const t of scan(ctx.read(rel)).tokens) {
        if (t.type === 'string') out.push({ rel, line: t.line, text: t.value.slice(1, -1) });
      }
    }
    return out;
  });
}

module.exports = { tsLiterals, cLiterals };
