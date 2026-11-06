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

const { atNode } = require('../location');

// H3 check-private-members (javascript/CLAUDE.md "Files and layout"): a `_`
// member is TypeScript `private`, and another class reaches it only with
// bracket access (`record['_toNative']()`), which `private` allows. So:
//   - every `_`-prefixed member of a class in lib/ (property, method,
//     accessor, or constructor parameter property) is declared `private`;
//   - every bracket access with a `_` string key resolves, through the type
//     checker, to a `private` member of a class other than the one the access
//     is written in (inside its own class, `this._m` needs no brackets).

/** Whether `node` carries the `private` modifier. */
function isPrivate(ts, node) {
  return (ts.getCombinedModifierFlags(node) & ts.ModifierFlags.Private) !== 0;
}

/** The name of a class-like declaration, `(anonymous)` for none. */
function className(node) {
  return node.name ? node.name.text : '(anonymous)';
}

/** The class declaration or expression enclosing `node`, or undefined. */
function enclosingClass(ts, node) {
  for (let n = node.parent; n; n = n.parent) if (ts.isClassLike(n)) return n;
  return undefined;
}

module.exports = {
  name: 'check-private-members',
  kinds: ['not-private', 'access-unresolved', 'access-not-private', 'access-own-class'],
  needs: ['program'],
  hint:
    'Declare a `_` member `private`, and reach another class\'s `_` member with `x[\'_m\']` only when it is ' +
    'that class\'s `private` member (javascript/CLAUDE.md "Files and layout").',
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const findings = [];
    let checked = 0;
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const line = (node) => sf.getLineAndCharacterOfPosition(node.getStart(sf)).line + 1;
      const member = (cls, node, name) => {
        checked += 1;
        if (name.startsWith('_') && !isPrivate(ts, node)) {
          findings.push({
            kind: 'not-private',
            subject: `${rel}:${className(cls)}.${name}`,
            at: atNode(ctx, node),
            message: `\`${name}\` is \`_\`-prefixed but not \`private\` (line ${line(node)})`,
          });
        }
      };
      const visit = (node) => {
        if (ts.isClassLike(node)) {
          for (const m of node.members) {
            if (m.name && (ts.isIdentifier(m.name) || ts.isStringLiteral(m.name))) member(node, m, m.name.text);
            if (ts.isConstructorDeclaration(m)) {
              for (const p of m.parameters) {
                const isProperty = ts.getEffectiveModifierFlags(p) & ts.ModifierFlags.ParameterPropertyModifier;
                if (isProperty && ts.isIdentifier(p.name)) member(node, p, p.name.text);
              }
            }
          }
        }
        const key = ts.isElementAccessExpression(node) ? node.argumentExpression : undefined;
        if (
          key !== undefined &&
          (ts.isStringLiteral(key) || ts.isNoSubstitutionTemplateLiteral(key)) &&
          key.text.startsWith('_')
        ) {
          checked += 1;
          const name = node.argumentExpression.text;
          const subject = `${rel}:${line(node)}`;
          // An optional chain (`f?.['_m']`) types the object with `undefined`.
          const type = checker.getNonNullableType(checker.getTypeAtLocation(node.expression));
          const symbol = checker.getPropertyOfType(checker.getApparentType(type), name);
          const declarations = (symbol && symbol.declarations) || [];
          if (declarations.length === 0) {
            findings.push({
              kind: 'access-unresolved',
              subject,
              message: `\`${node.getText(sf)}\` resolves to no member`,
            });
          } else if (!declarations.every((d) => isPrivate(ts, d))) {
            findings.push({
              kind: 'access-not-private',
              subject,
              message: `\`${node.getText(sf)}\` reaches \`${name}\`, which is not \`private\``,
            });
          } else {
            const owner = enclosingClass(ts, declarations[0]);
            if (owner !== undefined && owner === enclosingClass(ts, node)) {
              findings.push({
                kind: 'access-own-class',
                subject,
                message: `\`${node.getText(sf)}\` is inside \`${className(owner)}\` itself: use \`.${name}\``,
              });
            }
          }
        }
        ts.forEachChild(node, visit);
      };
      visit(sf);
    }
    return { findings, checked };
  },
};
