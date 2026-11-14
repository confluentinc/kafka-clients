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
const { isInternal, errorRegistry } = require('../java_ts');

// H3 check-internal-members (javascript/CLAUDE.md "Files and layout"): a
// member of a public class that only the library calls is a public member
// marked `@internal` in its TSDoc and named `internal...`
// (`ProducerRecord.internalToNative`, `RecordMetadata.internalCreate`), which
// tsc's `stripInternal` leaves out of the declarations; the library calls it
// with plain dot access. So, on the classes of src/:
//   - no member is `_`-prefixed (property, method, accessor, or constructor
//     parameter property): the underscore is no longer how the library hides
//     a member. The one exception is the error classes' FFI id, which the
//     root CLAUDE.md (JavaScript Binding Conventions, Errors) keeps "in the
//     private `_ffiId`, for the binding and the test harness": `_ffiId` is
//     allowed on a class of src/internal/error_registry.ts, and the head of
//     the error tree, `KafkaError` (src/common/kafka_error.ts), declares it
//     (`ffi-id-missing`), reached by dot or bracket access;
//   - a member named `internal<Upper>...` is marked `@internal`, and an
//     `@internal` member is named so, which keeps the two readable together;
//   - an `@internal` member is not `private`, `protected` or an ES `#` one
//     (another class of the library calls it; the TypeScript modifier would
//     refuse that, and the tag already hides it from users);
//   - no constructor is `@internal`: stripped, it would leave the class with
//     no declared constructor, which TypeScript reads as a public one (a
//     constructor users must not call is `private` instead);
//   - no bracket access names a member by a string key starting with `_` or
//     `internal` (`record['internalToNative']()`), `['_ffiId']` aside: call it
//     as `.name`.

/** Whether `node` carries the `private` or `protected` modifier. */
function isHidden(ts, node) {
  return (ts.getCombinedModifierFlags(node) & (ts.ModifierFlags.Private | ts.ModifierFlags.Protected)) !== 0;
}

/** The name of a class-like declaration, `(anonymous)` for none. */
function className(node) {
  return node.name ? node.name.text : '(anonymous)';
}

const INTERNAL_NAME = /^internal[A-Z]/;

/** The error classes' FFI id (root CLAUDE.md, JavaScript Binding Conventions, Errors). */
const FFI_ID = '_ffiId';

/** The module of the head of the error tree, which declares the FFI id. */
const ERROR_HEAD = { rel: 'src/common/kafka_error.ts', name: 'KafkaError' };

module.exports = {
  name: 'check-internal-members',
  kinds: ['underscore', 'not-tagged', 'not-named', 'hidden', 'internal-constructor', 'bracket-access', 'ffi-id-missing'],
  needs: ['program'],
  hint:
    'Make a member only the library calls a public `@internal` member named `internal...`, called with dot ' +
    'access; make a constructor users must not call `private`; keep an error\'s FFI id in `_ffiId` ' +
    '(javascript/CLAUDE.md "Files and layout"; root CLAUDE.md, JavaScript Binding Conventions, Errors).',
  check(ctx) {
    const { ts, sourceFiles } = ctx.program();
    const findings = [];
    let checked = 0;
    // The error classes, by module and name, which may hold `_ffiId`.
    const errorClasses = new Set([...errorRegistry(ctx)].map(([rel, row]) => `${rel}:${row.name}`));
    let headDeclares = false;
    let headFound = false;
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const line = (node) => sf.getLineAndCharacterOfPosition(node.getStart(sf)).line + 1;
      const push = (kind, cls, node, name, message) =>
        findings.push({ kind, subject: `${rel}:${className(cls)}.${name}`, at: atNode(ctx, node), message: `${message} (line ${line(node)})` });
      const member = (cls, node, name, isPrivateName) => {
        checked += 1;
        const tagged = isInternal(ts, node);
        const key = `${rel}:${className(cls)}`;
        if (name === FFI_ID && !isPrivateName && (errorClasses.has(key) || key === `${ERROR_HEAD.rel}:${ERROR_HEAD.name}`)) {
          if (key === `${ERROR_HEAD.rel}:${ERROR_HEAD.name}`) headDeclares = true;
          return;
        }
        if (name.startsWith('_')) {
          push('underscore', cls, node, name, `\`${name}\` is \`_\`-prefixed: a member only the library calls is \`@internal\` and named \`internal...\``);
        } else if (INTERNAL_NAME.test(name) && !tagged) {
          push('not-tagged', cls, node, name, `\`${name}\` is named \`internal...\` but its TSDoc has no \`@internal\``);
        } else if (tagged && !INTERNAL_NAME.test(name)) {
          push('not-named', cls, node, name, `\`${name}\` is \`@internal\` but not named \`internal...\``);
        }
        if (tagged && (isPrivateName || isHidden(ts, node))) {
          push('hidden', cls, node, name, `\`${name}\` is \`@internal\` and also hidden: an \`@internal\` member is public, called by the library with dot access`);
        }
      };
      const visit = (node) => {
        if (ts.isClassLike(node)) {
          if (rel === ERROR_HEAD.rel && className(node) === ERROR_HEAD.name) headFound = true;
          for (const m of node.members) {
            if (ts.isConstructorDeclaration(m)) {
              checked += 1;
              if (isInternal(ts, m)) {
                push('internal-constructor', node, m, 'constructor', '`constructor` is `@internal`: stripped, the class would declare none, which TypeScript reads as public; make it `private`');
              }
              for (const p of m.parameters) {
                const isProperty = ts.getEffectiveModifierFlags(p) & ts.ModifierFlags.ParameterPropertyModifier;
                if (isProperty && ts.isIdentifier(p.name)) member(node, p, p.name.text, false);
              }
              continue;
            }
            if (!m.name) continue;
            if (ts.isPrivateIdentifier(m.name)) {
              if (isInternal(ts, m)) member(node, m, m.name.text.slice(1), true);
              continue;
            }
            if (ts.isIdentifier(m.name) || ts.isStringLiteral(m.name)) member(node, m, m.name.text, false);
          }
        }
        const key = ts.isElementAccessExpression(node) ? node.argumentExpression : undefined;
        if (key !== undefined && (ts.isStringLiteral(key) || ts.isNoSubstitutionTemplateLiteral(key))) {
          checked += 1;
          if (key.text !== FFI_ID && (key.text.startsWith('_') || INTERNAL_NAME.test(key.text))) {
            findings.push({
              kind: 'bracket-access',
              subject: `${rel}:${line(node)}`,
              message: `\`${node.getText(sf)}\` names a member by a string: call it as \`.${key.text}\``,
            });
          }
        }
        ts.forEachChild(node, visit);
      };
      visit(sf);
    }
    if (headFound && !headDeclares) {
      checked += 1;
      findings.push({
        kind: 'ffi-id-missing',
        subject: `${ERROR_HEAD.rel}:${ERROR_HEAD.name}.${FFI_ID}`,
        message: `${ERROR_HEAD.name} declares no \`${FFI_ID}\`: the FFI id is kept in the private \`${FFI_ID}\`, for the binding and the test harness`,
      });
    }
    return { findings, checked };
  },
};
