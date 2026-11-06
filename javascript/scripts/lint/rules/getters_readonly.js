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

// J8 check-getters-readonly (javascript/CLAUDE.md "Java shape, JS idiom":
// Java getters are read-only properties; root CLAUDE.md §3: no public field):
// every public property of a class lib/ exports is a get accessor with no
// set accessor, or a `static readonly` constant. A data property, even
// `readonly` (TypeScript's `readonly` is gone at run time: only a frozen
// object keeps plain JavaScript from assigning it), and a set accessor are
// findings. The error classes keep the fields javascript/CLAUDE.md "Errors"
// lists, which are data properties as every JavaScript `Error`'s are.
//
// By the user's decision of 2026-11-05 (design/current/javascript-rule-linter/
// PLAN.md §8), a Java `public final` instance field may be a `readonly`
// property of a frozen object: a `readonly` instance data property passes when
// the class's Java class (or a Java supertype) declares a public final
// instance field of its name (a field wins over a getter of the same name)
// and the constructor runs `Object.freeze(this);` as a top-level statement.
// The same decision, extended: on a class translating a Java enum, the
// property `name` translating `java.lang.Enum#name()` passes on the same
// terms. Any other one translating a Java getter method, or of a class that
// does not certainly freeze its instances, is still a `readonly-field`.

const { atNode } = require('../location');
const { ERROR_ALLOWANCE, exportedItems, errorRegistry, publicMembers, mappedItems, javaIndexOf } = require('../java_ts');

/**
 * Whether class declaration `cls`'s constructor certainly freezes the
 * instance: an `Object.freeze(this);` statement at the top level of its body
 * (not under a condition, not in an inner function, whose `this` is not the
 * instance).
 */
function freezesInstances(ts, cls) {
  const ctor = cls.members.find((m) => ts.isConstructorDeclaration(m) && m.body);
  if (!ctor) return false;
  return ctor.body.statements.some((st) => {
    if (!ts.isExpressionStatement(st)) return false;
    const call = st.expression;
    return (
      ts.isCallExpression(call) &&
      ts.isPropertyAccessExpression(call.expression) &&
      ts.isIdentifier(call.expression.expression) &&
      call.expression.expression.text === 'Object' &&
      call.expression.name.text === 'freeze' &&
      call.arguments.length === 1 &&
      call.arguments[0].kind === ts.SyntaxKind.ThisKeyword
    );
  });
}

/** Whether Java class `cls`, or a Java supertype, declares a public final instance field `name`. */
function javaFinalField(index, cls, name) {
  return index
    .chain(cls)
    .some((c) => c.fields.some((f) => f.name === name && f.visibility === 'public' && f.final && !f.static));
}

module.exports = {
  name: 'check-getters-readonly',
  kinds: ['mutable-field', 'readonly-field', 'setter', 'static-mutable'],
  needs: ['program', 'java'],
  hint:
    'Make the property a get accessor over a private (#) field, or a constant a `static readonly` ' +
    '(javascript/CLAUDE.md "Java shape, JS idiom").',
  check(ctx) {
    const { ts } = ctx.program();
    const registry = errorRegistry(ctx);
    const index = javaIndexOf(ctx);
    const javaOf = new Map(mappedItems(ctx).map(({ item, java }) => [item.node, java && java.cls]));
    const findings = [];
    let checked = 0;
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'class') continue;
      const row = registry.get(item.rel);
      const isError = row !== undefined && row.name === item.name;
      for (const m of publicMembers(ts, item.node)) {
        if (m.kind === 'method' || m.kind === 'constructor' || m.kind === 'get') continue;
        checked += 1;
        if (isError && ERROR_ALLOWANCE.members.has(m.name)) continue;
        const subject = `${item.name}.${m.name}`;
        const at = atNode(ctx, m.node);
        if (m.kind === 'set') {
          findings.push({ kind: 'setter', subject, at, message: 'is a set accessor: the property is read-only' });
        } else if (m.isStatic && !m.isReadonly) {
          findings.push({ kind: 'static-mutable', subject, at, message: 'is a static property that is not readonly' });
        } else if (!m.isStatic && m.isReadonly) {
          const cls = javaOf.get(item.node);
          const enumName = cls && cls.kind === 'enum' && m.name === 'name';
          if (cls && (javaFinalField(index, cls, m.name) || enumName) && freezesInstances(ts, item.node)) continue;
          findings.push({
            kind: 'readonly-field',
            subject,
            at,
            message: 'is a readonly data property, not a get accessor',
          });
        } else if (!m.isStatic) {
          findings.push({ kind: 'mutable-field', subject, at, message: 'is a public mutable field: make it a get accessor' });
        }
      }
    }
    return { findings, checked };
  },
};
