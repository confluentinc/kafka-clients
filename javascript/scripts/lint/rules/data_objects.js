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

// J8 check-data-objects (javascript/CLAUDE.md "Java shape, JS idiom": a data
// object's Java getters are public `readonly` fields; every other class keeps
// Java's getters as methods; root CLAUDE.md §3: no public mutable field). On
// every class src/ exports:
//   - a data class (`DATA_CLASSES` in scripts/lint/java_ts.js) gives each
//     public instance method of no parameter returning a value its Java class
//     declares (a getter: `topic()`, `hasOffset()`), but `toString`, `equals`,
//     `hashCode` and a name starting with `get`, which stay methods, as a
//     public `readonly` instance field of the same name: not a get accessor,
//     not a method, not a mutable field, and its constructor freezes the
//     instance (`Object.freeze(this);` as a top-level statement, `not-frozen`:
//     root CLAUDE.md, JavaScript Binding Conventions, Class family, a data
//     object is set once when it is built);
//   - a class that is not a data class has no get accessor (a Java getter is
//     a method there) and no public instance field, except a `readonly` one
//     translating a Java `public final` instance field of its Java class or
//     a supertype, on a class whose constructor runs `Object.freeze(this);`
//     as a top-level statement (`SecurityProtocol.id`), and never one named
//     as a `java.lang.Enum` method on an enum (`name`, `ordinal`: Java's
//     `SecurityProtocol.name` field and `name()` are one member here, the
//     method);
//   - no class has a set accessor, or a static property that is not
//     `readonly`;
//   - the error classes keep the fields javascript/CLAUDE.md "Errors" lists,
//     data properties as every JavaScript `Error`'s are;
//   - a `DATA_CLASSES` entry that no exported class translates is a finding.
// Reading a data class's field twice gives the same answer, costs no work and
// cannot fail; anything that might not stays a method.

const { atNode } = require('../location');
const { DATA_CLASSES, ENUM_IMPLICIT, ERROR_ALLOWANCE, exportedItems, errorRegistry, publicMembers, mappedItems, javaIndexOf } = require('../java_ts');

/** The members every Java object has, which stay methods on a data class. */
const OBJECT_METHODS = new Set(['toString', 'equals', 'hashCode']);

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

/**
 * The names of the Java getters of data class `cls`: its public instance
 * methods of no parameter that return a value, Object's and `get...` ones
 * aside.
 */
function javaGetters(cls) {
  const ctor = cls.path[cls.path.length - 1];
  const out = new Set();
  for (const o of cls.overloads) {
    if (o.name === ctor || o.static || o.visibility !== 'public' || o.deprecated || o.params.length !== 0 || o.return_type === 'void') continue;
    if (OBJECT_METHODS.has(o.name) || /^get[A-Z]/.test(o.name)) continue;
    out.add(o.name);
  }
  return out;
}

module.exports = {
  name: 'check-data-objects',
  kinds: ['not-frozen', 'getter-not-field', 'accessor', 'field', 'mutable-field', 'setter', 'static-mutable', 'stale-data-class'],
  needs: ['program', 'java'],
  hint:
    'On a data class, make each Java getter a public readonly field set in the constructor; elsewhere, make it a ' +
    'method; a constant is a `static readonly` (javascript/CLAUDE.md "Java shape, JS idiom").',
  DATA_CLASSES,
  check(ctx) {
    const { ts } = ctx.program();
    const registry = errorRegistry(ctx);
    const index = javaIndexOf(ctx);
    const javaOf = new Map(mappedItems(ctx).map(({ item, java }) => [item.node, java && java.cls]));
    const dataClasses = this.DATA_CLASSES;
    const seenData = new Set();
    const findings = [];
    let checked = 0;
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'class') continue;
      const row = registry.get(item.rel);
      const isError = row !== undefined && row.name === item.name;
      const cls = javaOf.get(item.node);
      const isData = cls !== undefined && cls !== null && dataClasses.has(cls.fqn);
      if (isData) {
        seenData.add(cls.fqn);
        checked += 1;
        if (!freezesInstances(ts, item.node)) {
          findings.push({
            kind: 'not-frozen',
            subject: item.name,
            at: atNode(ctx, item.node),
            message: 'is a data class whose constructor does not run Object.freeze(this): its fields are set once, when it is built',
          });
        }
      }
      const getters = isData ? javaGetters(cls) : new Set();
      const members = publicMembers(ts, item.node);
      for (const m of members) {
        if (m.kind === 'constructor') continue;
        const subject = `${item.name}.${m.name}`;
        const at = atNode(ctx, m.node);
        const getter = !m.isStatic && getters.has(m.name);
        if (m.kind === 'method' && !getter) continue;
        checked += 1;
        if (isError && ERROR_ALLOWANCE.members.has(m.name)) continue;
        if (m.kind === 'set') {
          findings.push({ kind: 'setter', subject, at, message: 'is a set accessor: the property is read-only' });
        } else if (getter && m.kind !== 'property') {
          const what = m.kind === 'get' ? 'a get accessor' : 'a method';
          findings.push({ kind: 'getter-not-field', subject, at, message: `is ${what}: on a data class, Java's ${m.name}() is a public readonly field` });
        } else if (getter && !m.isReadonly) {
          findings.push({ kind: 'mutable-field', subject, at, message: 'is a public mutable field: make it readonly' });
        } else if (getter) {
          continue;
        } else if (m.kind === 'get') {
          const message = isData
            ? 'is a get accessor: a data class has fields, not accessors'
            : 'is a get accessor: on a class that is not a data class, a Java getter is a method';
          findings.push({ kind: 'accessor', subject, at, message });
        } else if (m.isStatic && !m.isReadonly) {
          findings.push({ kind: 'static-mutable', subject, at, message: 'is a static property that is not readonly' });
        } else if (m.isStatic) {
          continue;
        } else if (!m.isReadonly) {
          findings.push({ kind: 'mutable-field', subject, at, message: 'is a public mutable field: make it readonly' });
        } else if (cls && cls.kind === 'enum' && ENUM_IMPLICIT.has(m.name)) {
          // Java's SecurityProtocol has a public final field `name` beside
          // Enum's name(): one JavaScript member cannot be both, and the
          // method is the enum's.
          findings.push({ kind: 'field', subject, at, message: `is a field: on an enum, java.lang.Enum's ${m.name}() is a method` });
        } else if (!(cls && javaFinalField(index, cls, m.name) && freezesInstances(ts, item.node))) {
          const message = isData
            ? 'is a readonly field translating no Java getter of the data class'
            : 'is a readonly field of a class that is not a data class: make it a method';
          findings.push({ kind: 'field', subject, at, message });
        }
      }
    }
    for (const fqn of dataClasses) {
      checked += 1;
      if (!seenData.has(fqn)) {
        findings.push({ kind: 'stale-data-class', subject: fqn, message: 'is listed as a data class, but no exported class translates it: delete the entry' });
      }
    }
    return { findings, checked };
  },
};
