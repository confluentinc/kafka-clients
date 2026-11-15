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

// J3 check-java-completeness (root CLAUDE.md DoD #2: every method of the
// translated class is implemented): every public, non-deprecated Java
// constructor, method, `public static final` constant and public mutable
// field (as its setter, `set<Field>`, plan §5 item 2) of a class an
// exported class translates has its TypeScript member, by the mapping of
// scripts/lint/java_ts.js. One finding per Java name (`missing
// <Class>#<member>`, the overloads of a name are one TypeScript function), so
// what is not translated yet is listed in lint-baseline.txt and burnt down.
//
// Asked for:
//   - a client config class (`ProducerConfig`, ...): its constants only, the
//     `ConfigDef` machinery not being translated (javascript/CLAUDE.md
//     "Generated files");
//   - any class: not a method that takes a `ConfigDef` (the key-name
//     classes' `addClientSslSupport`, `addClientSaslSupport`);
//   - an error class: its payload (root CLAUDE.md, JavaScript Binding
//     Conventions, Errors, Q39): every public method the class declares (the
//     payload getters, a static factory, a `toString()` override) but Java's
//     `fillInStackTrace()`, a JVM stack-trace override with no JavaScript
//     counterpart, and every public constructor that takes a payload, as a
//     constructor signature whose one parameter is an options object of
//     Java's parameter names (a `String` may be the `message`, a `Throwable`
//     is the `cause`); its message and cause forms are checked by the error
//     tests;
//   - a function type (`Callback`): nothing;
//   - an enum: also the members `java.lang.Enum` gives it (`values`,
//     `valueOf`, `name`, `ordinal`), which every translated enum has
//     (javascript/CLAUDE.md "Java shape, JS idiom"), whether or not the Java
//     index lists them, and the invented `toJSON()`, which returns the name
//     (root CLAUDE.md, JavaScript Binding Conventions, Class family);
//   - an abstract class: not its constructor, which only a subclass calls;
//   - a placeholder type (`export type Measurable = unknown`, PLACEHOLDERS):
//     nothing, the Java class not being translated.
// `equals` and `hashCode` are asked as any member is: they stay methods (root
// CLAUDE.md, JavaScript Binding Conventions, Class family, Q29).
// An exported interface or object type mapped to a Java class (`Header`) is
// asked as a class is.

const { atNode } = require('../location');
const { ENUM_IMPLICIT, PLACEHOLDERS, javaIndexOf, mappedItems, itemMembers, javaMember, shortName, constantsOnly, functionType } =
  require('../java_ts');

/** The instance method JavaScript adds to every enum (root CLAUDE.md, JavaScript Binding Conventions, Class family). */
const ENUM_TO_JSON = 'toJSON';

/** The Java methods of an error class no JavaScript error translates: JVM stack-trace overrides. */
const ERROR_JVM_ONLY = new Set(['fillInStackTrace']);

/** Whether Java parameter type `t` is a cause (a Throwable, whatever its class). */
function throwableType(t) {
  return t === 'Throwable' || /Exception$/.test(t);
}

/**
 * The option names of each constructor signature of class `node`: the keys of
 * its one parameter's object type, or null for a signature of another shape.
 */
function constructorOptions(ts, node) {
  const out = [];
  for (const member of node.members || []) {
    if (!ts.isConstructorDeclaration(member) || member.body !== undefined) continue;
    const [p, ...rest] = member.parameters;
    if (p === undefined || rest.length > 0 || p.type === undefined || !ts.isTypeLiteralNode(p.type)) continue;
    out.push(
      p.type.members
        .filter((e) => e.name && (ts.isIdentifier(e.name) || ts.isStringLiteral(e.name)))
        .filter((e) => !(e.type && e.type.kind === ts.SyntaxKind.NeverKeyword))
        .map((e) => e.name.text),
    );
  }
  return out;
}

/** Whether options `keys` translate Java constructor `o`: one key per parameter, by Java's name, a String maybe `message`, a Throwable `cause`. */
function translatesConstructor(o, keys) {
  if (keys.length !== o.params.length) return false;
  return o.params.every((t, i) => {
    const name = o.param_names[i];
    if (throwableType(t)) return keys.includes('cause');
    if (t === 'String') return keys.includes(name) || keys.includes('message');
    return keys.includes(name);
  });
}

/** The findings of error class `item` (Java `cls`): its payload members and payload constructors. */
function errorFindings(ctx, ts, index, item, cls, findings) {
  const className = cls.path[cls.path.length - 1];
  const covered = new Set();
  for (const m of itemMembers(ts, item)) {
    const jm = javaMember(index, cls, m);
    if (jm && jm.owner === cls) covered.add(jm.name);
  }
  let checked = 0;
  const wanted = [];
  for (const o of cls.overloads) {
    if (o.visibility !== 'public' || o.deprecated || o.name === className || ERROR_JVM_ONLY.has(o.name)) continue;
    if (!wanted.includes(o.name)) wanted.push(o.name);
  }
  for (const name of wanted) {
    checked += 1;
    if (!covered.has(name)) {
      findings.push({ kind: 'missing', subject: `${shortName(cls)}#${name}`, at: atNode(ctx, item.node), message: `${item.name} does not translate it` });
    }
  }
  if (cls.abstract || item.kind !== 'class') return checked;
  const options = constructorOptions(ts, item.node);
  for (const o of cls.overloads) {
    if (o.name !== className || o.visibility !== 'public' || o.deprecated) continue;
    if (o.params.every((t) => t === 'String' || throwableType(t))) continue;
    checked += 1;
    if (!options.some((keys) => translatesConstructor(o, keys))) {
      findings.push({
        kind: 'missing',
        subject: `${shortName(cls)}#${className}(${o.param_types.join(', ')})`,
        at: atNode(ctx, item.node),
        message: `${item.name} has no constructor taking { ${o.param_names.join(', ')} }`,
      });
    }
  }
  return checked;
}

module.exports = {
  name: 'check-java-completeness',
  kinds: ['missing'],
  needs: ['program', 'java'],
  hint:
    'Translate the Java member (javascript/CLAUDE.md "Java shape, JS idiom"), or list it in lint-baseline.txt ' +
    'as `missing <Class>#<member>` with a comment saying why it is not translated yet.',
  check(ctx) {
    const { ts } = ctx.program();
    const index = javaIndexOf(ctx);
    const findings = [];
    let checked = 0;
    for (const { item, java, isError } of mappedItems(ctx)) {
      if (item.tags.jsOnly || java === null || !java.cls) continue;
      if (isError) {
        checked += errorFindings(ctx, ts, index, item, java.cls, findings);
        continue;
      }
      const cls = java.cls;
      if (functionType(cls)) continue;
      // A placeholder of a type the client does not translate has no members.
      if (PLACEHOLDERS.has(cls.path[cls.path.length - 1]) && item.kind !== 'class') continue;
      const covered = new Set();
      for (const m of itemMembers(ts, item)) {
        const jm = javaMember(index, cls, m);
        if (jm && jm.owner === cls) covered.add(jm.name);
        // java.lang.Enum's members, which the index may not list: the
        // method of that name, static for `values` / `valueOf` only.
        else if (cls.kind === 'enum' && ENUM_IMPLICIT.has(m.name) && m.kind === 'method' && m.isStatic === (m.name === 'values' || m.name === 'valueOf')) {
          covered.add(m.name);
        } else if (cls.kind === 'enum' && m.name === ENUM_TO_JSON && m.kind === 'method' && !m.isStatic) {
          covered.add(m.name);
        }
      }
      const className = cls.path[cls.path.length - 1];
      const wanted = [];
      for (const f of cls.fields) {
        if (f.visibility === 'public' && f.static && f.final && !f.deprecated) wanted.push(f.name);
        // A public mutable field is asked for as its setter (javaMember).
        else if (f.visibility === 'public' && !f.static && !f.final && !f.deprecated) wanted.push(f.name);
      }
      if (!constantsOnly(cls)) {
        for (const o of cls.overloads) {
          if (o.visibility !== 'public' || o.deprecated) continue;
          // A method that takes a `ConfigDef`, whose machinery is not
          // translated (root CLAUDE.md, JavaScript Binding Conventions,
          // Scope: SaslConfigs.addClientSaslSupport, SslConfigs.addClientSslSupport).
          if (o.param_types.some((t) => /\bConfigDef\b/.test(t))) continue;
          // An abstract class's constructor serves its subclasses only.
          if (cls.abstract && o.name === className) continue;
          if (!wanted.includes(o.name)) wanted.push(o.name);
        }
      }
      if (cls.kind === 'enum') for (const name of [...ENUM_IMPLICIT, ENUM_TO_JSON]) if (!wanted.includes(name)) wanted.push(name);
      for (const name of wanted) {
        checked += 1;
        if (!covered.has(name)) {
          findings.push({
            kind: 'missing',
            subject: `${shortName(cls)}#${name}`, at: atNode(ctx, item.node),
            message: name === className ? `${item.name} has no public constructor` : `${item.name} does not translate it`,
          });
        }
      }
    }
    return { findings, checked };
  },
};
