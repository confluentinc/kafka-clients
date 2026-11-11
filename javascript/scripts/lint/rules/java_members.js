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

// J2 check-java-members (javascript/CLAUDE.md "Java shape, JS idiom"): every
// public member of an exported class translates a member of its Java class
// (or of a Java supertype): a method by its name, a property as Java's getter
// (`x()`, `getX()`; an `isX` / `hasX` property keeps its name) or field, a
// `static readonly` as a `public static final` field, the constructor as
// Java's constructors (scripts/lint/java_ts.js), each of its own static-ness
// (a mismatch is a `static-mismatch`). An exported interface or object type
// mapped to a Java class is checked as a class is. A member on the list of
// invented names (lint-mapping.json's `invented`) is exempt, one in its
// `javaTargets` maps to the `<fqn>#<member>` listed. An error class keeps the
// fields javascript/CLAUDE.md "Errors" lists instead of Java's members; a
// function type (`Callback`) has none to map.

const { atNode } = require('../location');
const {
  ERROR_ALLOWANCE,
  javaIndexOf,
  mappedItems,
  publicMembers,
  itemMembers,
  javaMember,
  unGet,
  shortName,
  functionType,
  memberTags,
} = require('../java_ts');

/** `member` as the lint names it: `Class.member`, `Class.constructor`. */
function memberName(item, m) {
  return `${item.name}.${m.name}`;
}

/** The Java member a `javaTargets` entry `<fqn>#<member>` names, or null (check-invented-names reports a bad one). */
function taggedMember(index, tag) {
  const [fqn, member] = tag.split('#');
  const cls = index.resolve(fqn);
  if (!cls || !member) return null;
  const overloads = cls.overloads.filter((o) => o.name === member);
  if (overloads.length > 0) return { kind: 'methods', name: member, owner: cls, overloads };
  const field = cls.fields.find((f) => f.name === member);
  return field ? { kind: 'field', name: member, owner: cls, field } : null;
}

/**
 * Every public member of every exported class with a Java class, for the
 * member rules: `{item, java, m, jm}`, `jm` the Java member or null. Error
 * classes, function types and invented classes are left out.
 */
function mappedMembers(ctx) {
  return ctx.once('mappedMembers', () => {
    const { ts } = ctx.program();
    const index = javaIndexOf(ctx);
    const out = [];
    for (const { item, java, isError } of mappedItems(ctx)) {
      if (item.tags.jsOnly || isError || java === null || !java.cls || functionType(java.cls)) continue;
      for (const m of itemMembers(ts, item)) {
        const tags = memberTags(ctx, item, m);
        if (tags.jsOnly) continue;
        const jm = tags.java ? taggedMember(index, tags.java.text) : javaMember(index, java.cls, m);
        out.push({ item, java, m, jm });
      }
    }
    return out;
  });
}

module.exports = {
  name: 'check-java-members',
  kinds: ['no-java-member', 'static-mismatch', 'not-static-final', 'error-member'],
  needs: ['program', 'java'],
  hint:
    'Give the member its Java member\'s name (a Java getter is a readonly field on a data class and a method ' +
    'of Java\'s name elsewhere, a `public static final` a `static readonly`), or list it with its reason in ' +
    'lint-mapping.json\'s `invented`, the list of invented names ' +
    '(javascript/CLAUDE.md "Java shape, JS idiom").',
  mappedMembers,
  check(ctx) {
    const { ts } = ctx.program();
    const index = javaIndexOf(ctx);
    const findings = [];
    let checked = 0;
    for (const { item, java, m, jm } of mappedMembers(ctx)) {
      checked += 1;
      const other = jm === null ? javaMember(index, java.cls, m, { anyStatic: true }) : null;
      if (other !== null) {
        findings.push({
          kind: 'static-mismatch',
          subject: memberName(item, m), at: atNode(ctx, m.node),
          message: m.isStatic
            ? `is static, but ${shortName(other.owner)}#${other.name} is an instance member`
            : `is an instance member, but ${shortName(other.owner)}#${other.name} is static`,
        });
      } else if (jm === null) {
        let message = `no Java member of ${shortName(java.cls)}`;
        const bare = unGet(m.name);
        if (m.kind === 'method' && bare !== null) {
          const getter = javaMember(index, java.cls, { name: bare, kind: 'get', isStatic: false });
          if (getter && getter.kind === 'methods') message += `; Java's getter ${getter.name}() is the property ${bare}`;
        }
        if (m.kind === 'get' || m.kind === 'property') {
          const upper = m.name[0].toUpperCase() + m.name.slice(1);
          for (const kept of [`is${upper}`, `has${upper}`]) {
            const getter = javaMember(index, java.cls, { name: kept, kind: 'method', isStatic: m.isStatic });
            if (getter && getter.kind === 'methods') message += `; Java's ${kept}() is the property ${kept}`;
          }
        }
        findings.push({ kind: 'no-java-member', subject: memberName(item, m), at: atNode(ctx, m.node), message });
      } else if (m.isStatic && m.kind === 'property' && jm.kind === 'field' && !(jm.field.static && jm.field.final)) {
        findings.push({
          kind: 'not-static-final',
          subject: memberName(item, m), at: atNode(ctx, m.node),
          message: `is static, but ${shortName(jm.owner)}#${jm.name} is not a static final field`,
        });
      }
    }
    // The error classes: the allowance's fields only.
    for (const { item, isError } of mappedItems(ctx)) {
      if (!isError || item.kind !== 'class') continue;
      for (const m of publicMembers(ts, item.node)) {
        checked += 1;
        if (!ERROR_ALLOWANCE.members.has(m.name)) {
          findings.push({
            kind: 'error-member',
            subject: memberName(item, m), at: atNode(ctx, m.node),
            message: `is not one of the error fields (${[...ERROR_ALLOWANCE.members].join(', ')}): ${ERROR_ALLOWANCE.reason}`,
          });
        }
      }
    }
    return { findings, checked };
  },
};
