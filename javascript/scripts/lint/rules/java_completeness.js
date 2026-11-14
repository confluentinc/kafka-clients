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
// constructor, method and `public static final` constant of a class an
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
//   - an error class: nothing, its members being javascript/CLAUDE.md
//     "Errors"'s fixed fields (Java's constructor forms and payload getters
//     are not generated);
//   - a function type (`Callback`): nothing;
//   - an enum: also the members `java.lang.Enum` gives it (`values`,
//     `valueOf`, `name`, `ordinal`), which every translated enum has
//     (javascript/CLAUDE.md "Java shape, JS idiom"), whether or not the Java
//     index lists them, and the invented `toJSON()`, which returns the name
//     (root CLAUDE.md, JavaScript Binding Conventions, Class family);
//   - an abstract class: not its constructor, which only a subclass calls;
//   - any class: not `equals` / `hashCode`, which the client does not
//     translate, by the user's decision of 2026-11-05 (PLAN.md §8): no
//     JavaScript operator or collection calls them.
// An exported interface or object type mapped to a Java class (`Header`) is
// asked as a class is.

const { atNode } = require('../location');
const { ENUM_IMPLICIT, javaIndexOf, mappedItems, itemMembers, javaMember, shortName, constantsOnly, functionType } =
  require('../java_ts');

/** The instance method JavaScript adds to every enum (root CLAUDE.md, JavaScript Binding Conventions, Class family). */
const ENUM_TO_JSON = 'toJSON';

/** The Java methods the client does not translate on any class (PLAN.md §8, 2026-11-05). */
const NOT_TRANSLATED = new Set(['equals', 'hashCode']);

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
      if (item.tags.jsOnly || isError || java === null || !java.cls) continue;
      const cls = java.cls;
      if (functionType(cls)) continue;
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
      }
      if (!constantsOnly(cls)) {
        for (const o of cls.overloads) {
          if (o.visibility !== 'public' || o.deprecated) continue;
          if (NOT_TRANSLATED.has(o.name)) continue;
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
