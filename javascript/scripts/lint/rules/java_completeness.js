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
//   - a config class (`ProducerConfig`, ...): its constants only, the
//     `ConfigDef` machinery not being translated (javascript/CLAUDE.md
//     "Generated files");
//   - an error class: nothing, its members being javascript/CLAUDE.md
//     "Errors"'s fixed fields (Java's constructor forms, payload getters and
//     `INSTANCE` are not generated);
//   - a function type (`Callback`): nothing;
//   - an enum: not the members `java.lang.Enum` gives it (`values`,
//     `valueOf`, `name`, `ordinal`), which are not its class's own;
//   - an abstract class: not its constructor, which only a subclass calls;
//   - any class: not `equals` / `hashCode`, which the client does not
//     translate, by the user's decision of 2026-11-05 (PLAN.md §8): no
//     JavaScript operator or collection calls them.
// An exported interface or object type mapped to a Java class (`Header`) is
// asked as a class is.

const { ENUM_IMPLICIT, javaIndexOf, mappedItems, itemMembers, javaMember, shortName, constantsOnly, functionType } =
  require('../java_ts');

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
      }
      const className = cls.path[cls.path.length - 1];
      const wanted = [];
      for (const f of cls.fields) {
        if (f.visibility === 'public' && f.static && f.final && !f.deprecated) wanted.push(f.name);
      }
      if (!constantsOnly(cls)) {
        for (const o of cls.overloads) {
          if (o.visibility !== 'public' || o.deprecated) continue;
          if (cls.kind === 'enum' && ENUM_IMPLICIT.has(o.name)) continue;
          if (NOT_TRANSLATED.has(o.name)) continue;
          // An abstract class's constructor serves its subclasses only.
          if (cls.abstract && o.name === className) continue;
          if (!wanted.includes(o.name)) wanted.push(o.name);
        }
      }
      for (const name of wanted) {
        checked += 1;
        if (!covered.has(name)) {
          findings.push({
            kind: 'missing',
            subject: `${shortName(cls)}#${name}`,
            message: name === className ? `${item.name} has no public constructor` : `${item.name} does not translate it`,
          });
        }
      }
    }
    return { findings, checked };
  },
};
