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

// The mapping tags (design/current/javascript-rule-linter/PLAN.md §3.4): what
// the convention of scripts/lint/java_ts.js cannot derive is tagged in TSDoc,
// and a tag is a finding unless it is well formed and needed.
//   - `@jsOnly <reason>` marks an exported item, or a public member of an
//     exported class, with no Java counterpart: it needs a reason, and an item
//     the convention maps to Java needs no tag.
//   - `@java <fqn>[#member]` names the Java class, or member, the convention
//     does not find: it must resolve in the Java index, the member must be
//     declared by that class, and it must not name what the convention
//     already finds, nor re-map an item the convention maps elsewhere.
//   - An item takes one of the two tags, not both.

const { atNode } = require('../location');
const { javaIndexOf, exportedItems, conventionClass, tagsOf, itemMembers, javaMember, mappedItems, shortName } =
  require('../java_ts');

module.exports = {
  name: 'check-java-tags',
  kinds: ['jsonly-no-reason', 'jsonly-mapped', 'java-unresolved', 'java-no-member', 'java-unused', 'java-remap', 'tag-conflict'],
  needs: ['program', 'java'],
  hint:
    'Give `@jsOnly` its reason, point `@java` at a Java class (and member) of the index, and drop a tag the ' +
    'convention makes unnecessary (design/current/javascript-rule-linter/PLAN.md §3.4).',
  check(ctx) {
    const { ts } = ctx.program();
    const index = javaIndexOf(ctx);
    const findings = [];
    let checked = 0;

    /**
     * The findings of tags `tags` of `subject` (declared at `at`), whose
     * convention mapping is `found` (a Java name, or null).
     */
    const judge = (subject, at, tags, found) => {
      if (tags.jsOnly && tags.java) {
        findings.push({ kind: 'tag-conflict', subject, at, message: 'carries both @jsOnly and @java' });
        return;
      }
      if (tags.jsOnly) {
        if (tags.jsOnly.text === '') {
          findings.push({ kind: 'jsonly-no-reason', subject, at, message: '@jsOnly needs a reason' });
        }
        if (found !== null) {
          findings.push({
            kind: 'jsonly-mapped',
            subject,
            at,
            message: `is tagged @jsOnly, but it translates ${found}: drop the tag`,
          });
        }
        return;
      }
      if (!tags.java) return;
      const target = tags.java.text;
      const [fqn, name] = target.split('#');
      const cls = index.resolve(fqn);
      if (!cls) {
        findings.push({ kind: 'java-unresolved', subject, at, message: `@java ${target} names no class of the Java index` });
        return;
      }
      if (name !== undefined) {
        const declared = cls.overloads.some((o) => o.name === name) || cls.fields.some((f) => f.name === name);
        if (!declared) {
          findings.push({
            kind: 'java-no-member',
            subject,
            at,
            message: `@java ${target}: ${shortName(cls)} declares no member ${name}`,
          });
          return;
        }
      }
      const tagged = name === undefined ? shortName(cls) : `${shortName(cls)}#${name}`;
      if (found === tagged) {
        findings.push({ kind: 'java-unused', subject, at, message: `@java ${target} names what the convention finds: drop the tag` });
      } else if (found !== null) {
        findings.push({
          kind: 'java-remap',
          subject,
          at,
          message: `@java ${target} re-maps what the convention maps to ${found}: rename the item, or drop the tag`,
        });
      }
    };

    for (const item of exportedItems(ctx)) {
      checked += 1;
      const convention = conventionClass(ctx, index, item);
      const found = convention && convention.cls ? shortName(convention.cls) : convention ? convention.fqn : null;
      judge(`${item.rel}:${item.name}`, atNode(ctx, item.node), item.tags, found);
    }
    for (const { item, java } of mappedItems(ctx)) {
      for (const m of itemMembers(ts, item)) {
        checked += 1;
        const tags = tagsOf(ts, m.node);
        if (!tags.jsOnly && !tags.java) continue;
        const jm = java && java.cls ? javaMember(index, java.cls, m) : null;
        const found = jm ? `${shortName(jm.owner)}#${jm.name}` : null;
        judge(`${item.name}.${m.name}`, atNode(ctx, m.node), tags, found);
      }
    }
    return { findings, checked };
  },
};
