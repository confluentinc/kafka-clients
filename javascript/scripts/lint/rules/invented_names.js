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

// check-invented-names: the mapping lists of lint-mapping.json
// (scripts/lint/java_ts.js `mappingLists`), which cover what the naming
// convention of scripts/lint/java_ts.js cannot derive, in place of TSDoc tags
// (root CLAUDE.md, JavaScript Binding Conventions, Documentation: the tags are
// `@param`, `@returns`, `@throws`, `@defaultValue`, `@deprecated` and one
// `@remarks`). An entry is a finding unless it is needed and resolves:
//   - an `invented` entry names an exported item, or a public member of an
//     exported item, with no Java counterpart, and gives a reason; an entry
//     that matches nothing, or names what the convention maps to Java, is a
//     finding, so the list of invented names cannot grow stale;
//   - a `javaTargets` entry names the Java class, or member, the convention
//     does not find: it resolves in the Java index, the member is declared by
//     that class, and it neither names what the convention already finds nor
//     re-maps an item the convention maps elsewhere;
//   - an item is in one list, not both;
//   - an invented options type, an exported object type or interface of
//     named properties on the list, is named `...Args` (Layout and modules:
//     "An invented options type ends in `Args`").

const { atNode } = require('../location');
const {
  MAPPING_FILE,
  javaIndexOf,
  exportedItems,
  conventionClass,
  itemKey,
  memberKey,
  memberTags,
  itemMembers,
  javaMember,
  mappedItems,
  mappingLists,
  shortName,
} = require('../java_ts');

module.exports = {
  name: 'check-invented-names',
  kinds: [
    'invented-no-reason',
    'invented-mapped',
    'invented-unused',
    'args-name',
    'java-unresolved',
    'java-no-member',
    'java-unused',
    'java-remap',
    'java-target-unused',
    'listed-twice',
  ],
  needs: ['program', 'java'],
  hint:
    `Give each \`invented\` entry of ${MAPPING_FILE} its reason, point each \`javaTargets\` entry at a Java class ` +
    '(and member) of the index, drop an entry the convention makes unnecessary or that matches nothing, and ' +
    'name an invented options type `...Args` (root CLAUDE.md, JavaScript Binding Conventions, Layout and modules).',
  check(ctx) {
    const { ts } = ctx.program();
    const index = javaIndexOf(ctx);
    const lists = mappingLists(ctx);
    const findings = [];
    let checked = 0;
    const used = new Set();

    /** Whether exported type `item` is an options type: an object type, or an interface, of named properties. */
    const isOptionsType = (item) => {
      const node = item.node;
      const members = ts.isInterfaceDeclaration(node)
        ? node.members
        : ts.isTypeAliasDeclaration(node) && ts.isTypeLiteralNode(node.type)
          ? node.type.members
          : null;
      return members !== null && members.some((e) => ts.isPropertySignature(e) && e.name !== undefined);
    };

    /**
     * The findings of the mapping `tags` of `subject` (keyed `key`, declared
     * at `at`), whose convention mapping is `found` (a Java name, or null).
     */
    const judge = (key, subject, at, tags, found) => {
      if (tags.jsOnly || tags.java) used.add(key);
      if (tags.jsOnly && tags.java) {
        findings.push({ kind: 'listed-twice', subject, at, message: 'is both in invented and in javaTargets' });
        return;
      }
      if (tags.jsOnly) {
        if (typeof tags.jsOnly.text !== 'string' || tags.jsOnly.text.trim() === '') {
          findings.push({ kind: 'invented-no-reason', subject, at, message: 'is invented, with no reason' });
        }
        if (found !== null) {
          findings.push({
            kind: 'invented-mapped',
            subject,
            at,
            message: `is listed as invented, but it translates ${found}: drop the entry`,
          });
        }
        return;
      }
      if (!tags.java) return;
      const target = tags.java.text;
      const [fqn, name] = String(target).split('#');
      const cls = index.resolve(fqn);
      if (!cls) {
        findings.push({ kind: 'java-unresolved', subject, at, message: `javaTargets ${target} names no class of the Java index` });
        return;
      }
      if (name !== undefined) {
        const declared = cls.overloads.some((o) => o.name === name) || cls.fields.some((f) => f.name === name);
        if (!declared) {
          findings.push({
            kind: 'java-no-member',
            subject,
            at,
            message: `javaTargets ${target}: ${shortName(cls)} declares no member ${name}`,
          });
          return;
        }
      }
      const tagged = name === undefined ? shortName(cls) : `${shortName(cls)}#${name}`;
      if (found === tagged) {
        findings.push({ kind: 'java-unused', subject, at, message: `javaTargets ${target} names what the convention finds: drop the entry` });
      } else if (found !== null) {
        findings.push({
          kind: 'java-remap',
          subject,
          at,
          message: `javaTargets ${target} re-maps what the convention maps to ${found}: rename the item, or drop the entry`,
        });
      }
    };

    for (const item of exportedItems(ctx)) {
      checked += 1;
      const convention = conventionClass(ctx, index, item);
      const found = convention && convention.cls ? shortName(convention.cls) : convention ? convention.fqn : null;
      const key = itemKey(item);
      judge(key, key, atNode(ctx, item.node), item.tags, found);
      if (item.tags.jsOnly && !item.tags.java && (item.kind === 'type' || item.kind === 'interface') && isOptionsType(item) && !item.name.endsWith('Args')) {
        findings.push({
          kind: 'args-name',
          subject: key,
          at: atNode(ctx, item.node),
          message: `is an invented options type: name it ${item.name.replace(/(Options|Args)?$/, 'Args')}`,
        });
      }
    }
    for (const { item, java } of mappedItems(ctx)) {
      for (const m of itemMembers(ts, item)) {
        checked += 1;
        const tags = memberTags(ctx, item, m);
        if (!tags.jsOnly && !tags.java) continue;
        const jm = java && java.cls ? javaMember(index, java.cls, m) : null;
        const found = jm ? `${shortName(jm.owner)}#${jm.name}` : null;
        const key = memberKey(item, m);
        judge(key, key, atNode(ctx, m.node), tags, found);
      }
    }
    for (const [list, entries, kind] of [
      ['invented', lists.invented, 'invented-unused'],
      ['javaTargets', lists.javaTargets, 'java-target-unused'],
    ]) {
      for (const key of entries.keys()) {
        checked += 1;
        if (used.has(key)) continue;
        findings.push({
          kind,
          subject: key,
          message: `${list} lists it, but no exported item or public member is keyed so: drop the entry`,
        });
      }
    }
    return { findings, checked };
  },
};
