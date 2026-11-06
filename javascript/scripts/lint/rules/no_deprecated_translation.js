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

// J5 check-no-deprecated-translation (root CLAUDE.md §3: deprecated API is
// not translated): no exported class translates a Java class deprecated at
// the Kafka versions the Rust lint reads (`java::DEPRECATION_REFS`, the
// `kafka/` tree plus design/current/java-deprecated.txt, merged into the Java
// index), and no public member translates a Java member every overload of
// which is deprecated (a TypeScript function translating a name also
// translates its live overloads), nor a member a deprecated Java supertype
// declares.

const { javaIndexOf, mappedItems, shortName } = require('../java_ts');
const { mappedMembers } = require('./java_members');

module.exports = {
  name: 'check-no-deprecated-translation',
  kinds: ['deprecated-class', 'deprecated-member'],
  needs: ['program', 'java'],
  hint: 'Remove the translation of the deprecated Java API: it is not translated (root CLAUDE.md §3).',
  check(ctx) {
    const refs = ctx.javaIndex().deprecation_refs.join(' + ');
    javaIndexOf(ctx);
    const findings = [];
    let checked = 0;
    for (const { item, java } of mappedItems(ctx)) {
      if (item.tags.jsOnly || java === null || !java.cls) continue;
      checked += 1;
      if (java.cls.deprecated) {
        findings.push({
          kind: 'deprecated-class',
          subject: `${item.rel}:${item.name}`,
          message: `translates ${shortName(java.cls)}, deprecated in Kafka ${refs}`,
        });
      }
    }
    for (const { item, java, m, jm } of mappedMembers(ctx)) {
      if (jm === null) continue;
      checked += 1;
      const deprecated = jm.kind === 'field' ? jm.field.deprecated : jm.overloads.every((o) => o.deprecated);
      // A member of the item's own deprecated class is covered by the class's
      // finding; one inherited from a deprecated supertype is not.
      const ownDeprecated = jm.owner === java.cls && java.cls.deprecated;
      if ((deprecated || jm.owner.deprecated) && !ownDeprecated) {
        findings.push({
          kind: 'deprecated-member',
          subject: `${item.name}.${m.name}`,
          message: `translates ${shortName(jm.owner)}#${jm.name}, deprecated in Kafka ${refs}`,
        });
      }
    }
    return { findings, checked };
  },
};
