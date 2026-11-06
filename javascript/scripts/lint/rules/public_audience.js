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

// J4 check-public-audience (root CLAUDE.md §2, §3): what lib/ exports is
// public in Java.
//   - An exported class, interface or type translates a class annotated
//     `@InterfaceAudience.Public` at the Kafka version the Rust lint reads
//     (`java::AUDIENCE_REF`; a nested class takes its top-level class's), in
//     a package with no `internal` segment and without the "not a supported
//     Kafka API" disclaimer.
//   - A public member translates a public Java member: a method name with a
//     public overload, a public field, a public constructor.
// The error classes are exempt from the audience: there is one per Java
// exception the FFI enum names, Public or not (javascript/CLAUDE.md "Errors",
// as the Python client's errors). `@jsOnly` items translate nothing.

const { javaIndexOf, mappedItems, shortName } = require('../java_ts');
const { mappedMembers } = require('./java_members');

module.exports = {
  name: 'check-public-audience',
  kinds: ['not-public-audience', 'internal-package', 'unsupported-package', 'member-not-public'],
  needs: ['program', 'java'],
  hint:
    'Export only what Java makes public: a class of `@InterfaceAudience.Public`, outside `internals` and ' +
    'the unsupported packages, and its public members; keep the rest private (root CLAUDE.md §2).',
  check(ctx) {
    const ref = ctx.javaIndex().audience_ref;
    javaIndexOf(ctx);
    const findings = [];
    let checked = 0;
    for (const { item, java, isError } of mappedItems(ctx)) {
      if (item.tags.jsOnly || isError || java === null || !java.cls) continue;
      checked += 1;
      const cls = java.cls;
      const subject = `${item.rel}:${item.name}`;
      if (!cls.public_audience) {
        findings.push({
          kind: 'not-public-audience',
          subject,
          message: `translates ${shortName(cls)}, which is not @InterfaceAudience.Public in Kafka ${ref}`,
        });
      }
      if (cls.package.split('.').some((s) => s.includes('internal'))) {
        findings.push({
          kind: 'internal-package',
          subject,
          message: `translates ${shortName(cls)}, whose package ${cls.package} is internal`,
        });
      }
      if (cls.unsupported_package) {
        findings.push({
          kind: 'unsupported-package',
          subject,
          message: `translates ${shortName(cls)}, whose package ${cls.package} is not a supported Kafka API`,
        });
      }
    }
    for (const { item, m, jm } of mappedMembers(ctx)) {
      if (jm === null) continue;
      checked += 1;
      const visibilities =
        jm.kind === 'field' ? [jm.field.visibility] : jm.overloads.map((o) => o.visibility);
      if (!visibilities.includes('public')) {
        const what = jm.kind === 'constructors' ? 'constructor' : jm.kind === 'field' ? 'field' : 'method';
        findings.push({
          kind: 'member-not-public',
          subject: `${item.name}.${m.name}`,
          message: `translates the ${[...new Set(visibilities)].join(' / ')} ${what} ${shortName(jm.owner)}#${jm.name}`,
        });
      }
    }
    return { findings, checked };
  },
};
