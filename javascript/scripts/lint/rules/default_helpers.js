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

// check-default-helpers (root CLAUDE.md, JavaScript Binding Conventions,
// Class family, Q28: "Each interface the user implements that has a
// `default` method gets a helper class, such as
// `ConsumerRebalanceListenerDefaults`, holding Java's default bodies
// translated from the Java source"; "The helper classes are in
// `src/internal/`"): for each interface the user implements whose Java
// interface has a `default` method (the Java index's `default` flag), the
// class `<Interface>Defaults` is declared in
// `src/internal/<interface>_defaults.ts`, with a method of each default
// method's name (`defaults-missing`, `defaults-method-missing`). A
// `...Defaults` class of src/internal/ that no such interface asks for is a
// finding too (`defaults-unused`).
//
// An interface the user implements is an exported TypeScript interface or
// function type translating a Java interface that no exported class of src/
// implements: `Callback`, the user's, and not `Producer`, which
// `KafkaProducer` implements. Not checked: that the helper's bodies are
// Java's, and that the binding calls them for a method the user's object
// lacks.

const { javaIndexOf, mappedItems, exportedItems, snakeCase } = require('../java_ts');

module.exports = {
  name: 'check-default-helpers',
  kinds: ['defaults-missing', 'defaults-method-missing', 'defaults-unused'],
  needs: ['program', 'java'],
  hint:
    "Translate Java's default bodies into the unexported helper class src/internal/<interface>_defaults.ts " +
    '(<Interface>Defaults), one method per default method (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q28).',
  check(ctx) {
    const { ts, sourceFiles } = ctx.program();
    javaIndexOf(ctx);
    const implemented = new Set();
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'class') continue;
      for (const clause of item.node.heritageClauses || []) {
        if (clause.token !== ts.SyntaxKind.ImplementsKeyword) continue;
        for (const t of clause.types) implemented.add(t.expression.getText());
      }
    }
    // The helper classes src/internal/ declares: name -> {rel, methods}.
    const helpers = new Map();
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      if (!rel.startsWith('src/internal/')) continue;
      for (const st of sf.statements) {
        if (!ts.isClassDeclaration(st) || !st.name || !st.name.text.endsWith('Defaults')) continue;
        const methods = new Set(st.members.filter((m) => ts.isMethodDeclaration(m) && m.name && ts.isIdentifier(m.name)).map((m) => m.name.text));
        helpers.set(st.name.text, { rel, methods });
      }
    }
    const findings = [];
    const asked = new Set();
    let checked = 0;
    for (const { item, java } of mappedItems(ctx)) {
      if (item.kind !== 'interface' && item.kind !== 'type') continue;
      if (java === null || !java.cls || java.cls.kind !== 'interface' || implemented.has(item.name)) continue;
      checked++;
      const defaults = [...new Set(java.cls.overloads.filter((o) => o.default && o.visibility !== 'private').map((o) => o.name))];
      if (defaults.length === 0) continue;
      const name = `${item.name}Defaults`;
      const rel = `src/internal/${snakeCase(item.name)}_defaults.ts`;
      asked.add(name);
      const helper = helpers.get(name);
      if (!helper || helper.rel !== rel) {
        findings.push({
          kind: 'defaults-missing',
          subject: item.name,
          message: `has the default methods ${defaults.join(', ')} in Java, but ${rel} declares no class ${name}`,
        });
        continue;
      }
      for (const method of defaults) {
        if (!helper.methods.has(method)) {
          findings.push({
            kind: 'defaults-method-missing',
            subject: `${name}.${method}`,
            message: `${rel} does not translate Java's default ${java.fqn}#${method}`,
          });
        }
      }
    }
    for (const [name, helper] of helpers) {
      checked++;
      if (!asked.has(name)) {
        findings.push({
          kind: 'defaults-unused',
          subject: name,
          message: `${helper.rel} declares it, but no interface the user implements has default methods for it`,
        });
      }
    }
    return { findings, checked };
  },
};
