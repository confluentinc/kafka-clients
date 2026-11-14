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

// J1 check-java-name (javascript/CLAUDE.md "Files and layout", root CLAUDE.md
// §2): every class, interface and type a public module of src/ exports
// translates a Java class of its folder's package, by name, or is listed
// (lint-mapping.json's `invented` and `javaTargets`); its file is the Java class's
// name snake_cased; a file holds one public Java type; a Java exception's
// class is named `...Error`. An error class takes its Java class from
// src/internal/error_registry.ts, the generated hierarchy's list. A function or
// constant export is on the list of invented names; an enum, or a namespace
// that does not merge with its class (root CLAUDE.md, JavaScript Binding
// Conventions, Names: a namespace with the class's name holds its types), is a
// form the translation does not use. The exports are the type checker's, so a
// re-export (`export { X }`) and an ambient one (`export declare class`) count.

const { atNode } = require('../location');
const path = require('node:path');

const { PREFIX, javaIndexOf, exportedItems, mappedItems, snakeCase, shortName } = require('../java_ts');

module.exports = {
  name: 'check-java-name',
  kinds: ['no-java-class', 'jsonly-required', 'export-form', 'file-name', 'one-type-per-file', 'error-name', 'java-package'],
  needs: ['program', 'java'],
  hint:
    'Name the export as its Java class, in the file of that name snake_cased under the folder of its Java ' +
    'package, one per file; list an export with no Java class in lint-mapping.json\'s `invented` with its reason, and one ' +
    'the convention does not find in its `javaTargets` (javascript/CLAUDE.md "Files and layout").',
  check(ctx) {
    const index = javaIndexOf(ctx);
    const findings = [];
    const perFile = new Map();
    // The first item of each file, where a file-level finding is reported.
    const firstOf = new Map();
    let checked = 0;
    for (const { item, java, isError } of mappedItems(ctx)) {
      checked += 1;
      if (item.tags.jsOnly) continue;
      const subject = `${item.rel}:${item.name}`;
      const pkg = index.packageOf(item.dir);
      if (java === null) {
        findings.push({
          kind: 'no-java-class',
          subject, at: atNode(ctx, item.node),
          message: `exports ${item.name}, which names no Java class of ${pkg === null ? 'java.*' : pkg}: list it in lint-mapping.json's invented, or javaTargets`,
        });
        continue;
      }
      if (!perFile.has(item.rel)) perFile.set(item.rel, []);
      perFile.get(item.rel).push(item.name);
      if (!firstOf.has(item.rel)) firstOf.set(item.rel, item.node);
      if (java.via === 'tag') continue;
      const javaName = java.fqn.split(/[.$]/).pop();
      const javaShort = java.cls ? shortName(java.cls) : java.fqn;
      if (javaName.endsWith('Exception') && item.name !== `${javaName.slice(0, -'Exception'.length)}Error`) {
        findings.push({
          kind: 'error-name',
          subject, at: atNode(ctx, item.node),
          message: `translates ${javaShort}: an exception's class is named ${javaName.slice(0, -'Exception'.length)}Error`,
        });
      }
      const stem = path.posix.basename(item.rel, '.ts');
      if (stem !== snakeCase(item.name)) {
        findings.push({
          kind: 'file-name',
          subject, at: atNode(ctx, item.node),
          message: `exports ${item.name} (${javaShort}); the file must be ${snakeCase(item.name)}.ts`,
        });
      }
      if (isError) {
        const javaPkg = java.fqn.startsWith(PREFIX) ? java.fqn.slice(PREFIX.length).replace(/\.[^.]*$/, '') : null;
        const expected = javaPkg === null ? '' : javaPkg.replace(/^clients\./, '').split('.').join('/');
        if (item.dir !== expected) {
          findings.push({
            kind: 'java-package',
            subject, at: atNode(ctx, item.node),
            message: `translates ${javaShort}, of ${javaPkg === null ? 'java.*' : javaPkg}: its file belongs in src/${expected === '' ? '' : `${expected}/`}`,
          });
        }
      }
    }
    // The exports no Java class translates by form: a function or a constant
    // is the client's own (listed), an enum or a namespace a form the
    // translation does not use (a Java enum is a class of static readonly
    // constants, javascript/CLAUDE.md "Java shape, JS idiom").
    for (const item of exportedItems(ctx)) {
      if (item.kind === 'class' || item.kind === 'interface' || item.kind === 'type') continue;
      checked += 1;
      const subject = `${item.rel}:${item.name}`;
      if (item.kind === 'enum' || item.kind === 'namespace') {
        findings.push({
          kind: 'export-form',
          subject, at: atNode(ctx, item.node),
          message: `exports ${item.name} as a TypeScript ${item.kind}, a form no Java type translates to: make it a class`,
        });
      } else if (!item.tags.jsOnly) {
        findings.push({
          kind: 'jsonly-required',
          subject, at: atNode(ctx, item.node),
          message: `exports the ${item.kind} ${item.name}, which no Java class translates to: list it in lint-mapping.json's invented`,
        });
      }
    }
    for (const [rel, all] of perFile) {
      if (all.length > 1) {
        findings.push({
          kind: 'one-type-per-file',
          subject: rel,
          at: atNode(ctx, firstOf.get(rel)),
          message: `holds ${all.length} public Java types (${all.join(', ')}); a file holds one`,
        });
      }
    }
    return { findings, checked };
  },
};
