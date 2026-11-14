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

// check-module-exports (root CLAUDE.md, JavaScript Binding Conventions,
// "Layout and modules": each Java package has one import path, each package
// has an index that re-exports its classes, and the root exports only Java's
// built-in errors and `Duration`). On every `index.ts` of src/ but
// src/internal/:
//   - each `export ... from` names a module of the index's own folder
//     (`./kafka_producer`): not a child folder, whose package has its own
//     import path, nor a file elsewhere (`foreign-export`);
//   - no `export * as name from` re-exports a module as a namespace, which
//     would give a package a second import path (`namespace-export`);
//   - the index declares nothing of its own, but the
//     `export declare const __esModule: true;` marker, which emits no code
//     (the package is CommonJS, built with `module: nodenext`: root
//     conventions, Layout and modules)
//     (`index-declaration`);
//   - src/index.ts re-exports only Java's built-in errors (the error classes
//     src/internal/error_registry.ts maps to a `java.*` exception) and
//     `Duration` (`root-export`).

const path = require('node:path');

const { atNode } = require('../location');
const { errorRegistry } = require('../java_ts');

/** The names the root may export besides the built-in errors. */
const ROOT_EXTRA = new Set(['Duration']);

/** Whether statement `st` is the `export declare const __esModule: true;` marker. */
function isEsModuleMarker(ts, st) {
  if (!ts.isVariableStatement(st)) return false;
  const flags = ts.getCombinedModifierFlags(st.declarationList.declarations[0]);
  const decls = st.declarationList.declarations;
  return (
    decls.length === 1 &&
    ts.isIdentifier(decls[0].name) &&
    decls[0].name.text === '__esModule' &&
    (flags & ts.ModifierFlags.Ambient) !== 0 &&
    (flags & ts.ModifierFlags.Export) !== 0
  );
}

module.exports = {
  name: 'check-module-exports',
  kinds: ['foreign-export', 'namespace-export', 'index-declaration', 'root-export'],
  needs: ['program'],
  hint:
    'Make each index.ts re-export only the modules of its own folder, by name, with no namespace; the root ' +
    'exports only Java\'s built-in errors and Duration, and each package is imported by its own path (root ' +
    'CLAUDE.md, JavaScript Binding Conventions, Layout and modules).',
  check(ctx) {
    const { ts, sourceFiles } = ctx.program();
    const files = new Set(sourceFiles.map((sf) => ctx.rel(sf.fileName)));
    const registry = errorRegistry(ctx);
    // The built-in errors: their module and name.
    const builtIns = new Set();
    for (const [rel, row] of registry) if (row.java.startsWith('java.')) builtIns.add(`${rel}:${row.name}`);
    const findings = [];
    let checked = 0;
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      if (!rel.startsWith('src/') || rel.startsWith('src/internal/') || path.posix.basename(rel) !== 'index.ts') continue;
      const dir = path.posix.dirname(rel);
      const isRoot = rel === 'src/index.ts';
      for (const st of sf.statements) {
        checked += 1;
        const at = atNode(ctx, st);
        const line = sf.getLineAndCharacterOfPosition(st.getStart(sf)).line + 1;
        if (!ts.isExportDeclaration(st)) {
          if (ts.isImportDeclaration(st) || isEsModuleMarker(ts, st)) continue;
          findings.push({
            kind: 'index-declaration',
            subject: `${rel}:${line}`,
            at,
            message: 'declares something of its own: an index only re-exports the modules of its folder',
          });
          continue;
        }
        if (st.moduleSpecifier === undefined || !ts.isStringLiteral(st.moduleSpecifier)) {
          findings.push({ kind: 'index-declaration', subject: `${rel}:${line}`, at, message: 'exports what it declares: an index only re-exports the modules of its folder' });
          continue;
        }
        const spec = st.moduleSpecifier.text;
        const target = path.posix.normalize(path.posix.join(dir, spec));
        const own = path.posix.dirname(target) === dir && files.has(`${target}.ts`);
        if (st.exportClause !== undefined && ts.isNamespaceExport(st.exportClause)) {
          findings.push({
            kind: 'namespace-export',
            subject: `${rel}:${st.exportClause.name.text}`,
            at,
            message: `re-exports ${spec} as the namespace ${st.exportClause.name.text}: each package has its own import path, and an index re-exports its classes by name`,
          });
          continue;
        }
        if (!own) {
          findings.push({
            kind: 'foreign-export',
            subject: `${rel}:${spec}`,
            at,
            message: `re-exports ${spec}, which is not a module of ${dir}/: a package has one import path`,
          });
          continue;
        }
        if (!isRoot) continue;
        const names =
          st.exportClause !== undefined && ts.isNamedExports(st.exportClause)
            ? st.exportClause.elements.map((e) => (e.propertyName || e.name).text)
            : ['*'];
        for (const name of names) {
          if (ROOT_EXTRA.has(name) || builtIns.has(`${target}.ts:${name}`)) continue;
          findings.push({
            kind: 'root-export',
            subject: `${rel}:${name}`,
            at,
            message: `exports ${name === '*' ? `everything of ${spec}` : name}: the root exports only Java's built-in errors and Duration`,
          });
        }
      }
    }
    return { findings, checked };
  },
};
