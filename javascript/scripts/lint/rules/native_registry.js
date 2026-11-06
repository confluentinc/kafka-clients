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

// A2 check-native-registry (javascript/CLAUDE.md "Files and layout"):
//   - every function src/addon.c registers (its napi_property_descriptor
//     table) is used from lib/ through the handle lib/_native.ts exports
//     (under any name: by its type), and every function lib/ uses is
//     registered; a test-only registration is allowed unused, listed with
//     its reason (`TEST_ONLY`);
//   - only lib/_native_loader.ts names a `.node` file (the platform package's
//     addon, then build/Release/);
//   - only lib/_native.ts imports the loader and does the one-time setup
//     (`setFactories`);
//   - no index.ts imports lib/_native.ts: the addon is never loaded by a
//     module folder.

const path = require('node:path');

const { propertyDescriptors } = require('../c_source');
const { nativeSymbol, nativeUses } = require('../native_uses');

const NATIVE = 'lib/_native.ts';
const LOADER = 'lib/_native_loader.ts';
const SETUP = new Set(['setFactories']);

/**
 * The registrations lib/ does not use, by the user's decision of 2026-11-05
 * (design/current/javascript-rule-linter/PLAN.md §8 decision 12), each with
 * its reason. An entry that is not a registration lib/ leaves unused any more
 * is a finding, so the list cannot grow stale.
 */
const TEST_ONLY = new Map([
  ['probe', { test: 'test/unit/_native.test.js', reason: 'a test-only registration: the load check test/unit/_native.test.js calls' }],
]);

/** The module specifiers `sf` imports or requires, with their lines. */
function moduleSpecifiers(ts, sf) {
  const out = [];
  const add = (lit) => out.push({ text: lit.text, line: sf.getLineAndCharacterOfPosition(lit.getStart(sf)).line + 1 });
  const visit = (n) => {
    if ((ts.isImportDeclaration(n) || ts.isExportDeclaration(n)) && n.moduleSpecifier && ts.isStringLiteral(n.moduleSpecifier)) {
      add(n.moduleSpecifier);
    } else if (ts.isImportEqualsDeclaration(n) && ts.isExternalModuleReference(n.moduleReference)) {
      if (ts.isStringLiteral(n.moduleReference.expression)) add(n.moduleReference.expression);
    } else if (
      ts.isCallExpression(n) &&
      ((ts.isIdentifier(n.expression) && n.expression.text === 'require') || n.expression.kind === ts.SyntaxKind.ImportKeyword) &&
      n.arguments.length > 0 &&
      ts.isStringLiteralLike(n.arguments[0])
    ) {
      add(n.arguments[0]);
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return out;
}

/** Whether module specifier `spec`, written in `rel`, names lib file `target` (`lib/_native.ts`). */
function names(rel, spec, target) {
  if (!spec.startsWith('.')) return false;
  const resolved = path.posix.normalize(path.posix.join(path.posix.dirname(rel), spec));
  return resolved === target.replace(/\.ts$/, '') || resolved === target;
}

module.exports = {
  name: 'check-native-registry',
  TEST_ONLY,
  kinds: ['registered-unused', 'used-unregistered', 'unused-allowance', 'node-path', 'loader-import', 'setup-outside', 'index-native'],
  needs: ['program', 'src'],
  hint:
    'Register in src/addon.c exactly the functions lib/ calls; leave finding the addon to ' +
    'lib/_native_loader.ts and loading it to lib/_native.ts (javascript/CLAUDE.md "Files and layout").',
  check(ctx) {
    const { ts, sourceFiles } = ctx.program();
    if (!ctx.files(['src'], ['addon.c']).includes('src/addon.c')) throw new Error('no src/addon.c');
    if (nativeSymbol(ctx) === null) throw new Error(`${NATIVE} exports no handle (\`export = <handle>\`)`);
    const registered = propertyDescriptors(ctx.read('src/addon.c'));
    const uses = nativeUses(ctx);
    const findings = [];
    let checked = 0;
    const usedNames = new Set(uses.map((u) => u.name));
    const registeredNames = new Set(registered.map((r) => r.name));
    for (const [name, { test, reason }] of this.TEST_ONLY) {
      let why = null;
      if (!registeredNames.has(name)) why = 'src/addon.c does not register it';
      else if (usedNames.has(name)) why = 'lib/ uses it';
      else {
        let text = '';
        try {
          text = ctx.read(test);
        } catch (e) {
          if (e.code !== 'ENOENT') throw e;
        }
        if (!new RegExp(`\\.${name}\\s*\\(`).test(text)) why = `${test} does not call it`;
      }
      if (why !== null) {
        findings.push({ kind: 'unused-allowance', subject: name, message: `is allowed unused as ${reason}, but ${why}: delete the entry` });
      }
    }
    for (const r of registered) {
      checked += 1;
      if (!usedNames.has(r.name) && !this.TEST_ONLY.has(r.name)) {
        findings.push({
          kind: 'registered-unused',
          subject: `src/addon.c:${r.name}`,
          message: `registers ${r.name} (${r.method || r.getter}), which lib/ never uses`,
        });
      }
    }
    for (const u of uses) {
      checked += 1;
      if (!registeredNames.has(u.name)) {
        findings.push({
          kind: 'used-unregistered',
          subject: `${u.rel}:${u.line}`,
          message: `native.${u.name} is registered by no napi_property_descriptor of src/addon.c`,
        });
      }
      if (SETUP.has(u.name) && u.rel !== NATIVE) {
        findings.push({
          kind: 'setup-outside',
          subject: `${u.rel}:${u.line}`,
          message: `native.${u.name} is the one-time setup, which only ${NATIVE} does`,
        });
      }
    }
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      checked += 1;
      if (rel !== LOADER) {
        const visit = (n) => {
          // A string, or a template whose last part ends the path (`${dir}/x.node`).
          const tail = ts.isTemplateExpression(n) ? n.templateSpans[n.templateSpans.length - 1].literal.text : null;
          if ((ts.isStringLiteralLike(n) && /\.node$/.test(n.text)) || (tail !== null && /\.node$/.test(tail))) {
            findings.push({
              kind: 'node-path',
              subject: `${rel}:${sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1}`,
              message: `names the .node file '${n.getText(sf).slice(1, -1)}': only ${LOADER} finds the addon`,
            });
          }
          ts.forEachChild(n, visit);
        };
        visit(sf);
      }
      for (const spec of moduleSpecifiers(ts, sf)) {
        if (rel !== NATIVE && names(rel, spec.text, LOADER)) {
          findings.push({
            kind: 'loader-import',
            subject: `${rel}:${spec.line}`,
            message: `imports the loader '${spec.text}': only ${NATIVE} loads the addon`,
          });
        }
        if (path.posix.basename(rel) === 'index.ts' && names(rel, spec.text, NATIVE)) {
          findings.push({
            kind: 'index-native',
            subject: `${rel}:${spec.line}`,
            message: `imports '${spec.text}': no index.ts loads the addon`,
          });
        }
      }
    }
    return { findings, checked };
  },
};
