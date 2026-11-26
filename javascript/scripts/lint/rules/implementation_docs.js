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

// check-implementation-docs (javascript/CLAUDE.md, Comments, point 1): the
// TSDoc of a member sits on the interface that declares it. A member of a
// class of src/ that implements a member of an interface of src/ the class
// `implements` (found through the type checker, an inherited interface member
// included) carries no doc comment of its own, or one only, holding one
// `@remarks` block and nothing else, for what the class does differently. A
// doc comment of one `@remarks` alone sits nowhere else, since the docs lint
// asks no tags of it (scripts/lint/eslint/config.js). An interface outside
// src/ (`Iterable`, from TypeScript's lib) declares no TSDoc of the client's,
// so a member implementing one keeps its own.

const { atNode } = require('../location');
const { memberName } = require('../java_ts');

/** The doc comments TypeScript attaches to declaration `node`. */
function docsOf(ts, node) {
  return ts.getJSDocCommentsAndTags(node).filter((d) => ts.isJSDoc(d));
}

/** Whether doc comment `doc` is one `@remarks` block alone: no summary, and no other tag. */
function isRemarksOnly(ts, doc) {
  const summary = (ts.getTextOfJSDocComment(doc.comment) || '').trim();
  const tags = doc.tags || [];
  return summary === '' && tags.length === 1 && tags[0].tagName.text === 'remarks';
}

/** The name a finding gives declaration `node`: `Class.member`, a function's name, or null. */
function declarationName(ts, node) {
  const own = node.name ? memberName(ts, node.name) : null;
  if (own === null) return null;
  const parent = node.parent;
  if (parent && (ts.isClassLike(parent) || ts.isInterfaceDeclaration(parent)) && parent.name) return `${parent.name.text}.${own}`;
  return own;
}

module.exports = {
  name: 'check-implementation-docs',
  kinds: ['implementation-doc', 'remarks-only'],
  needs: ['program'],
  hint:
    'Put the TSDoc on the interface that declares the member; a class member implementing it keeps at most one ' +
    '`@remarks`, for what the class does differently, and a doc comment of one `@remarks` alone sits only there ' +
    '(javascript/CLAUDE.md, Comments, point 1).',
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const srcFiles = new Set(sourceFiles.map((sf) => sf.fileName));
    const findings = [];
    let checked = 0;
    // The doc comments of the members that implement an interface member: a
    // remarks-only one is theirs to carry.
    const implementing = new Set();
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const visit = (node) => {
        if (ts.isClassLike(node) && node.heritageClauses) {
          // Each name the class's `implements` interfaces declare in src/, with where.
          const implemented = new Map();
          for (const clause of node.heritageClauses) {
            if (clause.token !== ts.SyntaxKind.ImplementsKeyword) continue;
            for (const expr of clause.types) {
              for (const p of checker.getPropertiesOfType(checker.getTypeAtLocation(expr))) {
                const decl = (p.declarations || []).find((d) => srcFiles.has(d.getSourceFile().fileName));
                if (decl === undefined || !decl.name) continue;
                const name = memberName(ts, decl.name);
                if (name !== null && !implemented.has(name)) implemented.set(name, declarationName(ts, decl) || name);
              }
            }
          }
          // The class's own instance members of those names, by name: an
          // overloaded method is one member of several declarations.
          const members = new Map();
          for (const m of node.members) {
            if (!m.name || ts.isConstructorDeclaration(m) || ts.isPrivateIdentifier(m.name)) continue;
            if ((ts.getCombinedModifierFlags(m) & ts.ModifierFlags.Static) !== 0) continue;
            const name = memberName(ts, m.name);
            if (name === null || !implemented.has(name)) continue;
            if (!members.has(name)) members.set(name, []);
            members.get(name).push(m);
          }
          const cls = node.name ? node.name.text : '(anonymous)';
          for (const [name, decls] of members) {
            checked += 1;
            const docs = decls.flatMap((d) => docsOf(ts, d));
            for (const d of docs) implementing.add(d);
            if (docs.length === 0 || (docs.length === 1 && isRemarksOnly(ts, docs[0]))) continue;
            findings.push({
              kind: 'implementation-doc',
              subject: `${rel}:${cls}.${name}`,
              at: atNode(ctx, docs[0]),
              message:
                `\`${cls}.${name}\` implements \`${implemented.get(name)}\`, whose TSDoc TypeDoc shows, but carries ` +
                `${docs.length} doc comment(s): keep at most one, a \`@remarks\` alone, for what the class does ` +
                'differently, and move what holds for every implementation to the interface',
            });
          }
        }
        ts.forEachChild(node, visit);
      };
      visit(sf);
    }
    // A remarks-only doc comment on anything else: each doc comment once,
    // on the declaration TypeScript first attaches it to.
    const seen = new Set(implementing);
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const visit = (node) => {
        {
          for (const doc of docsOf(ts, node)) {
            if (seen.has(doc)) continue;
            seen.add(doc);
            checked += 1;
            if (!isRemarksOnly(ts, doc)) continue;
            const at = atNode(ctx, doc);
            const name = declarationName(ts, node);
            findings.push({
              kind: 'remarks-only',
              subject: `${rel}:${name === null ? at.line : name}`,
              at,
              message:
                `a doc comment of one \`@remarks\` alone (line ${at.line}) on ${name === null ? 'a declaration' : `\`${name}\``}, ` +
                'which implements no interface member: give it a summary and the tags the docs lint asks for',
            });
          }
        }
        ts.forEachChild(node, visit);
      };
      visit(sf);
    }
    return { findings, checked };
  },
};
