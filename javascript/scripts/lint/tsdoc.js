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

// The TSDoc of the items D1 and D2 read: each exported item of a public
// module and each public member of an exported class, interface or object
// type (scripts/lint/java_ts.js), with the JSDoc comment the parser attaches
// to it.

const { exportedItems, itemMembers } = require('./java_ts');

/** The JSDoc comments of declaration `node` (a variable's are its statement's). */
function docsOf(ts, node) {
  const target = ts.isVariableDeclaration(node) ? node.parent.parent : node;
  return (target.jsDoc || []).filter((d) => d.kind === ts.SyntaxKind.JSDoc);
}

/**
 * The documented items: `{subject, node, docs, item, member}`, `member` null
 * for an export itself. A get / set accessor pair is one member, and so are
 * the overload signatures of a function or method (any one's doc counts).
 */
function documentedItems(ctx) {
  return ctx.once('documentedItems', () => {
    const { ts, checker } = ctx.program();
    const out = [];
    for (const item of exportedItems(ctx)) {
      // A function's overload signatures and implementation are one item.
      const symbol = ts.isFunctionDeclaration(item.node) && item.node.name ? checker.getSymbolAtLocation(item.node.name) : null;
      const declarations = symbol ? symbol.declarations.filter((d) => ts.isFunctionDeclaration(d)) : [item.node];
      const docs = declarations.flatMap((d) => docsOf(ts, d));
      out.push({ subject: `${item.rel}:${item.name}`, node: item.node, docs, item, member: null });
      if (item.kind !== 'class' && item.kind !== 'interface' && item.kind !== 'type') continue;
      const seen = new Map();
      for (const m of itemMembers(ts, item)) {
        const key = `${m.isStatic}:${m.name}:${m.kind === 'get' || m.kind === 'set' ? 'accessor' : m.kind}`;
        const docs = docsOf(ts, m.node);
        if (seen.has(key)) {
          seen.get(key).docs.push(...docs);
          continue;
        }
        const entry = { subject: `${item.name}.${m.name}`, node: m.node, docs: [...docs], item, member: m };
        seen.set(key, entry);
        out.push(entry);
      }
    }
    return out;
  });
}

module.exports = { docsOf, documentedItems };
