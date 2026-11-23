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

// check-native-wrappers (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Implementation over the FFI: "Only KafkaProducer,
// KafkaConsumer, the admin client and the three mocks call the addon ...
// except Headers and RecordHeader, which wrap the C RecordHeaders and
// RecordHeader and call the addon too"):
//   - each exported class of that set (`WRAPPERS`; the JS conventions'
//     `RecordHeader` is this binding's `Header`) holds a native handle: its
//     constructor or a field initializer reaches a `native.<fn>` use
//     (`not-native`), as the mocks wrap the FFI's mocks;
//   - any other class of src/ outside src/internal/, exported or not, and any
//     exported function, whose constructor, methods, accessors or field
//     initializers reach one is listed in lint-mapping.json's `nativeCallers`
//     with the reason the rules give (`native-unlisted`), and each entry is
//     needed and has its reason (`native-entry-unused`, `native-entry-no-reason`).
// A use is reached directly, or through the src/internal/ functions and the
// module's own unexported functions the code calls; a call into another class
// of src/ outside src/internal/, or into another exported function, is that
// one's (scripts/lint/native_uses.js). Not checked: what a use does (that it
// creates the FFI's object, that every method goes through it).

const { atNode } = require('../location');
const { exportedItems, mappingLists, MAPPING_FILE } = require('../java_ts');
const { nativeUses, reachedBodies } = require('../native_uses');

/** The classes the JS conventions name as wrapping the FFI, by export name, with what each wraps. */
const WRAPPERS = [
  { name: /^KafkaProducer$/, wraps: "the FFI's producer" },
  { name: /^KafkaConsumer$/, wraps: "the FFI's consumer" },
  { name: /^KafkaAdminClient$/, wraps: "the FFI's admin client" },
  { name: /^Mock[A-Z]/, wraps: "the FFI's mock" },
  { name: /^Headers$/, wraps: 'the C RecordHeaders' },
  { name: /^Header$/, wraps: "the C RecordHeader (the JS conventions' RecordHeader)" },
];

/** The wrapper row of an exported class named `name`, or undefined. */
function wrapperOf(name) {
  return WRAPPERS.find((w) => w.name.test(name));
}

/** The class declaration or expression a declaration `node` is a member of (through nested functions), or null. */
function ownerClass(ts, node) {
  for (let n = node.parent; n; n = n.parent) {
    if (ts.isClassLike(n)) return n;
    if (ts.isSourceFile(n)) return null;
  }
  return null;
}

/** The name a finding gives class `node`: its own, or its variable's for a class expression. */
function className(ts, node) {
  if (node.name) return node.name.text;
  if (node.parent && ts.isVariableDeclaration(node.parent) && ts.isIdentifier(node.parent.name)) return node.parent.name.text;
  return '(anonymous)';
}

module.exports = {
  name: 'check-native-wrappers',
  kinds: ['not-native', 'native-unlisted', 'native-entry-unused', 'native-entry-no-reason'],
  needs: ['program'],
  hint:
    'Only the classes the JS conventions name call the addon, each over its native handle (the producer, ' +
    "consumer and admin client, the mocks over the FFI's mocks, Headers and Header); list any other class or " +
    `exported function that does in ${MAPPING_FILE}'s \`nativeCallers\` with the reason the rules give ` +
    '(javascript/CLAUDE.md, JavaScript Binding Conventions, Implementation over the FFI).',
  WRAPPERS,
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const listed = mappingLists(ctx).nativeCallers;
    const findings = [];
    let checked = 0;
    // The exported functions of each module, by declaration.
    const exportedFunctions = new Set();
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'function') continue;
      const symbol = item.node.name ? checker.getSymbolAtLocation(item.node.name) : undefined;
      for (const d of (symbol && symbol.declarations) || [item.node]) exportedFunctions.add(d);
    }
    /** Whether the reach from subject `subject` (a class or an exported function) follows into `body`. */
    const follows = (subject) => (body) => {
      if (ctx.rel(body.getSourceFile().fileName).startsWith('src/internal/')) return true;
      const owner = ownerClass(ts, body);
      if (owner !== null) return owner === subject;
      return body === subject || !exportedFunctions.has(body);
    };
    /** The first `native.<fn>` use the `roots` of `subject` reach, or null. */
    const firstUse = (subject, roots) => {
      for (const root of roots) {
        for (const body of reachedBodies(ctx, root, follows(subject))) {
          const uses = nativeUses(ctx, body);
          if (uses.length > 0) return uses[0];
        }
      }
      return null;
    };
    // The exported classes of the conventions' set: each holds a handle.
    const wrappers = new Set();
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'class' || !ts.isClassDeclaration(item.node)) continue;
      const row = wrapperOf(item.name);
      if (row === undefined) continue;
      checked += 1;
      wrappers.add(item.node);
      const roots = [];
      for (const m of item.node.members) {
        if (ts.isConstructorDeclaration(m) && m.body) roots.push(m);
        else if (ts.isPropertyDeclaration(m) && m.initializer) roots.push(m.initializer);
      }
      if (firstUse(item.node, roots) === null) {
        findings.push({
          kind: 'not-native',
          subject: `${item.rel}:${item.name}`,
          at: atNode(ctx, item.node),
          message: `holds no native handle: it wraps ${row.wraps}, its constructor or a field reaching native.<fn>`,
        });
      }
    }
    // Every other class, and every exported function, that reaches the addon.
    const reaching = new Map();
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      if (rel.startsWith('src/internal/')) continue;
      const visit = (node) => {
        if ((ts.isClassDeclaration(node) || ts.isClassExpression(node)) && !wrappers.has(node)) {
          checked += 1;
          const roots = [];
          for (const m of node.members) {
            if (m.body !== undefined && m.body !== null) roots.push(m);
            else if (ts.isPropertyDeclaration(m) && m.initializer) roots.push(m.initializer);
          }
          const use = firstUse(node, roots);
          if (use !== null) reaching.set(`${rel}:${className(ts, node)}`, { node, use });
        }
        ts.forEachChild(node, visit);
      };
      visit(sf);
    }
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'function' || !item.node.body) continue;
      checked += 1;
      const use = firstUse(item.node, [item.node]);
      if (use !== null) reaching.set(`${item.rel}:${item.name}`, { node: item.node, use });
    }
    for (const [key, { node, use }] of reaching) {
      if (listed.has(key)) continue;
      findings.push({
        kind: 'native-unlisted',
        subject: key,
        at: atNode(ctx, node),
        message:
          `reaches the addon (native.${use.name}, ${use.rel}:${use.line}), which only the classes the JS conventions ` +
          `name call: list it in ${MAPPING_FILE}'s nativeCallers with the reason the rules give, or move the call`,
      });
    }
    for (const [key, reason] of listed) {
      checked += 1;
      if (typeof reason !== 'string' || reason.trim() === '') {
        findings.push({ kind: 'native-entry-no-reason', subject: key, message: `nativeCallers lists it with no reason` });
      }
      if (!reaching.has(key)) {
        findings.push({
          kind: 'native-entry-unused',
          subject: key,
          message:
            'nativeCallers lists it, but no class or exported function so keyed reaches the addon (a wrapper the JS conventions name needs no entry): drop the entry',
        });
      }
    }
    return { findings, checked };
  },
};
