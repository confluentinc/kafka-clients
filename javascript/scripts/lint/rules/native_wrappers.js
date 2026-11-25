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
// KafkaConsumer, the admin client and the three mocks call the addon"; the
// value types, `Headers` and `RecordHeader` among them, are plain TypeScript):
//   - each exported class of that set (`WRAPPERS`) holds a native handle: its
//     construction (its constructor, its field initializers and, through
//     `extends`, a base class's) or a static method that constructs it
//     (`internalCreate`) reaches a `native.<fn>` use (`not-native`), as the
//     mocks wrap the FFI's mocks;
//   - any other code of a src/ module outside src/internal/ that reaches one
//     is listed in lint-mapping.json's `nativeCallers` with the reason the
//     rules give (`native-unlisted`): a class, exported or not (its members,
//     field initializers and construction, a base class's included), keyed
//     `src/<module>.ts:<Name>`; an exported function, or an exported const by
//     its initializer (an arrow function, an object of methods such as an
//     interface's statics), keyed `src/<module>.ts:<name>`; and the module's
//     top-level code, with any use in it that none of those reaches (a
//     function nothing calls or names), keyed `src/<module>.ts`. Each entry
//     is needed and has its reason (`native-entry-unused`,
//     `native-entry-no-reason`).
// A use is reached directly, or through the src/internal/ functions and the
// module's own unexported functions the code calls or names as a value (a
// callback it hands on); a call into another class of src/ outside
// src/internal/, or into another exported function or const, is that one's
// (scripts/lint/native_uses.js, whose header lists the uses it does not
// find). Not checked: what a use does (that it creates the FFI's object, that
// every method goes through it).

const { atNode } = require('../location');
const { exportedItems, mappingLists, MAPPING_FILE } = require('../java_ts');
const { nativeUses, reachedBodies } = require('../native_uses');

/** The classes the JS conventions name as wrapping the FFI, by export name, with what each wraps. */
const WRAPPERS = [
  { name: /^KafkaProducer$/, wraps: "the FFI's producer" },
  { name: /^KafkaConsumer$/, wraps: "the FFI's consumer" },
  { name: /^KafkaAdminClient$/, wraps: "the FFI's admin client" },
  { name: /^Mock[A-Z]/, wraps: "the FFI's mock" },
];

/** The wrapper row of an exported class named `name`, or undefined. */
function wrapperOf(name) {
  return WRAPPERS.find((w) => w.name.test(name));
}

/** The name a finding gives class `node`: its own, or its variable's for a class expression. */
function className(ts, node) {
  if (node.name) return node.name.text;
  if (node.parent && ts.isVariableDeclaration(node.parent) && ts.isIdentifier(node.parent.name)) return node.parent.name.text;
  return '(anonymous)';
}

/** The class declaration `extends` names for class `node`, or null (none, or not a class of the program). */
function baseClass(ts, checker, node) {
  for (const clause of node.heritageClauses || []) {
    if (clause.token !== ts.SyntaxKind.ExtendsKeyword || clause.types.length === 0) continue;
    let symbol = checker.getSymbolAtLocation(clause.types[0].expression);
    if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
    const decl = ((symbol && symbol.declarations) || []).find((d) => ts.isClassLike(d));
    return decl || null;
  }
  return null;
}

/** Whether `node` holds `new C(...)` or `new this(...)`, `C` the class named `name`. */
function constructs(ts, node, name) {
  let found = false;
  const visit = (n) => {
    if (found) return;
    if (
      ts.isNewExpression(n) &&
      ((ts.isIdentifier(n.expression) && n.expression.text === name) || n.expression.kind === ts.SyntaxKind.ThisKeyword)
    ) {
      found = true;
      return;
    }
    ts.forEachChild(n, visit);
  };
  visit(node);
  return found;
}

module.exports = {
  name: 'check-native-wrappers',
  kinds: ['not-native', 'native-unlisted', 'native-entry-unused', 'native-entry-no-reason'],
  needs: ['program'],
  hint:
    'Only the classes the JS conventions name call the addon, each over its native handle (the producer, ' +
    "consumer and admin client, the mocks over the FFI's mocks); list any other class, " +
    `export or module code that does in ${MAPPING_FILE}'s \`nativeCallers\` with the reason the rules give ` +
    '(javascript/CLAUDE.md, JavaScript Binding Conventions, Implementation over the FFI).',
  WRAPPERS,
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const listed = mappingLists(ctx).nativeCallers;
    const findings = [];
    let checked = 0;
    const files = sourceFiles.filter((sf) => !ctx.rel(sf.fileName).startsWith('src/internal/'));
    // Each module's exported functions (their implementation) and consts (with
    // an initializer), by declaration: the export's key.
    const exportKey = new Map();
    for (const sf of files) {
      const rel = ctx.rel(sf.fileName);
      const moduleSymbol = checker.getSymbolAtLocation(sf);
      for (const exported of moduleSymbol ? checker.getExportsOfModule(moduleSymbol) : []) {
        const target = exported.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(exported) : exported;
        for (const d of target.declarations || []) {
          if (d.getSourceFile() !== sf) continue;
          if ((ts.isFunctionDeclaration(d) && d.body) || (ts.isVariableDeclaration(d) && d.initializer))
            exportKey.set(d, `${rel}:${exported.name}`);
        }
      }
    }
    /** The class or export code `node` belongs to (its nearest class, else its export), or null: a module's own helper. */
    const ownerOf = (node) => {
      for (let n = node; n && !ts.isSourceFile(n); n = n.parent) {
        if (ts.isClassLike(n) || exportKey.has(n)) return n;
      }
      return null;
    };
    /** Whether the reach from subject `subject` (a class, an export or a source file) follows into `body`. */
    const follows = (subject) => (body) => {
      if (ctx.rel(body.getSourceFile().fileName).startsWith('src/internal/')) return true;
      const owner = ownerOf(body);
      return owner === null || owner === subject;
    };
    /** The `native.<fn>` uses the `roots` of `subject` reach, through its calls and the functions it hands on. */
    const usesOf = (subject, roots) => {
      const out = [];
      for (const root of roots) {
        for (const body of reachedBodies(ctx, root, follows(subject), { references: true })) out.push(...nativeUses(ctx, body));
      }
      return out;
    };
    /** What constructing class `node` runs: its constructor, its field initializers and its base class's, as `subject` follows them. */
    const construction = (node, subject = node, seen = new Set()) => {
      if (seen.has(node)) return [];
      seen.add(node);
      const out = [];
      for (const m of node.members) {
        if (ts.isConstructorDeclaration(m) && m.body) out.push(m);
        else if (ts.isPropertyDeclaration(m) && m.initializer && !(ts.getCombinedModifierFlags(m) & ts.ModifierFlags.Static))
          out.push(m.initializer);
      }
      const base = baseClass(ts, checker, node);
      if (base !== null && follows(subject)(base)) out.push(...construction(base, subject, seen));
      return out;
    };
    // Every use credited to some class, export or module, for the leftovers.
    const credited = new Set();
    const reaching = new Map();
    const credit = (key, node, uses) => {
      for (const u of uses) credited.add(u.node);
      if (uses.length > 0 && !reaching.has(key)) reaching.set(key, { node, use: uses[0] });
    };
    // The exported classes of the conventions' set: each holds a handle.
    const wrappers = new Set();
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'class' || !ts.isClassDeclaration(item.node)) continue;
      const row = wrapperOf(item.name);
      if (row === undefined) continue;
      checked += 1;
      wrappers.add(item.node);
      const roots = construction(item.node);
      for (const m of item.node.members) {
        const isStatic = (ts.getCombinedModifierFlags(m) & ts.ModifierFlags.Static) !== 0;
        if (isStatic && ts.isMethodDeclaration(m) && m.body && constructs(ts, m.body, item.name)) roots.push(m);
      }
      if (usesOf(item.node, roots).length === 0) {
        findings.push({
          kind: 'not-native',
          subject: `${item.rel}:${item.name}`,
          at: atNode(ctx, item.node),
          message: `holds no native handle: it wraps ${row.wraps}, its construction or a static method constructing it reaching native.<fn>`,
        });
      }
    }
    for (const sf of files) {
      const rel = ctx.rel(sf.fileName);
      // Every class, a wrapper's uses credited to it.
      const visit = (node) => {
        if (ts.isClassDeclaration(node) || ts.isClassExpression(node)) {
          if (!wrappers.has(node)) checked += 1;
          const roots = construction(node);
          for (const m of node.members) {
            if (ts.isConstructorDeclaration(m)) continue;
            if (m.body !== undefined && m.body !== null) roots.push(m);
            else if (ts.isPropertyDeclaration(m) && m.initializer && !roots.includes(m.initializer)) roots.push(m.initializer);
          }
          const uses = usesOf(node, roots);
          if (wrappers.has(node)) for (const u of uses) credited.add(u.node);
          else credit(`${rel}:${className(ts, node)}`, node, uses);
        }
        ts.forEachChild(node, visit);
      };
      visit(sf);
      // Every exported function and const.
      for (const [d, key] of exportKey) {
        if (d.getSourceFile() !== sf) continue;
        checked += 1;
        credit(key, d, usesOf(d, [ts.isVariableDeclaration(d) ? d.initializer : d]));
      }
      // The module's top-level code: what runs when it is loaded.
      checked += 1;
      const top = [];
      for (const st of sf.statements) {
        if (
          ts.isImportDeclaration(st) ||
          ts.isImportEqualsDeclaration(st) ||
          ts.isExportDeclaration(st) ||
          ts.isClassDeclaration(st) ||
          ts.isFunctionDeclaration(st) ||
          ts.isInterfaceDeclaration(st) ||
          ts.isTypeAliasDeclaration(st) ||
          ts.isEnumDeclaration(st) ||
          ts.isModuleDeclaration(st)
        ) {
          continue;
        }
        if (ts.isVariableStatement(st)) {
          for (const d of st.declarationList.declarations) {
            if (!d.initializer || exportKey.has(d)) continue;
            // A function a call runs, followed from the call.
            if (ts.isArrowFunction(d.initializer) || ts.isFunctionExpression(d.initializer)) continue;
            top.push(d.initializer);
          }
          continue;
        }
        top.push(st);
      }
      credit(rel, sf, usesOf(sf, top));
    }
    // The uses no class, export or top-level code reaches: the module's.
    for (const sf of files) {
      const rel = ctx.rel(sf.fileName);
      const left = nativeUses(ctx, sf).filter((u) => !credited.has(u.node));
      if (left.length > 0 && !reaching.has(rel)) reaching.set(rel, { node: sf, use: left[0] });
    }
    for (const [key, { node, use }] of reaching) {
      if (listed.has(key)) continue;
      findings.push({
        kind: 'native-unlisted',
        subject: key,
        at: ts.isSourceFile(node) ? atNode(ctx, use.node) : atNode(ctx, node),
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
            'nativeCallers lists it, but no class, export or module so keyed reaches the addon (a wrapper the JS conventions name needs no entry): drop the entry',
        });
      }
    }
    return { findings, checked };
  },
};
