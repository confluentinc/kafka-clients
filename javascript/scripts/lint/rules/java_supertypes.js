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

// check-java-supertypes (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family and Errors): each exported class or interface
// translating a Java type has, as its direct TypeScript supertypes (a class's
// `extends` and `implements`, an interface's `extends`), the translations of
// the Java type's direct supertypes (the index's `supertypes`), and no other:
//   - a translated type of the client by its export: a class in `extends`,
//     an interface in `implements` (in an interface's `extends`);
//   - a JDK type by the JS conventions' JDK table (JDK below), which a type
//     missing from it stops (`jdk-unmapped`);
//   - a type of the index the client does not translate (an internal one,
//     `AbstractConfig`, `AdminClient`) by its own supertypes, recursively:
//     Java's `KafkaAdminClient extends AdminClient`, and `AdminClient
//     implements Admin`, so `KafkaAdminClient` implements `Admin`.
// Type arguments are not checked, nor is an export whose form
// check-java-kind refuses, or whose Java class the index does not hold (the
// root's built-in errors).

const { atNode } = require('../location');
const { PREFIX, javaIndexOf, mappedItems, errorRegistry, itemKey, memberName, shortName } = require('../java_ts');
const { expectedForm, formsOf } = require('./java_kind');

/** Where the JS conventions give the JDK table and the exception roots. */
const TABLE = 'javascript/CLAUDE.md, JavaScript Binding Conventions, Class family';
const ERRORS = 'javascript/CLAUDE.md, JavaScript Binding Conventions, Errors';

/**
 * The JDK types a translated Java type may extend or implement, as the JS
 * conventions translate them: the members the type has for it (`closeable`:
 * `close()` returning a Promise and `[Symbol.asyncDispose]`), the
 * TypeScript lib type it may name (`lib`) or must extend (`base`), and the
 * rule. A JDK exception the root exports (`IllegalStateException`) is that
 * root class, by the error registry.
 */
const JDK = new Map([
  ['Closeable', { members: 'closeable', why: `${TABLE}: Closeable becomes close(): Promise<void> plus [Symbol.asyncDispose]` }],
  ['AutoCloseable', { members: 'closeable', why: `${TABLE}: AutoCloseable becomes close(): Promise<void> plus [Symbol.asyncDispose]` }],
  ['Iterable', { members: ['[Symbol.iterator]'], lib: 'Iterable', why: `${TABLE}: Iterable<T> becomes [Symbol.iterator]()` }],
  ['Comparable', { members: ['compareTo'], why: `${TABLE}: Comparable<T> keeps compareTo()` }],
  ['Future', { why: `${TABLE}: Future becomes KafkaFuture` }],
  ['Serializable', { why: `${TABLE}: Serializable is not translated` }],
  ['Principal', { why: `${TABLE}: Principal is not translated` }],
  ['Object', { why: "Java's root class" }],
  ['RuntimeException', { base: 'Error', why: `${ERRORS}: KafkaError extends Error heads the error tree` }],
  ['Exception', { base: 'Error', why: `${ERRORS}: KafkaError extends Error heads the error tree` }],
  ['Throwable', { base: 'Error', why: `${ERRORS}: KafkaError extends Error heads the error tree` }],
  [
    'NullPointerException',
    { base: 'TypeError', why: 'javascript/CLAUDE.md, JavaScript Binding Conventions, Types: NullPointerException is TypeError' },
  ],
]);

/**
 * The serde interfaces, whose `Closeable` is an optional `close?()`
 * (javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization: a
 * serde is `{ serialize(...), close?() }`), as the Types table's "A client's
 * `Closeable`" leaves it.
 */
const SERDES = new Set(['org.apache.kafka.common.serialization.Serializer', 'org.apache.kafka.common.serialization.Deserializer']);

/** The class Java names `written` in `cls`'s declaration, looked up in the classes enclosing `cls`, or null. */
function nestedFqn(index, cls, written) {
  const base = written.replace(/<.*$/, '').split('.').join('$');
  for (let k = cls.path.length - 1; k >= 1; k--) {
    const fqn = `${PREFIX}${cls.package}.${[...cls.path.slice(0, k), base].join('$')}`;
    if (index.byFqn.has(fqn)) return fqn;
  }
  return null;
}

/** The TypeScript type a heritage expression names: an export of src/, a lib type, or another declaration. */
function targetOf(ctx, expr, byNode) {
  const { ts, checker, program } = ctx.program();
  let symbol = checker.getSymbolAtLocation(expr.expression);
  if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
  const name = expr.expression.getText();
  if (!symbol) return { kind: 'other', name };
  for (const d of symbol.declarations || []) {
    if (byNode.has(d)) return { kind: 'item', item: byNode.get(d), name };
  }
  const sf = symbol.declarations && symbol.declarations[0] && symbol.declarations[0].getSourceFile();
  if (sf && program.isSourceFileDefaultLibrary(sf)) return { kind: 'lib', name: symbol.name };
  return { kind: 'other', name };
}

/** The names of type `type`'s members, inherited ones included (`[Symbol.iterator]` for a well-known symbol). */
function memberNames(ts, type) {
  const out = new Map();
  for (const p of type.getProperties()) {
    const d = p.declarations && p.declarations[0];
    const name = d && d.name ? memberName(ts, d.name) : null;
    out.set(name === null ? p.name : name, p);
  }
  return out;
}

module.exports = {
  name: 'check-java-supertypes',
  kinds: ['supertype-missing', 'supertype-clause', 'supertype-extra', 'jdk-member', 'jdk-unmapped'],
  needs: ['program', 'java'],
  hint:
    "Extend and implement the translations of the Java type's direct supertypes, a JDK one as the JS conventions' " +
    'table gives it (Closeable as close() and [Symbol.asyncDispose], Iterable as [Symbol.iterator](), the exception ' +
    'roots as Error), and nothing else (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family).',
  JDK,
  check(ctx) {
    const { ts, checker } = ctx.program();
    const index = javaIndexOf(ctx);
    const findings = [];
    let checked = 0;
    const mapped = mappedItems(ctx);
    const byNode = new Map(mapped.map(({ item }) => [item.node, item]));
    // The export translating each Java class, the convention's or javaTargets'.
    const translation = new Map();
    for (const { item, java } of mapped) {
      if (item.tags.jsOnly || java === null || translation.has(java.fqn)) continue;
      translation.set(java.fqn, item);
    }
    // The root's classes of Java's built-in errors, by Java simple name.
    const roots = new Map();
    for (const [rel, row] of errorRegistry(ctx)) {
      if (row.java.startsWith('java.')) roots.set(row.java.split('.').pop(), `${rel}:${row.name}`);
    }
    const byKey = new Map(mapped.map(({ item }) => [itemKey(item), item]));
    for (const { item, java } of mapped) {
      if (item.tags.jsOnly || java === null || !java.cls) continue;
      const isClass = ts.isClassDeclaration(item.node);
      if (!isClass && !ts.isInterfaceDeclaration(item.node)) continue;
      const cls = java.cls;
      const symbol = checker.getSymbolAtLocation(item.node.name);
      const { form } = expectedForm(cls);
      const forms = formsOf(ts, (symbol && symbol.declarations) || [item.node]);
      if (!forms.has(form) || (isClass ? form !== 'class' : form !== 'interface')) continue;
      checked += 1;
      const subject = `${item.rel}:${item.name}`;
      const at = atNode(ctx, item.node);
      const javaName = shortName(cls);
      // What the TypeScript declaration names, by clause.
      const written = [];
      for (const clause of item.node.heritageClauses || []) {
        const where = clause.token === ts.SyntaxKind.ExtendsKeyword ? 'extends' : 'implements';
        for (const expr of clause.types) written.push({ where, target: targetOf(ctx, expr, byNode), used: false });
      }
      const use = (pred) => {
        const w = written.find((x) => !x.used && pred(x));
        if (w) w.used = true;
        return w;
      };
      const members = symbol ? memberNames(ts, checker.getDeclaredTypeOfSymbol(symbol)) : new Map();
      // Java's supertypes, each with the class declaring it: the direct ones,
      // then, for one of the index the client does not translate, its own
      // (`via`, the untranslated types in between), each type once.
      const supertypesOf = (c, via) => c.supertypes.map((written, i) => ({ owner: c, written, fqn: c.supertype_fqns[i], via }));
      const work = supertypesOf(cls, []);
      const seen = new Set();
      while (work.length > 0) {
        const { owner, written: writtenSuper, fqn: resolvedFqn, via } = work.shift();
        const javaSuper = writtenSuper.replace(/<.*$/, '');
        // The index's resolution, else (a hand-written index) the loader's; a
        // name it leaves unresolved may be a type nested in an enclosing class.
        let fqn = resolvedFqn;
        if (fqn === undefined) {
          const resolved = index.supertype(owner, javaSuper);
          fqn = resolved ? resolved.fqn : null;
        }
        if (fqn === null) fqn = nestedFqn(index, owner, javaSuper);
        const through = via.length > 0 ? `, through ${via.map(shortName).join(' and ')}, which the client does not translate` : '';
        if (fqn) {
          if (seen.has(fqn)) continue;
          seen.add(fqn);
          const sup = index.byFqn.get(fqn);
          if (!sup) continue;
          const target = translation.get(fqn);
          // Not translated: its own supertypes in its place.
          if (!target) {
            work.push(...supertypesOf(sup, [...via, sup]));
            continue;
          }
          // A form no class extends: nothing.
          const supForm = expectedForm(sup).form;
          if (supForm !== 'class' && supForm !== 'interface') continue;
          const want = isClass && supForm === 'class' ? 'extends' : isClass ? 'implements' : 'extends';
          const key = itemKey(target);
          const found = use((x) => x.target.kind === 'item' && itemKey(x.target.item) === key);
          if (!found) {
            findings.push({
              kind: 'supertype-missing',
              subject,
              at,
              message: `does not ${want === 'extends' ? 'extend' : 'implement'} ${target.name}, which translates ${shortName(sup)}, a ${via.length > 0 ? '' : 'direct '}supertype of Java's ${javaName}${through}`,
            });
          } else if (found.where !== want) {
            findings.push({
              kind: 'supertype-clause',
              subject,
              at,
              message: `${found.where} ${target.name}, the ${supForm} translating ${shortName(sup)}: ${want} it`,
            });
          }
          continue;
        }
        const simple = javaSuper.split('.').pop();
        if (seen.has(simple)) continue;
        seen.add(simple);
        const root = roots.get(simple);
        if (root !== undefined) {
          const target = byKey.get(root);
          if (!target) continue;
          if (!use((x) => x.target.kind === 'item' && itemKey(x.target.item) === root && x.where === 'extends')) {
            findings.push({
              kind: 'supertype-missing',
              subject,
              at,
              message: `does not extend ${target.name}, the root's class of Java's ${simple}, a ${via.length > 0 ? '' : 'direct '}supertype of Java's ${javaName}${through} (${ERRORS})`,
            });
          }
          continue;
        }
        const row = JDK.get(simple);
        if (row === undefined) {
          findings.push({
            kind: 'jdk-unmapped',
            subject,
            at,
            message: `Java's ${shortName(owner)} has the supertype ${javaSuper}, which the JS conventions' JDK table does not translate: it needs a row (${TABLE}, Q29)`,
          });
          continue;
        }
        if (row.base && !use((x) => x.target.kind === 'lib' && x.target.name === row.base && x.where === 'extends')) {
          findings.push({
            kind: 'supertype-missing',
            subject,
            at,
            message: `does not extend ${row.base}, for Java's ${simple} (${row.why})`,
          });
        }
        if (row.lib) use((x) => x.target.kind === 'lib' && x.target.name === row.lib);
        let needed = row.members || [];
        if (row.members === 'closeable') {
          const serde = index.chain(cls).some((c) => SERDES.has(c.fqn));
          needed = serde ? ['close'] : ['close', '[Symbol.asyncDispose]'];
          const close = members.get('close');
          if (!serde && close) {
            const type = checker.getTypeOfSymbolAtLocation(close, item.node);
            const signatures = type.getCallSignatures();
            const waits =
              signatures.length > 0 && signatures.every((s) => /^Promise</.test(checker.typeToString(checker.getReturnTypeOfSignature(s))));
            if (!waits) {
              findings.push({
                kind: 'jdk-member',
                subject,
                at,
                message: `has a close() that does not return a Promise, for Java's ${simple} (${row.why})`,
              });
            }
          }
        }
        for (const name of needed) {
          if (members.has(name)) continue;
          findings.push({ kind: 'jdk-member', subject, at, message: `has no ${name}, for Java's ${simple} (${row.why})` });
        }
      }
      for (const w of written) {
        if (w.used) continue;
        findings.push({
          kind: 'supertype-extra',
          subject,
          at,
          message: `${w.where} ${w.target.name}, which translates no supertype of Java's ${javaName}${cls.supertypes.length > 0 ? ` (${cls.supertypes.join(', ')})` : ' (it has none)'}`,
        });
      }
    }
    return { findings, checked };
  },
};
