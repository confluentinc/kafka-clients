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

// D2 check-tsdoc-tags (javascript/CLAUDE.md "Files and layout": TSDoc with
// `@param name -`, `@returns`, `@throws` with the error class in backquotes,
// no JSDoc type annotations). On the documented items of D1 that have a doc:
//   - every parameter of a function, method or constructor, or of a function
//     type (a type alias to one, `Callback`, or a function-typed property),
//     has `@param <name> - <text>` (the hyphen TSDoc requires), and no
//     `@param` names a parameter the function lacks;
//   - every property of an options-object parameter (an object type, as J6
//     reads it, not an interface or class the TypeScript or Node.js libraries
//     declare, such as `Error`; `Readonly<{ … }>` is one) is documented: by a TSDoc comment of its own in the type, or
//     named in backquotes in the parameter's `@param` text (`` `timeoutMs` ``);
//   - a function or method whose return type is not `void`, `undefined` or
//     `Promise<void>` has `@returns` (a getter does not: TSDoc documents an
//     accessor by its description);
//   - every `@throws` starts with the error class in backquotes
//     (`` @throws `XError` text ``), and that class is one lib/ exports or a
//     JavaScript built-in error;
//   - each class a Java `@throws` of the member names (the Java index's
//     Javadoc of the live public overloads it maps to) has a `@throws`
//     naming it as J1 names it (`XException` -> `XError`). On a method
//     returning a Promise, such a `@throws` documents a rejection, its text
//     saying it rejects (PLAN.md §8 decision 18);
//   - no JSDoc tag of lib/ carries a type (`@param {string} x`).
//
// A finding about one parameter, option or error names it after a `/` in its
// subject (`KafkaProducer.send/KafkaError`), so a baseline line waives that
// one alone.

const { documentedItems } = require('../tsdoc');
const { mappedItems, javaIndexOf, javaMember } = require('../java_ts');
const { liveForms } = require('./overload_shape');

const BUILTIN_ERRORS = new Set(['Error', 'TypeError', 'RangeError', 'SyntaxError', 'ReferenceError', 'EvalError', 'URIError', 'AggregateError']);

/** The text of a JSDoc tag's comment. */
function commentText(ts, tag) {
  const c = tag.comment;
  if (c === undefined) return '';
  return typeof c === 'string' ? c : ts.getTextOfJSDocComment(c) || '';
}

/** A Java exception class's name as lib/ names it. */
function errorName(java) {
  const simple = java.split('.').pop();
  return simple.endsWith('Exception') ? `${simple.slice(0, -'Exception'.length)}Error` : simple;
}

module.exports = {
  name: 'check-tsdoc-tags',
  kinds: ['param-missing', 'param-hyphen', 'param-unknown', 'option-undocumented', 'returns-missing', 'throws-form', 'throws-missing', 'throws-unknown', 'jsdoc-type'],
  needs: ['program', 'java'],
  hint:
    'Document every parameter as @param name - text, the return as @returns, each error Java throws as ' +
    '@throws `XError` text, with no JSDoc types (javascript/CLAUDE.md "Files and layout").',
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const index = javaIndexOf(ctx);
    const javaOf = new Map(mappedItems(ctx).map(({ item, java, isError }) => [item.node, isError ? null : java && java.cls]));
    const exported = new Set(mappedItems(ctx).map(({ item }) => item.name));
    const findings = [];
    let checked = 0;
    for (const d of documentedItems(ctx)) {
      if (d.docs.length === 0) continue;
      const node = d.node;
      const isFunction =
        ts.isFunctionDeclaration(node) || ts.isMethodDeclaration(node) || ts.isConstructorDeclaration(node) || ts.isMethodSignature(node);
      // A function type: a type alias to one (`Callback`) or a function-typed property.
      const fnType =
        (ts.isTypeAliasDeclaration(node) || ts.isPropertySignature(node) || ts.isPropertyDeclaration(node)) && node.type && ts.isFunctionTypeNode(node.type)
          ? node.type
          : null;
      if (!isFunction && !fnType) continue;
      checked += 1;
      const signature = fnType || node;
      const tags = d.docs.flatMap((doc) => doc.tags || []);
      const params = tags.filter((t) => ts.isJSDocParameterTag(t));
      const names = signature.parameters.map((p) => p.name.getText());
      for (const p of signature.parameters) {
        const name = p.name.getText();
        const tag = params.find((t) => t.name.getText() === name);
        if (!tag) {
          findings.push({ kind: 'param-missing', subject: `${d.subject}/${name}`, message: `has no @param ${name} -` });
          continue;
        }
        const text = commentText(ts, tag);
        if (!/^\s*-/.test(text)) findings.push({ kind: 'param-hyphen', subject: `${d.subject}/${name}`, message: `@param ${name} lacks the TSDoc hyphen (@param ${name} - text)` });
        // An options object: each property documented in the type or named in the text.
        const type = checker.getNonNullableType(checker.getTypeAtLocation(p));
        const isOptions =
          (type.flags & ts.TypeFlags.Object) !== 0 &&
          !type.isUnion() &&
          !checker.isArrayType(type) &&
          type.getCallSignatures().length === 0 &&
          !(type.symbol && type.symbol.flags & ts.SymbolFlags.Class) &&
          // An interface or class the TypeScript or Node.js libraries declare
          // (`Error`) is no options object; a library mapped type or alias over an
          // object literal (`Readonly<{ … }>`) is, its properties being the literal's.
          !(
            type.symbol &&
            type.symbol.flags & (ts.SymbolFlags.Interface | ts.SymbolFlags.Class) &&
            (type.symbol.declarations || []).length > 0 &&
            type.symbol.declarations.every((x) => x.getSourceFile().isDeclarationFile)
          );
        if (isOptions) {
          for (const prop of checker.getPropertiesOfType(type)) {
            const ownDoc = (prop.declarations || []).some((decl) => (decl.jsDoc || []).length > 0);
            const named = text.includes(`\`${prop.name}\``);
            if (!ownDoc && !named) {
              findings.push({
                kind: 'option-undocumented',
                subject: `${d.subject}/${name}.${prop.name}`,
                message: `option ${prop.name} of @param ${name} is documented neither in its type nor in backquotes in the @param text`,
              });
            }
          }
        }
      }
      for (const t of params) {
        if (!names.includes(t.name.getText())) findings.push({ kind: 'param-unknown', subject: `${d.subject}/${t.name.getText()}`, message: `@param ${t.name.getText()} names no parameter` });
      }
      if (isFunction && !ts.isConstructorDeclaration(node)) {
        const sig = checker.getSignatureFromDeclaration(node);
        const ret = sig ? checker.getReturnTypeOfSignature(sig) : null;
        const text = ret ? checker.typeToString(ret) : 'void';
        if (!['void', 'undefined', 'Promise<void>'].includes(text) && !tags.some((t) => t.tagName.text === 'returns' || t.tagName.text === 'return')) {
          findings.push({ kind: 'returns-missing', subject: d.subject, message: `returns ${text} but has no @returns` });
        }
      }
      const throwsTags = tags.filter((t) => t.tagName.text === 'throws');
      const thrownNames = [];
      for (const t of throwsTags) {
        const m = /^\s*\`(\w+)\`/.exec(commentText(ts, t));
        if (m) thrownNames.push(m[1]);
        else {
          const first = commentText(ts, t).trim().split(/\s+/)[0] || '';
          findings.push({
            kind: 'throws-form',
            subject: `${d.subject}/${first}`,
            message: `@throws ${first || '(empty)'} does not start with the error class in backquotes (@throws \`XError\` text)`,
          });
        }
      }
      for (const n of thrownNames) {
        if (!exported.has(n) && !BUILTIN_ERRORS.has(n)) {
          findings.push({ kind: 'throws-unknown', subject: `${d.subject}/${n}`, message: `@throws \`${n}\` names no class lib/ exports nor a JavaScript error` });
        }
      }
      const cls = d.member ? javaOf.get(d.item.node) : null;
      if (cls && d.member) {
        const jm = javaMember(index, cls, d.member);
        if (jm && jm.kind !== 'field') {
          const javaThrows = new Set();
          for (const o of liveForms(jm.overloads)) for (const th of (o.javadoc && o.javadoc.throws) || []) javaThrows.add(th.class);
          for (const th of [...javaThrows].sort()) {
            const want = errorName(th);
            if (!thrownNames.includes(want)) {
              findings.push({
                kind: 'throws-missing',
                subject: `${d.subject}/${want}`,
                message: `Java's ${jm.name} throws ${th.split('.').pop()}: document it as @throws \`${want}\``,
              });
            }
          }
        }
      }
    }
    // No JSDoc type anywhere in lib/.
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const visit = (n) => {
        for (const doc of n.jsDoc || []) {
          for (const t of doc.tags || []) {
            if (t.typeExpression) {
              findings.push({
                kind: 'jsdoc-type',
                subject: `${rel}:${sf.getLineAndCharacterOfPosition(t.getStart(sf)).line + 1}`,
                message: `@${t.tagName.text} carries the JSDoc type ${t.typeExpression.getText(sf)}: TypeScript gives the type`,
              });
            }
          }
        }
        ts.forEachChild(n, visit);
      };
      visit(sf);
    }
    return { findings, checked };
  },
};
