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

// H2 check-exception-word (root CLAUDE.md §2, §4): no identifier of src/ or
// native/ contains the word "exception", in any case. Comments and string
// literals may name the Java classes, so only identifiers are read: the
// TypeScript sources through the TypeScript parser, the C sources through
// scripts/lint/c_source.js's tokenizer.
//
// Names the client does not choose are exempt: Node-API's own functions
// (`napi_` / `node_api_`), which the addon calls, and the Java member names
// the JavaScript rules keep, listed below with their reason and, where the
// name is exempt only in some positions, the positions. A listed name that no
// longer appears (in an exempt position) is a finding, so the list cannot
// grow stale.

const { scan } = require('../c_source');

const WORD = /exception/i;

/** The C prefixes of Node-API, whose names the addon calls but does not choose. */
const NODE_API_PREFIXES = ['napi_', 'node_api_'];

/**
 * Whether identifier `node` names a property of an object: a type-literal or
 * interface member, an object-literal key (a shorthand one included), a
 * property of an object binding pattern, or the name read by a property
 * access. A local variable, a parameter or a `catch` binding is not.
 */
function isPropertyName(ts, node) {
  const parent = node.parent;
  if (parent === undefined) return false;
  if (ts.isPropertySignature(parent) || ts.isPropertyAssignment(parent)) return parent.name === node;
  if (ts.isShorthandPropertyAssignment(parent)) return parent.name === node;
  if (ts.isPropertyAccessExpression(parent)) return parent.name === node;
  if (ts.isBindingElement(parent) && ts.isObjectBindingPattern(parent.parent)) {
    return parent.propertyName === node || (parent.propertyName === undefined && parent.name === node);
  }
  return false;
}

/** The class declaration enclosing `node`, or undefined. */
function enclosingClass(ts, node) {
  for (let n = node.parent; n !== undefined; n = n.parent) if (ts.isClassDeclaration(n)) return n;
  return undefined;
}

/**
 * Whether identifier `node` is a field of class `className`: the name of a
 * property it declares, or of a property access in its body
 * (`this.sendException`).
 */
function fieldOf(className) {
  return (ts, node) => {
    const parent = node.parent;
    if (parent === undefined) return false;
    const named = (ts.isPropertyDeclaration(parent) || ts.isPropertyAccessExpression(parent)) && parent.name === node;
    const cls = enclosingClass(ts, node);
    return named && cls !== undefined && cls.name !== undefined && cls.name.text === className;
  };
}

/**
 * The TypeScript names kept from Java: why, and in which positions (`where`,
 * absent for every position).
 */
const JAVA_NAMES = new Map([
  [
    'isCompletedExceptionally',
    {
      reason: "Java's KafkaFuture#isCompletedExceptionally, which javascript/CLAUDE.md keeps (\"KafkaFuture is Java's\")",
    },
  ],
  ...['sendException', 'flushException', 'closeException'].map((name) => [
    name,
    {
      reason: `Java's public field MockProducer#${name}, which the TypeScript MockProducer keeps as a field of the same name, as a field of MockProducer only`,
      where: fieldOf('MockProducer'),
    },
  ]),
  [
    'exception',
    {
      reason:
        "the parameter of Java's Callback#onCompletion, a named parameter of the send callback's one object " +
        '(javascript/CLAUDE.md "Java shape, JS idiom": parameters are named as Java names them, as the Python client\'s are), ' +
        'as a property name only',
      where: isPropertyName,
    },
  ],
]);

/** Every identifier node of a parsed TypeScript source, with its 1-based line. */
function tsIdentifiers(ts, sf) {
  const out = [];
  const visit = (node) => {
    if (ts.isIdentifier(node) || ts.isPrivateIdentifier(node)) {
      out.push({ name: node.text, node, line: sf.getLineAndCharacterOfPosition(node.getStart(sf)).line + 1 });
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return out;
}

module.exports = {
  name: 'check-exception-word',
  kinds: ['exception-word', 'unused-exemption'],
  /**
   * The sources this instance reads (PLAN.md §3): `all`, `ts` (src/'s
   * TypeScript, ESLint's `kafka/exception-word`, with the Java names) or `c`
   * (native/'s C, the runner's, with Node-API's prefixes).
   */
  PART: 'all',
  get needs() {
    return { all: ['typescript', 'src', 'native'], ts: ['typescript', 'src'], c: ['native'] }[this.PART];
  },
  hint:
    'Rename it after the Error: the word "exception" appears only in comments and strings about the Java ' +
    'client (root CLAUDE.md §2, §4).',
  check(ctx) {
    const readTs = this.PART !== 'c';
    const readC = this.PART !== 'ts';
    const findings = [];
    let checked = 0;
    const report = (rel, line, name) =>
      findings.push({
        kind: 'exception-word',
        subject: `${rel}:${line}`,
        message: `identifier \`${name}\` contains "exception"`,
      });
    const used = new Set();
    for (const rel of readTs ? ctx.files(['src'], ['.ts']) : []) {
      for (const id of tsIdentifiers(ctx.typescript(), ctx.sourceFile(rel))) {
        checked += 1;
        if (!WORD.test(id.name)) continue;
        const kept = JAVA_NAMES.get(id.name);
        if (kept !== undefined && (kept.where === undefined || kept.where(ctx.typescript(), id.node))) used.add(id.name);
        else report(rel, id.line, id.name);
      }
    }
    for (const rel of readC ? ctx.files(['native'], ['.c', '.h']) : []) {
      for (const token of scan(ctx.read(rel)).tokens) {
        if (token.type !== 'ident') continue;
        checked += 1;
        const nodeApi = NODE_API_PREFIXES.some((p) => token.value.startsWith(p));
        if (WORD.test(token.value) && !nodeApi) report(rel, token.line, token.value);
      }
    }
    for (const [name, { reason }] of readTs ? JAVA_NAMES : []) {
      if (!used.has(name)) {
        findings.push({
          kind: 'unused-exemption',
          subject: name,
          unusedEntry: name,
          message: `exempt as ${reason}, but no src/ identifier is named so: delete the exemption`,
        });
      }
    }
    return { findings, checked };
  },
};
