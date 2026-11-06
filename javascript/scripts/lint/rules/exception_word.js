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

// H2 check-exception-word (root CLAUDE.md §2, §4): no identifier of lib/ or
// src/ contains the word "exception", in any case. Comments and string
// literals may name the Java classes, so only identifiers are read: the
// TypeScript sources through the TypeScript parser, the C sources through
// scripts/lint/c_source.js's tokenizer.
//
// Names the client does not choose are exempt: Node-API's own functions
// (`napi_` / `node_api_`), which the addon calls, and the Java member names
// the JavaScript rules keep, listed below with their reason. A listed name
// that no longer appears is a finding, so the list cannot grow stale.

const { scan } = require('../c_source');

const WORD = /exception/i;

/** The C prefixes of Node-API, whose names the addon calls but does not choose. */
const NODE_API_PREFIXES = ['napi_', 'node_api_'];

/** The TypeScript names kept from Java, and why. */
const JAVA_NAMES = new Map([
  [
    'isCompletedExceptionally',
    "Java's KafkaFuture#isCompletedExceptionally, which javascript/CLAUDE.md keeps (\"KafkaFuture is Java's\")",
  ],
]);

/** Every identifier node of a parsed TypeScript source, with its 1-based line. */
function tsIdentifiers(ts, sf) {
  const out = [];
  const visit = (node) => {
    if (ts.isIdentifier(node) || ts.isPrivateIdentifier(node)) {
      out.push({ name: node.text, line: sf.getLineAndCharacterOfPosition(node.getStart(sf)).line + 1 });
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return out;
}

module.exports = {
  name: 'check-exception-word',
  kinds: ['exception-word', 'unused-exemption'],
  needs: ['typescript', 'lib', 'src'],
  hint:
    'Rename it after the Error: the word "exception" appears only in comments and strings about the Java ' +
    'client (root CLAUDE.md §2, §4).',
  check(ctx) {
    const ts = ctx.typescript();
    const findings = [];
    let checked = 0;
    const report = (rel, line, name) =>
      findings.push({
        kind: 'exception-word',
        subject: `${rel}:${line}`,
        message: `identifier \`${name}\` contains "exception"`,
      });
    const used = new Set();
    for (const rel of ctx.files(['lib'], ['.ts'])) {
      for (const id of tsIdentifiers(ts, ctx.sourceFile(rel))) {
        checked += 1;
        if (!WORD.test(id.name)) continue;
        if (JAVA_NAMES.has(id.name)) used.add(id.name);
        else report(rel, id.line, id.name);
      }
    }
    for (const rel of ctx.files(['src'], ['.c', '.h'])) {
      for (const token of scan(ctx.read(rel)).tokens) {
        if (token.type !== 'ident') continue;
        checked += 1;
        const nodeApi = NODE_API_PREFIXES.some((p) => token.value.startsWith(p));
        if (WORD.test(token.value) && !nodeApi) report(rel, token.line, token.value);
      }
    }
    for (const [name, reason] of JAVA_NAMES) {
      if (!used.has(name)) {
        findings.push({
          kind: 'unused-exemption',
          subject: name,
          message: `exempt as ${reason}, but no lib/ identifier is named so: delete the exemption`,
        });
      }
    }
    return { findings, checked };
  },
};
