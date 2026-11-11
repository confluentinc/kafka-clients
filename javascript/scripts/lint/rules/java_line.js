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

// D4 check-java-line (.claude/rules/documentation.md §7): no comment of
// src/, native/, test/, scripts/, tools/ or the gRPC harness (grpc-server/)
// cites a Java source line; it names the Java member
// instead. The forms are the Rust lint's (scripts/lint/java_line.js). One finding per file, as the Rust lint
// reports it, keyed by the file.

const { scan } = require('../c_source');
const { sourceComments } = require('../comments');
const { javaLineRefsOfComments } = require('../java_line');

const SCOPE = ['src', 'native', 'test', 'scripts', 'tools', 'grpc-server'];
const TS_LIKE = ['.ts', '.js', '.cjs', '.mjs'];
const C_LIKE = ['.c', '.h'];

module.exports = {
  name: 'check-java-line',
  kinds: ['java-line'],
  /**
   * The files this instance reads (PLAN.md §3): `all`, `ts` (TypeScript and
   * JavaScript, ESLint's `kafka/java-line`) or `c` (C, the runner's).
   */
  PART: 'all',
  get needs() {
    return this.PART === 'c' ? [] : ['typescript'];
  },
  hint:
    'Refer to the Java member by its fully qualified name ' +
    '(`org.apache.kafka.clients.producer.ProducerRecord#partition`), ' +
    'not by a line of its file: lines move with every Kafka release (.claude/rules/documentation.md §7).',
  check(ctx) {
    const findings = [];
    const exts = { all: [...TS_LIKE, ...C_LIKE], ts: TS_LIKE, c: C_LIKE }[this.PART];
    const files = ctx.files(SCOPE, exts);
    for (const rel of files) {
      const comments = C_LIKE.some((e) => rel.endsWith(e))
        ? scan(ctx.read(rel)).comments
        : sourceComments(ctx.typescript(), ctx.sourceFile(rel));
      const refs = javaLineRefsOfComments(comments);
      if (refs.length === 0) continue;
      const [line, first] = refs[0];
      findings.push({
        kind: 'java-line',
        subject: rel,
        at: { file: rel, line, column: 0 },
        message:
          `${refs.length} Java file:line reference(s) in comments, the first \`${first}\` at line ${line}: ` +
          'name the Java member instead (`org.apache.kafka.…Class#member`)',
      });
    }
    return { findings, checked: files.length };
  },
};
