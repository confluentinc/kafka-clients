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

// D4 check-java-line (scripts/lint/rules/java_line.js) and the reference
// forms of scripts/lint/java_line.js, on the vectors of the Rust lint's own
// tests (rust/xtask/src/lint_custom/api_docs.rs), which it ports.

const test = require('node:test');
const assert = require('node:assert/strict');

const { javaLineRefs, javaLineRefsRun } = require('../../scripts/lint/java_line');
const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-java-line';

/** The references of `text` with `context` the comment line before it. */
const refsIn = (text, context) =>
  javaLineRefsRun([context, text])
    .filter(([line]) => line === 1)
    .map(([, r]) => r);

test('the file forms and the short code-span form', () => {
  assert.deepEqual(javaLineRefs('See ProducerRecord.java:69 for the check.'), ['ProducerRecord.java:69']);
  assert.deepEqual(javaLineRefs('at `:142` and `:12-34`, not `:x` or `a:1`'), ['`:142`', '`:12-34`']);
  assert.deepEqual(javaLineRefs('Foo.java:12-34 and Bar.java: no'), ['Foo.java:12']);
});

test('the word forms', () => {
  assert.deepEqual(javaLineRefs('(Java 73), Java line 140-142, (Java 967/972), Java lines 12–34.'), [
    'Java 73',
    'Java 140-142',
    'Java 967/972',
    'Java 12–34',
  ]);
  assert.deepEqual(javaLineRefs("Java 4.4 removed it; Java 11's constructors; JavaScript 5; Java's 3 forms"), []);
  assert.deepEqual(javaLineRefs('Translated from `MockProducerTest.testAutoCompleteMock` (line 73).'), ['line 73']);
  assert.deepEqual(javaLineRefs('`toOffsetCommitRequestData`, lines 62-67'), ['line 62-67']);
  assert.deepEqual(refsIn('(line 85): `assignment()` holds tp.', '`testFoo` checks'), ['line 85']);
  assert.deepEqual(javaLineRefs('the Java `poll` at line 12'), ['line 12']);
  assert.deepEqual(javaLineRefs('`close_internal` line 2675'), []);
  assert.deepEqual(javaLineRefs('`KafkaProducer` at line 30'), []);
  assert.deepEqual(javaLineRefs("Rust's `ConsumerHeartbeatRequestManager.poll()` (lines 797-812"), []);
  assert.deepEqual(javaLineRefs('`testFoo` (producer_perf_test.rs closes at line 944)'), []);
  assert.deepEqual(javaLineRefs('the broker at localhost:9092 on line 3 of the config'), []);
  assert.deepEqual(refsIn('(line 85)', 'a `test_foo` translation'), []);
});

test('a reference wrapped across comment lines', () => {
  assert.deepEqual(javaLineRefsRun(['// Update the position (Java line', '// 303-306). Java uses `seek`.']), [
    [0, 'Java 303-306'],
  ]);
  assert.deepEqual(javaLineRefsRun(['/// of `ConsumerBounceTest` (lines', '/// 79-103) on a cluster']), [
    [0, 'line 79-103'],
  ]);
  assert.deepEqual(
    javaLineRefsRun(['    // ends with `config.providers` (`ProducerConfig.java`', '    // 613-618);']),
    [[0, 'ProducerConfig.java 613-618']],
  );
  assert.deepEqual(
    javaLineRefsRun(['/*', ' * Translated from `KafkaProducerTest.foo` (Java', ' * 1989-1993)', ' */']),
    [[1, 'Java 1989-1993']],
  );
  assert.deepEqual(javaLineRefsRun(['/// Translates `testFooBar`', '/// (line', '/// 278).']), [[1, 'line 278']]);
  assert.deepEqual(javaLineRefsRun(['/// `testFooBar` is in foo.rs (line', '/// 278).']), []);
  assert.deepEqual(javaLineRefsRun(['/// see the config file, line', '/// 12.']), []);
  assert.deepEqual(javaLineRefsRun(['// the `SenderTest` clock starts at', '// 1000 for the same reason']), []);
});

test('the at forms', () => {
  assert.deepEqual(
    javaLineRefsRun(['/// for `testSplitBatchAndSend`, Java spells that out at 2373-2375 with']),
    [[0, 'at 2373-2375']],
  );
  assert.deepEqual(javaLineRefsRun(['// `SenderTest#testFoo` covers it,', '// the call at', '// 12-14.']), [
    [1, 'at 12-14'],
  ]);
  assert.deepEqual(javaLineRefs('and `assertNotNull(md)` at 727 does not close it'), ['at 727']);
  assert.deepEqual(javaLineRefs('`SenderTest.java` at 88'), ['at 88']);
  assert.deepEqual(javaLineRefs('`testFoo` stops at 10, where the first fetch leaves it'), []);
  assert.deepEqual(javaLineRefs('bounded at 50 ms, as `pollTimeout` says'), []);
  assert.deepEqual(javaLineRefs('partitions 0 at 3-5 are fetched'), []);
  assert.deepEqual(javaLineRefs('`test_foo` at 727'), []);
  assert.deepEqual(javaLineRefs('`SenderTest` in sender.rs, at 12-14'), []);
  assert.deepEqual(javaLineRefs('the bootstrap cluster leaves `lastMetadataUpdateMs` at 0'), []);
  assert.deepEqual(javaLineRefsRun(['/// `SenderTest#testFoo` reads `lastSendMs`', '/// at 0 and line 0.']), []);
});

/** `parts` joined: a reference split so this file's own comments and strings stay clean. */
const j = (...parts) => parts.join('');

test('the rule: one finding per file, comments of TypeScript, JavaScript and C, never strings or regexes', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'src/a.ts': j('export const a = 1;\n// Java checks it (Producer', 'Record.java:69),\n// and Java 12-14.\n'),
    'src/clean.ts': j("export const s = 'Foo.java", ":12';\nexport const t = `Java 12-14`;\n"),
    'native/a.c': j('/*\n * see Sender', '.java:120\n */\nconst char* s = "Foo.java', ':1"; // Java 7\n'),
    'test/t.test.js': j("const re = /\\/\\/ Foo.java", ":1/;\nconst x = 'a'; /* `:", "12` */\n"),
    'scripts/s.js': '// Java 4.4 is a version, not a line\n',
    'tools/x/y.js': j('/** Translated from `KafkaProducerTest.testFoo` (line\n * 73). */\n'),
    'examples/e.js': j('// Foo.java', ':1 is out of scope\n'),
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  const rest = ': name the Java member instead (`org.apache.kafka.…Class#member`)';
  assert.deepEqual(findings(r), [
    // ESLint's half (TypeScript and JavaScript) first, then the runner's (C).
    j('src/a.ts: 2 Java file:line reference(s) in comments, the first `ProducerRecord.java', ':69` at line 2', rest),
    j('test/t.test.js: 1 Java file:line reference(s) in comments, the first `', '`:12`', '` at line 2', rest),
    j('tools/x/y.js: 1 Java file:line reference(s) in comments, the first `line 73` at line 1', rest),
    j('native/a.c: 2 Java file:line reference(s) in comments, the first `Sender.java', ':120` at line 2', rest),
  ]);
  assert.ok(r.err.includes('❌ check-java-line: 4 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      'Refer to the Java member by its fully qualified name (`org.apache.kafka.clients.producer.ProducerRecord#partition`), not by a line of its file: lines move with every Kafka release (.claude/rules/documentation.md §7).',
    ),
  );
});

test('a clean project passes; a baseline line waives a file', () => {
  let r = lint(project({ 'src/a.ts': '// Java names `ProducerRecord#partition`.\nexport {};\n', 'native/a.c': '// fine\n' }), [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-java-line: 2 item(s) checked'), r.text);
  r = lint(project({ 'src/a.ts': j('// Foo.java', ':1\n'), 'eslint-baseline.txt': 'java-line src/a.ts\n' }), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('cannot run without the typescript package', () => {
  const r = lint(project({ 'src/a.ts': 'export {};\n' }), [RULE], {
    loadTypescript: () => {
      throw new Error('missing');
    },
  });
  assert.equal(r.status, 1);
  assert.deepEqual(r.err.slice(0, 2), [
    '',
    '❌ check-java-line: cannot run, cannot load the typescript package (missing): run `npm ci` in javascript/',
  ]);
});

test('nothing in scope fails: 0 items checked', () => {
  const r = lint(project({ 'README.md': 'x\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-java-line: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

test('the gRPC harness is in scope', () => {
  const root = project({
    'grpc-server/grpc_server.js': j('// as Sender', '.java:12 does\n'),
    'grpc-server/grpc_translate.js': '// clean\n',
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    j(
      'grpc-server/grpc_server.js: 1 Java file:line reference(s) in comments, the first `Sender.java',
      ':12` at line 1: name the Java member instead (`org.apache.kafka.…Class#member`)',
    ),
  ]);
});
