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

// check-doc-size (scripts/lint/rules/doc_size.js).

const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { javaClass, javaIndex, method, field, mapping } = require('./_java_fixture');
const { wordCount, parseTsdoc } = require('../../scripts/lint/rules/doc_size');

const RULE = 'check-doc-size';

test("words are counted as the Rust lint counts them, TSDoc's links as the Javadoc translator renders them", () => {
  // The Rust lint's own cases (rust/xtask/src/javadoc.rs, test_word_count).
  assert.equal(wordCount("# Returns\n\nReturns [`None`](Option::None) if it isn't.\n```java\nx y\n```"), 8);
  assert.equal(wordCount('## Not a heading inside\n```\n# kept\n```'), 1);
  assert.equal(wordCount(''), 0);
  assert.equal(wordCount('names `kafka_common_Error_is_kafka_error` or `Self::foo_bar`'), 4);
  assert.equal(wordCount('out_err is set'), 3);
  assert.equal(wordCount('a ` b c'), 3);
  // A link with a label is its label, one without a code span; another inline tag is nothing.
  assert.equal(wordCount('See {@link Header.key | the key}, {@link KafkaProducer.send} and {@inheritDoc}.'), 5);
});

test('a doc comment splits into its description and its block tags, outside a code fence', () => {
  assert.deepEqual(
    parseTsdoc(
      '/**\n * Sends it.\n *\n * ```\n * @notATag\n * ```\n * @param options - the record\n *   and more\n * @returns a future\n */',
    ),
    {
      description: 'Sends it.\n\n```\n@notATag\n```',
      blocks: [
        { tag: 'param', text: ' options - the record\n  and more' },
        { tag: 'returns', text: ' a future' },
      ],
    },
  );
});

const P = 'org.apache.kafka.clients.producer.';

/** The Java sizes of the fixture: each Javadoc's words by part, as `cargo xtask java-index` records them. */
const INDEX = javaIndex([
  javaClass('clients.producer.Rec', {
    javadoc: [{ body: 4 }],
    overloads: [
      method('Rec', ['String'], { param_names: ['topic'], javadoc: [{ body: 3, params: [['topic', 3]] }] }),
      method('close', [], { javadoc: [{ body: 6 }] }),
      method('close', ['Duration'], {
        param_names: ['timeout'],
        javadoc: [{ body: 4, params: [['timeout', 5]], throws: [['IllegalArgumentException', 4]] }],
      }),
      method('topic', [], { return_type: 'String', javadoc: [{ body: 0, return: 2 }] }),
      // Two Javadocs (a pointer to two): the member passes when it fits one.
      method('size', [], { return_type: 'int', javadoc: [{ body: 2 }, { body: 20 }] }),
      method('flush', []),
    ],
    fields: [
      field('UNKNOWN', { type: 'int', javadoc: [{ body: 3 }] }),
      // A public mutable field, which a setter translates.
      field('mode', { static: false, final: false, javadoc: [{ body: 4 }] }),
    ],
  }),
  javaClass('clients.producer.Callback', {
    kind: 'interface',
    javadoc: [{ body: 5 }],
    overloads: [
      method('onCompletion', ['RecordMetadata'], { param_names: ['metadata'], javadoc: [{ body: 6, params: [['metadata', 4]] }] }),
    ],
  }),
]);

/** A TSDoc comment of `lines`, indented by `indent`. */
const doc = (/** @type {string} */ indent, /** @type {string[]} */ lines) =>
  `${indent}/**\n${lines.map((l) => `${indent} *${l === '' ? '' : ` ${l}`}`).join('\n')}\n${indent} */\n`;

/** The fixture's Rec, each doc within its budget. */
function rec({ classDoc = ['A record of a topic.'], ctorDoc = ['Creates it.', '', '@param options - the topic'], extra = '' } = {}) {
  return (
    doc('', classDoc) +
    'export class Rec {\n' +
    doc('  ', ['The unknown value.']) +
    '  static readonly UNKNOWN: number = -1;\n' +
    doc('  ', ctorDoc) +
    '  constructor(options: { topic: string });\n' +
    '  constructor(options: unknown) { void options; }\n' +
    doc('  ', ['Closes it now, at once.']) +
    '  close(): void;\n' +
    doc('  ', [
      'Closes it, waiting at most the timeout.',
      '',
      '@param timeout - how long to wait',
      '@throws `IllegalArgumentError` when negative',
    ]) +
    '  close(timeout: number): void;\n' +
    '  close(timeout?: unknown): void { void timeout; }\n' +
    doc('  ', ['The topic name.']) +
    '  topic(): string { return ""; }\n' +
    doc('  ', ['The size, with many more words than its first Javadoc has.']) +
    '  size(): number { return 0; }\n' +
    doc('  ', ['Flushes, with any number of words: Java documents nothing here, so nothing limits them.']) +
    '  flush(): void {}\n' +
    doc('  ', ['Sets the mode of the record.', '', '@param options - the mode']) +
    '  setMode(options: { mode: string }): void;\n' +
    doc('  ', ['Sets the mode, given bare.', '', '@param mode - the mode']) +
    '  setMode(mode: string): void;\n' +
    '  setMode(options: unknown): void { void options; }\n' +
    extra +
    '}\n'
  );
}

const CALLBACK =
  doc('', ['A callback the user implements: called when the record sent to the server is acknowledged.']) +
  'export type Callback = (options: { metadata: unknown }) => void;\n';

const fixture = (/** @type {Record<string, string>} */ files = {}) =>
  project({
    'tsconfig.json': TSCONFIG,
    'java-index.json': INDEX,
    'src/producer/rec.ts': rec(),
    'src/producer/callback.ts': CALLBACK,
    ...files,
  });

test('docs within 150% of their Javadoc pass; a member Java does not document has no limit', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec, UNKNOWN, the constructor, both close signatures, topic, size, both setMode signatures and Callback
  // (flush has no Javadoc).
  assert.ok(r.out.includes('✅ check-doc-size: 10 item(s) checked'), r.text);
});

test('a doc past 150% of its Javadoc is a finding, with its exact text', () => {
  const r = lint(
    fixture({
      'src/producer/rec.ts': rec({
        classDoc: ['A record of a topic, with three words more.'],
        // The options' `@param` counts, as Java has a `@param`.
        ctorDoc: ['Creates a record of a topic.', '', '@param options - the topic the record is sent to'],
      }).replace('Closes it now, at once.', 'Closes it now, at once, and frees all of it.'),
      'src/producer/callback.ts': CALLBACK.replace('acknowledged.', 'acknowledged, as Java runs it on its I/O thread after the send.'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    "src/producer/callback.ts:Callback: 27 words, more than 150% of the Javadoc's 15 (at most 22) of clients.producer.Callback",
    "src/producer/rec.ts:Rec: 9 words, more than 150% of the Javadoc's 4 (at most 6) of clients.producer.Rec",
    "Rec.constructor: 14 words, more than 150% of the Javadoc's 6 (at most 9) of clients.producer.Rec#Rec(String)",
    "Rec.close/1: 10 words, more than 150% of the Javadoc's 6 (at most 9) of clients.producer.Rec#close()",
  ]);
});

test('only what has a Java counterpart counts: tags Java lacks, @defaultValue and @typeParam do not; @remarks, @see and @example do', () => {
  const extraTags = [
    'Creates it.',
    '',
    '@param options - the topic',
    '@param check - none: a type-level check no call gives, which Java has no parameter for',
    '@returns the record, which Java documents with no return tag at all here',
    '@throws `TypeError` when the topic is not a string, which Java does not throw',
    '@defaultValue many words that are not counted since they document a default',
    '@typeParam T - many words that are not counted since they document a type',
    '@deprecated many words that are not counted since they document a deprecation',
  ];
  assert.equal(lint(fixture({ 'src/producer/rec.ts': rec({ ctorDoc: extraTags }) }), [RULE]).status, 0);
  for (const tag of ['@remarks', '@see', '@example']) {
    const r = lint(fixture({ 'src/producer/rec.ts': rec({ ctorDoc: [...extraTags, `${tag} five more words right here`] }) }), [RULE]);
    assert.deepEqual(
      findings(r),
      ["Rec.constructor: 10 words, more than 150% of the Javadoc's 6 (at most 9) of clients.producer.Rec#Rec(String)"],
      tag,
    );
  }
});

test('a signature is measured against the Java form it translates, a getter field and a constant against theirs', () => {
  // The bare close(timeout) is close(Duration)'s: its 15 words fit that form's 19, not close()'s 9.
  const r = lint(
    fixture({
      'src/producer/rec.ts': rec()
        .replace('The topic name.', 'The topic name, read from the record once it was built.')
        .replace('The unknown value.', 'The unknown value of the offset field.'),
    }),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    "Rec.UNKNOWN: 7 words, more than 150% of the Javadoc's 3 (at most 4) of clients.producer.Rec#UNKNOWN",
    "Rec.topic: 11 words, more than 150% of the Javadoc's 2 (at most 3) of clients.producer.Rec#topic()",
  ]);
});

test("each signature of a setter is measured against its field's Javadoc", () => {
  const r = lint(
    fixture({ 'src/producer/rec.ts': rec().replace('Sets the mode, given bare.', 'Sets the mode, given bare, its only argument.') }),
    [RULE],
  );
  assert.deepEqual(findings(r), ["Rec.setMode/2: 8 words, more than 150% of the Javadoc's 4 (at most 6) of clients.producer.Rec#mode"]);
});

test('an exported function javaTargets maps to a Java member is measured against its Javadoc', () => {
  const files = {
    'src/producer/topics.ts':
      doc('', ['The topic of a record, read from it once it was built.', '', '@param options - the record']) +
      'export function recordTopic(options: { record: unknown }): string;\n' +
      doc('', ['The topic.', '', '@param record - the record']) +
      'export function recordTopic(record: unknown): string;\n' +
      'export function recordTopic(options: unknown): string { void options; return ""; }\n',
    'lint-mapping.json': mapping({ javaTargets: { 'src/producer/topics.ts:recordTopic': `${P}Rec#topic` } }),
  };
  assert.deepEqual(findings(lint(fixture(files), [RULE])), [
    "src/producer/topics.ts:recordTopic/1: 12 words, more than 150% of the Javadoc's 2 (at most 3) of clients.producer.Rec#topic",
  ]);
});

test('a member mapped by javaTargets is measured against its target; an invented one has no limit', () => {
  const files = {
    'src/producer/rec.ts': rec({
      extra:
        doc('  ', ['A JavaScript addition, documented at any length, as no Javadoc limits it at all.']) +
        '  toJSON(): string { return ""; }\n',
    }),
    'lint-mapping.json': mapping({ invented: { 'Rec.toJSON': "JavaScript's JSON protocol." } }),
  };
  assert.equal(lint(fixture(files), [RULE]).status, 0);
  const tagged = lint(fixture({ ...files, 'lint-mapping.json': mapping({ javaTargets: { 'Rec.toJSON': `${P}Rec#topic` } }) }), [RULE]);
  assert.deepEqual(findings(tagged), [
    "Rec.toJSON: 14 words, more than 150% of the Javadoc's 2 (at most 3) of clients.producer.Rec#topic()",
  ]);
});
