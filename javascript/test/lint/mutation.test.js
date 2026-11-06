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

// The mutation smoke test of the rule linter
// (design/current/javascript-rule-linter/PLAN.md §6): a copy of the real lib/
// and src/ passes every rule; one known violation per rule, injected into the
// copy, makes exactly that rule fail. It catches a rule whose path through
// the real sources is broken, which a fixture of the rule alone would miss.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { TODO_MARK, project, copy, lint, failedRules } = require('./_project');

const ROOT = path.join(__dirname, '..', '..');

/** The last line of every source's license header. */
const LICENSE_END = 'limitations under the License.\n';

/**
 * A copy of the client's lib/, src/, tsconfig.json, CLAUDE.md (whose rule
 * sentence check-no-todo exempts), both baselines, Java index
 * (build/java-index.json, which `make test-lint` writes first) and bindings
 * header.
 */
function realCopy() {
  const root = project({});
  for (const rel of ['lib', 'src', 'tsconfig.json', 'CLAUDE.md', 'lint-baseline.txt', 'eslint-baseline.txt']) copy(ROOT, root, rel);
  // The test that calls the test-only registration probe (check-native-registry).
  fs.mkdirSync(path.join(root, 'test', 'unit'), { recursive: true });
  copy(ROOT, root, 'test/unit/_native.test.js');
  fs.mkdirSync(path.join(root, 'build'));
  copy(ROOT, root, 'build/java-index.json');
  // The bindings header, where the lint helper looks for it.
  fs.copyFileSync(require('../../scripts/header').headerPath(), path.join(root, 'confluent_kafka_bindings.h'));
  return root;
}

/** Replaces `from` (which must occur) by `to` in root-relative file `rel`. */
function edit(root, rel, from, to) {
  const file = path.join(root, rel);
  const text = fs.readFileSync(file, 'utf8');
  assert.ok(text.includes(from), `${rel} has no ${JSON.stringify(from)}`);
  fs.writeFileSync(file, text.replace(from, to));
}

test('the unmutated copy passes every rule', () => {
  const r = lint(realCopy());
  assert.equal(r.status, 0, r.text);
});

const MUTATIONS = [
  {
    rule: 'check-no-todo',
    what: 'a marker in a comment',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', LICENSE_END, `${LICENSE_END}// ${TODO_MARK} later\n`),
  },
  {
    rule: 'check-no-todo',
    what: 'a marker in the addon',
    apply: (root) => edit(root, 'src/producer.c', '#include', `/* ${TODO_MARK} */\n#include`),
  },
  {
    rule: 'check-exception-word',
    what: 'a C identifier with the word',
    apply: (root) => edit(root, 'src/error.c', '#include', 'static int last_exception_code;\n#include'),
  },
  {
    rule: 'check-exception-word',
    what: 'a TypeScript identifier with the word',
    apply: (root) => edit(root, 'lib/common/kafka_future.ts', LICENSE_END, `${LICENSE_END}const lastException = 0;\nvoid lastException;\n`),
  },
  {
    rule: 'check-private-members',
    what: 'a `_` member made public',
    apply: (root) => edit(root, 'lib/producer/producer_record.ts', 'private _toNative(', '_toNative('),
  },
  {
    rule: 'check-java-line',
    what: 'a comment citing a Java line',
    apply: (root) =>
      edit(root, 'lib/producer/record_metadata.ts', LICENSE_END, [LICENSE_END, "// As Java's Record", 'Metadata.java', ':49.\n'].join('')),
  },
];

/** Appends `text` as a new public module `rel` of the copy. */
function add(root, rel, text) {
  fs.mkdirSync(path.dirname(path.join(root, rel)), { recursive: true });
  fs.writeFileSync(path.join(root, rel), text);
}

MUTATIONS.push(
  {
    // A Java getter turned into a method: J2 finds no Java `getOffset`, and J3 misses `offset`.
    rules: ['check-java-members', 'check-java-completeness'],
    what: 'the offset getter renamed to a getOffset method',
    apply: (root) =>
      edit(
        root,
        'lib/producer/record_metadata.ts',
        '  /** The offset of the record in the topic/partition, or -1 if hasOffset is false. */\n  get offset(): number {',
        '  /**\n   * The offset of the record.\n   *\n   * @returns the offset\n   */\n  getOffset(): number {',
      ),
  },
  {
    rule: 'check-getters-readonly',
    what: 'a public mutable field',
    apply: (root) => edit(root, 'lib/common/security/auth/security_protocol.ts', 'readonly id: number;', 'id: number;'),
  },
  {
    rule: 'check-public-audience',
    what: 'an export translating an internal, non-Public Java class',
    apply: (root) => add(root, 'lib/producer/internals/txn_partition_entry.ts', '/** An entry. */\nexport class TxnPartitionEntry {}\n'),
  },
  {
    rule: 'check-no-deprecated-translation',
    what: 'a translated deprecated constant',
    apply: (root) =>
      edit(
        root,
        'lib/producer/producer_config.ts',
        'export class ProducerConfig {',
        "export class ProducerConfig {\n  /** Deprecated. */\n  static readonly PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG: string = 'x';",
      ),
  },
  {
    rule: 'check-java-completeness',
    what: 'a translated method dropped',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', '  flush(): Promise<void> {', '  private flushAll(): Promise<void> {'),
  },
  {
    rule: 'check-java-name',
    what: 'an export naming no Java class',
    apply: (root) => add(root, 'lib/producer/record_meta.ts', '/** Metadata. */\nexport class RecordMeta {}\n'),
  },
  {
    // J2 reports the static-ness, and J3 no longer counts the static forId as translated.
    rules: ['check-java-members', 'check-java-completeness'],
    what: 'a static method made an instance method',
    apply: (root) => edit(root, 'lib/common/security/auth/security_protocol.ts', 'static forId(', 'forId('),
  },
  {
    // `is` stays: a `done` property is no translation of isDone().
    rules: ['check-java-members', 'check-java-completeness'],
    what: 'an isX method turned into a property without its is',
    apply: (root) => {
      edit(root, 'lib/common/kafka_future.ts', 'isDone(): boolean {', 'get done(): boolean {');
      // A getter needs no @returns: its D2 baseline line goes with it.
      edit(root, 'eslint-baseline.txt', 'returns-missing KafkaFuture.isDone\n', '');
    },
  },
  {
    rule: 'check-waiting-call-promise',
    what: 'flush returning void',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', '  flush(): Promise<void> {', '  flush(): void {'),
  },
  {
    rule: 'check-overload-shape',
    what: 'a ProducerRecordOptions key renamed',
    apply: (root) => edit(root, 'lib/producer/producer_record.ts', '  key?: Data | null | undefined;', '  recordKey?: Data | null | undefined;'),
  },
  {
    rule: 'check-overload-shape',
    what: 'ProducerRecordOptions.topic made optional',
    apply: (root) => edit(root, 'lib/producer/producer_record.ts', '  topic: string;\n', '  topic?: string;\n'),
  },
  {
    rule: 'check-overload-shape',
    what: 'close({timeout}) instead of timeoutMs',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', '  timeoutMs?: number | undefined;', '  timeout?: number | undefined;'),
  },
  {
    // The untranslated get(long, TimeUnit) is baselined by its own form: a
    // positional timeout on get() is a wrong shape the baseline does not cover.
    rule: 'check-overload-shape',
    what: 'KafkaFuture.get taking a positional timeout',
    apply: (root) =>
      edit(
        root,
        'lib/common/kafka_future.ts',
        '   * @returns resolving to the result value, rejecting with the error the\n   *   future failed with\n   */\n  get(): Promise<T> {',
        '   * @param timeoutMs - The maximum time to wait\n   * @returns resolving to the result value, rejecting with the error the\n   *   future failed with\n   */\n  get(timeoutMs?: number): Promise<T> {\n    void timeoutMs;',
      ),
  },
  {
    // The registry misses a use, flush's native path no longer resolves, and
    // so its Promise is no waiting call's.
    rules: ['check-type-mapping', 'check-waiting-call-promise', 'check-native-registry'],
    what: 'a registration removed from addon.c',
    apply: (root) =>
      edit(root, 'src/addon.c', '        {"producerFlush", NULL, js_producer_flush, NULL, NULL, NULL, napi_default, NULL},\n', ''),
  },
  {
    rule: 'check-native-registry',
    what: 'a second import of the loader',
    apply: (root) =>
      edit(root, 'lib/producer/kafka_producer.ts', "import native = require('../_native');", "import native = require('../_native');\nimport { ADDON_FILE } from '../_native_loader';\nvoid ADDON_FILE;"),
  },
  {
    rule: 'check-type-mapping',
    what: 'a getter of another type than Java\'s',
    apply: (root) => edit(root, 'lib/producer/record_metadata.ts', 'get offset(): number {', 'get offset(): string {'),
  },
  {
    rule: 'check-waiting-call-promise',
    what: 'flush returning void, its native call in a module helper',
    apply: (root) => {
      edit(root, 'lib/producer/kafka_producer.ts', '  flush(): Promise<void> {', '  flush(): void {\n    flushVia(this.#handle);\n  }\n\n  private oldFlush(): Promise<void> {');
      fs.appendFileSync(
        path.join(root, 'lib/producer/kafka_producer.ts'),
        '\nfunction flushVia(handle: ProducerHandle): void {\n  native.producerFlush(handle, () => undefined);\n}\n',
      );
    },
  },
  {
    rule: 'check-waiting-call-promise',
    what: 'flush returning void, its native call through an alias of the handle',
    apply: (root) =>
      edit(
        root,
        'lib/producer/kafka_producer.ts',
        '  flush(): Promise<void> {',
        '  flush(): void {\n    const n = native;\n    n.producerFlush(this.#handle, () => undefined);\n  }\n\n  private oldFlush(): Promise<void> {',
      ),
  },
  {
    rule: 'check-type-mapping',
    what: 'a non-blocking method returning a Promise',
    apply: (root) => edit(root, 'lib/common/kafka_future.ts', 'isDone(): boolean {', 'isDone(): Promise<boolean> {'),
  },
  {
    rule: 'check-cb-twin-only',
    what: 'a blocking flush instead of its _cb twin',
    apply: (root) =>
      edit(
        root,
        'src/producer.c',
        '    kafka_producer_Producer_flush_cb(p->view, flush_on_complete, ctx);',
        '    {\n        kafka_common_Error_t* ffi_err = NULL;\n        FFI_DISCARD(kafka_producer_Producer_flush(p->view, &ffi_err));\n    }\n    kafka_producer_Producer_flush_cb(p->view, flush_on_complete, ctx);',
      ),
  },
  {
    rule: 'check-cb-twin-only',
    what: "the hand-over's future read through get_cb",
    apply: (root) =>
      edit(
        root,
        'src/producer.c',
        '    if (value != NULL) FFI_DISCARD(kafka_common_KafkaFuture_destroy_err(value, &ffi_err));',
        '    if (value != NULL) kafka_common_KafkaFuture_get_cb(value, send_on_future, ctx);\n    if (value != NULL) FFI_DISCARD(kafka_common_KafkaFuture_destroy_err(value, &ffi_err));',
      ),
  },
  {
    rule: 'check-cb-twin-only',
    what: 'a blocking flush in a macro of util.h',
    apply: (root) => {
      edit(root, 'src/util.h', '#define FFI_DISCARD(call) error_discard(FFI_ERR(call))\n', '#define FFI_DISCARD(call) error_discard(FFI_ERR(call))\n#define FLUSH_VIEW(view) FFI_DISCARD(kafka_producer_Producer_flush((view), &ffi_err))\n');
      edit(
        root,
        'src/producer.c',
        '    kafka_producer_Producer_flush_cb(p->view, flush_on_complete, ctx);',
        '    {\n        kafka_common_Error_t* ffi_err = NULL;\n        FLUSH_VIEW(p->view);\n    }\n    kafka_producer_Producer_flush_cb(p->view, flush_on_complete, ctx);',
      );
    },
  },
  {
    rule: 'check-out-err',
    what: 'an FFI_ERR dropped',
    apply: (root) =>
      edit(
        root,
        'src/producer.c',
        '        err = FFI_ERR(kafka_Map_put_err(props, strings[2 * i], strings[2 * i + 1], &ffi_err));',
        '        kafka_Map_put_err(props, strings[2 * i], strings[2 * i + 1], &ffi_err);',
      ),
  },
  {
    rule: 'check-out-err',
    what: "the drain's read of the executor's error deleted",
    apply: (root) => edit(root, 'src/callback_queue.c', '        error_discard(ffi_take(&ffi_err));\n', ''),
  },
  {
    rule: 'check-out-err',
    what: 'the executor called with a NULL slot',
    apply: (root) => edit(root, 'src/callback_queue.c', 'q->execute(q->client, &ffi_err)', 'q->execute(q->client, NULL)'),
  },
  {
    rule: 'check-out-err',
    what: 'FFI_DISCARD around a call given another slot',
    apply: (root) =>
      edit(
        root,
        'src/producer.c',
        '    if (props != NULL) FFI_DISCARD(kafka_Map_destroy_err(props, &ffi_err));',
        '    kafka_common_Error_t* map_err = NULL;\n    if (props != NULL) FFI_DISCARD(kafka_Map_destroy_err(props, &map_err));',
      ),
  },
  {
    rule: 'check-notify-hook',
    what: 'a call added in cbq_notify',
    apply: (root) =>
      edit(root, 'src/callback_queue.c', '    pthread_mutex_unlock(&q->notify_mtx);\n}\n\nvoid cbq_schedule', '    pthread_mutex_unlock(&q->notify_mtx);\n    cbq_schedule(q);\n}\n\nvoid cbq_schedule'),
  },
  {
    rule: 'check-notify-hook',
    what: 'a return between the lock and the unlock of cbq_notify',
    apply: (root) =>
      edit(root, 'src/callback_queue.c', '    pthread_mutex_lock(&q->notify_mtx);\n    if (q->notify_open)', '    pthread_mutex_lock(&q->notify_mtx);\n    if (q->stopped) return;\n    if (q->notify_open)'),
  },
  {
    rule: 'check-single-drainer',
    what: '__execute_callbacks_err called outside cbq_drain',
    apply: (root) =>
      edit(
        root,
        'src/producer.c',
        '    if (p->kp != NULL) cbq_drain(env, &p->queue);',
        '    if (p->kp != NULL) cbq_drain(env, &p->queue);\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_producer_Producer__execute_callbacks_err(p->view, &ffi_err));',
      ),
  },
  {
    rule: 'check-completion-order',
    what: 'the future completed before the user callback',
    apply: (root) => {
      edit(root, 'lib/producer/kafka_producer.ts', "      future['_complete'](metadata, error);\n", '');
      edit(root, 'lib/producer/kafka_producer.ts', '      let failure: string | undefined;\n', "      let failure: string | undefined;\n      future['_complete'](metadata, error);\n");
    },
  },
  {
    rule: 'check-completion-order',
    what: "the delivery callback's body moved into a module function completing the future first",
    apply: (root) => {
      const file = path.join(root, 'lib/producer/kafka_producer.ts');
      const text = fs.readFileSync(file, 'utf8');
      const body = /    const onComplete = \(metadata: RecordMetadata, error: Error \| null\): string \| undefined => \{\n[^]*?\n    \};\n/.exec(text);
      assert.ok(body, 'kafka_producer.ts has no onComplete');
      fs.writeFileSync(
        file,
        text.replace(body[0], '    const onComplete = (metadata: RecordMetadata, error: Error | null): string | undefined =>\n      deliver(future, userCallback, metadata, error);\n') +
          '\nfunction deliver(future: KafkaFuture<RecordMetadata>, userCallback: Callback | null, metadata: RecordMetadata, error: Error | null): string | undefined {\n' +
          "  future['_complete'](metadata, error);\n  if (userCallback !== null) userCallback(metadata, error);\n  return undefined;\n}\n",
      );
    },
  },
  {
    rule: 'check-completion-order',
    what: 'the destroying guard guarding another statement than the report',
    apply: (root) =>
      edit(
        root,
        'src/producer.c',
        '    if (!p->destroying) {\n        FFI_DISCARD(kafka_producer_Producer__set_callback_result_err(p->view, callback_id, NULL, report, &ffi_err));\n    } else {\n        // Destroy completes the unreported callbacks itself.\n        error_discard(report);\n    }\n',
        '    if (!p->destroying) ctx->delivered = 1;\n    FFI_DISCARD(kafka_producer_Producer__set_callback_result_err(p->view, callback_id, NULL, report, &ffi_err));\n',
      ),
  },
  {
    rule: 'check-ownership',
    what: 'a configuration never destroyed',
    apply: (root) =>
      edit(root, 'src/producer.c', '    if (config != NULL) FFI_DISCARD(kafka_producer_ProducerConfig_destroy_err(config, &ffi_err));\n', ''),
  },
  {
    rule: 'check-ownership',
    what: "the delivery callback's borrowed error destroyed through a cast",
    apply: (root) =>
      edit(root, 'src/producer.c', '    ctx->delivered = 1;\n', '    error_discard((kafka_common_Error_t*)error);\n    ctx->delivered = 1;\n'),
  },
  {
    rule: 'check-zero-copy',
    what: 'a memcpy of the key',
    apply: (root) =>
      edit(root, 'src/record.c', '    out->bytes.len = (int32_t)length;', '    out->bytes.len = (int32_t)length;\n    memcpy((void*)out->bytes.data, data, length);'),
  },
  {
    rule: 'check-zero-copy',
    what: 'a Uint8Array key copied by Buffer.from in lib/',
    apply: (root) =>
      edit(root, 'lib/producer/producer_record.ts', "Buffer.from(value, 'utf8') : value;", "Buffer.from(value, 'utf8') : Buffer.from(value);"),
  },
  {
    rule: 'check-no-binding-queue',
    what: 'a mutex in producer.c',
    apply: (root) => edit(root, 'src/producer.c', 'typedef struct SendCtx SendCtx;', 'static pthread_mutex_t producer_lock = PTHREAD_MUTEX_INITIALIZER;\n\ntypedef struct SendCtx SendCtx;'),
  },
  {
    rule: 'check-no-binding-queue',
    what: 'a send queue in a private helper of lib/',
    apply: (root) =>
      add(
        root,
        'lib/_send_queue.ts',
        `${fs.readFileSync(path.join(root, 'lib/_native.ts'), 'utf8').split(LICENSE_END)[0]}${LICENSE_END}\n` +
          '// Holds records back.\nexport class SendQueue {\n  private readonly records: unknown[] = [];\n}\n',
      ),
  },
  {
    rule: 'check-single-drainer',
    what: 'the producer destroyed in its close completion',
    apply: (root) => edit(root, 'src/producer.c', '    p->close_error = error;\n', '    p->close_error = error;\n    producer_destroy(env, p);\n'),
  },
  {
    rule: 'check-single-drainer',
    what: 'the producer destroyed in its executor',
    apply: (root) =>
      edit(
        root,
        'src/producer.c',
        '    return kafka_producer_Producer__execute_callbacks_err(((Producer*)client)->view, out_err);',
        '    Producer* p = (Producer*)client;\n    int32_t ran = kafka_producer_Producer__execute_callbacks_err(p->view, out_err);\n' +
          '    if (p->destroy_requested && !p->destroying) producer_destroy(p->env, p);\n    return ran;',
      ),
  },
  {
    rule: 'check-single-drainer',
    what: "the queue's after_drain moved inside the drain loop",
    apply: (root) => {
      edit(root, 'src/callback_queue.c', '    if (q->after_drain != NULL) q->after_drain(env, q->client);\n', '');
      edit(root, 'src/callback_queue.c', '        if (ran <= 0) break;\n', '        if (ran <= 0) break;\n        if (q->after_drain != NULL) q->after_drain(env, q->client);\n');
    },
  },
  {
    rule: 'check-tsdoc-tags',
    what: 'a @param dropped',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', '   * @param record - The record to send\n', ''),
  },
  {
    rule: 'check-tsdoc-tags',
    what: 'a JSDoc type on a @param',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', '@param record - The record to send', '@param {ProducerRecord} record - The record to send'),
  },
  {
    rule: 'check-tsdoc-tags',
    what: 'a @throws Java declares dropped',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', '   * @throws `IllegalStateError` when the producer is closed\n', ''),
  },
  {
    rule: 'check-tsdoc-tags',
    what: "a Callback parameter's @param dropped",
    apply: (root) => edit(root, 'lib/producer/callback.ts', /\n \* @param error - [^\n]*/.exec(fs.readFileSync(path.join(root, 'lib/producer/callback.ts'), 'utf8'))[0], ''),
  },
  {
    rule: 'check-tsdoc-present',
    what: 'a class doc emptied',
    apply: (root) => edit(root, 'lib/producer/record_metadata.ts', /\/\*\*[^]*?\*\/\nexport class RecordMetadata/.exec(fs.readFileSync(path.join(root, 'lib/producer/record_metadata.ts'), 'utf8'))[0], '/** */\nexport class RecordMetadata'),
  },
  {
    rule: 'check-tsdoc-present',
    what: "a public getter's TSDoc dropped",
    apply: (root) => edit(root, 'lib/producer/record_metadata.ts', '  /** The topic the record was appended to */\n', ''),
  },
  {
    rule: 'check-java-tags',
    what: 'an unneeded @jsOnly',
    apply: (root) =>
      edit(root, 'lib/producer/callback.ts', 'export type Callback =', '/** @jsOnly Not needed. */\nexport type Callback ='),
  },
  {
    // The directive does waive the getter's finding: only kafka/directives fails.
    rule: 'check-directives',
    what: 'a kafka/ finding waived by an inline directive',
    apply: (root) =>
      edit(
        root,
        'lib/producer/record_metadata.ts',
        '  /** The topic the record was appended to */\n',
        '  // eslint-disable-next-line kafka/tsdoc-present -- later\n',
      ),
  },
  {
    rule: 'check-config',
    what: 'a nested ESLint configuration',
    apply: (root) => add(root, 'lib/producer/eslint.config.cjs', "'use strict';\n\nmodule.exports = [];\n"),
  },
);

for (const m of MUTATIONS) {
  const rules = m.rules || [m.rule];
  test(`${m.what} fails exactly ${rules.join(' and ')}`, () => {
    const root = realCopy();
    m.apply(root);
    const r = lint(root);
    assert.equal(r.status, 1, r.text);
    assert.deepEqual(failedRules(r), rules, r.text);
    // No other rule catches it: with its rules turned off, the copy passes.
    const off = lint(root, [], { disable: rules });
    assert.equal(off.status, 0, off.text);
  });
}
