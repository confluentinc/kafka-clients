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
// (design/current/javascript-rule-linter/PLAN.md §6): a copy of the real src/
// and native/ passes every rule; one known violation per rule, injected into the
// copy, makes exactly that rule fail. It catches a rule whose path through
// the real sources is broken, which a fixture of the rule alone would miss.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { TODO_MARK, project, copy, lint, failedRules, baselineCurrent } = require('./_project');

const ROOT = path.join(__dirname, '..', '..');

/** The last line of every source's license header. */
const LICENSE_END = 'limitations under the License.\n';

/**
 * A copy of the client's src/, native/, tsconfig.json, CLAUDE.md (whose rule
 * sentence check-no-todo exempts), both baselines, the mapping lists
 * (lint-mapping.json), the committed Java index (java-index.json) and
 * bindings header, with the findings the rules
 * report on the real tree now added to the copy's baselines
 * (`baselineCurrent`): the real tree may not follow every rule yet, and a
 * mutation is judged by what it adds.
 */
function realCopy() {
  const root = project({});
  for (const rel of ['src', 'native', 'tsconfig.json', 'CLAUDE.md', 'lint-baseline.txt', 'eslint-baseline.txt', 'lint-mapping.json']) copy(ROOT, root, rel);
  // The test that calls the test-only registration probe (check-native-registry).
  fs.mkdirSync(path.join(root, 'test', 'unit'), { recursive: true });
  copy(ROOT, root, 'test/unit/internal/native.test.js');
  copy(ROOT, root, 'java-index.json');
  // The bindings header, where the lint helper looks for it.
  fs.copyFileSync(require('../../scripts/header').headerPath(), path.join(root, 'confluent_kafka_bindings.h'));
  baselineCurrent(root);
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
    apply: (root) => edit(root, 'src/producer/kafka_producer.ts', LICENSE_END, `${LICENSE_END}// ${TODO_MARK} later\n`),
  },
  {
    rule: 'check-no-todo',
    what: 'a marker in the addon',
    apply: (root) => edit(root, 'native/producer.c', '#include', `/* ${TODO_MARK} */\n#include`),
  },
  {
    rule: 'check-exception-word',
    what: 'a C identifier with the word',
    apply: (root) => edit(root, 'native/error.c', '#include', 'static int last_exception_code;\n#include'),
  },
  {
    rule: 'check-exception-word',
    what: 'a TypeScript identifier with the word',
    apply: (root) => edit(root, 'src/common/kafka_future.ts', LICENSE_END, `${LICENSE_END}const lastException = 0;\nvoid lastException;\n`),
  },
  {
    rule: 'check-internal-members',
    what: 'a library-only member reached with bracket access',
    apply: (root) => edit(root, 'src/producer/kafka_producer.ts', 'record.internalToNative(key, value)', "record['internalToNative'](key, value)"),
  },
  {
    rule: 'check-internal-members',
    what: 'an `@internal` member made private',
    apply: (root) => edit(root, 'src/common/kafka_future.ts', '  internalComplete(value: T', '  private internalComplete(value: T'),
  },
  {
    rule: 'check-java-line',
    what: 'a comment citing a Java line',
    apply: (root) =>
      edit(root, 'src/producer/record_metadata.ts', LICENSE_END, [LICENSE_END, "// As Java's Record", 'Metadata.java', ':49.\n'].join('')),
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
        'src/producer/record_metadata.ts',
        '  /** The offset of the record in the topic/partition, or -1 if hasOffset is false. */\n  readonly offset: number;',
        '  /**\n   * The offset of the record.\n   *\n   * @returns the offset\n   */\n  getOffset(): number {\n    return 0;\n  }',
      ),
  },
  {
    rule: 'check-java-completeness',
    what: 'SslClientAuth.valueOf removed',
    apply: (root) => {
      const file = path.join(root, 'src/common/config/ssl_client_auth.ts');
      const text = fs.readFileSync(file, 'utf8');
      const method = /  \/\*\*\n   \* The constant named exactly `name`, Java's `valueOf\(String\)`[^]*?\n  static valueOf\(options: unknown\): SslClientAuth \{\n[^]*?\n  \}\n/.exec(text);
      assert.ok(method, 'ssl_client_auth.ts has no valueOf()');
      fs.writeFileSync(file, text.replace(method[0], ''));
    },
  },
  {
    rule: 'check-java-completeness',
    what: 'SecurityProtocol.name removed',
    apply: (root) => {
      const file = path.join(root, 'src/common/security/auth/security_protocol.ts');
      const text = fs.readFileSync(file, 'utf8');
      const method = /  \/\*\*\n   \* The name of this constant[^]*?\n  name\(\): string \{\n[^]*?\n  \}\n/.exec(text);
      assert.ok(method, 'security_protocol.ts has no name()');
      fs.writeFileSync(file, text.replace(method[0], ''));
    },
  },
  {
    // Its entry on the list of invented names then matches nothing.
    rules: ['check-java-completeness', 'check-invented-names'],
    what: 'SecurityProtocol.toJSON removed',
    apply: (root) => edit(root, 'src/common/security/auth/security_protocol.ts', '  toJSON(key?: string): string {\n    void key;\n    return this.#name;\n  }\n', ''),
  },
  {
    rule: 'check-data-objects',
    what: 'a public mutable field',
    apply: (root) => edit(root, 'src/common/security/auth/security_protocol.ts', 'readonly id: number;', 'id: number;'),
  },
  {
    rule: 'check-data-objects',
    what: "a data class's field made mutable",
    apply: (root) => edit(root, 'src/producer/producer_record.ts', '  readonly topic: string;\n', '  topic: string;\n'),
  },
  {
    rule: 'check-data-objects',
    what: "a data class's field turned into a get accessor",
    apply: (root) =>
      edit(
        root,
        'src/producer/record_metadata.ts',
        '  /** The partition the record was sent to */\n  readonly partition: number;',
        '  /** The partition the record was sent to */\n  get partition(): number {\n    return 0;\n  }',
      ),
  },
  {
    rule: 'check-data-objects',
    what: "an enum's name() turned into Java's public final field name",
    apply: (root) => {
      const file = path.join(root, 'src/common/security/auth/security_protocol.ts');
      const text = fs.readFileSync(file, 'utf8');
      const method = /  \/\*\*\n   \* The name of this constant[^]*?\n  name\(\): string \{\n[^]*?\n  \}\n/.exec(text);
      assert.ok(method, 'security_protocol.ts has no name()');
      fs.writeFileSync(file, text.replace(method[0], '').replace('  #name: string;\n', "  /** Name of the security protocol. */\n  readonly name: string = '';\n  #name: string;\n"));
    },
  },
  {
    rule: 'check-public-audience',
    what: 'an export translating an internal, non-Public Java class',
    apply: (root) => add(root, 'src/producer/internals/txn_partition_entry.ts', '/** An entry. */\nexport class TxnPartitionEntry {}\n'),
  },
  {
    rule: 'check-no-deprecated-translation',
    what: 'a translated deprecated constant',
    apply: (root) =>
      edit(
        root,
        'src/producer/producer_config.ts',
        'export class ProducerConfig {',
        "export class ProducerConfig {\n  /** Deprecated. */\n  static readonly PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG: string = 'x';",
      ),
  },
  {
    rule: 'check-java-completeness',
    what: 'a translated method dropped',
    apply: (root) => edit(root, 'src/producer/kafka_producer.ts', '  flush(): Promise<void>;\n  flush(options?: unknown): Promise<void> {', '  private flushAll(options?: unknown): Promise<void> {'),
  },
  {
    rule: 'check-java-name',
    what: 'an export naming no Java class',
    apply: (root) => add(root, 'src/producer/record_meta.ts', '/** Metadata. */\nexport class RecordMeta {}\n'),
  },
  {
    // J2 reports the static-ness, and J3 no longer counts the static forId as translated.
    rules: ['check-java-members', 'check-java-completeness'],
    what: 'a static method made an instance method',
    // Every signature of it: the two overloads and the implementation.
    apply: (root) => {
      for (let i = 0; i < 3; i++) edit(root, 'src/common/security/auth/security_protocol.ts', 'static forId(', 'forId(');
    },
  },
  {
    // `is` stays: a `done` property is no translation of isDone(); and
    // KafkaFuture is no data class, so a get accessor is a finding too.
    rules: ['check-java-members', 'check-java-completeness', 'check-data-objects'],
    what: 'an isX method turned into a property without its is',
    apply: (root) => {
      edit(
        root,
        'src/common/kafka_future.ts',
        '  isDone(): boolean {\n    if (arguments.length !== 0) IS_DONE_FORMS.match(undefined, arguments.length);\n',
        '  get done(): boolean {\n    void IS_DONE_FORMS;\n',
      );
    },
  },
  {
    rule: 'check-waiting-call-promise',
    what: 'flush returning void',
    apply: (root) => edit(root, 'src/producer/kafka_producer.ts', '  flush(): Promise<void>;\n  flush(options?: unknown): Promise<void> {', '  flush(): void;\n  flush(options?: unknown): void {'),
  },
  {
    rule: 'check-overload-shape',
    what: "a key renamed in one ProducerRecord constructor's signature",
    apply: (root) => edit(root, 'src/producer/producer_record.ts', '    key: K | null;\n    /** The record contents, or null (a tombstone). */\n    value: V | null;\n    headers?: never;\n  });\n  /**\n   * Create a record with no key', '    recordKey: K | null;\n    /** The record contents, or null (a tombstone). */\n    value: V | null;\n    headers?: never;\n  });\n  /**\n   * Create a record with no key'),
  },
  {
    rule: 'check-overload-shape',
    what: "the topic made optional in a ProducerRecord constructor's signature",
    apply: (root) => edit(root, 'src/producer/producer_record.ts', '    topic: string;\n', '    topic?: string;\n'),
  },
  {
    rule: 'check-overload-shape',
    what: 'close({timeoutMs}) instead of Java\'s timeout',
    apply: (root) => edit(root, 'src/producer/kafka_producer.ts', '  timeout: Duration;\n};', '  timeoutMs: Duration;\n};'),
  },
  {
    // A signature that leaves another form's name open: a value holding it
    // would match that signature.
    rule: 'check-overload-shape',
    what: "a never closure dropped from a ProducerRecord constructor's signature",
    apply: (root) =>
      edit(
        root,
        'src/producer/producer_record.ts',
        '    key?: never;\n    /** The record contents, or null (a tombstone). */\n    value: V | null;\n    headers?: never;\n  });',
        '    key?: never;\n    /** The record contents, or null (a tombstone). */\n    value: V | null;\n  });',
      ),
  },
  {
    // J7 reads every signature, not only the first.
    rule: 'check-type-mapping',
    what: "a wrong option type in a later ProducerRecord constructor's signature",
    apply: (root) =>
      edit(
        root,
        'src/producer/producer_record.ts',
        '    partition: number | null;\n    timestamp?: never;\n    /** The key that will be included in the record, or null. */\n    key: K | null;\n    /** The record contents, or null (a tombstone). */\n    value: V | null;\n    headers?: never;',
        '    partition: string | null;\n    timestamp?: never;\n    /** The key that will be included in the record, or null. */\n    key: K | null;\n    /** The record contents, or null (a tombstone). */\n    value: V | null;\n    headers?: never;',
      ),
  },
  {
    rule: 'check-type-mapping',
    what: "a wrong option type in forId's options signature",
    apply: (root) =>
      edit(root, 'src/common/security/auth/security_protocol.ts', "    /** The protocol's id. */\n    id: number;", "    /** The protocol's id. */\n    id: string;"),
  },
  {
    // get() and get(long, TimeUnit), the TimeUnit merged into one Duration,
    // take one options object: a positional timeout is a wrong shape, and,
    // translating Java's `long timeout`, one not named `timeout`.
    rules: ['check-overload-shape', 'check-type-mapping'],
    what: 'KafkaFuture.get taking a positional timeout',
    apply: (root) => {
      edit(root, 'src/common/kafka_future.ts', '   * @param options - the timeout\n   * @returns resolving to the result value', '   * @param timeoutMs - the timeout\n   * @returns resolving to the result value');
      edit(root, 'src/common/kafka_future.ts', '  get(options: {\n    /**', '  get(timeoutMs: number): Promise<T>;\n  get(unused: {\n    /**');
    },
  },
  {
    // The registry misses a use, flush's native path no longer resolves, and
    // so its Promise is no waiting call's.
    rules: ['check-type-mapping', 'check-waiting-call-promise', 'check-native-registry'],
    what: 'a registration removed from addon.c',
    apply: (root) =>
      edit(root, 'native/addon.c', '        {"producerFlush", NULL, js_producer_flush, NULL, NULL, NULL, napi_default, NULL},\n', ''),
  },
  {
    rule: 'check-native-registry',
    what: 'a second import of the loader',
    apply: (root) =>
      edit(root, 'src/producer/kafka_producer.ts', "import native = require('../internal/native');", "import native = require('../internal/native');\nimport { ADDON_FILE } from '../internal/native_loader';\nvoid ADDON_FILE;"),
  },
  {
    rule: 'check-type-mapping',
    what: 'a field of another type than Java\'s',
    apply: (root) => edit(root, 'src/producer/record_metadata.ts', 'readonly offset: number;', 'readonly offset: string;'),
  },
  {
    rule: 'check-waiting-call-promise',
    what: 'flush returning void, its native call in a module helper',
    apply: (root) => {
      edit(root, 'src/producer/kafka_producer.ts', '  flush(): Promise<void>;\n  flush(options?: unknown): Promise<void> {', '  flush(): void {\n    flushVia(this.#handle);\n  }\n\n  private oldFlush(options?: unknown): Promise<void> {');
      fs.appendFileSync(
        path.join(root, 'src/producer/kafka_producer.ts'),
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
        'src/producer/kafka_producer.ts',
        '  flush(): Promise<void>;\n  flush(options?: unknown): Promise<void> {',
        '  flush(): void {\n    const n = native;\n    n.producerFlush(this.#handle, () => undefined);\n  }\n\n  private oldFlush(options?: unknown): Promise<void> {',
      ),
  },
  {
    // The index keeps whenComplete's parameter as written,
    // BiConsumer<? super T, ? super Throwable>: an Error is not Throwable's
    // `unknown`.
    rule: 'check-type-mapping',
    what: "whenComplete's error parameter typed Error",
    apply: (root) =>
      edit(
        root,
        'src/common/kafka_future.ts',
        '  whenComplete(action: KafkaFuture.BiConsumer<T | null, unknown>): KafkaFuture<T>;',
        '  whenComplete(action: KafkaFuture.BiConsumer<T | null, Error | null>): KafkaFuture<T>;',
      ),
  },
  {
    // Only a rest parameter typed BareCheck<...> is skipped as the bare-value
    // check: one named `check` of another type is an extra positional
    // parameter, which no Java form has.
    rule: 'check-overload-shape',
    what: "get(timeout) with an extra rest parameter named check",
    apply: (root) =>
      edit(
        root,
        'src/common/kafka_future.ts',
        '   * @param timeout - The maximum time to wait\n   * @returns resolving to the result value, rejecting with an\n   *   `ExecutionError` whose `cause` is the failure\n   */\n  get(timeout: Duration): Promise<T>;',
        '   * @param timeout - The maximum time to wait\n   * @param check - Extra values\n   * @returns resolving to the result value, rejecting with an\n   *   `ExecutionError` whose `cause` is the failure\n   */\n  get(timeout: Duration, ...check: string[]): Promise<T>;',
      ),
  },
  {
    // A narrowing of a Java type variable is a mismatch, at the bare parameter too.
    rule: 'check-type-mapping',
    what: "completedFuture's bare value narrowed to U & string",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', 'static completedFuture<U>(value: U, ...check: BareCheck<U>): KafkaFuture<U>;', 'static completedFuture<U>(value: U & string, ...check: BareCheck<U>): KafkaFuture<U>;'),
  },
  {
    // getNow's bare Bare<T | null> is read as Java's T, and only that.
    rule: 'check-type-mapping',
    what: "getNow's bare value typed for another value",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', 'getNow(valueIfAbsent: Bare<T | null>): T | null;', 'getNow(valueIfAbsent: Bare<string | null>): T | null;'),
  },
  {
    // Bare<T> is read as T only at a bare parameter: elsewhere a narrowing of T is a mismatch.
    rule: 'check-type-mapping',
    what: "thenApply's function narrowed: BaseFunction<T & string, R>",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', 'thenApply<R>(fn: KafkaFuture.BaseFunction<T, R>): KafkaFuture<R>;', 'thenApply<R>(fn: KafkaFuture.BaseFunction<T & string, R>): KafkaFuture<R>;'),
  },
  {
    // An Extract of T is no T either.
    rule: 'check-type-mapping',
    what: "thenApply's function narrowed: BaseFunction<Extract<T, string>, R>",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', 'thenApply<R>(fn: KafkaFuture.BaseFunction<T, R>): KafkaFuture<R>;', 'thenApply<R>(fn: KafkaFuture.BaseFunction<Extract<T, string>, R>): KafkaFuture<R>;'),
  },
  {
    // A return narrowed.
    rule: 'check-type-mapping',
    what: "thenApply's return narrowed: KafkaFuture<R & string>",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', 'thenApply<R>(fn: KafkaFuture.BaseFunction<T, R>): KafkaFuture<R>;', 'thenApply<R>(fn: KafkaFuture.BaseFunction<T, R>): KafkaFuture<R & string>;'),
  },
  {
    // A type argument narrowed.
    rule: 'check-type-mapping',
    what: "whenComplete's value narrowed: (T & number) | null",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', 'whenComplete(action: KafkaFuture.BiConsumer<T | null, unknown>): KafkaFuture<T>;', 'whenComplete(action: KafkaFuture.BiConsumer<(T & number) | null, unknown>): KafkaFuture<T>;'),
  },
  {
    // An option narrowed.
    rule: 'check-type-mapping',
    what: "getNow's option narrowed: (T & string) | null",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', '    valueIfAbsent: T | null;\n  }): T | null;', '    valueIfAbsent: (T & string) | null;\n  }): T | null;'),
  },
  {
    // A return narrowed.
    rule: 'check-type-mapping',
    what: "getNow's return narrowed: (T & string) | null",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', '    valueIfAbsent: T | null;\n  }): T | null;', '    valueIfAbsent: T | null;\n  }): (T & string) | null;'),
  },
  {
    // An option of a static member narrowed.
    rule: 'check-type-mapping',
    what: "completedFuture's option narrowed: U & string",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', '    /** The value. */\n    value: U;', '    /** The value. */\n    value: U & string;'),
  },
  {
    // And thenApply's function, BaseFunction<T, R>, takes the future's T.
    rule: 'check-type-mapping',
    what: "thenApply's function typed for another value",
    apply: (root) => edit(root, 'src/common/kafka_future.ts', 'thenApply<R>(fn: KafkaFuture.BaseFunction<T, R>): KafkaFuture<R>;', 'thenApply<R>(fn: KafkaFuture.BaseFunction<number, R>): KafkaFuture<R>;'),
  },
  {
    rule: 'check-type-mapping',
    what: 'a non-blocking method returning a Promise',
    apply: (root) => edit(root, 'src/common/kafka_future.ts', '  isDone(): boolean {\n', '  isDone(): Promise<boolean> {\n'),
  },
  {
    rule: 'check-cb-twin-only',
    what: 'a blocking flush instead of its _cb twin',
    apply: (root) =>
      edit(
        root,
        'native/producer.c',
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
        'native/producer.c',
        '    if (value != NULL) error_warn(p->env, !p->queue.finalizing, FFI_ERR(kafka_common_KafkaFuture_destroy_err(value, &ffi_err)));',
        '    if (value != NULL) kafka_common_KafkaFuture_get_cb(value, send_on_future, ctx);\n    if (value != NULL) error_warn(p->env, !p->queue.finalizing, FFI_ERR(kafka_common_KafkaFuture_destroy_err(value, &ffi_err)));',
      ),
  },
  {
    rule: 'check-cb-twin-only',
    what: 'a blocking flush in a macro of util.h',
    apply: (root) => {
      edit(root, 'native/util.h', '#define FFI_DISCARD(call) error_discard(FFI_ERR(call))\n', '#define FFI_DISCARD(call) error_discard(FFI_ERR(call))\n#define FLUSH_VIEW(view) FFI_DISCARD(kafka_producer_Producer_flush((view), &ffi_err))\n');
      edit(
        root,
        'native/producer.c',
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
        'native/producer.c',
        '        err = FFI_ERR(kafka_Map_put_err(props, strings[2 * i], strings[2 * i + 1], &ffi_err));',
        '        kafka_Map_put_err(props, strings[2 * i], strings[2 * i + 1], &ffi_err);',
      ),
  },
  {
    rule: 'check-out-err',
    what: "the drain's read of the executor's error deleted",
    apply: (root) => edit(root, 'native/callback_queue.c', '        error_warn(env, !q->finalizing, ffi_take(&ffi_err));\n', ''),
  },
  {
    rule: 'check-out-err',
    what: 'the executor called with a NULL slot',
    apply: (root) => edit(root, 'native/callback_queue.c', 'q->execute(q->client, &ffi_err)', 'q->execute(q->client, NULL)'),
  },
  {
    rule: 'check-out-err',
    what: 'FFI_DISCARD around a call given another slot',
    apply: (root) =>
      edit(
        root,
        'native/producer.c',
        '    if (props != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(props, &ffi_err)));',
        '    kafka_common_Error_t* map_err = NULL;\n    if (props != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(props, &map_err)));',
      ),
  },
  {
    rule: 'check-notify-hook',
    what: 'a call added in cbq_notify',
    apply: (root) =>
      edit(root, 'native/callback_queue.c', '    pthread_mutex_unlock(&q->notify_mtx);\n}\n\nvoid cbq_schedule', '    pthread_mutex_unlock(&q->notify_mtx);\n    cbq_schedule(q);\n}\n\nvoid cbq_schedule'),
  },
  {
    rule: 'check-notify-hook',
    what: 'a return between the lock and the unlock of cbq_notify',
    apply: (root) =>
      edit(root, 'native/callback_queue.c', '    pthread_mutex_lock(&q->notify_mtx);\n    if (q->notify_open)', '    pthread_mutex_lock(&q->notify_mtx);\n    if (q->stopped) return;\n    if (q->notify_open)'),
  },
  {
    rule: 'check-single-drainer',
    what: '__execute_callbacks_err called outside cbq_drain',
    apply: (root) =>
      edit(
        root,
        'native/producer.c',
        '    if (p->kp != NULL) cbq_drain(env, &p->queue);',
        '    if (p->kp != NULL) cbq_drain(env, &p->queue);\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_producer_Producer__execute_callbacks_err(p->view, &ffi_err));',
      ),
  },
  {
    rule: 'check-completion-order',
    what: 'the future completed before the user callback',
    apply: (root) => {
      edit(root, 'src/producer/kafka_producer.ts', '      let failure: string | undefined;\n', '      let failure: string | undefined;\n      future.internalComplete(metadata, error);\n');
    },
  },
  {
    rule: 'check-completion-order',
    what: "the delivery callback's body moved into a module function completing the future first",
    apply: (root) => {
      const file = path.join(root, 'src/producer/kafka_producer.ts');
      const text = fs.readFileSync(file, 'utf8');
      const body = /    const onComplete = \(metadata: RecordMetadata, error: Error \| null\): string \| undefined \| Promise<string \| undefined> => \{\n[^]*?\n    \};\n/.exec(text);
      assert.ok(body, 'kafka_producer.ts has no onComplete');
      fs.writeFileSync(
        file,
        text.replace(body[0], '    const onComplete = (metadata: RecordMetadata, error: Error | null): string | undefined =>\n      deliver(future, userCallback, metadata, error);\n') +
          '\nfunction deliver(future: KafkaFuture<RecordMetadata>, userCallback: Callback | null, metadata: RecordMetadata, error: Error | null): string | undefined {\n' +
          '  future.internalComplete(metadata, error);\n  if (userCallback !== null) userCallback({ metadata, exception: error });\n  return undefined;\n}\n',
      );
    },
  },
  {
    rule: 'check-completion-order',
    what: 'the destroying guard guarding another statement than the report',
    apply: (root) =>
      edit(
        root,
        'native/producer.c',
        '    if (!p->destroying) {\n        error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_Producer__set_callback_result_err(p->view, callback_id, NULL, report, &ffi_err)));\n    } else {\n        // Destroy completes the unreported callbacks itself.\n        error_discard(report);\n    }\n',
        '    if (!p->destroying) ctx->delivered = 1;\n    error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_Producer__set_callback_result_err(p->view, callback_id, NULL, report, &ffi_err)));\n',
      ),
  },
  {
    rule: 'check-ownership',
    what: 'a configuration never destroyed',
    apply: (root) =>
      edit(root, 'native/producer.c', '    if (config != NULL) error_warn(env, 1, FFI_ERR(kafka_producer_ProducerConfig_destroy_err(config, &ffi_err)));\n', ''),
  },
  {
    rule: 'check-ownership',
    what: "the delivery callback's borrowed error destroyed through a cast",
    apply: (root) =>
      edit(
        root,
        'native/producer.c',
        '    ctx->delivered = 1;\n    send_ctx_release(env, ctx);\n',
        '    error_discard((kafka_common_Error_t*)error);\n    ctx->delivered = 1;\n    send_ctx_release(env, ctx);\n',
      ),
  },
  {
    rule: 'check-one-copy',
    what: 'a memcpy of the key',
    apply: (root) =>
      edit(root, 'native/record.c', '    out->bytes.len = (int32_t)length;', '    out->bytes.len = (int32_t)length;\n    memcpy((void*)out->bytes.data, data, length);'),
  },
  {
    rule: 'check-one-copy',
    what: 'a Uint8Array key copied by Buffer.from in src/',
    apply: (root) => edit(root, 'src/producer/producer_record.ts', '  if (isBytes(value)) return value;', '  if (isBytes(value)) return Buffer.from(value);'),
  },
  {
    rule: 'check-one-copy',
    what: "the addon's copy of the key and value gone",
    apply: (root) => edit(root, 'native/record.c', '        memcpy(bytes, key->bytes.data, key_len);\n', ''),
  },
  {
    rule: 'check-string-encoding',
    what: 'a config value encoded by Buffer.from outside encode.ts',
    apply: (root) => edit(root, 'src/internal/config.ts', "    case 'string':\n      return value;", "    case 'string':\n      return Buffer.from(value, 'utf8').toString();"),
  },
  {
    rule: 'check-string-encoding',
    what: 'a header key encoded by TextEncoder',
    apply: (root) =>
      edit(
        root,
        'src/common/header/headers.ts',
        'out[i] = [encodeLikeJava(Header.internalKeyOf(header)), Header.internalValueOf(header)];',
        'out[i] = [new TextEncoder().encode(Header.internalKeyOf(header)), Header.internalValueOf(header)];',
      ),
  },
  {
    rule: 'check-no-binding-queue',
    what: 'a mutex in producer.c',
    apply: (root) => edit(root, 'native/producer.c', 'typedef struct SendCtx SendCtx;', 'static pthread_mutex_t producer_lock = PTHREAD_MUTEX_INITIALIZER;\n\ntypedef struct SendCtx SendCtx;'),
  },
  {
    rule: 'check-no-binding-queue',
    what: 'a send queue in a private helper of src/',
    apply: (root) =>
      add(
        root,
        'src/internal/send_queue.ts',
        `${fs.readFileSync(path.join(root, 'src/internal/native.ts'), 'utf8').split(LICENSE_END)[0]}${LICENSE_END}\n` +
          '// Holds records back.\nexport class SendQueue {\n  private readonly records: unknown[] = [];\n}\n',
      ),
  },
  {
    rule: 'check-single-drainer',
    what: 'the producer destroyed in its close completion',
    apply: (root) => edit(root, 'native/producer.c', '    p->close_error = error;\n', '    p->close_error = error;\n    producer_destroy(env, p);\n'),
  },
  {
    rule: 'check-single-drainer',
    what: 'the producer destroyed in its executor',
    apply: (root) =>
      edit(
        root,
        'native/producer.c',
        '    return kafka_producer_Producer__execute_callbacks_err(((Producer*)client)->view, out_err);',
        '    Producer* p = (Producer*)client;\n    int32_t ran = kafka_producer_Producer__execute_callbacks_err(p->view, out_err);\n' +
          '    if (p->destroy_requested && !p->destroying) producer_destroy(p->env, p);\n    return ran;',
      ),
  },
  {
    rule: 'check-single-drainer',
    what: "the queue's after_drain moved inside the drain loop",
    apply: (root) => {
      edit(root, 'native/callback_queue.c', '    if (q->after_drain != NULL) q->after_drain(env, q->client);\n', '');
      edit(root, 'native/callback_queue.c', '        if (ran <= 0) break;\n', '        if (ran <= 0) break;\n        if (q->after_drain != NULL) q->after_drain(env, q->client);\n');
    },
  },
  {
    // The directive does waive the field's finding: only kafka/directives fails.
    rule: 'check-directives',
    what: 'a kafka/ finding waived by an inline directive',
    apply: (root) =>
      edit(
        root,
        'src/common/security/auth/security_protocol.ts',
        'readonly id: number;',
        '// eslint-disable-next-line kafka/data-objects -- later\n  id: number;',
      ),
  },
  {
    rule: 'check-invented-names',
    what: 'an entry of the list of invented names that matches nothing',
    apply: (root) => edit(root, 'lint-mapping.json', '"invented": {\n', '"invented": {\n    "KafkaProducer.gone": "Removed.",\n'),
  },
  {
    rule: 'check-module-exports',
    what: "an index re-exporting another package's module",
    apply: (root) => edit(root, 'src/producer/index.ts', "export { KafkaProducer } from './kafka_producer';\n", "export { KafkaProducer } from './kafka_producer';\nexport { KafkaFuture } from '../common/kafka_future';\n"),
  },
  {
    rule: 'check-internal-members',
    what: 'an FFI id on a class that is no error',
    apply: (root) => edit(root, 'src/common/header/headers.ts', 'export class Headers implements Iterable<Header> {\n', 'export class Headers implements Iterable<Header> {\n  private readonly _ffiId = 0;\n'),
  },
  {
    rule: 'check-string-encoding',
    what: 'a second encoder in encode.ts, beside encodeLikeJava',
    apply: (root) => edit(root, 'src/internal/encode.ts', LICENSE_END, `${LICENSE_END}export const encoder = new TextEncoder();\n`),
  },
  {
    rule: 'check-config',
    what: 'a nested ESLint configuration',
    apply: (root) => add(root, 'src/producer/eslint.config.cjs', "'use strict';\n\nmodule.exports = [];\n"),
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
