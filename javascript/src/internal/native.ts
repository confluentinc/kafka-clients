// Copyright 2025 Confluent Inc.
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

// Loads the Node-API addon (private) and does its one-time setup: the native
// layer builds its errors and metadata through the JS classes registered
// here, so every module that needs the addon requires this one, and no
// index.ts does any setup.
//
// The addon is `confluent-kafka-javascript.node`, from this machine's
// platform package or else the development build (`native_loader.ts`).

import { join } from 'node:path';

import { debug } from './debug';
import { fromNative } from './errors';
import { loadAddon, platformKey } from './native_loader';
import { RecordMetadata } from '../producer/record_metadata';

/** A metric's fields and its value read when metrics() ran (native/producer.c, metric_to_js). */
interface NativeMetric {
  name: string;
  group: string;
  description: string;
  // The tags sorted by key.
  tags: Array<[string, string]>;
  // A double or an int a number, a long a bigint, a string gauge a string.
  value: number | bigint | string;
  measurable: boolean;
}

/** A node's fields, as the addon reads them (native/producer.c, node_to_js). */
interface NativeNode {
  id: number;
  host: string | null;
  port: number;
  rack: string | null;
  isFenced: boolean;
}

/** A partition's fields, as the addon reads them (native/producer.c, partition_info_to_js). */
interface NativePartitionInfo {
  topic: string;
  partition: number;
  leader: NativeNode | null;
  replicas: NativeNode[] | null;
  inSyncReplicas: NativeNode[] | null;
  offlineReplicas: NativeNode[] | null;
}

/** The fields the native layer reads a delivered record's metadata into (native/producer.c). */
interface RecordMetadataFields {
  topic: string;
  partition: number;
  hasOffset: boolean;
  offset: bigint;
  hasTimestamp: boolean;
  timestamp: bigint;
  serializedKeySize: number;
  serializedValueSize: number;
}

/** A native producer handle (native/producer.c), opaque to JavaScript. */
type ProducerHandle = object;

/**
 * A native RecordHeaders handle (native/headers.c), opaque to JavaScript: the
 * object src/common/header/headers.ts holds, whose finalizer drops its
 * reference on the native headers.
 */
type HeadersHandle = object;

/**
 * A native RecordHeader handle (native/headers.c), opaque to JavaScript: an
 * owned header, or a view of one inside a read-only RecordHeaders, which it
 * keeps alive.
 */
type HeaderHandle = object;

/** The counts of native/headers.c, for the lifetime tests. */
interface HeaderStats {
  headersMade: number;
  headersFreed: number;
  headerMade: number;
  headerFreed: number;
  // The binding's copies of the headers a changeable Headers hands out (option 1).
  headerCopies: number;
  // The views a read-only Headers hands out, with no copy.
  headerViews: number;
  // Value Buffers over native bytes, and copied ones (a runtime refusing external buffers).
  valueExternal: number;
  valueCopied: number;
}

/** The functions the addon registers (native/addon.c). */
interface Native {
  probe(): boolean;
  setFactories(
    error: (fields: unknown) => unknown,
    metadata: (fields: RecordMetadataFields) => RecordMetadata,
    // An error, or a thrown value, the addon has nowhere else to report.
    warn: (error: unknown) => void,
  ): void;
  // True the first time any environment of the process asks.
  previewNoticeDue(): boolean;
  // The configuration's [key, value] pairs, encodeLikeJava's bytes of each.
  producerCreate(entries: Array<[Uint8Array, Uint8Array]>): ProducerHandle;
  producerSend(
    handle: ProducerHandle,
    topic: string,
    partition: number,
    // -1n for Java's null.
    timestamp: bigint,
    key: Uint8Array | null,
    // Borrowed, not copied: the addon holds a reference on each until the
    // hand-over (javascript/CLAUDE.md, rule 8).
    value: Uint8Array | null,
    // The record's RecordHeaders, which the core copies into the record, or
    // null for none.
    headers: HeadersHandle | null,
    // The slot of the record's native future in the producer's table, which
    // a wait started before the delivery takes, null when the hand-over
    // failed (native/producer.c, the record's future).
    onHandover: (error: unknown, future: number | null) => void,
    // Returns undefined (the callback returned), the text of what it threw,
    // or a Promise of either, settling once the callback's own Promise did:
    // the report waits for it (native/producer.c, send_defer).
    onComplete: (metadata: RecordMetadata, error: Error | null) => string | undefined | Promise<string | undefined>,
  ): void;
  producerFlush(handle: ProducerHandle, onComplete: (error: unknown) => void): void;
  // Java's metrics() through kafka_producer_Producer_metrics (no _cb twin):
  // every metric, read now.
  producerMetrics(handle: ProducerHandle): NativeMetric[];
  // Java's partitionsFor through partitions_for_cb; the partitions' fields
  // as src/common/partition_info.ts's constructor takes them, each node as
  // src/common/node.ts's does. A null topic goes to the core as NULL.
  producerPartitionsFor(
    handle: ProducerHandle,
    topic: string | null,
    onComplete: (error: unknown, partitions: NativePartitionInfo[] | null) => void,
  ): void;
  producerClose(
    handle: ProducerHandle,
    // null when the timeout was left out (Java's close()), else the
    // timeout as it was given, a negative one included.
    timeoutMs: number | bigint | null,
    // closedNow: this close closed the producer (a rejected timeout, or a
    // later close, did not).
    onComplete: (error: unknown, closedNow: boolean) => void,
  ): void;
  producerPump(handle: ProducerHandle): void;
  // A native producer handle over the FFI's MockProducer, Java's
  // MockProducer(autoComplete, null, null, null): every producer* function
  // takes it, through its __as_Producer view.
  mockProducerCreate(autoComplete: boolean): ProducerHandle;
  // A mock's send of the app's own record, which the mock keeps: its key and
  // value cross as the FFI's `void *`, never read (native/producer.c,
  // MockRecord). The completions are producerSend's.
  mockProducerSend(
    handle: ProducerHandle,
    topic: string,
    partition: number,
    // -1n for Java's null.
    timestamp: bigint,
    record: object,
    onHandover: (error: unknown, future: number | null) => void,
    onComplete: (metadata: RecordMetadata, error: Error | null) => string | undefined | Promise<string | undefined>,
  ): void;
  // The records the mock holds, the app's own, in send order.
  mockProducerHistory(handle: ProducerHandle): object[];
  mockProducerClear(handle: ProducerHandle): void;
  // Java's public error field of the mock, set to `error` or cleared by null.
  mockProducerSetError(handle: ProducerHandle, field: 'send' | 'flush' | 'partitionsFor' | 'close', error: Error | null): void;
  // Java's errorNext(error), completeNext() for null: whether a send waited.
  mockProducerCompleteNext(handle: ProducerHandle, error: Error | null): boolean;
  // Java's closed() or flushed().
  mockProducerState(handle: ProducerHandle, which: 'closed' | 'flushed'): boolean;
  // A wait on a record's future through get_cb (timeoutMs null) or
  // get_with_timeout_cb; `done` runs once it completed, from the producer's
  // drain, or inside the call once the producer is destroyed. timedOut: the
  // wait timed out, the error the core's TimeoutError.
  recordFutureGet(
    handle: ProducerHandle,
    future: number,
    timeoutMs: number | null,
    done: (metadata: RecordMetadata | null, error: Error | null, timedOut: boolean) => void,
  ): void;
  // Java's RecordHeaders() (form 0, headers null), RecordHeaders(Header[])
  // (form 1) and RecordHeaders(Iterable<Header>) (form 2); the core copies
  // the given headers, and a null one is the core's error.
  headersNew(headers: (HeaderHandle | null)[] | null, form: 0 | 1 | 2): HeadersHandle;
  // Java's new RecordHeaders(headers) of a RecordHeaders: a writable copy.
  headersCopy(headers: HeadersHandle): HeadersHandle;
  // Java's add(Header): the core stores a copy; null is the core's error.
  headersAdd(headers: HeadersHandle, header: HeaderHandle | null): void;
  // Java's add(String, byte[]): the key encodeLikeJava's bytes, or null (the core's error).
  headersAddKeyValue(headers: HeadersHandle, key: Uint8Array | null, value: Uint8Array | null): void;
  // Java's remove(String).
  headersRemove(headers: HeadersHandle, key: Uint8Array | null): void;
  // Java's lastHeader(String), headers(String), toArray() and iterator():
  // the binding's own copies while the headers can still change, views once
  // they are read-only (option 1).
  headersLastHeader(headers: HeadersHandle, key: Uint8Array | null): HeaderHandle | null;
  headersHeaders(headers: HeadersHandle, key: Uint8Array | null): HeaderHandle[];
  headersToArray(headers: HeadersHandle): HeaderHandle[];
  headersIterator(headers: HeadersHandle): HeaderHandle[];
  // Java's setReadOnly() and isReadOnly().
  headersSetReadOnly(headers: HeadersHandle): void;
  headersIsReadOnly(headers: HeadersHandle): boolean;
  // Java's new RecordHeader(String, byte[]): the key encodeLikeJava's bytes,
  // or null (the core's error); the core copies both.
  headerNew(key: Uint8Array | null, value: Uint8Array | null): HeaderHandle;
  // Java's key(), and value(): a Buffer over the header's native bytes,
  // which keeps the header alive, or null.
  headerKey(header: HeaderHandle): string;
  headerValue(header: HeaderHandle): Buffer | null;
  // The counts of native/headers.c. Test-only.
  headerStats(): HeaderStats;
}

// Loads an addon instance of this copy of the package's own: process.dlopen
// with a module object of its own, not require(), whose cache hands every copy
// that resolves the same .node file (two versions in node_modules sharing one
// platform package) the one instance. The factories registered below are an
// instance's, so this copy's producers build this copy's errors and
// RecordMetadata, as each copy of Java's kafka-clients, loaded by a class
// loader of its own, throws its own classes. The file is resolved as require()
// resolves it.
function loadOwnInstance(id: string): unknown {
  const addonModule = { exports: {} };
  process.dlopen(addonModule, require.resolve(id));
  return addonModule.exports;
}

// This module is dist/internal/native.js, and dist/ is beside native/ in the
// checkout, so the development build is <dist>/../native/build/Release/.
const loaded = loadAddon(loadOwnInstance, platformKey(process), join(__dirname, '..', '..'));
const native = loaded.addon as Native;
debug('addon loaded from %s', loaded.path);

// An error the addon has nowhere else to report (a failed report to the core,
// an error freeing a handle, a throw from the binding's own JS while a
// callback queue drains) goes to process.emitWarning (root CLAUDE.md,
// JavaScript Binding Conventions, Errors). It never throws.
function warn(error: unknown): void {
  try {
    process.emitWarning(error instanceof Error ? error : String(error));
  } catch {
    // Nothing is left to report it to.
  }
}

// The native layer builds RecordMetadata through its library-only factory.
native.setFactories(
  (fields) => fromNative(fields),
  (fields) => RecordMetadata.internalCreate(fields),
  warn,
);

export = native;
