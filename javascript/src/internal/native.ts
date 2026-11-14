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

/** A header as the native send takes it: key and value bytes. */
type NativeHeader = [Uint8Array, Uint8Array | null];

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
    headers: NativeHeader[] | null,
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
}

// This module is dist/internal/native.js, and dist/ is beside native/ in the
// checkout, so the development build is <dist>/../native/build/Release/.
const loaded = loadAddon(require, platformKey(process), join(__dirname, '..', '..'));
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

// RecordMetadata's constructor is private: users never build one. The native
// layer does, through its library-only factory.
native.setFactories(
  (fields) => fromNative(fields),
  (fields) => RecordMetadata.internalCreate(fields),
  warn,
);

export = native;
