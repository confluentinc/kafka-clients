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
// The addon is `confluent-kafka-javascript.node`. Until prebuilt per-platform
// packages exist, the only place it is looked for is the development build,
// build/Release/ (`make -C javascript build`), beside dist/, which holds this
// module compiled.

import { join } from 'node:path';

import { fromNative } from './_errors';
import { RecordMetadata } from './producer/record_metadata';

/** The fields the native layer reads a delivered record's metadata into (src/producer.c). */
interface RecordMetadataFields {
  topic: string;
  partition: number;
  hasOffset: boolean;
  offset: number;
  hasTimestamp: boolean;
  timestamp: number;
  serializedKeySize: number;
  serializedValueSize: number;
}

/** A native producer handle (src/producer.c), opaque to JavaScript. */
type ProducerHandle = object;

/** A header as the native send takes it: key and value bytes. */
type NativeHeader = [string, Uint8Array | null];

/** The functions the addon registers (src/addon.c). */
interface Native {
  probe(): boolean;
  setFactories(
    error: (fields: unknown) => unknown,
    metadata: (fields: RecordMetadataFields) => RecordMetadata,
  ): void;
  producerCreate(entries: Array<[string, string]>): ProducerHandle;
  producerSend(
    handle: ProducerHandle,
    topic: string,
    partition: number,
    timestamp: number,
    key: Uint8Array | null,
    value: Uint8Array | null,
    headers: NativeHeader[] | null,
    onHandover: (error: unknown) => void,
    onComplete: (metadata: RecordMetadata, error: Error | null) => string | undefined,
  ): void;
  producerFlush(handle: ProducerHandle, onComplete: (error: unknown) => void): void;
  producerClose(
    handle: ProducerHandle,
    timeoutMs: number | null,
    onComplete: (error: unknown, destroyed: boolean) => void,
  ): void;
  producerPump(handle: ProducerHandle): void;
}

const ADDON = join(__dirname, '..', 'build', 'Release', 'confluent-kafka-javascript.node');

let native: Native;
try {
  native = require(ADDON) as Native;
} catch (e) {
  throw new Error(
    `cannot load the native addon ${ADDON}: build it with \`make -C javascript build\` ` +
      `after building libconfluent_kafka (\`make -C rust build-all-features\`)`,
    { cause: e },
  );
}

// RecordMetadata's constructor is private: users never build one. The native
// layer does, through this factory; the cast only lifts the compile-time
// `private`, the class is called as it is.
const RecordMetadataOf = RecordMetadata as unknown as new (fields: RecordMetadataFields) => RecordMetadata;

native.setFactories(
  (fields) => fromNative(fields),
  (fields) => new RecordMetadataOf(fields),
);

export = native;
