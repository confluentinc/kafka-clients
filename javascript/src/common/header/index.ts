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

/**
 * The package of Java's `org.apache.kafka.common.header`: the `Header`
 * interface, `Headers`, which joins Java's interface with its internal class
 * `RecordHeaders`, and `RecordHeader`, the class an app builds a header with
 * *(deviation, as in Python: Java's `RecordHeader` is in
 * `common.header.internals`, and "one import path per Java package" would
 * make the class apps build headers with public under an "internals" path.
 * Why: it belongs beside `Header` and `Headers`, as Python's header class
 * sits in `confluent_kafka.common.header`)*.
 *
 * Not translated: `RecordHeader`'s `(ByteBuffer, ByteBuffer)` constructor, as
 * JavaScript has no `ByteBuffer` (a `Buffer` is a `Uint8Array`, the `byte[]`
 * of the other form).
 *
 * @packageDocumentation
 */

export type { Header } from './header';
export { Headers } from './headers';
export { RecordHeader } from './record_header';

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
