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

// Java's Serializer interface, as an object type the user implements (root
// CLAUDE.md, JavaScript Binding Conventions, Serialization): one serialize
// method taking one object of named parameters, for Java's two, and Java's
// default close() as an optional member. The client runs it in JavaScript,
// inside send(). This module holds only its type.

import type { Headers } from '../header/headers';

/**
 * An interface for converting objects to bytes.
 *
 * @remarks *(deviation)* Java's `serialize(topic, data)` and
 * `serialize(topic, headers, data)` are one method taking one object,
 * `serialize({ topic, headers, data })`. Why: JavaScript cannot have two
 * methods of one name, and with positions a serializer written like Java's
 * two-argument form would get the headers as its data. *(js-only)*
 * `serialize` may return a Promise, which the producer awaits before it
 * hands the record over. There is no `configure()`: a serializer is given
 * only as a constructor argument, for which Java does not call it either.
 *
 * @typeParam T - Type to be serialized from.
 */
export interface Serializer<in T> {
  /**
   * Convert `data` into a byte array.
   *
   * It is called for a null key or value too, and decides what Java's null
   * maps to.
   *
   * @param options - the topic, the record's headers and the data
   * @returns serialized bytes, or a Promise of them, which the producer
   *   awaits
   */
  serialize(options: {
    /** topic associated with data */
    topic: string;
    /** headers associated with the record */
    headers: Headers;
    /** typed data; may be null */
    data: T | null;
  }): Uint8Array | null | Promise<Uint8Array | null>;

  /**
   * Close this serializer.
   *
   * This method must be idempotent as it may be called multiple times.
   *
   * @remarks It is optional: a serializer without it is closed by doing
   * nothing, Java's default. The producer calls it when it closes; what it
   * throws is reported through `process.emitWarning`, never thrown.
   */
  close?(): void;
}
