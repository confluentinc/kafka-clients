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

// Java's Deserializer interface, as an object type the user implements
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization): one
// deserialize method taking one object of named parameters, for Java's three,
// and Java's default close() as an optional member. The client runs it in
// JavaScript, inside poll(). This module holds only its type.

import type { Headers } from '../header/headers';

/**
 * An interface for converting bytes to objects.
 *
 * @remarks *(deviation)* Java's `deserialize(topic, data)`,
 * `deserialize(topic, headers, data)` and its `ByteBuffer` form are one method
 * taking one object, `deserialize({ topic, headers, data })`, whose `data` is
 * a `Buffer` over the fetched bytes. Why: JavaScript cannot have three methods
 * of one name. *(js-only)* `deserialize` may return a Promise, which the
 * consumer awaits. There is no `configure()`: a constructor argument comes
 * ready, and a function in a serde config key gets the configuration as its
 * argument.
 *
 * @typeParam T - Type to be deserialized into.
 */
export interface Deserializer<out T> {
  /**
   * Deserialize a record value from a byte array into a value or object.
   *
   * The consumer does not call it for a null key or value. The passed in
   * headers may be empty, but never null. The implementation is allowed to
   * modify the passed in headers, as a side effect of deserialization. It is
   * considered best practice to not delete or modify existing headers, but
   * rather only add new ones.
   *
   * @param options - the topic, the record's headers and the data
   * @returns deserialized typed data; may be null; or a Promise of it, which
   *   the consumer awaits
   */
  deserialize(options: {
    /** topic associated with the data */
    topic: string;
    /** headers associated with the record */
    headers: Headers;
    /** serialized bytes, a `Buffer` over the fetched bytes */
    data: Buffer;
  }): T | null | Promise<T | null>;

  /**
   * Close this deserializer.
   *
   * This method must be idempotent as it may be called multiple times.
   *
   * @remarks Optional; what it throws goes to `process.emitWarning`.
   */
  close?(): void;
}
