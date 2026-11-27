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

// Java's UUIDSerializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): a UUID, its canonical text, as the UTF-8 bytes of
// that text in lowercase, as Java's UUID.toString() writes it.

import { CANONICAL_UUID, wrongType } from '../../internal/builtin_serde';
import { encodeLikeJava } from '../../internal/encode';
import { SerializationError } from '../errors/serialization_error';
import type { Headers } from '../header/headers';
import type { Serializer } from './serializer';

/**
 * We are converting UUID to String before serializing: a UUID is its
 * canonical text, in either case, written in lowercase. String encoding is
 * UTF8.
 *
 * @remarks *(deviation)* UTF-8 only, with no `configure()`, as the String
 * serdes. *(deviation: Java's parser also reads shorter forms, such as
 * `1-2-3-4-5`. Why: a quirk of the JDK's parser.)*
 */
export class UUIDSerializer implements Serializer<string> {
  /** Creates a UUIDSerializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  serialize(options: { topic: string; headers: Headers; data: string | null }): Uint8Array | null {
    const data: unknown = options.data;
    if (data === null) return null;
    if (typeof data !== 'string') throw wrongType('UUIDSerializer', 'a string or null', data);
    if (!CANONICAL_UUID.test(data)) throw new SerializationError({ message: 'Error parsing data into UUID' });
    return encodeLikeJava(data.toLowerCase());
  }
}
