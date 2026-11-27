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

// Java's UUIDDeserializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): the UTF-8 bytes of a UUID's canonical text as
// that text in lowercase, as Java's UUID.fromString(...).toString() gives it.

import { CANONICAL_UUID, deserializerBytes } from '../../internal/builtin_serde';
import { decodeLikeJava } from '../../internal/encode';
import { SerializationError } from '../errors/serialization_error';
import type { Headers } from '../header/headers';
import type { Deserializer } from './deserializer';

/**
 * We are converting the byte array to String before deserializing to UUID:
 * its canonical text, in either case, read in lowercase. String encoding is
 * UTF8.
 *
 * @remarks *(deviation)* UTF-8 only, with no `configure()`, as the String
 * serdes. *(deviation: Java's parser also reads shorter forms, such as
 * `1-2-3-4-5`. Why: a quirk of the JDK's parser.)*
 */
export class UUIDDeserializer implements Deserializer<string> {
  /** Creates a UUIDDeserializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  deserialize(options: { topic: string; headers: Headers; data: Buffer | null }): string | null {
    const data = options.data;
    if (data === null) return null;
    const text = decodeLikeJava(deserializerBytes('UUIDDeserializer', data));
    if (!CANONICAL_UUID.test(text)) throw new SerializationError({ message: 'Error parsing data into UUID' });
    return text.toLowerCase();
  }
}
