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

// Java's StringDeserializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): UTF-8 bytes as a string, each malformed sequence
// as U+FFFD, as Java's new String(bytes, UTF_8) reads it.

import { deserializerBytes } from '../../internal/builtin_serde';
import { decodeLikeJava } from '../../internal/encode';
import type { Headers } from '../header/headers';
import type { Deserializer } from './deserializer';

/**
 * String encoding is UTF8.
 *
 * @remarks *(deviation)* UTF-8 only, with no `configure()`, so
 * `key.deserializer.encoding`, `value.deserializer.encoding` and
 * `deserializer.encoding` are not read. Why: Node reads strings in only a
 * few of Java's charsets.
 */
export class StringDeserializer implements Deserializer<string> {
  /** Creates a StringDeserializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  deserialize(options: { topic: string; headers: Headers; data: Buffer | null }): string | null {
    const data = options.data;
    if (data === null) return null;
    return decodeLikeJava(deserializerBytes('StringDeserializer', data));
  }
}
