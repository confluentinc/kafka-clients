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

// Java's StringSerializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): a string as its UTF-8 bytes, each lone surrogate
// as `?`, as Java's String.getBytes writes it (encodeLikeJava, Types).

import { wrongType } from '../../internal/builtin_serde';
import { encodeLikeJava } from '../../internal/encode';
import type { Headers } from '../header/headers';
import type { Serializer } from './serializer';

/**
 * String encoding is UTF8.
 *
 * @remarks *(deviation)* UTF-8 only, with no `configure()`, so
 * `key.serializer.encoding`, `value.serializer.encoding` and
 * `serializer.encoding` are not read. Why: Node writes strings in only a few
 * of Java's charsets. A lone surrogate is written as `?`, as Java writes it.
 */
export class StringSerializer implements Serializer<string> {
  /** Creates a StringSerializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  serialize(options: { topic: string; headers: Headers; data: string | null }): Uint8Array | null {
    const data: unknown = options.data;
    if (data === null) return null;
    if (typeof data !== 'string') throw wrongType('StringSerializer', 'a string or null', data);
    return encodeLikeJava(data);
  }
}
