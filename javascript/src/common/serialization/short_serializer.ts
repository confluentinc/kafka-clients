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

// Java's ShortSerializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): a whole number within Java's short, as its
// 2 big-endian bytes.

import { javaInteger } from '../../internal/builtin_serde';
import type { Headers } from '../header/headers';
import type { Serializer } from './serializer';

/**
 * Converts a number to the 2 big-endian bytes of Java's `Short`.
 */
export class ShortSerializer implements Serializer<number> {
  /** Creates a ShortSerializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  serialize(options: { topic: string; headers: Headers; data: number | null }): Uint8Array | null {
    const data: unknown = options.data;
    if (data === null) return null;
    const value = javaInteger('ShortSerializer', data, 16);
    const bytes = Buffer.allocUnsafe(2);
    bytes.writeInt16BE(value);
    return bytes;
  }
}
