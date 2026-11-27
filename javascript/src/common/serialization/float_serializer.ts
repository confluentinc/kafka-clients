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

// Java's FloatSerializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): a number as the 4 big-endian bytes of the nearest
// float, its raw bits, as Java's Float.floatToRawIntBits.

import { wrongType } from '../../internal/builtin_serde';
import type { Headers } from '../header/headers';
import type { Serializer } from './serializer';

/**
 * Converts a number to the 4 big-endian bytes of Java's `Float`, rounded to the
 * nearest float.
 */
export class FloatSerializer implements Serializer<number> {
  /** Creates a FloatSerializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  serialize(options: { topic: string; headers: Headers; data: number | null }): Uint8Array | null {
    const data: unknown = options.data;
    if (data === null) return null;
    if (typeof data !== 'number') throw wrongType('FloatSerializer', 'a number or null', data);
    const bytes = Buffer.allocUnsafe(4);
    bytes.writeFloatBE(data);
    return bytes;
  }
}
