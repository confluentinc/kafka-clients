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

// Java's DoubleSerializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): a number as its 8 big-endian bytes, every NaN as
// the canonical one, as Java's Double.doubleToLongBits.

import { wrongType } from '../../internal/builtin_serde';
import type { Headers } from '../header/headers';
import type { Serializer } from './serializer';

/**
 * Converts a number to the 8 big-endian bytes of Java's `Double`.
 */
export class DoubleSerializer implements Serializer<number> {
  /** Creates a DoubleSerializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  serialize(options: { topic: string; headers: Headers; data: number | null }): Uint8Array | null {
    const data: unknown = options.data;
    if (data === null) return null;
    if (typeof data !== 'number') throw wrongType('DoubleSerializer', 'a number or null', data);
    // Java's Double.doubleToLongBits writes every NaN as the canonical one.
    if (Number.isNaN(data)) return Buffer.of(0x7f, 0xf8, 0, 0, 0, 0, 0, 0);
    const bytes = Buffer.allocUnsafe(8);
    bytes.writeDoubleBE(data);
    return bytes;
  }
}
