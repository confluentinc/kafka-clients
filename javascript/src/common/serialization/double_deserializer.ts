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

// Java's DoubleDeserializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): 8 big-endian bytes as a number, Java's message for
// another size.

import { deserializerBytes } from '../../internal/builtin_serde';
import { SerializationError } from '../errors/serialization_error';
import type { Headers } from '../header/headers';
import type { Deserializer } from './deserializer';

/**
 * Converts 8 big-endian bytes to a number, Java's `Double`.
 */
export class DoubleDeserializer implements Deserializer<number> {
  /** Creates a DoubleDeserializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  deserialize(options: { topic: string; headers: Headers; data: Buffer | null }): number | null {
    const data = options.data;
    if (data === null) return null;
    const bytes = deserializerBytes('DoubleDeserializer', data);
    if (bytes.length !== 8) throw new SerializationError({ message: 'Size of data received by DoubleDeserializer is not 8' });
    return bytes.readDoubleBE(0);
  }
}
